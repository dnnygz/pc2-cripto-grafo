import os, time, random, json
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd
from tqdm import tqdm

DATA_DIR = Path("data")
NODES_TMP = DATA_DIR / "nodes_tmp.csv"
EDGES_TMP = DATA_DIR / "edges_tmp.csv"

N_TOP = 200                
SLEEP = (0.6, 1.0)
MAX_RETRIES = 6
CACHE_NAME = "cg_cache_enrich"

import requests
try:
    import requests_cache
    requests_cache.install_cache(
        cache_name=CACHE_NAME,
        backend="sqlite",
        expire_after=24*3600,
        allowable_methods=("GET",),
        stale_if_error=True,
    )
except Exception:
    pass

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/json",
    "User-Agent": "cripto-grafo/enrich-exchanges",
})
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

def cg_get(url: str, params: Dict[str, Any] = None, retries: int = MAX_RETRIES):
    attempt = 0
    while True:
        r = SESSION.get(url, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 502, 503, 504) and attempt < retries:
            attempt += 1
            ra = r.headers.get("Retry-After")
            if ra:
                try: wait_s = max(1.0, float(ra))
                except: wait_s = 5.0
            else:
                wait_s = min(60.0, (2 ** attempt) + random.uniform(0.0, 1.0))
            time.sleep(wait_s)
            continue
        r.raise_for_status()

def fetch_coin_detail_with_tickers(coin_id: str) -> Dict[str, Any]:
    return cg_get(
        f"{COINGECKO_BASE}/coins/{coin_id}",
        params={
            "localization": "false",
            "tickers": "true",        
            "market_data": "false",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        }
    )

def _atomic_to_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)

def _jsonify_lists(df: pd.DataFrame, cols: List[str]):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
    return df

def _parse_list_maybe(x):
    if isinstance(x, list): return x
    if isinstance(x, str):
        try:
            import ast
            v = ast.literal_eval(x)
            if isinstance(v, list): return v
        except Exception:
            return []
    return []

if not NODES_TMP.exists():
    raise SystemExit("No existe data/nodes_tmp.csv")
nodes = pd.read_csv(NODES_TMP)
edges = pd.read_csv(EDGES_TMP) if EDGES_TMP.exists() else pd.DataFrame(columns=["src_id","dst_id","edge_type","weight_pr","reasons"])

# mapa (para PR)
A = nodes[nodes["node_type"]=="A"].copy()
total_mc = A["market_cap"].fillna(0).sum()
max_vol = A["volume_24h"].fillna(0).max() or 1.0

have_ac = set(edges.loc[edges["edge_type"]=="A-C", "src_id"].unique().tolist())

# orden por market cap y recorte al top N que aun no tengan A-C
todo = (A[~A["node_id"].isin(have_ac)]
        .sort_values("market_cap", ascending=False)
        .head(N_TOP))

print(f"Enriqueciendo exchanges para {len(todo)} tokens A (top {N_TOP}).")

new_nodes = []   # CEX nuevos
new_edges = []   # A-C nuevas

for _, r in tqdm(todo.iterrows(), total=len(todo)):
    node_id = r["node_id"]          
    coin_id = node_id.split("cg:")[-1]

    # PR con tu formula ponderada 
    mc = r.get("market_cap") or 0.0
    vol = r.get("volume_24h") or 0.0
    vol_norm = (vol / max_vol) if max_vol else 0.0
    pr = (mc / total_mc if total_mc else 0.0) ** 0.5 * (vol_norm) ** 0.5

    # pedir tickers
    det = fetch_coin_detail_with_tickers(coin_id)
    tickers = det.get("tickers") or []
    exchanges = sorted({(t.get("market", {}) or {}).get("identifier") for t in tickers if t.get("market")})
    if not exchanges:
        exchanges = sorted({(t.get("market", {}) or {}).get("name") for t in tickers if t.get("market")})

    for ex in exchanges:
        if not ex: 
            continue
        ex_id = f"cex:{ex}"
        # Nodo C (CEX)
        new_nodes.append({"node_id": ex_id, "node_type": "C", "name": ex})
        # Arista A-C
        new_edges.append({
            "src_id": node_id,
            "dst_id": ex_id,
            "edge_type": "A-C",
            "weight_pr": pr,
            "reasons": "listado en CEX"
        })

    time.sleep(random.uniform(*SLEEP))

if new_nodes or new_edges:
    nodes2 = pd.concat([nodes, pd.DataFrame(new_nodes)], ignore_index=True)
    edges2 = pd.concat([edges, pd.DataFrame(new_edges)], ignore_index=True)

    nodes2 = nodes2.drop_duplicates(subset=["node_id"])
    edges2 = edges2.drop_duplicates()

    nodes2 = _jsonify_lists(nodes2, ["chain_platforms", "links_explorers", "wallets_inferred", "categories"])

    _atomic_to_csv(nodes2, NODES_TMP)
    _atomic_to_csv(edges2, EDGES_TMP)
    print(f"TMP actualizados: {len(nodes2)} nodos, {len(edges2)} aristas")
else:
    print("No se anadieron nuevos nodos o aristas.")
