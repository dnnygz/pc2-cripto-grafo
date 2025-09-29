import requests, time, random
from typing import Dict, List, Any
import pandas as pd
from tqdm import tqdm
from config import COINGECKO_BASE, CATEGORIES, PER_CATEGORY_LIMIT, WALLETS_BY_CHAIN, DETAIL_SLEEP_SECONDS, MAX_RETRIES, INCLUDE_TICKERS
import requests_cache
import os
from datetime import datetime 

requests_cache.install_cache(
    cache_name="cg_cache",
    backend="sqlite",
    expire_after=24*3600,    
    allowable_methods=("GET",),
    stale_if_error=True,
)

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/json",
    "User-Agent": "cripto-grafo/1.0 (+https://example.com)"
})

def cg_get(url: str, params: Dict[str, Any] = None, retries: int = None):
    """GET con backoff para 429/5xx y respeto de Retry-After."""
    if retries is None:
        retries = MAX_RETRIES
    attempt = 0
    while True:
        r = SESSION.get(url, params=params, timeout=30)
        from_cache = getattr(r, "from_cache", False)

        if r.status_code == 200:
            return r.json()

        # Manejo de 429/5xx
        if r.status_code in (429, 502, 503, 504) and attempt < retries:
            attempt += 1
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    wait_s = max(1.0, float(retry_after))
                except Exception:
                    wait_s = 5.0
            else:
                # backoff exponencial con jitter
                wait_s = min(60.0, (2 ** attempt) + random.uniform(0.0, 1.0))
            if not from_cache:
                time.sleep(wait_s)
            continue

        r.raise_for_status()

def _normalize_slug(s: str) -> str:
    return (
        (s or "")
        .strip()
        .lower()
        .replace("&", "and")
        .replace(" ", "-")
    )

def fetch_coins_for_category(category_slugs, per_page: int = 250) -> List[Dict]:
    # construir lista de candidatos
    if isinstance(category_slugs, str):
        candidates = [_normalize_slug(category_slugs)]
    else:
        candidates = [_normalize_slug(x) for x in (category_slugs or []) if x]

    extra = []
    if any(c in {"real-world-assets", "rwa"} for c in candidates):
        extra.append("real-world-assets-rwa")
    if any(c in {"memes", "memecoin", "meme-token"} for c in candidates):
        extra.append("meme")

    # quitar duplicados preservando orden
    seen = set()
    candidates = [c for c in candidates + extra if not (c in seen or seen.add(c))]

    # Intentar cada candidato
    for cand in candidates:
        out, page = [], 1
        while True:
            try:
                data = cg_get(
                    f"{COINGECKO_BASE}/coins/markets",
                    params={
                        "vs_currency": "usd",
                        "category": cand,
                        "order": "market_cap_desc",
                        "per_page": min(250, per_page),
                        "page": page,
                        "price_change_percentage": "24h",
                        "locale": "en",
                    }
                )
            except requests.HTTPError as e:
                # Si 404, probar siguiente alias
                if getattr(e.response, "status_code", None) == 404:
                    out = []
                    break
                raise

            if not data:
                break
            out.extend(data)
            if len(data) < 250 or len(out) >= per_page:
                break
            page += 1
            time.sleep(0.8)  # pausa entre paginas

        if out:
            return out[:per_page]

    return []


def fetch_coin_detail(coin_id: str) -> Dict:
    return cg_get(
        f"{COINGECKO_BASE}/coins/{coin_id}",
        params={
            "localization": "false",
            "tickers": "true" if INCLUDE_TICKERS else "false",
            "market_data": "true",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        }
    )

def normalize_explorers(links: Dict) -> List[str]:
    sites = links.get("blockchain_site") or []
    sites = [s for s in sites if s]
    seen = set(); out = []
    for s in sites:
        base = s.split("/")[2] if "://" in s else s
        if base not in seen:
            out.append(base)
            seen.add(base)
    return out

def infer_wallets(platforms: Dict) -> List[str]:
    wallets = set()
    for chain, addr in (platforms or {}).items():
        if not chain: 
            continue
        for w in WALLETS_BY_CHAIN.get(chain, []):
            wallets.add(w)
    return sorted(wallets)


def save_checkpoint_data(nodes: List[Dict], edges: List[Dict], suffix: str = "_tmp"):
    nodes_df_ck = pd.DataFrame(nodes).drop_duplicates(subset=["node_id"])
    edges_df_ck = pd.DataFrame(edges).drop_duplicates()
    os.makedirs("data", exist_ok=True)
    
    nodes_df_ck.to_csv(f"data/nodes{suffix}.csv", index=False)
    edges_df_ck.to_csv(f"data/edges{suffix}.csv", index=False)
    
    print(f"\n[{datetime.now().isoformat(timespec='seconds')}] progreso guardado en data/nodes{suffix}.csv y data/edges{suffix}.csv.")

def build_nodes_edges() -> None:
    # recolección inicial de candidatos
    category_rows = []
    for label, slugs in CATEGORIES.items():
        items = fetch_coins_for_category(slugs, per_page=PER_CATEGORY_LIMIT)
        for it in items:
            category_rows.append({
            "coin_id": it["id"],
            "symbol": it.get("symbol"),
            "name": it.get("name"),
            "category": label,  
            "market_cap": it.get("market_cap"),
            "volume_24h": it.get("total_volume"),
            "rank": it.get("market_cap_rank"),
            "current_price": it.get("current_price"),
        })
    time.sleep(0.7)

    cat_df = pd.DataFrame(category_rows)
    print("Resumen por categoría (sin dedupe):")
    print(cat_df.groupby("category")["coin_id"].nunique())

    base_df = pd.DataFrame(category_rows).drop_duplicates(subset=["coin_id"])
    total_mc = base_df["market_cap"].fillna(0).sum()
    max_vol = base_df["volume_24h"].fillna(0).max() or 1.0

    node_rows = []
    edge_rows = []
    processed_ids = set()
    
    os.makedirs("data", exist_ok=True)
    nodes_tmp_file = "data/nodes_tmp.csv"
    edges_tmp_file = "data/edges_tmp.csv"

    if os.path.exists(nodes_tmp_file):
        print(f"cargando progreso anterior de {nodes_tmp_file}")
        tmp_nodes_df = pd.read_csv(nodes_tmp_file)
        node_rows.extend(tmp_nodes_df.to_dict('records'))
        processed_ids.update(tmp_nodes_df.loc[tmp_nodes_df["node_type"]=="A", "node_id"].tolist())

    if os.path.exists(edges_tmp_file):
        print(f"cargando aristas anteriores de {edges_tmp_file}")
        tmp_edges_df = pd.read_csv(edges_tmp_file)
        edge_rows.extend(tmp_edges_df.to_dict('records'))
    
    initial_processed_count = len(processed_ids)
    print(f"total de {initial_processed_count} nodos de moneda (A) detectados")

    # pre-crear clases (B)
    classes = [{"node_id": f"class:{k}", "node_type": "B", "name": k} for k in CATEGORIES.keys()]
    node_rows.extend(classes)

    try:
        for i, (_, row) in enumerate(tqdm(
            base_df.iterrows(), 
            total=len(base_df), 
            initial=initial_processed_count, 
            unit="monedas"
        )):
            cid = row["coin_id"]
            node_id = f"cg:{cid}"
            
            if node_id in processed_ids:
                continue

            det = fetch_coin_detail(cid)

            platforms = det.get("platforms") or {}
            links = det.get("links") or {}
            explorers = normalize_explorers(links)
            wallets = infer_wallets(platforms)

            mc = row["market_cap"] or 0.0
            vol = row["volume_24h"] or 0.0
            vol_norm = vol / max_vol if max_vol else 0.0
            pr = (mc / total_mc if total_mc else 0.0) ** 0.5 * (vol_norm) ** 0.5

            node_rows.append({
                "node_id": node_id,
                "node_type": "A",
                "name": row["name"],
                "symbol": row["symbol"],
                "category": row["category"],
                "market_cap": mc,
                "volume_24h": vol,
                "rank": row["rank"],
                "is_multichain": len([k for k in (platforms or {}).keys() if k]) > 1,
                "chain_platforms": list(platforms.keys()),
                "main_contract": next((addr for addr in (platforms or {}).values() if addr), None),
                "links_explorers": explorers,
                "wallets_inferred": wallets
            })

            # A-B (siempre)
            edge_rows.append({
                "src_id": node_id,
                "dst_id": f"class:{row['category']}",
                "edge_type": "A-B",
                "weight_pr": pr,
                "reasons": "pertenece a categoria"
            })

            # A-D (exploradores)
            for exp in explorers:
                exp_id = f"exp:{exp}"
                node_rows.append({"node_id": exp_id, "node_type": "D", "name": exp})
                edge_rows.append({
                    "src_id": node_id,
                    "dst_id": exp_id,
                    "edge_type": "A-D",
                    "weight_pr": pr,
                    "reasons": "explorador asociado"
                })

            # A-C (CEX)
            if INCLUDE_TICKERS:
                tickers = (det.get("tickers") or [])
                exchanges = sorted({(t.get("market", {}) or {}).get("identifier") for t in tickers if t.get("market")})
                for ex in exchanges:
                    if not ex:
                        continue
                    ex_id = f"cex:{ex}"
                    node_rows.append({"node_id": ex_id, "node_type": "C", "name": ex}) 
                    edge_rows.append({
                        "src_id": node_id,
                        "dst_id": ex_id,
                        "edge_type": "A-C",
                        "weight_pr": pr,
                        "reasons": "listado en CEX"
                    })

            processed_ids.add(node_id) # marcar como completado
            
            lo, hi = DETAIL_SLEEP_SECONDS
            time.sleep(random.uniform(lo, hi))

    except KeyboardInterrupt:
        print("\n (Ctrl+C) Guardando .. .")
        save_checkpoint_data(node_rows, edge_rows, suffix="_tmp")
        return 

    print("\nrecoleccion completa, guardar archiuvo final")
    save_checkpoint_data(node_rows, edge_rows, suffix="")
    
    final_nodes_df = pd.DataFrame(node_rows).drop_duplicates(subset=['node_id'])
    final_edges_df = pd.DataFrame(edge_rows).drop_duplicates()
    print(f"finalizado. Total: {len(final_nodes_df)} nodos, {len(final_edges_df)} aristas")


if __name__ == "__main__":
    build_nodes_edges()