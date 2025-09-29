import json
import math
from pathlib import Path

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

DATA_DIR = Path("data")
NODES_PATHS = [DATA_DIR/"nodes_tmp.csv", DATA_DIR/"nodes.csv"]
EDGES_PATHS = [DATA_DIR/"edges_tmp.csv", DATA_DIR/"edges.csv"]

def _first_existing(paths):
    for p in paths:
        if p.exists():
            return p
    raise FileNotFoundError(f"no se encontro ninguno de: {paths}")

def _to_num(s):
    return pd.to_numeric(s, errors="coerce")

def load_graph_from_csv():
    nodes_csv = _first_existing(NODES_PATHS)
    edges_csv = _first_existing(EDGES_PATHS)

    nodes = pd.read_csv(nodes_csv)
    edges = pd.read_csv(edges_csv)

    if "node_id" not in nodes.columns or "node_type" not in nodes.columns:
        raise ValueError("nodes*.csv debe tener columnas 'node_id' y 'node_type'")
    if not {"src_id", "dst_id"}.issubset(edges.columns):
        raise ValueError("edges*.csv debe tener columnas 'src_id' y 'dst_id'")

    for col in ["market_cap", "volume_24h", "rank"]:
        if col in nodes.columns:
            nodes[col] = _to_num(nodes[col])

    G = nx.Graph()
    for _, r in nodes.iterrows():
        attrs = r.to_dict()
        node_id = attrs.pop("node_id")
        G.add_node(node_id, **attrs)

    for _, e in edges.iterrows():
        data = e.to_dict()
        src, dst = data.pop("src_id"), data.pop("dst_id")
        G.add_edge(src, dst, **data)

    return G, nodes, edges

def color_by_type(t):
    return {"A":"#1f77b4","B":"#2ca02c","C":"#ff7f0e","D":"#9467bd"}.get(str(t), "#7f7f7f")

if __name__ == "__main__":
    G, nodes, edges = load_graph_from_csv()
    if G.number_of_nodes() == 0:
        raise SystemExit("grafo vacio")

    sample_n = min(450, len(nodes))
    subset = set(nodes.sample(n=sample_n, random_state=42)["node_id"])
    SG = G.subgraph(subset).copy()
    if SG.number_of_edges() == 0:
        print("el subgrafo no tiene aristas")

    colors = [color_by_type(SG.nodes[n].get("node_type")) for n in SG.nodes()]
    sizes = []
    for n in SG.nodes():
        mc = SG.nodes[n].get("market_cap")
        try:
            val = float(mc) if mc is not None and not pd.isna(mc) else None
        except Exception:
            val = None
        sizes.append(50 if val is None else max(50, min(800, val/1e8)))

    # Layout y dibujo
    pos = nx.spring_layout(SG, k=0.35, iterations=50, seed=7)
    plt.figure(figsize=(12,10))
    nx.draw_networkx_nodes(SG, pos, node_size=sizes, node_color=colors, alpha=0.85)
    nx.draw_networkx_edges(SG, pos, alpha=0.25, width=0.6)

    labels = {n: SG.nodes[n].get("name") for n in SG.nodes()
              if SG.nodes[n].get("node_type") in {"B","C"}}
    nx.draw_networkx_labels(SG, pos, labels=labels, font_size=8)
    plt.axis("off")
    plt.tight_layout()
    (DATA_DIR/"preview_graph1.png").parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(DATA_DIR/"preview_graph1.png", dpi=180)
    print("guardado")