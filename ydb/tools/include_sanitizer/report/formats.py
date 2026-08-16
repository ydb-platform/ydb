"""Graph rendering helpers: DOT and Mermaid output."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from ..aggregate.graph import Graph


log = logging.getLogger("report.formats")


def _safe_id(path: str) -> str:
    return "n_" + "".join(c if c.isalnum() else "_" for c in path)[:64]


def write_dot(
    graph: Graph,
    out_path: Path,
    subtree_root: Optional[str] = None,
    max_nodes: int = 200,
) -> int:
    """Write a Graphviz DOT file.

    If ``subtree_root`` is provided, include only nodes whose path starts
    with ``subtree_root``. The result is capped at ``max_nodes`` to keep
    DOT files renderable.
    """
    selected = []
    for path in graph.nodes:
        if subtree_root and not path.startswith(subtree_root):
            continue
        selected.append(path)
    if not selected:
        return 0

    selected.sort(key=lambda p: -graph.nodes[p].fanin)
    selected = selected[:max_nodes]
    sel_set = set(selected)

    lines = ["digraph includes {"]
    lines.append("  rankdir=LR;")
    lines.append("  node [shape=box, fontname=monospace];")
    for path in selected:
        n = graph.nodes[path]
        color = "#cfe8ff" if n.kind == "header" else "#d6f5d6"
        label = f"{path}\\n[fanin={n.fanin}, fanout={n.fanout}, {n.size_bytes}B]"
        lines.append(f'  {_safe_id(path)} [label="{label}", style=filled, fillcolor="{color}"];')

    n_edges = 0
    for parent, kids in graph.out_edges.items():
        if parent not in sel_set:
            continue
        for child in kids:
            if child not in sel_set:
                continue
            lines.append(f"  {_safe_id(parent)} -> {_safe_id(child)};")
            n_edges += 1

    lines.append("}")
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return n_edges


def write_mermaid(
    graph: Graph,
    out_path: Path,
    subtree_root: Optional[str] = None,
    max_nodes: int = 50,
) -> int:
    selected = []
    for path in graph.nodes:
        if subtree_root and not path.startswith(subtree_root):
            continue
        selected.append(path)
    if not selected:
        return 0
    selected.sort(key=lambda p: -graph.nodes[p].fanin)
    selected = selected[:max_nodes]
    sel_set = set(selected)

    lines = ["flowchart LR"]
    for path in selected:
        nid = _safe_id(path)
        lines.append(f'    {nid}["{path}"]')

    n_edges = 0
    for parent, kids in graph.out_edges.items():
        if parent not in sel_set:
            continue
        for child in kids:
            if child not in sel_set:
                continue
            lines.append(f"    {_safe_id(parent)} --> {_safe_id(child)}")
            n_edges += 1

    out_path.write_text("\n".join(lines), encoding="utf-8")
    return n_edges
