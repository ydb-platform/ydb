"""Fused optimization worklist: timing cost x include-graph structure.

Combines two signals we already produce:

- per-header frontend cost (``reports/timing/hot_headers.csv`` from the
  ``timing`` subcommand) — how much compile time each header costs,
  summed across the build, plus how many TUs touch it;
- the include graph (``reports/graph.json`` from ``aggregate``) — fan-in
  (how many files include a header) and each header's direct includes.

From these it produces an ACTIONABLE, ranked worklist:

1. Editable headers (ydb/ yql/ yt/ ...) ranked by their own frontend
   cost, each annotated with fan-in, the expensive headers they drag in,
   and a suggested action (move-to-cpp / forward-declare / split).
2. Expensive *external* headers (contrib/, generated *.pb.h, abseil, std)
   ranked by cost x how many editable headers pull them in — i.e. the
   include edges whose removal saves the most.

Nothing is modified; this writes ``reports/worklist.md`` + CSVs.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..common import PATHS, die, ensure_dir, setup_logging


log = logging.getLogger("report.worklist")

# Map an absolute path (as timing records them) to a repo-relative key by
# locating a known top-level source directory.
_TOP_RE = re.compile(r"/(ydb|yql|yt|contrib|util|library|tools|vendor)/")

_EDITABLE_PREFIXES = ("ydb/", "yql/", "yt/")
_EXTERNAL_PREFIXES = ("contrib/", "vendor/")


def to_rel(p: str) -> str:
    if not p.startswith("/"):
        return p
    m = _TOP_RE.search(p)
    if m:
        return p[m.start() + 1:]
    return p


def is_editable(rel: str) -> bool:
    return rel.startswith(_EDITABLE_PREFIXES)


def is_external(rel: str) -> bool:
    return rel.startswith(_EXTERNAL_PREFIXES) or not rel.startswith(
        _EDITABLE_PREFIXES + ("util/", "library/"))


def is_generated(rel: str) -> bool:
    return rel.endswith((".pb.h", ".grpc.pb.h", ".fbs.h"))


def load_costs(path: Path) -> Dict[str, Tuple[float, int]]:
    """rel-path -> (total_seconds, num_tus) from hot_headers.csv."""
    out: Dict[str, Tuple[float, int]] = {}
    if not path.exists():
        die(f"{path} not found; run 'timing' first")
    with path.open("r", encoding="utf-8", newline="") as fh:
        r = csv.reader(fh)
        next(r, None)  # skip header row
        for row in r:
            if len(row) < 3:
                continue
            rel = to_rel(row[0])
            try:
                sec = float(row[1])
                tus = int(row[2])
            except ValueError:
                continue
            # Same file may appear under abs + already-rel spellings; keep max.
            prev = out.get(rel)
            if prev is None or sec > prev[0]:
                out[rel] = (sec, tus)
    return out


def load_graph(path: Path) -> Tuple[Dict[str, int], Dict[str, List[str]]]:
    """Return (fanin per node, direct-children per node) from graph.json."""
    if not path.exists():
        die(f"{path} not found; run 'aggregate' first (it builds the include graph)")
    data = json.loads(path.read_text(encoding="utf-8"))
    fanin: Dict[str, int] = {}
    for n in data.get("nodes", []):
        fanin[n["path"]] = n.get("fanin", 0)
    children: Dict[str, List[str]] = defaultdict(list)
    for e in data.get("edges", []):
        children[e["from"]].append(e["to"])
    return fanin, children


def _suggest(rel: str, own_s: float, exp_children: List[Tuple[str, float]]) -> str:
    gen = [c for c, _ in exp_children if is_generated(c)]
    ext = [c for c, _ in exp_children if is_external(c) and not is_generated(c)]
    parts: List[str] = []
    if gen:
        parts.append("move generated protobuf includes to .cpp / forward-declare "
                     f"(e.g. {gen[0]}) — try `selfcontain --protobuf`")
    if ext:
        parts.append(f"stop including heavy external header(s) from this header "
                     f"(e.g. {ext[0]}); include in the .cpp that needs it")
    if own_s >= 100 and not exp_children:
        parts.append("header's own declarations are expensive — split it and/or "
                     "move template/inline definitions into a .cpp")
    if not parts:
        parts.append("review: slim includes, forward-declare, or split")
    return "; ".join(parts)


def build_worklist(
    costs: Dict[str, Tuple[float, int]],
    fanin: Dict[str, int],
    children: Dict[str, List[str]],
    out_dir: Path,
    top: int = 100,
    min_child_s: float = 20.0,
) -> None:
    ensure_dir(out_dir)

    # 1) Editable headers ranked by own frontend cost.
    editable = [(rel, c[0], c[1]) for rel, c in costs.items()
                if is_editable(rel) and rel.endswith((".h", ".hh", ".hpp", ".hxx"))]
    editable.sort(key=lambda t: -t[1])

    rows: List[dict] = []
    for rel, own_s, tus in editable[:top]:
        kids = children.get(rel, [])
        exp_children = sorted(
            ((c, costs.get(c, (0.0, 0))[0]) for c in kids
             if costs.get(c, (0.0, 0))[0] >= min_child_s),
            key=lambda kv: -kv[1])[:6]
        rows.append({
            "header": rel,
            "own_s": own_s,
            "fanin": fanin.get(rel, tus),
            "tus": tus,
            "exp_children": exp_children,
            "action": _suggest(rel, own_s, exp_children),
        })

    # 2) Expensive headers pulled into many EDITABLE headers (edges to cut).
    pulled_by_editable: Dict[str, int] = defaultdict(int)
    for parent, kids in children.items():
        if not is_editable(parent):
            continue
        for c in kids:
            pulled_by_editable[c] += 1
    edge_targets = []
    for rel, (sec, _tus) in costs.items():
        n = pulled_by_editable.get(rel, 0)
        if n <= 0 or sec < min_child_s:
            continue
        edge_targets.append((rel, sec, n, sec * n))
    edge_targets.sort(key=lambda t: -t[3])

    _write_worklist_md(out_dir / "worklist.md", rows, edge_targets[:top], top)
    _write_worklist_csv(out_dir / "worklist.csv", rows)
    log.info("wrote %s and worklist.csv (%d editable headers, %d edge targets)",
             out_dir / "worklist.md", len(rows), len(edge_targets))


def _write_worklist_csv(path: Path, rows: List[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["header", "own_frontend_s", "fanin", "num_tus",
                    "expensive_includes", "suggested_action"])
        for r in rows:
            kids = " | ".join(f"{c}({s:.0f}s)" for c, s in r["exp_children"])
            w.writerow([r["header"], f"{r['own_s']:.1f}", r["fanin"], r["tus"],
                        kids, r["action"]])


def _write_worklist_md(path: Path, rows: List[dict],
                       edge_targets: List[Tuple[str, float, int, float]],
                       top: int) -> None:
    lines: List[str] = []
    lines.append("# Include-optimization worklist")
    lines.append("")
    lines.append("Fuses per-header frontend cost (`timing`) with include-graph "
                 "structure (`aggregate`). Edit headers top-down; re-run "
                 "`timetrace`+`timing` after a batch to measure the win.")
    lines.append("")
    lines.append(f"## Top {len(rows)} editable headers by own frontend cost")
    lines.append("")
    lines.append("`own cost` = time parsing/sema of declarations physically in "
                 "this header (summed across all TUs). `fanin` = files that "
                 "include it. `expensive includes` = heavy headers it drags in.")
    lines.append("")
    lines.append("| header | own s | fanin | expensive includes it pulls in | action |")
    lines.append("|---|---:|---:|---|---|")
    for r in rows:
        kids = "<br>".join(f"`{c}` ({s:.0f}s)" for c, s in r["exp_children"]) or "—"
        lines.append(f"| `{r['header']}` | {r['own_s']:.0f} | {r['fanin']} | "
                     f"{kids} | {r['action']} |")
    lines.append("")
    lines.append(f"## Top {len(edge_targets)} include edges to cut")
    lines.append("")
    lines.append("Expensive headers pulled into many *editable* headers. "
                 "Removing them from those headers (move the include into the "
                 ".cpp that needs it, or forward-declare) stops the cost from "
                 "propagating to every transitive consumer. `impact` = cost x "
                 "number of editable headers that include it directly.")
    lines.append("")
    lines.append("| header to stop including | cost s | #editable headers | impact |")
    lines.append("|---|---:|---:|---:|")
    for rel, sec, n, impact in edge_targets:
        tag = " (generated)" if is_generated(rel) else (" (external)" if is_external(rel) else "")
        lines.append(f"| `{rel}`{tag} | {sec:.0f} | {n} | {impact:.0f} |")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes worklist",
        description="Fuse timing cost with the include graph into a ranked, "
                    "explained header-optimization worklist.",
    )
    parser.add_argument("--hot-headers", type=Path,
                        default=PATHS.reports_dir / "timing" / "hot_headers.csv")
    parser.add_argument("--graph", type=Path, default=PATHS.graph_json)
    parser.add_argument("--top", type=int, default=100)
    parser.add_argument("--min-child-s", type=float, default=20.0,
                        help="minimum cost (s) for an include to be flagged as "
                             "an expensive child / edge target.")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)

    costs = load_costs(args.hot_headers)
    fanin, children = load_graph(args.graph)
    out_dir = ensure_dir(PATHS.reports_dir)
    build_worklist(costs, fanin, children, out_dir, top=args.top,
                   min_child_s=args.min_child_s)
    return 0


if __name__ == "__main__":
    sys.exit(main())
