"""Report runner: read aggregator outputs and emit graph, CSV, diffs, summary."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

from ..aggregate.graph import Graph, Node, Verdict
from ..common import PATHS, REPO_ROOT, die, ensure_dir, repo_relative, setup_logging
from .diff_preview import emit_diffs
from .formats import write_dot, write_mermaid
from .summary import write_hot_headers_csv, write_summary_md


log = logging.getLogger("report")


def load_graph(path: Path) -> Graph:
    if not path.exists():
        die(f"graph.json not found at {path}; run 'aggregate' first")
    data = json.loads(path.read_text(encoding="utf-8"))
    g = Graph()
    for n in data.get("nodes", []):
        g.nodes[n["path"]] = Node(
            path=n["path"],
            kind=n["kind"],
            size_bytes=n.get("size_bytes", 0),
            fanin=n.get("fanin", 0),
            fanout=n.get("fanout", 0),
        )
    for e in data.get("edges", []):
        g.out_edges.setdefault(e["from"], set()).add(e["to"])
        g.in_edges.setdefault(e["to"], set()).add(e["from"])
    return g


def load_verdicts(path: Path) -> List[Verdict]:
    if not path.exists():
        die(f"verdicts.jsonl not found at {path}; run 'aggregate' first")
    out: List[Verdict] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            out.append(Verdict(
                in_file=d["in_file"],
                spelled=d["spelled"],
                line=d["line"],
                verdict=d["verdict"],
                reason=d.get("reason", ""),
                consumers_needing=list(d.get("consumers_needing", [])),
                headers_blocking=list(d.get("headers_blocking", [])),
                resolved=d.get("resolved"),
            ))
    return out


def _verdict_breakdown(verdicts: List[Verdict]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for v in verdicts:
        counts[v.verdict] = counts.get(v.verdict, 0) + 1
    return counts


def _filter_by_subdir(verdicts: List[Verdict], subdir: Optional[str]) -> List[Verdict]:
    if not subdir:
        return verdicts
    pfx = subdir.rstrip("/") + "/"
    return [v for v in verdicts if v.in_file == subdir or v.in_file.startswith(pfx)]


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes report",
        description="Emit graphs, CSV, diff previews and summary.md",
    )
    parser.add_argument("--top", type=int, default=100)
    parser.add_argument(
        "--subdir",
        default=None,
        help="restrict verdicts (and diffs / summary) to headers under "
             "this repo-relative subtree. By default we report on every "
             "verdict; narrow with e.g. --subdir ydb/core/base to focus.",
    )
    parser.add_argument(
        "--subtree",
        default=None,
        help="restrict DOT/Mermaid graph to nodes under this prefix. "
             "Defaults to --subdir; falls back to the top subdirectory "
             "with the most actionable verdicts.",
    )
    parser.add_argument("--max-graph-nodes", type=int, default=200)
    parser.add_argument("--no-diffs", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    setup_logging(args.verbose)

    graph = load_graph(PATHS.graph_json)
    verdicts_all = load_verdicts(PATHS.verdicts_jsonl)
    log.info("loaded graph (%d nodes) and %d verdicts",
             len(graph.nodes), len(verdicts_all))

    # Full breakdown is always logged so the user can see where the
    # actionable verdicts hide even when --subdir narrows.
    counts_all = _verdict_breakdown(verdicts_all)
    log.info("full verdict breakdown: %s",
             ", ".join(f"{k}={v}" for k, v in sorted(counts_all.items())))

    verdicts = _filter_by_subdir(verdicts_all, args.subdir)
    if args.subdir:
        counts_sub = _verdict_breakdown(verdicts)
        log.info("verdicts under %s: %d (%s)",
                 args.subdir, len(verdicts),
                 ", ".join(f"{k}={v}" for k, v in sorted(counts_sub.items())))

    actionable = sum(1 for v in verdicts if v.verdict in ("remove", "move-to-cpp"))
    if actionable == 0:
        # If --subdir wasn't given, point at the top-3 subtrees with the
        # most actionable verdicts so the user has a concrete next step.
        if not args.subdir:
            by_dir: Dict[str, int] = {}
            for v in verdicts_all:
                if v.verdict not in ("remove", "move-to-cpp"):
                    continue
                # Bucket by first 3 path components, e.g. ydb/core/base.
                parts = v.in_file.split("/")
                key = "/".join(parts[:3]) if len(parts) >= 3 else "/".join(parts)
                by_dir[key] = by_dir.get(key, 0) + 1
            if by_dir:
                top = sorted(by_dir.items(), key=lambda kv: -kv[1])[:5]
                log.warning(
                    "0 actionable verdicts overall — try narrowing the report: "
                    "%s",
                    ", ".join(f"--subdir {d} ({n})" for d, n in top),
                )
            else:
                log.warning(
                    "0 actionable (remove + move-to-cpp) verdicts in the "
                    "entire run. All headers ended up classified as keep "
                    "(common when isolated header probes fail because the "
                    "header isn't self-contained). Inspect "
                    "reports/summary.md and per-header cache files for "
                    "details."
                )
        else:
            log.warning(
                "0 actionable verdicts under %s. The full breakdown is "
                "above — pick a different --subdir.", args.subdir,
            )
    else:
        log.info("%d actionable verdicts in scope", actionable)

    ensure_dir(PATHS.reports_dir)

    subtree = args.subtree or args.subdir or "ydb/"
    if not subtree.endswith("/"):
        subtree = subtree + "/"
    dot_path = PATHS.reports_dir / "graph.dot"
    n_edges = write_dot(graph, dot_path, subtree_root=subtree, max_nodes=args.max_graph_nodes)
    log.info("wrote %s (%d edges, subtree=%s)",
             repo_relative(str(dot_path)), n_edges, subtree)

    mermaid_path = PATHS.reports_dir / "graph.mmd"
    n_edges = write_mermaid(graph, mermaid_path, subtree_root=subtree,
                            max_nodes=min(50, args.max_graph_nodes))
    log.info("wrote %s (%d edges, subtree=%s)",
             repo_relative(str(mermaid_path)), n_edges, subtree)

    write_hot_headers_csv(graph, PATHS.hot_headers_csv, top=args.top)
    log.info("wrote %s", repo_relative(str(PATHS.hot_headers_csv)))

    write_summary_md(verdicts, graph, PATHS.summary_md, top=args.top)
    log.info("wrote %s", repo_relative(str(PATHS.summary_md)))

    if not args.no_diffs:
        ensure_dir(PATHS.diffs_dir)
        for p in PATHS.diffs_dir.glob("*.patch"):
            try:
                p.unlink()
            except OSError:
                pass
        n_h, n_t = emit_diffs(verdicts, PATHS.diffs_dir, repo_root=REPO_ROOT)
        log.info("wrote %d header diffs and %d TU-addition diffs to %s",
                 n_h, n_t, repo_relative(str(PATHS.diffs_dir)))
        if n_h == 0 and n_t == 0 and actionable > 0:
            log.warning(
                "actionable verdicts exist but emit_diffs produced no "
                "files — the diff renderer judged before==after. This "
                "usually means a stale verdict pointing at a file that "
                "has since changed."
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
