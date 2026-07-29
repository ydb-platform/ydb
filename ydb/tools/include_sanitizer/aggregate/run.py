"""Cross-TU aggregation runner.

Reads per-file JSON results from ``.cache/per_tu/``, builds the include
graph, classifies every header include, and writes:

- ``reports/graph.json``
- ``reports/verdicts.jsonl``
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Dict, Optional

from ..common import (
    PATHS,
    die,
    ensure_dir,
    is_under_subdir,
    repo_relative,
    setup_logging,
    write_jsonl,
)
from .graph import Graph, build_graph, classify


log = logging.getLogger("aggregate")


def load_per_file(cache_dir: Path, subdir: str) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    if not cache_dir.exists():
        die(f"cache directory {cache_dir} does not exist; run 'analyze' first")
    for p in cache_dir.glob("**/*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            log.warning("skipping unreadable %s: %s", p, e)
            continue
        f = data.get("file")
        if not f:
            continue
        rel = repo_relative(f)
        if not is_under_subdir(rel, subdir):
            continue
        out[rel] = data
    return out


def serialize_graph(graph: Graph) -> dict:
    return {
        "nodes": [asdict(n) for n in graph.nodes.values()],
        "edges": [
            {"from": parent, "to": child}
            for parent, kids in graph.out_edges.items()
            for child in kids
        ],
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes aggregate",
        description="Build the include graph and per-header verdicts",
    )
    parser.add_argument("--subdir", default="ydb")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    setup_logging(args.verbose)

    per_file = load_per_file(PATHS.per_tu_dir, args.subdir)
    log.info("loaded %d per-file results", len(per_file))
    if not per_file:
        die("no analysis cache found; run 'analyze' first")

    graph = build_graph(per_file)
    log.info("graph: %d nodes, %d edges",
             len(graph.nodes),
             sum(len(v) for v in graph.out_edges.values()))

    verdicts = classify(per_file, graph)

    counts: Dict[str, int] = {}
    for v in verdicts:
        counts[v.verdict] = counts.get(v.verdict, 0) + 1
    log.info("produced %d verdicts: %s",
             len(verdicts),
             ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    actionable = counts.get("remove", 0) + counts.get("move-to-cpp", 0)
    if actionable == 0:
        log.warning(
            "zero actionable verdicts (remove + move-to-cpp). Likely "
            "causes: (a) most header probes failed in isolation -- many "
            "headers rely on the including TU having pre-included a "
            "sibling first, so the classifier falls back to keep-everything; "
            "(b) clang-include-cleaner had no opinion. Inspect "
            "reports/summary.md for the breakdown by verdict, or run "
            "`sanitize_includes report --subdir <smaller-subtree>` to "
            "focus the graph and diffs."
        )
    else:
        log.info("%d actionable verdicts (remove + move-to-cpp)", actionable)

    ensure_dir(PATHS.reports_dir)
    PATHS.graph_json.write_text(json.dumps(serialize_graph(graph), indent=1), encoding="utf-8")
    log.info("wrote %s", repo_relative(str(PATHS.graph_json)))

    n = write_jsonl(PATHS.verdicts_jsonl, (v.to_json() for v in verdicts))
    log.info("wrote %d verdicts to %s", n, repo_relative(str(PATHS.verdicts_jsonl)))

    return 0


if __name__ == "__main__":
    sys.exit(main())
