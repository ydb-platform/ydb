#!/usr/bin/env python3
"""Keep ya test -L suites that appear in an increment diff graph."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from extract_tests_from_graph import drop_redundant_scope_roots, extract_suite_paths


def filter_list_summary(summary: dict, increment_paths: set[str]) -> dict:
    filtered = [suite for suite in summary.get("suites") or [] if suite.get("path") in increment_paths]
    total_tests = sum(int(suite.get("test_count") or 0) for suite in filtered)
    total_weight = sum(int(suite.get("weight") or 0) for suite in filtered)
    return {
        "total_suites": len(filtered),
        "total_tests": total_tests,
        "total_weight": total_weight,
        "size_weights": summary.get("size_weights") or {"small": 60, "medium": 600},
        "reported_suites": summary.get("reported_suites"),
        "reported_tests": summary.get("reported_tests"),
        "increment_filtered": True,
        "increment_graph_suites": len(increment_paths),
        "suites": filtered,
    }


def load_increment_suite_paths(graph_path: Path, target_prefix: str | None) -> set[str]:
    graph = json.loads(graph_path.read_text(encoding="utf-8"))
    suites = extract_suite_paths(graph, target_prefix)
    suites = drop_redundant_scope_roots(suites, target_prefix)
    return set(suites)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("list_summary", type=Path, help="Full list_summary.json from ya test -L")
    parser.add_argument("graph", type=Path, help="increment diff graph.json from graph_compare")
    parser.add_argument(
        "--target-prefix",
        default="ydb/",
        help="Scope for increment graph suite extraction (default: ydb/)",
    )
    parser.add_argument("-o", "--output", type=Path, help="Filtered suite paths, one per line")
    parser.add_argument("--summary-json", type=Path, help="Filtered list_summary.json")
    args = parser.parse_args()

    if not args.list_summary.is_file():
        print(f"error: missing list summary: {args.list_summary}", file=sys.stderr)
        return 2
    if not args.graph.is_file():
        print(f"error: missing increment graph: {args.graph}", file=sys.stderr)
        return 2

    summary = json.loads(args.list_summary.read_text(encoding="utf-8"))
    increment_paths = load_increment_suite_paths(args.graph, args.target_prefix)
    filtered = filter_list_summary(summary, increment_paths)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            "\n".join(suite["path"] for suite in filtered["suites"]) + ("\n" if filtered["suites"] else ""),
            encoding="utf-8",
        )
    if args.summary_json:
        args.summary_json.parent.mkdir(parents=True, exist_ok=True)
        args.summary_json.write_text(
            json.dumps(filtered, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    print(
        f"Increment filter: {filtered['total_suites']} suites, {filtered['total_tests']} tests "
        f"(graph had {filtered['increment_graph_suites']} runnable suites under scope)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
