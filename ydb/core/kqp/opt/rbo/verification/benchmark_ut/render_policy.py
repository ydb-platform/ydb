#!/usr/bin/env python3
"""Render/check the documentation table derived from the checked-in policy."""

import argparse
import json
from pathlib import Path


START = "<!-- coverage-policy:start -->"
END = "<!-- coverage-policy:end -->"


def render(policy):
    rows = []
    for name, suite in policy["suites"].items():
        pair = set(suite["required_snapshot_pair_queries"])
        pair.update(suite["required_verifier_entry_queries"])
        pair.update(suite["required_formula_queries"])
        rows.append([
            name,
            suite["query_count"],
            len(suite["required_prepare_success_queries"]),
            len(pair),
            len(suite["required_verifier_entry_queries"]),
            len(suite["required_formula_queries"]),
            len(suite["required_verified_queries"]),
        ])
    rows.append(["Total", *(
        sum(row[index] for row in rows) for index in range(1, 7)
    )])
    return "\n".join([
        "| Suite | Corpus | Prepare | Exact pair | Explicit entry | Formula | Proof |",
        "|---|---:|---:|---:|---:|---:|---:|",
        *("| " + " | ".join(map(str, row)) + " |" for row in rows),
    ])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="fail if the current documentation table differs")
    args = parser.parse_args()
    directory = Path(__file__).resolve().parent
    policy = json.loads((directory / "coverage_policy.json").read_text(encoding="utf-8"))
    table = render(policy)
    if not args.check:
        print(table)
        return 0
    document = (directory.parent / "BENCHMARK_COVERAGE.md").read_text(encoding="utf-8")
    if document.count(START) != 1 or document.count(END) != 1:
        parser.error("coverage document must contain exactly one generated-table block")
    actual = document.split(START, 1)[1].split(END, 1)[0].strip()
    if actual != table:
        parser.exit(1, "coverage table differs; render it with benchmark_ut/render_policy.py\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
