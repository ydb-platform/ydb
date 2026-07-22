#!/usr/bin/env python3
"""Compare coverage reports produced by ya make.

Supports two JSON schemas:

Legacy (SDK): {"line_pct": <float>}
Extended (CLI): {
    "overall": {"line_pct": <float>, ...},
    "per_prefix": {"<prefix>/": {"line_pct": <float>, ...}, ...},
    "per_group": {"cli"|"workload": {"line_pct": <float>, ...}, ...}
}

The `extract` subcommand parses ya's HTML report and emits the legacy schema.
The extended schema is produced separately by `aggregate_cli_coverage.py`.

The `check` subcommand compares two JSONs and reports the delta on stderr.
Tolerance is enforced on the overall line coverage only; per-prefix details
are informational.
"""
import argparse
import json
import re
import sys
from pathlib import Path


def line_pct_from_html(report_dir):
    html = (Path(report_dir) / "index.html").read_text(encoding="utf-8", errors="replace")
    # New ya/llvm-cov HTML: "Overall coverage" table, project row, "Lines, %" column.
    match = re.search(r"<div>project</div></td><td[^>]*>.*?>([\d.]+)%", html, re.S)
    if not match:
        # Legacy llvm-cov HTML summary row.
        match = re.search(r"Lines:</td>\s*<td[^>]*>([\d.]+)\s*%", html, re.I)
    if not match:
        raise SystemExit(f"line coverage not found in {report_dir}/index.html")
    return float(match.group(1))


def get_overall(data):
    if "overall" in data:
        return data["overall"]["line_pct"]
    return data["line_pct"]


def get_per_prefix(data):
    return data.get("per_prefix") or {}


def get_per_group(data):
    return data.get("per_group") or {}


def cmd_extract(args):
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump({"line_pct": line_pct_from_html(args.report_dir)}, f)
    return 0


def _format_detail_block(title, cur_map, base_map):
    if not cur_map:
        return []
    lines = ["", title]
    width = max(len(p) for p in cur_map)
    for key, cur_data in cur_map.items():
        cur_pct = cur_data["line_pct"]
        base_pct = base_map.get(key, {}).get("line_pct", 0.0)
        delta = round(cur_pct - base_pct, 2)
        sign = "+" if delta > 0 else ("" if delta < 0 else "±")
        covered = cur_data.get("covered")
        total = cur_data.get("total")
        counts = f"  [{covered}/{total}]" if covered is not None and total is not None else ""
        lines.append(
            f"  {key.ljust(width)}  {cur_pct:>6.2f}% (baseline: {base_pct:>6.2f}%)  "
            f"[{sign}{delta:.2f}]{counts}"
        )
    return lines


def cmd_check(args):
    with open(args.current_json, encoding="utf-8") as f:
        current = json.load(f)
    with open(args.baseline_json, encoding="utf-8") as f:
        baseline = json.load(f)

    cur_overall = get_overall(current)
    base_overall = get_overall(baseline)
    ok = cur_overall >= base_overall - args.tolerance

    lines = [
        f"{args.label} line coverage: {cur_overall:.2f}% (baseline: {base_overall:.2f}%, "
        f"tolerance: {args.tolerance}pp) — {'OK' if ok else 'REGRESSED'}"
    ]
    if "overall" in current and "covered" in current["overall"]:
        o = current["overall"]
        lines.append(f"  overall lines: {o['covered']}/{o['total']}")

    lines.extend(
        _format_detail_block(
            "Details by group:",
            get_per_group(current),
            get_per_group(baseline),
        )
    )
    lines.extend(
        _format_detail_block(
            "Details by directory:",
            get_per_prefix(current),
            get_per_prefix(baseline),
        )
    )

    print("\n".join(lines), file=sys.stderr)
    return 0 if ok else 1


def main():
    parser = argparse.ArgumentParser(description="Compare coverage reports produced by ya make.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_extract = sub.add_parser("extract", help="Extract line coverage percentage from ya HTML report.")
    p_extract.add_argument("report_dir")
    p_extract.add_argument("out_json")
    p_extract.set_defaults(func=cmd_extract)

    p_check = sub.add_parser("check", help="Compare current line coverage against a baseline.")
    p_check.add_argument("current_json")
    p_check.add_argument("baseline_json")
    p_check.add_argument("--label", required=True, help='Label used in log messages (e.g. "CPP SDK").')
    p_check.add_argument(
        "--tolerance",
        type=float,
        required=True,
        help="Allowed drop in overall line coverage percentage points before the check fails.",
    )
    p_check.set_defaults(func=cmd_check)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
