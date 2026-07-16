#!/usr/bin/env python3
import argparse
import json
import re
import sys
from pathlib import Path


def line_pct(report_dir: str) -> float:
    html = (Path(report_dir) / "index.html").read_text(encoding="utf-8", errors="replace")
    # New ya/llvm-cov HTML: "Overall coverage" table, project row, "Lines, %" column.
    match = re.search(r"<div>project</div></td><td[^>]*>.*?>([\d.]+)%", html, re.S)
    if not match:
        # Legacy llvm-cov HTML summary row.
        match = re.search(r"Lines:</td>\s*<td[^>]*>([\d.]+)\s*%", html, re.I)
    if not match:
        raise SystemExit(f"line coverage not found in {report_dir}/index.html")
    return float(match.group(1))


def cmd_extract(args: argparse.Namespace) -> int:
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump({"line_pct": line_pct(args.report_dir)}, f)
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    with open(args.current_json, encoding="utf-8") as f:
        current = json.load(f)["line_pct"]
    with open(args.baseline_json, encoding="utf-8") as f:
        baseline = json.load(f)["line_pct"]
    print(
        f"{args.label} line coverage: {current}% (baseline: {baseline}%, tolerance: {args.tolerance}pp)",
        file=sys.stderr,
    )
    return 1 if current < baseline - args.tolerance else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare coverage reports produced by ya make.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_extract = sub.add_parser("extract", help="Extract line coverage percentage from ya coverage report.")
    p_extract.add_argument("report_dir")
    p_extract.add_argument("out_json")
    p_extract.set_defaults(func=cmd_extract)

    p_check = sub.add_parser("check", help="Compare current line coverage against a baseline.")
    p_check.add_argument("current_json")
    p_check.add_argument("baseline_json")
    p_check.add_argument("--label", required=True, help="Label used in log messages (e.g. \"CPP SDK\").")
    p_check.add_argument(
        "--tolerance",
        type=float,
        required=True,
        help="Allowed drop in line coverage percentage points before the check fails.",
    )
    p_check.set_defaults(func=cmd_check)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
