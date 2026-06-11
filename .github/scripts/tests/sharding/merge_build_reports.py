#!/usr/bin/env python3
"""Merge multiple ya make build-results-report JSON files into one."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from report_utils import merge_reports, write_report, load_report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="Output merged report.json path",
    )
    parser.add_argument(
        "reports",
        nargs="+",
        type=Path,
        help="Input report.json files (one per shard)",
    )
    args = parser.parse_args()

    reports = []
    for path in args.reports:
        if not path.is_file():
            print(f"error: missing report file: {path}", file=sys.stderr)
            return 2
        reports.append(load_report(path))

    merged = merge_reports(reports)
    write_report(merged, args.output)
    print(f"Merged {len(args.reports)} reports, {len(merged.get('results', []))} rows -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
