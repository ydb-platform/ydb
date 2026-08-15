#!/usr/bin/env python3
"""Print ya --test-filter values for failed tests in a build-results-report."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from report_utils import iter_failed_test_filters, load_report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "report",
        type=Path,
        help="build-results-report JSON (e.g. merged try_1 report)",
    )
    args = parser.parse_args()

    if not args.report.is_file():
        print(f"error: missing report file: {args.report}", file=sys.stderr)
        return 2

    report = load_report(args.report)
    for ya_filter in iter_failed_test_filters(report):
        print(ya_filter)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
