#!/usr/bin/env python3
"""Print ya.make suite paths for failed tests in a build-results-report."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from report_utils import iter_failed_suite_paths, load_report


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
    for suite_path in iter_failed_suite_paths(report):
        print(suite_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
