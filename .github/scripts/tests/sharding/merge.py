#!/usr/bin/env python3
"""Merge ya make build-results-report JSON files across shards or attempts."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Iterator


def load_report(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fp:
        return json.load(fp)


def write_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(report, fp, ensure_ascii=False, indent=2)
        fp.write("\n")


def _iter_test_rows(report: dict[str, Any]) -> Iterator[dict[str, Any]]:
    for row in report.get("results") or []:
        if not isinstance(row, dict):
            continue
        if row.get("status"):
            yield row


def _report_row_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        row.get("path") or "",
        row.get("name") or "",
        row.get("subtest_name") or "",
        row.get("type") or "",
    )


def _status_severity(status: str | None) -> int:
    """Higher = worse. Used when the same test appears in multiple shard reports."""
    if not status:
        return 0
    value = status.upper()
    if value in {"FAILED", "FAIL"}:
        return 4
    if value == "ERROR":
        return 3
    if value in {"SKIPPED", "SKIP", "MUTE"}:
        return 2
    return 1


def merge_reports(reports: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Merge disjoint shard reports, deduplicating overlapping runs.

    When parent and child suite targets both ran the same test (same path/name/
    subtest_name), keep the worst status so a failure on any shard is preserved.
    """
    merged: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for report in reports:
        for row in _iter_test_rows(report):
            key = _report_row_key(row)
            prev = merged.get(key)
            if prev is None or _status_severity(row.get("status")) > _status_severity(prev.get("status")):
                merged[key] = row
    return {"results": list(merged.values())}


def merge_reports_latest_wins(reports: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Merge attempt reports of one shard: a later report overrides earlier rows."""
    merged: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for report in reports:
        for row in _iter_test_rows(report):
            merged[_report_row_key(row)] = row
    return {"results": list(merged.values())}


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
        "--latest-wins",
        action="store_true",
        help="Later reports override earlier rows (merge try_1..try_N of one shard)",
    )
    parser.add_argument(
        "reports",
        nargs="+",
        type=Path,
        help="Input report.json files (one per shard, or per attempt with --latest-wins)",
    )
    args = parser.parse_args()

    reports = []
    for path in args.reports:
        if not path.is_file():
            print(f"error: missing report file: {path}", file=sys.stderr)
            return 2
        reports.append(load_report(path))

    if args.latest_wins:
        merged = merge_reports_latest_wins(reports)
    else:
        merged = merge_reports(reports)
    write_report(merged, args.output)
    print(f"Merged {len(args.reports)} reports, {len(merged.get('results', []))} rows -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
