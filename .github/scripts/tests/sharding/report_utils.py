#!/usr/bin/env python3
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator


@dataclass(frozen=True)
class TestEntry:
    path: str
    name: str

    @property
    def full_name(self) -> str:
        if self.path and self.name:
            return f"{self.path}/{self.name}"
        return self.path or self.name

    @classmethod
    def from_report_row(cls, row: dict[str, Any]) -> TestEntry | None:
        status = row.get("status")
        if not status:
            return None

        path_str = row.get("path") or ""
        name_part = row.get("name") or ""
        subtest_name = row.get("subtest_name") or ""
        if subtest_name:
            if name_part:
                name = f"{name_part}.{subtest_name}"
            else:
                name = subtest_name
        else:
            name = name_part
        if not path_str and not name:
            return None
        return cls(path=path_str, name=name)


def iter_test_rows(report: dict[str, Any]) -> Iterator[dict[str, Any]]:
    for row in report.get("results") or []:
        if not isinstance(row, dict):
            continue
        if row.get("status"):
            yield row


def load_report(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fp:
        return json.load(fp)


def load_reports(paths: Iterable[Path]) -> list[dict[str, Any]]:
    return [load_report(path) for path in paths]


def merge_reports(reports: Iterable[dict[str, Any]]) -> dict[str, Any]:
    merged_results: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for report in reports:
        for row in iter_test_rows(report):
            key = (
                row.get("path") or "",
                row.get("name") or "",
                row.get("subtest_name") or "",
                row.get("status") or "",
            )
            if key in seen:
                continue
            seen.add(key)
            merged_results.append(row)
    return {"results": merged_results}


def count_failures(report: dict[str, Any]) -> int:
    count = 0
    for row in iter_test_rows(report):
        if row.get("status") in {"FAILED", "ERROR"}:
            count += 1
    return count


def test_name_to_ya_filter(test_name: str) -> str:
    """Convert report test name to ya ``--test-filter`` value (see summary.html)."""
    name_pieces = test_name.split(".")
    if len(name_pieces) == 2:
        return f"{name_pieces[0]}::{name_pieces[1]}"
    if len(name_pieces) > 2:
        return f"{name_pieces[0]}.{name_pieces[1]}::{'::'.join(name_pieces[2:])}"
    return test_name


def is_chunk_failure(row: dict[str, Any]) -> bool:
    if row.get("chunk"):
        return True
    subtest_name = (row.get("subtest_name") or "").lower()
    if subtest_name and "chunk" in subtest_name:
        return True
    name = (row.get("name") or "").lower()
    return "chunk" in name


def iter_failed_rows(
    report: dict[str, Any],
    *,
    include_chunks: bool = False,
) -> Iterator[dict[str, Any]]:
    for row in iter_test_rows(report):
        if row.get("status") not in {"FAILED", "ERROR"}:
            continue
        if row.get("type") == "build":
            continue
        if is_chunk_failure(row) and not include_chunks:
            continue
        yield row


def iter_failed_suite_paths(report: dict[str, Any]) -> Iterator[str]:
    """Yield unique ya.make suite directories for failed/errored tests."""
    seen: set[str] = set()
    for row in iter_failed_rows(report, include_chunks=True):
        path_str = row.get("path") or ""
        if not path_str or path_str in seen:
            continue
        seen.add(path_str)
        yield path_str


def iter_failed_test_filters(report: dict[str, Any]) -> Iterator[str]:
    """Yield ya ``--test-filter`` values for failed/errored tests in a report."""
    seen: set[str] = set()
    for row in iter_failed_rows(report, include_chunks=False):
        entry = TestEntry.from_report_row(row)
        if entry is None or not entry.name:
            continue
        ya_filter = test_name_to_ya_filter(entry.name)
        if ya_filter in seen:
            continue
        seen.add(ya_filter)
        yield ya_filter


def write_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(report, fp, ensure_ascii=False, indent=2)
        fp.write("\n")
