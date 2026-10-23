"""Shared utilities for loading resource monitoring data (JSONL from monitor_resources.py)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

GENERIC_SUITE_NAMES = frozenset(
    {
        "unittest",
        "py3test",
        "pytest",
        "gtest",
        "exectest",
        "fuzz",
        "go_test",
        "java",
        "playwright",
        "unknown",
    }
)


def load_resources_jsonl(path: Path, encoding: str = "utf-8") -> list[dict[str, Any]]:
    """Load JSONL file from monitor_resources.py. Returns list of record dicts."""
    records: list[dict[str, Any]] = []
    text = path.read_text(encoding=encoding, errors="replace").strip()
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def is_host_record(record: dict[str, Any]) -> bool:
    """Host-wide sample. Test-cgroup ticks use kind=test."""
    kind = record.get("kind")
    return kind in (None, "", "host")


def measured_test_key(record: dict[str, Any]) -> str:
    cgroup = str(record.get("cgroup") or "").strip()
    if cgroup:
        return cgroup
    suite = str(record.get("suite") or "").strip()
    chunk = record.get("chunk")
    if chunk is None or chunk == "":
        return suite or "unknown"
    return f"{suite}#{chunk}"


def is_generic_suite(name: str | None) -> bool:
    text = str(name or "").strip()
    if not text:
        return True
    lower = text.lower()
    return lower in GENERIC_SUITE_NAMES or lower.startswith("ya-test-")


def measured_test_label(record: dict[str, Any]) -> str:
    suite = str(record.get("suite") or record.get("cgroup") or "unknown")
    chunk = record.get("chunk")
    if chunk is None or chunk == "":
        return suite
    return f"{suite} [{chunk}]"


def _run_window_sec(run: dict[str, Any]) -> tuple[float, float] | None:
    start_us = run.get("start_us")
    end_us = run.get("end_us")
    if start_us is not None and end_us is not None:
        start = float(start_us) / 1_000_000.0
        end = float(end_us) / 1_000_000.0
        if end > start:
            return start, end
    start_sec = run.get("start_sec")
    end_sec = run.get("end_sec")
    if start_sec is not None and end_sec is not None:
        start = float(start_sec)
        end = float(end_sec)
        if end > start:
            return start, end
    return None


def match_measured_to_runs(
    grouped: dict[str, dict[str, Any]],
    runs: list[dict[str, Any]],
    min_iou: float = 0.2,
) -> None:
    """Fill suite/chunk/label from overlapping evlog runs when the monitor only saw TEST_NAME."""
    windows: list[tuple[str, Any, float, float]] = []
    for run in runs or []:
        window = _run_window_sec(run)
        suite = str(run.get("suite_path") or run.get("suite") or "").strip()
        if window is None or not suite or is_generic_suite(suite):
            continue
        windows.append((suite, run.get("chunk"), window[0], window[1]))
    scores: list[tuple[float, float, str, int]] = []
    for key, bucket in grouped.items():
        xs = bucket.get("xs") or []
        if not xs:
            continue
        if not is_generic_suite(bucket.get("suite")) and bucket.get("chunk") is not None:
            continue
        t0 = float(min(xs))
        t1 = float(max(xs))
        if t1 <= t0:
            t1 = t0 + 1.0
        for idx, (suite, chunk, start, end) in enumerate(windows):
            overlap = max(0.0, min(t1, end) - max(t0, start))
            union = max(t1, end) - min(t0, start)
            if overlap <= 0 or union <= 0:
                continue
            iou = overlap / union
            if iou < min_iou:
                continue
            scores.append((iou, overlap, key, idx))
    scores.sort(reverse=True)
    used_keys: set[str] = set()
    used_runs: set[int] = set()
    for _iou, _overlap, key, idx in scores:
        if key in used_keys or idx in used_runs:
            continue
        used_keys.add(key)
        used_runs.add(idx)
        suite, chunk, _start, _end = windows[idx]
        bucket = grouped[key]
        bucket["suite"] = suite
        bucket["chunk"] = chunk
        bucket["label"] = measured_test_label({"suite": suite, "chunk": chunk})
