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


def downsample_xs(xs: list[float], max_points: int) -> list[float]:
    if max_points <= 0 or len(xs) <= max_points:
        return list(xs)
    last = len(xs) - 1
    return [xs[int(i * last / (max_points - 1))] for i in range(max_points)]


def align_series(xs_out: list[float], xs_src: list[float], ys_src: list[float]) -> list[float]:
    """Step-hold ys_src onto xs_out; 0 outside the source window."""
    if not xs_out:
        return []
    if not xs_src or not ys_src:
        return [0.0] * len(xs_out)
    out: list[float] = []
    j = 0
    t0 = float(xs_src[0])
    t1 = float(xs_src[-1])
    for raw in xs_out:
        t = float(raw)
        if t < t0 or t > t1:
            out.append(0.0)
            continue
        while j + 1 < len(xs_src) and float(xs_src[j + 1]) <= t:
            j += 1
        out.append(float(ys_src[j] or 0.0))
    return out


def align_optional_series(
    xs_out: list[float], xs_src: list[float], ys_src: list[float | None]
) -> list[float | None]:
    """Step-hold optional values while preserving unavailable intervals as gaps."""
    if not xs_out or not xs_src or not ys_src:
        return [None] * len(xs_out)
    out: list[float | None] = []
    j = 0
    for raw in xs_out:
        t = float(raw)
        if t < float(xs_src[0]) or t > float(xs_src[-1]):
            out.append(None)
            continue
        while j + 1 < len(xs_src) and float(xs_src[j + 1]) <= t:
            j += 1
        value = ys_src[j]
        out.append(float(value) if value is not None else None)
    return out


def build_measured_layers(
    grouped: dict[str, dict[str, Any]],
    max_points: int = 400,
) -> dict[str, Any]:
    """Common-axis stacked tracks for measured cgroup series."""
    xs: list[float] = sorted({float(x) for bucket in grouped.values() for x in (bucket.get("xs") or [])})
    xs = downsample_xs(xs, max_points)
    cpu_tracks: dict[str, list[float]] = {}
    ram_tracks: dict[str, list[float]] = {}
    write_tracks: dict[str, list[float]] = {}
    read_tracks: dict[str, list[float]] = {}
    pressure_tracks: dict[str, list[float]] = {}
    pressure_names = {
        "cpu_throttled_pct": "CPU throttled",
        "cpu_psi_some_pct": "CPU PSI some",
        "cpu_psi_full_pct": "CPU PSI full",
        "memory_psi_some_pct": "Memory PSI some",
        "memory_psi_full_pct": "Memory PSI full",
        "io_psi_some_pct": "I/O PSI some",
        "io_psi_full_pct": "I/O PSI full",
    }
    by_label: dict[str, dict[str, Any]] = {}
    for key, bucket in grouped.items():
        label = str(bucket.get("label") or key)
        src_xs = [float(x) for x in (bucket.get("xs") or [])]
        cpu_tracks[label] = align_series(xs, src_xs, bucket.get("cpu_cores") or [])
        ram_tracks[label] = align_series(xs, src_xs, bucket.get("ram_gb") or [])
        write_tracks[label] = align_series(xs, src_xs, bucket.get("disk_write_mbps") or [])
        read_tracks[label] = align_series(xs, src_xs, bucket.get("disk_read_mbps") or [])
        for field, display_name in pressure_names.items():
            values = (bucket.get("cgroup_signals") or {}).get(field)
            if values is None or not any(value is not None for value in values):
                continue
            pressure_tracks[f"{label} · {display_name}"] = align_optional_series(xs, src_xs, values)
        by_label[label] = {
            "key": key,
            "suite": bucket.get("suite"),
            "chunk": bucket.get("chunk"),
            "cgroup": bucket.get("cgroup"),
        }
    return {
        "xs_evlog_sec": xs,
        "cpu_tracks": cpu_tracks,
        "ram_tracks": ram_tracks,
        "write_tracks": write_tracks,
        "read_tracks": read_tracks,
        "pressure_tracks": pressure_tracks,
        "by_label": by_label,
    }


def first_link_url(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.strip():
                return item.strip()
    return ""


def normalize_report_links(links: Any) -> dict[str, str]:
    """Keep report log URLs as name → url (stderr/stdout/log/logsdir)."""
    if not isinstance(links, dict):
        return {}
    out: dict[str, str] = {}
    for key, value in links.items():
        name = str(key or "").strip()
        url = first_link_url(value)
        if name and url:
            out[name] = url
    return out


def place_chunk_tests(
    tests: list[dict[str, Any]],
    start_sec: float,
    delay_sec: float = 0.0,
) -> list[dict[str, Any]]:
    """Lay tests end-to-end after chunk start (UNITTEST/gtest run sequentially)."""
    cursor = float(start_sec) + max(0.0, float(delay_sec or 0.0))
    placed: list[dict[str, Any]] = []
    for test in tests or []:
        duration = max(0.0, float(test.get("duration_sec") or 0.0))
        name = str(test.get("name") or "").strip() or "test"
        placed.append(
            {
                "name": name,
                "status": str(test.get("status") or ""),
                "error_type": str(test.get("error_type") or ""),
                "links": normalize_report_links(test.get("links")),
                "duration_sec": round(duration, 3),
                "start_sec": round(cursor, 3),
                "end_sec": round(cursor + duration, 3),
            }
        )
        cursor += duration
    return placed


def measured_chunk_tests_by_label(
    grouped: dict[str, dict[str, Any]],
    runs: list[dict[str, Any]],
    tests_by_chunk: dict[tuple[Any, ...], list[dict[str, Any]]],
    chunks: dict[tuple[Any, ...], dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Map measured series labels to timed tests inside that chunk."""
    run_start: dict[tuple[str, int], float] = {}
    for run in runs or []:
        suite = str(run.get("suite_path") or run.get("suite") or "").strip()
        chunk = run.get("chunk")
        if not suite or chunk is None or chunk == "":
            continue
        window = _run_window_sec(run)
        if window is None:
            continue
        run_start[(suite, int(chunk))] = window[0]

    def tests_for(suite: str, chunk: int) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        for (raw_suite, _group, idx), rows in (tests_by_chunk or {}).items():
            if int(idx) != chunk:
                continue
            if str(raw_suite) == suite or str(raw_suite).startswith(suite + "/"):
                found = rows
                if str(raw_suite) == suite:
                    return rows
        return found

    def delay_for(suite: str, chunk: int) -> float:
        for (raw_suite, _group, idx), meta in (chunks or {}).items():
            if int(idx) != chunk:
                continue
            if str(raw_suite) != suite and not str(raw_suite).startswith(suite + "/"):
                continue
            raw = meta.get("suite_delay_until_first_test_secs")
            try:
                return max(0.0, float(raw or 0.0))
            except (TypeError, ValueError):
                return 0.0
        return 0.0

    out: dict[str, list[dict[str, Any]]] = {}
    for key, bucket in grouped.items():
        suite = str(bucket.get("suite") or "").strip()
        chunk = bucket.get("chunk")
        label = str(bucket.get("label") or key)
        if chunk is None or chunk == "" or not suite:
            continue
        chunk_i = int(chunk)
        start = run_start.get((suite, chunk_i))
        xs = bucket.get("xs") or []
        if start is None and xs:
            start = float(min(xs))
        if start is None:
            continue
        placed = place_chunk_tests(tests_for(suite, chunk_i), start, delay_for(suite, chunk_i))
        if placed:
            out[label] = placed
            out[str(key)] = placed
    return out
