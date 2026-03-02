#!/usr/bin/env python3
"""
Generate tests resource dashboard and trace for chunk runs by joining:
  - ya evlog (Run(.../chunkN/...))
  - report.json chunk records ([N/total] chunk)

Supports:
  - single suite (--suite-path ...)
  - all suites (omit --suite-path)
"""

from __future__ import annotations

import argparse
import bisect
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

# Same-dir library for ya.make REQUIREMENTS (reserved cpu/ram)
if __name__ != "__main__":
    from . import ya_make_requirements
else:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import ya_make_requirements

# Supports both:
#   [3/10] chunk
#   [test_file.py 3/10] chunk
CHUNK_FROM_SUBTEST_RE = re.compile(r"\[(?:[^\]]*?\s)?(\d+)/(?:\d+)\]\s+chunk")
CHUNK_SOLE_RE = re.compile(r"^\s*sole\s+chunk\s*$", re.IGNORECASE)
CHUNK_BRACKET_ONLY_RE = re.compile(r"^\s*\[[^\]]+\]\s+chunk\s*$", re.IGNORECASE)
CHUNK_GROUP_FROM_SUBTEST_RE = re.compile(r"\[([^\]\s]+)\s+\d+/\d+\]\s+chunk", re.IGNORECASE)
CHUNK_GROUP_BRACKET_ONLY_RE = re.compile(r"\[([^\]]+)\]\s+chunk", re.IGNORECASE)
RUN_UID_RE = re.compile(r"Run\((rnd-[^\$\)]+)")
RUN_SUITE_GROUP_CHUNK_RE = re.compile(
    r"\$\(BUILD_ROOT\)/(.+?)/test-results/.+?/testing_out_stuff/([^/]+)/chunk(\d+)/"
)
RUN_SUITE_CHUNK_IN_STUFF_RE = re.compile(
    r"\$\(BUILD_ROOT\)/(.+?)/test-results/.+?/testing_out_stuff/chunk(\d+)/"
)
RUN_SUITE_CHUNK_RE = re.compile(r"\$\(BUILD_ROOT\)/(.+?)/test-results/.+?/chunk(\d+)/")
PART_SUFFIX_RE = re.compile(r"/part\d+$")


def load_json_or_jsonl(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    if not text:
        return []
    if text[0] == "[":
        data = json.loads(text)
        return [x for x in data if isinstance(x, dict)] if isinstance(data, list) else []
    out: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def cpu_seconds(metrics: dict[str, Any]) -> float:
    vals: list[float] = []
    for k in ("ru_utime", "ru_stime"):
        v = metrics.get(k)
        if v is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if fv > 1000:
            fv /= 1_000_000.0
        vals.append(fv)
    return sum(vals)


def ram_kb(metrics: dict[str, Any]) -> float:
    mx = metrics.get("ru_maxrss")
    if mx is not None:
        try:
            return float(mx)
        except (TypeError, ValueError):
            pass
    rss = metrics.get("ru_rss")
    if rss is not None:
        try:
            return float(rss) / 1024.0
        except (TypeError, ValueError):
            pass
    return 0.0


def normalize_suite_path(path: str) -> str:
    """Merge partitioned suites like .../part7 into one suite ..."""
    return PART_SUFFIX_RE.sub("", path)


def normalize_chunk_group(group: Optional[str]) -> Optional[str]:
    if not group:
        return None
    g = str(group).strip().strip("/")
    if not g:
        return None
    # For report groups like "test_postgres.py" use stem.
    if g.endswith(".py"):
        g = g[:-3]
    # Keep only last path part if path-like.
    if "/" in g:
        g = g.rsplit("/", 1)[-1]
    return g or None


def chunk_group_from_subtest(subtest_name: str) -> Optional[str]:
    m = CHUNK_GROUP_FROM_SUBTEST_RE.search(subtest_name)
    if m:
        return normalize_chunk_group(m.group(1))
    m2 = CHUNK_GROUP_BRACKET_ONLY_RE.search(subtest_name)
    if m2:
        raw = m2.group(1).strip()
        # For plain indexed chunks like "[3/10] chunk" there is no group.
        if re.fullmatch(r"\d+/\d+", raw):
            return None
        return normalize_chunk_group(raw)
    return None


def extract_suite_group_chunk_from_run_name(arg_name: str) -> tuple[Optional[str], Optional[str], Optional[int]]:
    m = RUN_SUITE_GROUP_CHUNK_RE.search(arg_name)
    if m:
        return m.group(1), normalize_chunk_group(m.group(2)), int(m.group(3))
    m1 = RUN_SUITE_CHUNK_IN_STUFF_RE.search(arg_name)
    if m1:
        return m1.group(1), None, int(m1.group(2))
    m2 = RUN_SUITE_CHUNK_RE.search(arg_name)
    if m2:
        return m2.group(1), None, int(m2.group(2))
    return None, None, None


def _status_bucket() -> dict[str, int]:
    return {"errors": 0, "timeouts": 0, "muted": 0, "muted_timeouts": 0, "fails_total": 0}


def _classify_failure(status: str, error_type: str, is_muted: bool) -> tuple[bool, bool, bool]:
    status_u = (status or "").upper()
    error_u = (error_type or "").upper()
    timeout = error_u == "TIMEOUT" or ("TIMEOUT" in status_u)
    muted = bool(is_muted) or status_u in {"MUTE", "MUTED"}
    failedish = status_u in {"FAILED", "ERROR", "INTERNAL"}
    return timeout, muted, failedish


def parse_report_chunks(
    report_path: Path, suite_filter: Optional[str]
) -> tuple[dict[tuple[str, Optional[str], int], dict[str, Any]], dict[str, dict[str, dict[str, int]]]]:
    report = json.loads(report_path.read_text(encoding="utf-8", errors="replace"))
    results = report.get("results", []) if isinstance(report, dict) else []
    chunks: dict[tuple[str, Optional[str], int], dict[str, Any]] = {}
    report_status_by_suite: dict[str, dict[str, dict[str, int]]] = {}

    def ensure_suite(suite: str) -> dict[str, dict[str, int]]:
        if suite not in report_status_by_suite:
            report_status_by_suite[suite] = {"chunks": _status_bucket(), "tests": _status_bucket()}
        return report_status_by_suite[suite]

    parsed_chunk_rows: list[tuple[str, Optional[str], int, dict[str, Any]]] = []

    for item in results:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "test":
            continue
        suite = str(item.get("path", ""))
        if not suite:
            continue
        if suite_filter and suite != suite_filter:
            continue
        suite = normalize_suite_path(suite)

        status = str(item.get("status", ""))
        error_type = str(item.get("error_type", "") or "")
        is_muted = bool(item.get("is_muted") or item.get("muted"))
        timeout, muted, failedish = _classify_failure(status, error_type, is_muted)
        bucket_key = "chunks" if bool(item.get("chunk")) else "tests"
        bucket = ensure_suite(suite)[bucket_key]
        if timeout:
            bucket["timeouts"] += 1
        if muted:
            bucket["muted"] += 1
        if timeout and muted:
            bucket["muted_timeouts"] += 1
        if failedish and not timeout and not muted:
            bucket["errors"] += 1

        if not item.get("chunk"):
            continue

        sub = str(item.get("subtest_name", ""))
        group = chunk_group_from_subtest(sub)
        m = CHUNK_FROM_SUBTEST_RE.search(sub)
        if m:
            idx = int(m.group(1))
        elif CHUNK_SOLE_RE.search(sub) or CHUNK_BRACKET_ONLY_RE.search(sub):
            # Old report format may not include [i/N]; treat as a single chunk.
            idx = 0
        else:
            continue
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        meta = {
            "status": status,
            "error_type": item.get("error_type"),
            "is_muted": is_muted,
            "duration_sec": float(item.get("duration") or 0.0),
            "cpu_sec": cpu_seconds(metrics),
            "ram_kb": ram_kb(metrics),
            "hid": item.get("hid"),
            "id": item.get("id"),
        }
        parsed_chunk_rows.append((suite, group, idx, meta))

    for suite, group, idx, meta in parsed_chunk_rows:
        chunks[(suite, group, idx)] = meta

    # Backward-compatible fallback key: (suite, None, chunk_idx).
    # Add only when chunk_idx is unique within suite to avoid cross-group mixups.
    per_suite_idx_count: dict[tuple[str, int], int] = defaultdict(int)
    for suite, _group, idx, _meta in parsed_chunk_rows:
        per_suite_idx_count[(suite, idx)] += 1
    for suite, group, idx, meta in parsed_chunk_rows:
        if per_suite_idx_count[(suite, idx)] == 1 and (suite, None, idx) not in chunks:
            alias_meta = dict(meta)
            alias_meta["_fallback_alias"] = True
            chunks[(suite, None, idx)] = alias_meta
    for by_kind in report_status_by_suite.values():
        by_kind["chunks"]["fails_total"] = by_kind["chunks"]["errors"] + by_kind["chunks"]["timeouts"]
        by_kind["tests"]["fails_total"] = by_kind["tests"]["errors"] + by_kind["tests"]["timeouts"]
    return chunks, report_status_by_suite


def parse_evlog_runs(evlog_path: Path, suite_filter: Optional[str]) -> list[dict[str, Any]]:
    events = load_json_or_jsonl(evlog_path)
    stacks: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    runs: list[dict[str, Any]] = []

    def _tid_from_thread_name(name: str) -> int:
        m = re.search(r"_(\d+)$", name or "")
        if m:
            return int(m.group(1))
        return abs(hash(name or "worker")) % 100_000

    for ev in events:
        # Old evlog format (jsonl): worker_threads/node-finished with value.time.
        if "ph" not in ev and ev.get("namespace") == "worker_threads" and ev.get("event") == "node-finished":
            value = ev.get("value") if isinstance(ev.get("value"), dict) else {}
            arg_name = str(value.get("name", ""))
            if not arg_name.startswith("Run("):
                continue
            suite, chunk_group, chunk = extract_suite_group_chunk_from_run_name(arg_name)
            if suite is None or chunk is None:
                continue
            if suite_filter is not None and suite != suite_filter:
                continue
            time_range = value.get("time") if isinstance(value.get("time"), list) else None
            if (
                isinstance(time_range, list)
                and len(time_range) == 2
                and isinstance(time_range[0], (int, float))
                and isinstance(time_range[1], (int, float))
                and float(time_range[1]) > float(time_range[0])
            ):
                start_us = float(time_range[0]) * 1_000_000.0
                end_us = float(time_range[1]) * 1_000_000.0
            else:
                continue
            tid = _tid_from_thread_name(str(ev.get("thread_name", "")))
            m_uid = RUN_UID_RE.search(arg_name)
            runs.append(
                {
                    "pid": 1,
                    "tid": tid,
                    "start_us": start_us,
                    "end_us": end_us,
                    "dur_us": end_us - start_us,
                    "suite_path": suite,
                    "chunk_group": chunk_group,
                    "chunk": chunk,
                    "uid": m_uid.group(1) if m_uid else None,
                    "raw_name": arg_name,
                }
            )
            continue

        ph = ev.get("ph")
        if ph not in ("B", "E"):
            continue
        pid = int(ev.get("pid", 0))
        tid = int(ev.get("tid", 0))
        ts = float(ev.get("ts", 0.0))
        key = (pid, tid)
        if ph == "B":
            args = ev.get("args") if isinstance(ev.get("args"), dict) else {}
            arg_name = str(args.get("name", ""))
            m_uid = RUN_UID_RE.search(arg_name)
            suite, chunk_group, chunk = extract_suite_group_chunk_from_run_name(arg_name)
            is_target = (
                arg_name.startswith("Run(")
                and suite is not None
                and chunk is not None
                and (suite_filter is None or suite == suite_filter)
            )
            stacks[key].append(
                {
                    "ts": ts,
                    "arg_name": arg_name,
                    "is_target": is_target,
                    "suite": suite,
                    "chunk_group": chunk_group,
                    "chunk": chunk,
                    "uid": m_uid.group(1) if m_uid else None,
                }
            )
        else:
            if not stacks[key]:
                continue
            begin = stacks[key].pop()
            if not begin["is_target"] or ts <= begin["ts"]:
                continue
            runs.append(
                {
                    "pid": pid,
                    "tid": tid,
                    "start_us": begin["ts"],
                    "end_us": ts,
                    "dur_us": ts - begin["ts"],
                    "suite_path": begin["suite"],
                    "chunk_group": begin.get("chunk_group"),
                    "chunk": begin["chunk"],
                    "uid": begin["uid"],
                    "raw_name": begin["arg_name"],
                }
            )
    runs.sort(key=lambda x: (x["start_us"], x["tid"]))
    return runs


def add_counter_series(trace_events: list[dict[str, Any]], pid: int, name: str, deltas: list[tuple[float, float]]) -> None:
    if not deltas:
        return
    cur = 0.0
    for ts, d in sorted(deltas, key=lambda x: x[0]):
        cur += d
        trace_events.append({"ph": "C", "pid": pid, "tid": 0, "ts": ts, "name": name, "cat": "resource", "args": {"value": cur}})


def build_trace(
    runs: list[dict[str, Any]],
    chunks: dict[tuple[str, Optional[str], int], dict[str, Any]],
    suite_filter: Optional[str],
    requirements_cache: Optional[dict[str, dict[str, Any]]] = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    out: list[dict[str, Any]] = []
    pid_chunks = 1
    pid_counters = 2
    active_delta: list[tuple[float, float]] = []
    cpu_delta: list[tuple[float, float]] = []
    ram_delta: list[tuple[float, float]] = []

    matched = 0
    missing_metrics = 0
    runs_with_synthetic_metrics = 0
    enriched_runs: list[dict[str, Any]] = []

    run_keys = {
        (normalize_suite_path(str(r["suite_path"])), normalize_chunk_group(r.get("chunk_group")), int(r["chunk"]))
        for r in runs
        if r.get("suite_path") is not None and r.get("chunk") is not None
    }
    report_chunk_keys = {k for k, v in chunks.items() if not bool(v.get("_fallback_alias"))}
    missing_runs_for_chunk = len(report_chunk_keys - run_keys)

    suite_matched: dict[str, int] = defaultdict(int)
    suite_missing: dict[str, int] = defaultdict(int)

    for r in runs:
        suite_raw = str(r["suite_path"])
        suite = normalize_suite_path(suite_raw)
        chunk_group = normalize_chunk_group(r.get("chunk_group"))
        idx = int(r["chunk"])
        meta = chunks.get((suite, chunk_group, idx), {}) or chunks.get((suite, None, idx), {})
        if meta:
            matched += 1
            suite_matched[suite] += 1
        else:
            missing_metrics += 1
            suite_missing[suite] += 1

        synthetic_metrics = False
        if meta:
            cpu = float(meta.get("cpu_sec", 0.0) or 0.0)
            ram = float(meta.get("ram_kb", 0.0) or 0.0)
            status = str(meta.get("status", "UNKNOWN"))
            error_type = str(meta.get("error_type", "") or "")
            is_muted = bool(meta.get("is_muted"))
        elif requirements_cache and suite in requirements_cache:
            req = requirements_cache[suite]
            dur_sec = r["dur_us"] / 1_000_000.0
            cpu = float(req.get("cpu_cores", 0) or 0) * dur_sec
            ram = float((req.get("ram_gb", 0) or 0) * 1024 * 1024)
            status = "UNKNOWN"
            error_type = ""
            is_muted = False
            synthetic_metrics = True
            runs_with_synthetic_metrics += 1
        else:
            cpu = 0.0
            ram = 0.0
            status = "UNKNOWN"
            error_type = ""
            is_muted = False

        out.append(
            {
                "ph": "X",
                "pid": pid_chunks,
                "tid": int(r["tid"]),
                "ts": r["start_us"],
                "dur": r["dur_us"],
                "name": f"{suite}::{chunk_group}/chunk{idx}" if chunk_group else f"{suite}::chunk{idx}",
                "cat": "suite_chunk",
                "args": {
                    "suite_path": suite,
                    "suite_path_raw": suite_raw,
                    "chunk_group": chunk_group,
                    "chunk": idx,
                    "status": status,
                    "error_type": error_type,
                    "is_muted": is_muted,
                    "cpu_sec_report": cpu,
                    "ram_kb_report": ram,
                    "evlog_dur_sec": round(r["dur_us"] / 1_000_000.0, 3),
                    "uid": r.get("uid"),
                    "report_hid": meta.get("hid") if meta else None,
                    "report_id": meta.get("id") if meta else None,
                    "synthetic_metrics": synthetic_metrics,
                },
            }
        )

        active_delta.append((r["start_us"], +1.0))
        active_delta.append((r["end_us"], -1.0))
        cpu_delta.append((r["start_us"], +cpu))
        cpu_delta.append((r["end_us"], -cpu))
        ram_delta.append((r["start_us"], +ram))
        ram_delta.append((r["end_us"], -ram))

        er = dict(r)
        er["cpu_sec_report"] = cpu
        er["ram_kb_report"] = ram
        er["suite_path"] = suite
        er["suite_path_raw"] = suite_raw
        er["chunk_group"] = chunk_group
        er["chunk"] = idx
        er["synthetic_metrics"] = synthetic_metrics
        er["status"] = status
        er["error_type"] = error_type
        er["is_muted"] = is_muted
        enriched_runs.append(er)

    add_counter_series(out, pid_counters, "active_chunks", active_delta)
    add_counter_series(out, pid_counters, "cpu_sec_sum", cpu_delta)
    add_counter_series(out, pid_counters, "ram_kb_sum", ram_delta)
    out.sort(key=lambda x: (x.get("ts", 0), x.get("ph", "")))

    def peak_from_deltas(deltas: list[tuple[float, float]]) -> float:
        cur = 0.0
        peak = 0.0
        for _, d in sorted(deltas, key=lambda x: x[0]):
            cur += d
            peak = max(peak, cur)
        return peak

    suites_missing_list = [s for s in sorted(suite_missing.keys()) if suite_missing[s] > 0]
    per_suite_metrics = {
        s: {"with_metrics": suite_matched[s], "without_metrics": suite_missing[s]}
        for s in sorted(set(suite_matched) | set(suite_missing))
        if suite_missing.get(s, 0) > 0
    }

    stats = {
        "suite_path_filter": suite_filter,
        "runs_from_evlog": len(runs),
        "chunks_in_report": len(report_chunk_keys),
        "matched_runs_with_metrics": matched,
        "runs_without_report_metrics": missing_metrics,
        "runs_with_synthetic_metrics": runs_with_synthetic_metrics,
        "report_chunks_without_run": missing_runs_for_chunk,
        "suites_without_report_metrics": suites_missing_list,
        "per_suite_metrics": per_suite_metrics,
        "peak_active_chunks": peak_from_deltas(active_delta),
        "peak_cpu_sec_sum": peak_from_deltas(cpu_delta),
        "peak_ram_kb_sum": peak_from_deltas(ram_delta),
    }
    return out, stats, enriched_runs


def _palette() -> list[str]:
    return [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2",
        "#7f7f7f", "#bcbd22", "#17becf", "#4e79a7", "#f28e2b", "#59a14f", "#e15759",
        "#76b7b2", "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ab",
    ]


def _topn_other_map(values: dict[str, float], top_n: int) -> list[str]:
    return [k for k, _ in sorted(values.items(), key=lambda x: x[1], reverse=True)[:top_n]]


def _build_step_series(runs: list[dict[str, Any]], value_key: str, top_labels: set[str], label_mode: str = "suite_chunk") -> tuple[list[float], dict[str, list[float]]]:
    by_label = defaultdict(float)
    events: list[tuple[float, int, str, float]] = []
    for r in runs:
        if label_mode == "suite":
            label = str(r["suite_path"])
        else:
            group = normalize_chunk_group(r.get("chunk_group"))
            label = f"{r['suite_path']}::{group}/chunk{r['chunk']}" if group else f"{r['suite_path']}::chunk{r['chunk']}"
        v = float(r.get(value_key, 0.0) or 0.0)
        events.append((float(r["start_us"]), +1, label, v))
        events.append((float(r["end_us"]), -1, label, v))
    events.sort(key=lambda x: (x[0], -x[1]))

    tracks = {lb: [] for lb in top_labels}
    tracks["other"] = []
    xs: list[float] = []
    i = 0
    while i < len(events):
        ts = events[i][0]
        while i < len(events) and events[i][0] == ts:
            _, sign, label, val = events[i]
            by_label[label] += sign * val
            i += 1
        xs.append(ts / 1_000_000.0)
        other = 0.0
        for lb, cur in by_label.items():
            if cur <= 0:
                continue
            if lb in top_labels:
                tracks[lb].append(cur)
            else:
                other += cur
        for lb in top_labels:
            if len(tracks[lb]) < len(xs):
                tracks[lb].append(0.0)
        tracks["other"].append(other)
    return xs, tracks


def _build_active_series(runs: list[dict[str, Any]]) -> tuple[list[float], list[float]]:
    events: list[tuple[float, float]] = []
    for r in runs:
        events.append((float(r["start_us"]), +1.0))
        events.append((float(r["end_us"]), -1.0))
    events.sort(key=lambda x: x[0])
    xs: list[float] = []
    ys: list[float] = []
    cur = 0.0
    i = 0
    while i < len(events):
        ts = events[i][0]
        while i < len(events) and events[i][0] == ts:
            cur += events[i][1]
            i += 1
        xs.append(ts / 1_000_000.0)
        ys.append(cur)
    return xs, ys


def _downsample_step_series(
    xs: list[float], tracks: dict[str, list[float]], max_points: int
) -> tuple[list[float], dict[str, list[float]]]:
    """Reduce step series to at most max_points by sampling uniformly in time."""
    if not xs or max_points <= 0 or len(xs) <= max_points:
        return xs, tracks
    t_min, t_max = xs[0], xs[-1]
    if t_max <= t_min:
        return xs, tracks
    xs_sampled = [
        t_min + (t_max - t_min) * i / (max_points - 1) if max_points > 1 else t_min
        for i in range(max_points)
    ]
    tracks_sampled: dict[str, list[float]] = {}
    for lb, vals in tracks.items():
        tracks_sampled[lb] = [
            vals[bisect.bisect_right(xs, t) - 1] if xs[0] <= t else 0.0
            for t in xs_sampled
        ]
    return xs_sampled, tracks_sampled


def _downsample_active_series(
    xs: list[float], ys: list[float], max_points: int
) -> tuple[list[float], list[float]]:
    """Reduce (xs, ys) to at most max_points by sampling uniformly in time."""
    if not xs or max_points <= 0 or len(xs) <= max_points:
        return xs, ys
    t_min, t_max = xs[0], xs[-1]
    if t_max <= t_min:
        return xs, ys
    xs_sampled = [
        t_min + (t_max - t_min) * i / (max_points - 1) if max_points > 1 else t_min
        for i in range(max_points)
    ]
    ys_sampled = [
        ys[bisect.bisect_right(xs, t) - 1] if xs[0] <= t else 0.0 for t in xs_sampled
    ]
    return xs_sampled, ys_sampled


def _round_cpu_tier(cores: float) -> int:
    """Round up to runner-friendly tier: 1, 2, 4, 8, 16."""
    if cores <= 1:
        return 1
    if cores <= 2:
        return 2
    if cores <= 4:
        return 4
    if cores <= 8:
        return 8
    return 16


def _next_cpu_tier(cpu: int) -> int:
    """Move to the next runner tier: 1->2->4->8->16."""
    if cpu <= 1:
        return 2
    if cpu <= 2:
        return 4
    if cpu <= 4:
        return 8
    if cpu <= 8:
        return 16
    return 16


def build_cpu_suggestions(
    runs: list[dict[str, Any]],
    requirements_cache: Optional[dict[str, dict[str, Any]]] = None,
    report_status_by_suite: Optional[dict[str, dict[str, dict[str, int]]]] = None,
    maximize_reqs_for_timeout_tests: bool = False,
) -> list[dict[str, Any]]:
    """
    Per-suite stats from runs (cpu_sec_report, dur_us) to suggest cpuN for the runner.
    Returns list of dicts: suite_path, chunks_count, median_cores, p95_cores,
    recommended_cpu (tier 1/2/4/8/16 or "all"), total_cpu_sec, total_ram_gb, total_dur_sec, has_synthetic.
    """
    by_suite: dict[str, list[float]] = defaultdict(list)
    by_suite_cpu: dict[str, float] = defaultdict(float)
    by_suite_ram_kb: dict[str, float] = defaultdict(float)
    by_suite_dur: dict[str, float] = defaultdict(float)
    by_suite_synthetic: dict[str, bool] = defaultdict(bool)
    by_suite_errors: dict[str, int] = defaultdict(int)
    by_suite_timeouts: dict[str, int] = defaultdict(int)
    by_suite_muted: dict[str, int] = defaultdict(int)
    by_suite_muted_timeouts: dict[str, int] = defaultdict(int)
    for r in runs:
        suite = str(r.get("suite_path", ""))
        if not suite:
            continue
        cpu = float(r.get("cpu_sec_report", 0.0) or 0.0)
        ram_kb = float(r.get("ram_kb_report", 0.0) or 0.0)
        dur_s = float(r.get("dur_us", 0) or 0) / 1_000_000.0
        by_suite_cpu[suite] += cpu
        by_suite_ram_kb[suite] += ram_kb
        by_suite_dur[suite] += dur_s
        if r.get("synthetic_metrics"):
            by_suite_synthetic[suite] = True
        status = str(r.get("status", "") or "").upper()
        error_type = str(r.get("error_type", "") or "").upper()
        is_timeout = error_type == "TIMEOUT" or ("TIMEOUT" in status)
        is_muted = bool(r.get("is_muted")) or status == "MUTE"
        is_failedish = status in {"FAILED", "ERROR", "INTERNAL"}
        if is_timeout:
            by_suite_timeouts[suite] += 1
        if is_muted:
            by_suite_muted[suite] += 1
        if is_timeout and is_muted:
            by_suite_muted_timeouts[suite] += 1
        if is_failedish and not is_timeout and not is_muted:
            by_suite_errors[suite] += 1
        if dur_s > 0:
            by_suite[suite].append(cpu / dur_s)
    all_suites = sorted(set(by_suite_cpu.keys()) | set((report_status_by_suite or {}).keys()) | set((requirements_cache or {}).keys()))
    out: list[dict[str, Any]] = []
    for suite in all_suites:
        cores_list = by_suite[suite]
        if not cores_list:
            median_c = 0.0
            p95_c = 0.0
        else:
            sorted_cores = sorted(cores_list)
            n = len(sorted_cores)
            median_c = sorted_cores[(n - 1) // 2] if n else 0.0
            idx95 = min(int(0.95 * n + 0.5), n - 1) if n else 0
            p95_c = sorted_cores[idx95] if n else 0.0
        if report_status_by_suite and suite in report_status_by_suite:
            chunk_status = report_status_by_suite[suite].get("chunks", _status_bucket())
            test_status = report_status_by_suite[suite].get("tests", _status_bucket())
        else:
            chunk_status = {
                "errors": by_suite_errors[suite],
                "timeouts": by_suite_timeouts[suite],
                "muted": by_suite_muted[suite],
                "muted_timeouts": by_suite_muted_timeouts[suite],
                "fails_total": by_suite_errors[suite] + by_suite_timeouts[suite],
            }
            test_status = _status_bucket()

        timeout_tests_count = int(test_status.get("timeouts", 0) or 0)
        base_recommended = _round_cpu_tier(p95_c)
        timeout_boost_applied = timeout_tests_count > 0
        recommended = _next_cpu_tier(base_recommended) if timeout_boost_applied else base_recommended
        req = (requirements_cache or {}).get(suite, {})
        ya_cpu = req.get("cpu_cores")
        ya_ram = req.get("ram_gb")
        ya_size = req.get("size")
        if not ya_size:
            ya_size = "SMALL"
        # Policy: MEDIUM suites must not request more than cpu:4.
        medium_cap_applied = str(ya_size or "").upper() == "MEDIUM" and recommended > 4
        if medium_cap_applied:
            recommended = 4
        timeout_max_policy_applied = bool(maximize_reqs_for_timeout_tests and timeout_tests_count > 0)
        size_u = str(ya_size or "").upper()
        timeout_max_value: Any = None
        if timeout_max_policy_applied:
            if size_u == "SMALL":
                timeout_max_value = 1
            elif size_u == "MEDIUM":
                timeout_max_value = 4
            elif size_u == "LARGE":
                timeout_max_value = "all"
            else:
                timeout_max_value = "all"
        recommended_req: Any = timeout_max_value if timeout_max_policy_applied else recommended
        explain_parts = [
            f"p95_cores={p95_c:.3f}",
            f"base_tier={base_recommended}",
        ]
        if timeout_boost_applied:
            explain_parts.append(f"test_timeouts={timeout_tests_count} -> next_tier={_next_cpu_tier(base_recommended)}")
        if timeout_max_policy_applied:
            explain_parts.append(f"maximize_reqs_for_timeout_tests(size={size_u or 'UNKNOWN'}) -> {timeout_max_value}")
        if medium_cap_applied:
            explain_parts.append("MEDIUM cap -> 4")
        explain_parts.append(f"final={recommended_req}")
        recommended_cpu_explain = "; ".join(explain_parts)
        if recommended_req == "all":
            if ya_cpu is None:
                cpu_action = "set"
            else:
                cpu_action = "raise"
        else:
            recommended_num = int(recommended_req)
            if ya_cpu is None:
                # In ya.make implicit default is cpu:1, so do not suggest explicit set(cpu:1).
                cpu_action = "ok" if recommended_num <= 1 else "set"
            else:
                ya_cpu_i = int(ya_cpu)
                if recommended_num > ya_cpu_i:
                    cpu_action = "raise"
                elif recommended_num < ya_cpu_i:
                    cpu_action = "lower"
                else:
                    cpu_action = "ok"
        out.append({
            "suite_path": suite,
            "chunks_count": len(cores_list),
            "median_cores": round(median_c, 3),
            "p95_cores": round(p95_c, 3),
            "recommended_cpu": recommended_req,
            "recommended_cpu_base": base_recommended,
            "recommended_cpu_timeout_boost": timeout_boost_applied,
            "recommended_cpu_timeout_max_policy_applied": timeout_max_policy_applied,
            "recommended_cpu_timeout_max_policy_value": timeout_max_value,
            "recommended_cpu_medium_cap_applied": medium_cap_applied,
            "recommended_cpu_explain": recommended_cpu_explain,
            "total_cpu_sec": round(by_suite_cpu[suite], 2),
            "total_ram_gb": round(by_suite_ram_kb[suite] / (1024.0 * 1024.0), 3),
            "total_dur_sec": round(by_suite_dur[suite], 2),
            "has_synthetic": by_suite_synthetic.get(suite, False),
            "ya_cpu_cores": ya_cpu,
            "ya_ram_gb": ya_ram,
            "ya_size": ya_size,
            "chunk_errors": chunk_status["errors"],
            "chunk_timeouts": chunk_status["timeouts"],
            "chunk_muted": chunk_status["muted"],
            "chunk_muted_timeouts": chunk_status["muted_timeouts"],
            "chunk_fails_total": chunk_status["fails_total"],
            "test_errors": test_status["errors"],
            "test_timeouts": test_status["timeouts"],
            "test_muted": test_status["muted"],
            "test_muted_timeouts": test_status["muted_timeouts"],
            "test_fails_total": test_status["fails_total"],
            # Legacy aliases kept in output schema for stability.
            "errors": chunk_status["errors"],
            "timeouts": chunk_status["timeouts"],
            "muted": chunk_status["muted"],
            "muted_timeouts": chunk_status["muted_timeouts"],
            "fails_total": chunk_status["fails_total"],
            "cpu_action": cpu_action,
        })
    return out


def build_html_dashboard(
    suite_filter: Optional[str],
    runs: list[dict[str, Any]],
    stats: dict[str, Any],
    out_html: Path,
    top_n: int,
    max_points: int = 1000,
    by_chunk: bool = False,
    cpu_suggestions: Optional[list[dict[str, Any]]] = None,
    run_config: Optional[dict[str, Any]] = None,
) -> None:
    cpu_by_suite: dict[str, float] = defaultdict(float)
    ram_by_suite: dict[str, float] = defaultdict(float)
    active_time_by_suite: dict[str, float] = defaultdict(float)
    suite_set = sorted({str(r["suite_path"]) for r in runs})

    for r in runs:
        cpu_by_suite[str(r["suite_path"])] += float(r.get("cpu_sec_report", 0.0) or 0.0)
        ram_by_suite[str(r["suite_path"])] += float(r.get("ram_kb_report", 0.0) or 0.0)
        active_time_by_suite[str(r["suite_path"])] += float(r.get("dur_us", 0.0) or 0.0) / 1_000_000.0

    top_cpu_suite = _topn_other_map(cpu_by_suite, top_n)
    top_ram_suite = _topn_other_map(ram_by_suite, top_n)
    top_active_suite = _topn_other_map(active_time_by_suite, top_n)

    enriched = []
    for r in runs:
        rr = dict(r)
        dur_s = float(rr["dur_us"]) / 1_000_000.0 if float(rr["dur_us"]) > 0 else 0.0
        rr["cpu_cores_est"] = (float(rr.get("cpu_sec_report", 0.0) or 0.0) / dur_s) if dur_s > 0 else 0.0
        rr["ram_gb"] = float(rr.get("ram_kb_report", 0.0) or 0.0) / (1024.0 * 1024.0)
        rr["active_one"] = 1.0
        enriched.append(rr)

    xs_cpu_suite, cpu_tracks_suite = _build_step_series(enriched, "cpu_cores_est", set(top_cpu_suite), label_mode="suite")
    xs_ram_suite, ram_tracks_suite = _build_step_series(enriched, "ram_gb", set(top_ram_suite), label_mode="suite")
    xs_active_suite, active_tracks_suite = _build_step_series(enriched, "active_one", set(top_active_suite), label_mode="suite")
    xs_active, ys_active = _build_active_series(enriched)

    xs_cpu_suite, cpu_tracks_suite = _downsample_step_series(xs_cpu_suite, cpu_tracks_suite, max_points)
    xs_ram_suite, ram_tracks_suite = _downsample_step_series(xs_ram_suite, ram_tracks_suite, max_points)
    xs_active_suite, active_tracks_suite = _downsample_step_series(xs_active_suite, active_tracks_suite, max_points)
    xs_active, ys_active = _downsample_active_series(xs_active, ys_active, max_points)

    has_synthetic = (stats.get("runs_with_synthetic_metrics") or 0) > 0

    if by_chunk:
        cpu_by_label = defaultdict(float)
        ram_by_label = defaultdict(float)
        active_time_by_label = defaultdict(float)
        for r in runs:
            group = normalize_chunk_group(r.get("chunk_group"))
            lb = f"{r['suite_path']}::{group}/chunk{r['chunk']}" if group else f"{r['suite_path']}::chunk{r['chunk']}"
            cpu_by_label[lb] += float(r.get("cpu_sec_report", 0.0) or 0.0)
            ram_by_label[lb] += float(r.get("ram_kb_report", 0.0) or 0.0)
            active_time_by_label[lb] += float(r.get("dur_us", 0.0) or 0.0) / 1_000_000.0

        def suite_of_label(lb: str) -> str:
            return lb.split("::", 1)[0]

        top_cpu = [lb for lb, _ in sorted(cpu_by_label.items(), key=lambda x: x[1], reverse=True) if suite_of_label(lb) in set(top_cpu_suite)]
        top_ram = [lb for lb, _ in sorted(ram_by_label.items(), key=lambda x: x[1], reverse=True) if suite_of_label(lb) in set(top_ram_suite)]
        top_active = [lb for lb, _ in sorted(active_time_by_label.items(), key=lambda x: x[1], reverse=True) if suite_of_label(lb) in set(top_active_suite)]

        xs_cpu, cpu_tracks = _build_step_series(enriched, "cpu_cores_est", set(top_cpu), label_mode="suite_chunk")
        xs_ram, ram_tracks = _build_step_series(enriched, "ram_gb", set(top_ram), label_mode="suite_chunk")
        xs_active_chunk, active_tracks_chunk = _build_step_series(enriched, "active_one", set(top_active), label_mode="suite_chunk")
        xs_cpu, cpu_tracks = _downsample_step_series(xs_cpu, cpu_tracks, max_points)
        xs_ram, ram_tracks = _downsample_step_series(xs_ram, ram_tracks, max_points)
        xs_active_chunk, active_tracks_chunk = _downsample_step_series(xs_active_chunk, active_tracks_chunk, max_points)
    else:
        xs_cpu = []
        cpu_tracks = {}
        xs_ram = []
        ram_tracks = {}
        xs_active_chunk = []
        active_tracks_chunk = {}
        cpu_by_label = {}
        ram_by_label = {}
        active_time_by_label = {}
        top_cpu = []
        top_ram = []
        top_active = []

    cpu_tracks_suite_no_synthetic = cpu_tracks_suite
    ram_tracks_suite_no_synthetic = ram_tracks_suite
    cpu_tracks_no_synthetic = cpu_tracks if by_chunk else {}
    ram_tracks_no_synthetic = ram_tracks if by_chunk else {}
    if has_synthetic:
        enriched_no_synth = []
        for r in enriched:
            rr = dict(r)
            if rr.get("synthetic_metrics"):
                rr["cpu_sec_report"] = 0.0
                rr["ram_kb_report"] = 0.0
                rr["cpu_cores_est"] = 0.0
                rr["ram_gb"] = 0.0
            enriched_no_synth.append(rr)
        _xs, cpu_tracks_suite_no_synthetic = _build_step_series(
            enriched_no_synth, "cpu_cores_est", set(top_cpu_suite), label_mode="suite"
        )
        _, cpu_tracks_suite_no_synthetic = _downsample_step_series(
            _xs, cpu_tracks_suite_no_synthetic, max_points
        )
        _xs, ram_tracks_suite_no_synthetic = _build_step_series(
            enriched_no_synth, "ram_gb", set(top_ram_suite), label_mode="suite"
        )
        _, ram_tracks_suite_no_synthetic = _downsample_step_series(
            _xs, ram_tracks_suite_no_synthetic, max_points
        )
        if by_chunk:
            _xs, cpu_tracks_no_synthetic = _build_step_series(
                enriched_no_synth, "cpu_cores_est", set(top_cpu), label_mode="suite_chunk"
            )
            _, cpu_tracks_no_synthetic = _downsample_step_series(
                _xs, cpu_tracks_no_synthetic, max_points
            )
            _xs, ram_tracks_no_synthetic = _build_step_series(
                enriched_no_synth, "ram_gb", set(top_ram), label_mode="suite_chunk"
            )
            _, ram_tracks_no_synthetic = _downsample_step_series(
                _xs, ram_tracks_no_synthetic, max_points
            )

    pal = _palette()
    suite_color = {s: pal[i % len(pal)] for i, s in enumerate(suite_set)}

    track_suite = {}
    for lb in set(
        list(cpu_tracks.keys())
        + list(ram_tracks.keys())
        + list(active_tracks_chunk.keys())
        + list(cpu_tracks_suite.keys())
        + list(ram_tracks_suite.keys())
        + list(active_tracks_suite.keys())
    ):
        if lb == "other":
            track_suite[lb] = "other"
        elif "::" not in lb:
            track_suite[lb] = lb
        else:
            track_suite[lb] = lb.split("::", 1)[0]

    synthetic_suites = {str(r["suite_path"]) for r in runs if bool(r.get("synthetic_metrics"))}
    track_has_synthetic = {
        lb: (track_suite.get(lb) in synthetic_suites) for lb in track_suite.keys()
    }

    payload = {
        "suite_filter": suite_filter,
        "stats": stats,
        "by_chunk": by_chunk,
        "has_synthetic_metrics": has_synthetic,
        "cpu_tracks_suite_no_synthetic": cpu_tracks_suite_no_synthetic,
        "ram_tracks_suite_no_synthetic": ram_tracks_suite_no_synthetic,
        "cpu_tracks_no_synthetic": cpu_tracks_no_synthetic,
        "ram_tracks_no_synthetic": ram_tracks_no_synthetic,
        "cpu_suggestions": cpu_suggestions or [],
        "run_config": run_config or {},
        "xs_cpu": xs_cpu,
        "cpu_tracks": cpu_tracks,
        "xs_ram": xs_ram,
        "ram_tracks": ram_tracks,
        "xs_active_chunk": xs_active_chunk,
        "active_tracks_chunk": active_tracks_chunk,
        "xs_cpu_suite": xs_cpu_suite,
        "cpu_tracks_suite": cpu_tracks_suite,
        "xs_ram_suite": xs_ram_suite,
        "ram_tracks_suite": ram_tracks_suite,
        "xs_active_suite": xs_active_suite,
        "active_tracks_suite": active_tracks_suite,
        "xs_active": xs_active,
        "ys_active": ys_active,
        "pie_cpu_labels": top_cpu + (["other"] if len(cpu_by_label) > len(top_cpu) else []),
        "pie_cpu_vals": [cpu_by_label.get(lb, 0.0) for lb in top_cpu] + ([sum(v for k, v in cpu_by_label.items() if k not in set(top_cpu))] if len(cpu_by_label) > len(top_cpu) else []),
        "pie_ram_labels": top_ram + (["other"] if len(ram_by_label) > len(top_ram) else []),
        "pie_ram_vals": [ram_by_label.get(lb, 0.0) for lb in top_ram] + ([sum(v for k, v in ram_by_label.items() if k not in set(top_ram))] if len(ram_by_label) > len(top_ram) else []),
        "pie_active_labels": top_active + (["other"] if len(active_time_by_label) > len(top_active) else []),
        "pie_active_vals": [active_time_by_label.get(lb, 0.0) for lb in top_active] + ([sum(v for k, v in active_time_by_label.items() if k not in set(top_active))] if len(active_time_by_label) > len(top_active) else []),
        "pie_cpu_suite_labels": top_cpu_suite + (["other"] if len(cpu_by_suite) > len(top_cpu_suite) else []),
        "pie_cpu_suite_vals": [cpu_by_suite.get(lb, 0.0) for lb in top_cpu_suite] + ([sum(v for k, v in cpu_by_suite.items() if k not in set(top_cpu_suite))] if len(cpu_by_suite) > len(top_cpu_suite) else []),
        "pie_ram_suite_labels": top_ram_suite + (["other"] if len(ram_by_suite) > len(top_ram_suite) else []),
        "pie_ram_suite_vals": [ram_by_suite.get(lb, 0.0) for lb in top_ram_suite] + ([sum(v for k, v in ram_by_suite.items() if k not in set(top_ram_suite))] if len(ram_by_suite) > len(top_ram_suite) else []),
        "pie_active_suite_labels": top_active_suite + (["other"] if len(active_time_by_suite) > len(top_active_suite) else []),
        "pie_active_suite_vals": [active_time_by_suite.get(lb, 0.0) for lb in top_active_suite] + ([sum(v for k, v in active_time_by_suite.items() if k not in set(top_active_suite))] if len(active_time_by_suite) > len(top_active_suite) else []),
        "suite_color": suite_color,
        "track_suite": track_suite,
        "track_has_synthetic": track_has_synthetic,
    }

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Chunk Resource Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 16px; }}
    .toolbar {{ position: sticky; top: 0; z-index: 20; display: flex; gap: 8px; align-items: center; margin: 8px 0 10px 0; padding: 8px 0; background: rgba(255,255,255,0.96); backdrop-filter: blur(2px); }}
    .toolbar input[type="text"] {{ min-width: 340px; padding: 6px 8px; border: 1px solid #d0d7de; border-radius: 6px; }}
    .toolbar button {{ padding: 6px 10px; border: 1px solid #d0d7de; border-radius: 6px; background: #f6f8fa; cursor: pointer; }}
    .cpu-help {{ display: none; margin-top: 8px; padding: 8px 10px; border-radius: 8px; border: 1px solid #d0d7de; background: #f6f8fa; font-size: 12px; line-height: 1.45; }}
    .cpu-help ol, .cpu-help ul {{ margin: 6px 0 6px 18px; padding: 0; }}
    .metrics-help {{ margin: 8px 0 12px 0; }}
    .metrics-help .box {{ background: #f6f8fa; border: 1px solid #d0d7de; border-radius: 8px; padding: 10px 12px; font-size: 12px; line-height: 1.45; }}
    .metrics-help ul {{ margin: 6px 0 0 18px; padding: 0; }}
    .tabs {{ display: flex; gap: 8px; margin: 8px 0 12px 0; }}
    .tabbtn {{ padding: 6px 10px; border: 1px solid #d0d7de; border-radius: 6px; background: #f6f8fa; cursor: pointer; }}
    .tabbtn.active {{ background: #e7f3ff; border-color: #8cc8ff; }}
    .tab {{ display: none; }}
    .tab.active {{ display: block; }}
    .row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    .row1 {{ display: grid; grid-template-columns: 1fr; gap: 16px; }}
    .chart {{ width: 100%; height: 420px; min-width: 0; }}
    .wide {{ width: 100%; height: 520px; }}
    .hoverbox {{ background: #f6f8fa; border: 1px solid #d0d7de; border-radius: 8px; padding: 8px 10px; margin: 6px 0 16px 0; max-height: 220px; overflow: auto; font-size: 12px; }}
    .clickbox {{ background: #fff; border: 1px solid #d0d7de; border-radius: 8px; padding: 8px 10px; margin: 6px 0 20px 0; max-height: 320px; overflow: auto; font-size: 12px; }}
    .clickbox table {{ width: 100%; border-collapse: collapse; }}
    .clickbox th, .clickbox td {{ border-bottom: 1px solid #eee; padding: 4px 6px; text-align: left; vertical-align: top; }}
    .clickbox th {{ position: sticky; top: 0; background: #fafafa; }}
    .clickbox thead tr:first-child th {{ background: #f2f4f7; }}
    .clickbox thead tr:nth-child(2) th {{ background: #f8fafc; }}
    /* Visual separators between logical column groups in CPU suggestions. */
    #cpuSuggestionsInner td.group-end, #cpuSuggestionsInner th.group-end {{
      position: relative;
      border-right: 1px solid #c9d1db;
    }}
    #cpuSuggestionsInner td.group-end::after, #cpuSuggestionsInner th.group-end::after {{
      content: "";
      position: absolute;
      top: -1px;
      right: -10px;
      width: 10px;
      height: calc(100% + 2px);
      pointer-events: none;
      background: linear-gradient(to right, rgba(144, 158, 176, 0.28), rgba(144, 158, 176, 0.0));
    }}
    pre {{ background: #f6f8fa; padding: 12px; border-radius: 8px; }}
  </style>
</head>
<body>
  <h2>Suite filter: {suite_filter or 'ALL SUITES'}</h2>
  <details style="margin: 8px 0 12px 0;">
    <summary><b>Run stats</b></summary>
    <pre id="stats"></pre>
  </details>
  <details style="margin: 8px 0 12px 0;">
    <summary><b>Config</b></summary>
    <pre id="runConfig"></pre>
  </details>
  <div id="cpuSuggestionsSection" style="display: none; margin: 16px 0;">
    <h3 style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
      <span>CPU suggestions (recommended_cpu for runner)</span>
      <button id="cpuHelpToggle" type="button" title="Show recommendation logic" style="padding:0 8px;min-width:auto;">?</button>
    </h3>
    <div id="cpuHelpText" class="cpu-help">
      <ol>
        <li><b>cores_est</b> for a chunk is calculated as <code>cpu_sec_report / duration_sec</code>.</li>
        <li>Per suite, recommendations use the distribution of chunk <b>cores_est</b> values.</li>
        <li><b>recommended_cpu</b> is p95 rounded to runner tiers: <code>1/2/4/8/16</code>.</li>
        <li>If there is at least one <b>test timeout</b>, <b>recommended_cpu</b> is increased by one tier (then MEDIUM cap is applied).</li>
        <li>When CLI flag <code>--maximize-reqs-for-timeout-tests</code> is enabled, suites with test timeouts use size max: <b>SMALL=1, MEDIUM=4, LARGE=all</b>.</li>
        <li><b>cpu_action</b> compares recommended cpu to <code>ya_cpu</code> from <code>ya.make</code>.</li>
      </ol>
      <ul>
        <li><b>status chunks</b>: counters from report rows where <code>chunk=true</code>.</li>
        <li><b>status tests</b>: counters from report rows where <code>chunk=false</code> (regular tests).</li>
        <li><b>timeouts</b>: <code>error_type == TIMEOUT</code> (fallback: status contains <code>timeout</code>).</li>
        <li><b>muted</b>: <code>muted/is_muted</code> or status <code>MUTE</code>.</li>
        <li><b>fails_total</b>: <code>errors + timeouts</code>.</li>
      </ul>
    </div>
    <p id="syntheticNote" style="display: none; font-size: 12px; color: #b8860b; margin: 4px 0;"></p>
    <div style="display:flex;align-items:center;gap:8px;margin:6px 0 8px 0;">
      <button id="generateCpuScriptBtn" type="button">Generate ya.make CPU update script (.py)</button>
      <span id="generateCpuScriptHint" style="font-size:12px;color:#586069;"></span>
    </div>
    <div id="cpuSuggestionsTable" class="clickbox"></div>
  </div>
  <div class="toolbar">
    <label for="suiteSearch"><b>Search suite:</b></label>
    <input id="suiteSearch" type="text" placeholder="e.g. ydb/core/tx/schemeshard/ut_cdc_stream_reboots" />
    <button onclick="clearSuiteSearch()">Clear</button>
    <span id="syntheticCheckboxWrap" style="margin-left: 16px;" title="">
      <label style="display:flex;align-items:center;gap:6px;">
        <input type="checkbox" id="includeSyntheticCb" checked />
        <span>Include estimated (from ya.make) CPU/RAM</span>
      </label>
    </span>
  </div>
  <details class="metrics-help">
    <summary><b>How CPU/RAM chart metrics are calculated</b></summary>
    <div class="box">
      <ul>
        <li><b>Chunk duration (seconds):</b> <code>evlog_dur_sec = (end_us - start_us) / 1e6</code> from evlog B/E events.</li>
        <li><b>CPU time per chunk (report):</b> <code>cpu_sec_report = ru_utime + ru_stime</code> from report metrics. If value looks like microseconds (&gt;1000), it is divided by <code>1e6</code>.</li>
        <li><b>CPU shown on charts (cores_est):</b> <code>cores_est = cpu_sec_report / evlog_dur_sec</code>. Chart value at time <code>t</code> is the sum of active chunks at <code>t</code>.</li>
        <li><b>RAM per chunk (report):</b> <code>ram_kb_report = ru_maxrss</code>, fallback <code>ru_rss / 1024</code>.</li>
        <li><b>RAM shown on charts:</b> <code>ram_gb = ram_kb_report / (1024 * 1024)</code>. Chart value at time <code>t</code> is the sum of active chunks at <code>t</code>.</li>
        <li><b>Estimated mode (from ya.make):</b> if report metrics are missing and checkbox is enabled, script uses <code>REQUIREMENTS(cpu:X ram:Y)</code>: <code>cpu_sec_report = X * evlog_dur_sec</code>, <code>ram_kb_report = Y * 1024 * 1024</code>.</li>
      </ul>
    </div>
  </details>
  <div id="activeChunks" class="wide"></div>
  <div class="tabs" id="tabsBar" style="display: none;">
    <button id="tabBtnChunk" class="tabbtn active" onclick="showTab('chunkTab')">By Suite+Chunk</button>
    <button id="tabBtnSuite" class="tabbtn" onclick="showTab('suiteTab')">By Suite</button>
  </div>
  <div id="chunkTab" class="tab active" style="display: none;">
    <div id="activeLayerChunk" class="wide"></div>
    <div id="activeHoverChunk" class="hoverbox">Hover active-layer chart to see sorted contributors</div>
    <div id="activeClickChunk" class="clickbox">Click active-layer chart to pin a time and show sorted table</div>
    <div id="cpuLayer" class="wide"></div>
    <div id="cpuHover" class="hoverbox">Hover CPU chart to see sorted contributors</div>
    <div id="cpuClick" class="clickbox">Click CPU chart to pin a time and show sorted table</div>
    <div id="ramLayer" class="wide"></div>
    <div id="ramHover" class="hoverbox">Hover RAM chart to see sorted contributors</div>
    <div id="ramClick" class="clickbox">Click RAM chart to pin a time and show sorted table</div>
    <div class="row">
      <div id="cpuPie" class="chart"></div>
      <div id="ramPie" class="chart"></div>
    </div>
    <div class="row1">
      <div id="activePie" class="chart"></div>
    </div>
  </div>
  <div id="suiteTab" class="tab">
    <div id="activeLayerSuite" class="wide"></div>
    <div id="activeHoverSuite" class="hoverbox">Hover active-layer chart to see sorted contributors</div>
    <div id="activeClickSuite" class="clickbox">Click active-layer chart to pin a time and show sorted table</div>
    <div id="cpuLayerSuite" class="wide"></div>
    <div id="cpuHoverSuite" class="hoverbox">Hover CPU chart to see sorted contributors</div>
    <div id="cpuClickSuite" class="clickbox">Click CPU chart to pin a time and show sorted table</div>
    <div id="ramLayerSuite" class="wide"></div>
    <div id="ramHoverSuite" class="hoverbox">Hover RAM chart to see sorted contributors</div>
    <div id="ramClickSuite" class="clickbox">Click RAM chart to pin a time and show sorted table</div>
    <div class="row">
      <div id="cpuPieSuite" class="chart"></div>
      <div id="ramPieSuite" class="chart"></div>
    </div>
    <div class="row1">
      <div id="activePieSuite" class="chart"></div>
    </div>
  </div>
  <script>
    function showTab(id) {{
      const tabs = ['chunkTab', 'suiteTab'];
      tabs.forEach(t => {{
        document.getElementById(t).classList.toggle('active', t === id);
      }});
      document.getElementById('tabBtnChunk').classList.toggle('active', id === 'chunkTab');
      document.getElementById('tabBtnSuite').classList.toggle('active', id === 'suiteTab');
      // Hidden-tab Plotly charts need explicit resize after becoming visible.
      setTimeout(() => {{
        const ids = id === 'suiteTab'
          ? ['activeLayerSuite', 'cpuLayerSuite', 'ramLayerSuite', 'cpuPieSuite', 'ramPieSuite', 'activePieSuite', 'activeChunks']
          : ['activeLayerChunk', 'cpuLayer', 'ramLayer', 'cpuPie', 'ramPie', 'activePie', 'activeChunks'];
        ids.forEach(cid => {{
          const el = document.getElementById(cid);
          if (el && window.Plotly) {{
            Plotly.Plots.resize(el);
          }}
        }});
      }}, 0);
    }}

    const data = {json.dumps(payload, ensure_ascii=False)};
    document.getElementById('stats').textContent = JSON.stringify(data.stats, null, 2);
    document.getElementById('runConfig').textContent = JSON.stringify(data.run_config || {{}}, null, 2);

    if (data.by_chunk) {{
      document.getElementById('tabsBar').style.display = 'flex';
      document.getElementById('chunkTab').style.display = 'block';
      document.getElementById('suiteTab').style.display = 'none';
    }} else {{
      document.getElementById('suiteTab').classList.add('active');
      document.getElementById('suiteTab').style.display = 'block';
    }}

    if (data.cpu_suggestions && data.cpu_suggestions.length > 0) {{
      document.getElementById('cpuSuggestionsSection').style.display = 'block';
      const nSyn = (data.stats && data.stats.runs_with_synthetic_metrics) ? Number(data.stats.runs_with_synthetic_metrics) : 0;
      const synEl = document.getElementById('syntheticNote');
      if (nSyn > 0 && synEl) {{
        synEl.style.display = 'block';
        synEl.textContent = 'Part of CPU/RAM is estimated from ya.make REQUIREMENTS for ' + nSyn + ' runs without report metrics.';
      }}

      // Default: show suites with the highest non-chunk failures first.
      let suggestionsSortCol = 17;  // test_fails_total
      let suggestionsSortAsc = false;

      function getSuggestionsForScript() {{
        const q = (document.getElementById('suiteSearch')?.value || '').trim().toLowerCase();
        const visible = data.cpu_suggestions.filter(s => !q || String(s.suite_path || '').toLowerCase().includes(q));
        const hasCpuReqValue = (v) => {{
          if (typeof v === 'number') return v > 0;
          const t = String(v ?? '').trim().toLowerCase();
          return t !== '' && t !== '0';
        }};
        return visible
          .filter(s => ['set', 'raise', 'lower'].includes(String(s.cpu_action || 'ok')) && hasCpuReqValue(s.recommended_cpu))
          .map(s => ({{
            suite_path: String(s.suite_path || ''),
            recommended_cpu: s.recommended_cpu,
            current_ya_cpu: s.ya_cpu_cores == null ? null : Number(s.ya_cpu_cores),
            action: String(s.cpu_action || 'ok'),
          }}));
      }}

      function buildCpuUpdateScript(items) {{
        const payloadJson = JSON.stringify(items, null, 2);
        const nowIso = new Date().toISOString();
        return [
          '#!/usr/bin/env python3',
          '# Apply cpu REQUIREMENTS updates in ya.make files.',
          '# Generated by tests_resource_dashboard HTML.',
          '# Usage: python3 apply_cpu_requirements.py --repo-root /path/to/ydb [--dry-run] [--mode all|only-up|only-down|only-new]',
          '',
          'from __future__ import annotations',
          '',
          'import argparse',
          'import json',
          'import re',
          'from pathlib import Path',
          '',
          'GENERATED_AT = ' + JSON.stringify(nowIso),
          'UPDATES = json.loads(' + JSON.stringify(payloadJson) + ')',
          '',
          "PART_SUFFIX_RE = re.compile(r'/part\\\\d+$')",
          "RE_REQ_LINE = re.compile(r'^(\\\\s*REQUIREMENTS\\\\s*\\\\()(.*?)(\\\\)\\\\s*)$')",
          "RE_CPU = re.compile(r'\\\\bcpu\\\\s*:\\\\s*([^\\\\s)]+(?:\\\\([^)]*\\\\))?)', re.IGNORECASE)",
          "RE_IF = re.compile(r'^\\\\s*IF\\\\s*\\\\((.*)\\\\)\\\\s*$')",
          "RE_ELSE = re.compile(r'^\\\\s*ELSE\\\\s*\\\\(\\\\s*\\\\)\\\\s*$')",
          "RE_ENDIF = re.compile(r'^\\\\s*ENDIF\\\\s*\\\\(\\\\s*\\\\)\\\\s*$')",
          "ACTION_BY_MODE = dict()",
          "ACTION_BY_MODE['all'] = set(['raise', 'lower', 'set'])",
          "ACTION_BY_MODE['only-up'] = set(['raise'])",
          "ACTION_BY_MODE['only-down'] = set(['lower'])",
          "ACTION_BY_MODE['only-new'] = set(['set'])",
          '',
          'def normalize_suite_path(path: str) -> str:',
          "    return PART_SUFFIX_RE.sub('', path or '')",
          '',
          'def normalize_cpu_req(value: object) -> str:',
          "    s = str(value).strip()",
          "    if s.lower() == 'all':",
          "        return 'all'",
          '    try:',
          '        return str(int(float(s)))',
          '    except Exception:',
          "        return s or '1'",
          '',
          'def update_requirements_line(line: str, cpu: str) -> str:',
          '    m = RE_REQ_LINE.match(line)',
          '    if not m:',
          '        return line',
          '    prefix, body, suffix = m.group(1), m.group(2), m.group(3)',
          '    if RE_CPU.search(body):',
          "        body = RE_CPU.sub('cpu:' + str(cpu), body, count=1)",
          '    else:',
          "        body = (body.strip() + ' ' if body.strip() else '') + 'cpu:' + str(cpu)",
          '    return prefix + body + suffix',
          '',
          'def _eval_condition_default(cond: str) -> bool:',
          "    expr = (cond or '').strip()",
          '    has_san = False',
          "    expr = re.sub(r'\\\\bOS_WINDOWS\\\\b', 'False', expr)",
          "    expr = re.sub(r'\\\\bOS_LINUX\\\\b', 'True', expr)",
          "    expr = re.sub(r'\\\\bOS_DARWIN\\\\b', 'False', expr)",
          "    expr = re.sub(r'\\\\bSANITIZER_TYPE\\\\b', 'True' if has_san else 'False', expr)",
          "    expr = re.sub(r'\\\\bWITH_VALGRIND\\\\b', 'False', expr)",
          "    expr = re.sub(r'\\\\bOR\\\\b', 'or', expr)",
          "    expr = re.sub(r'\\\\bAND\\\\b', 'and', expr)",
          "    expr = re.sub(r'\\\\bNOT\\\\b', 'not', expr)",
          '    try:',
          "        return bool(eval(expr, dict(__builtins__=None), dict()))",
          '    except Exception:',
          "        return False",
          '',
          'def _find_default_requirements_line(lines: list[str]) -> int | None:',
          '    stack: list[tuple[bool, bool, bool, bool]] = []',
          '    current_active = True',
          '    for i, raw in enumerate(lines):',
          "        line = raw.strip()",
          '        m_if = RE_IF.match(line)',
          '        if m_if:',
          '            parent_active = current_active',
          '            if_res = _eval_condition_default(m_if.group(1))',
          '            current_active = parent_active and if_res',
          '            stack.append((parent_active, if_res, current_active, False))',
          '            continue',
          '        if RE_ELSE.match(line):',
          '            if not stack:',
          '                continue',
          '            parent_active, if_res, prev_cur, seen_else = stack.pop()',
          '            if seen_else:',
          '                stack.append((parent_active, if_res, prev_cur, seen_else))',
          '                continue',
          '            current_active = parent_active and (not if_res)',
          '            stack.append((parent_active, if_res, current_active, True))',
          '            continue',
          '        if RE_ENDIF.match(line):',
          '            if stack:',
          '                stack.pop()',
          '            current_active = stack[-1][2] if stack else True',
          '            continue',
          '        if current_active and RE_REQ_LINE.match(raw):',
          '            return i',
          '    return None',
          '',
          'def _find_default_insert_index(lines: list[str]) -> int:',
          '    stack: list[tuple[bool, bool, bool, bool]] = []',
          '    current_active = True',
          '    last_active_size_idx: int | None = None',
          '    end_idx: int | None = None',
          '    for i, raw in enumerate(lines):',
          "        line = raw.strip()",
          "        if line == 'END()' and end_idx is None:",
          '            end_idx = i',
          '        m_if = RE_IF.match(line)',
          '        if m_if:',
          '            parent_active = current_active',
          '            if_res = _eval_condition_default(m_if.group(1))',
          '            current_active = parent_active and if_res',
          '            stack.append((parent_active, if_res, current_active, False))',
          '            continue',
          '        if RE_ELSE.match(line):',
          '            if not stack:',
          '                continue',
          '            parent_active, if_res, prev_cur, seen_else = stack.pop()',
          '            if seen_else:',
          '                stack.append((parent_active, if_res, prev_cur, seen_else))',
          '                continue',
          '            current_active = parent_active and (not if_res)',
          '            stack.append((parent_active, if_res, current_active, True))',
          '            continue',
          '        if RE_ENDIF.match(line):',
          '            if stack:',
          '                stack.pop()',
          '            current_active = stack[-1][2] if stack else True',
          '            continue',
          "        if current_active and line.startswith('SIZE('):",
          '            last_active_size_idx = i',
          '    if last_active_size_idx is not None:',
          '        return last_active_size_idx + 1',
          '    if end_idx is not None:',
          '        return end_idx',
          '    return len(lines)',
          '',
          'def _line_indent(raw: str) -> str:',
          '    return raw[:len(raw) - len(raw.lstrip())]',
          '',
          'def _choose_insert_indent(lines: list[str], insert_idx: int) -> str:',
          '    candidates: list[str] = []',
          '    if 0 <= insert_idx - 1 < len(lines):',
          '        candidates.append(lines[insert_idx - 1])',
          '    if 0 <= insert_idx < len(lines):',
          '        candidates.append(lines[insert_idx])',
          '    for raw in candidates:',
          '        stripped = raw.strip()',
          '        if not stripped:',
          '            continue',
          "        if stripped in {'ELSE()', 'ENDIF()', 'END()'}:",
          '            continue',
          '        if RE_IF.match(stripped) or RE_ELSE.match(stripped) or RE_ENDIF.match(stripped):',
          '            continue',
          '        return _line_indent(raw)',
          '    for raw in candidates:',
          '        if raw.strip():',
          '            return _line_indent(raw)',
          "    return ''",
          '',
          'def _has_module_block(lines: list[str]) -> bool:',
          "    return any(raw.strip() == 'END()' for raw in lines)",
          '',
          'def apply_one(repo_root: Path, suite_path: str, cpu: str, dry_run: bool) -> tuple[str, str]:',
          '    suite = normalize_suite_path(suite_path)',
          "    ya_make = repo_root / suite / 'ya.make'",
          '    if not ya_make.exists():',
          "        return suite, 'missing ya.make'",
          '',
          "    text = ya_make.read_text(encoding='utf-8', errors='replace')",
          '    lines = text.splitlines()',
          '',
          '    changed = False',
          '    req_idx = _find_default_requirements_line(lines)',
          '',
          '    if req_idx is not None:',
          '        new_line = update_requirements_line(lines[req_idx], cpu)',
          '        if new_line != lines[req_idx]:',
          '            lines[req_idx] = new_line',
          '            changed = True',
          '    else:',
          '        if not _has_module_block(lines):',
          "            return suite, 'skip (no module block)'",
          '        insert_idx = _find_default_insert_index(lines)',
          '        if insert_idx < 0 or insert_idx > len(lines):',
          '            insert_idx = len(lines)',
          '        indent = _choose_insert_indent(lines, insert_idx)',
          "        insert_line = indent + 'REQUIREMENTS(cpu:' + cpu + ')'",
          '        lines.insert(insert_idx, insert_line)',
          '        changed = True',
          '',
          '    if not changed:',
          "        return suite, 'no change'",
          '',
          '    if not dry_run:',
          "        ya_make.write_text('\\\\n'.join(lines) + '\\\\n', encoding='utf-8')",
          "    return suite, 'updated' if not dry_run else 'would update'",
          '',
          'def _action_allowed(action: str, mode: str) -> bool:',
          "    allowed = ACTION_BY_MODE.get(mode, ACTION_BY_MODE['all'])",
          "    return (action or '').lower() in allowed",
          '',
          'def main() -> None:',
          "    p = argparse.ArgumentParser(description='Apply generated cpu REQUIREMENTS updates to ya.make files')",
          "    p.add_argument('--repo-root', type=Path, required=True, help='Repository root path')",
          "    p.add_argument('--dry-run', action='store_true', help='Print planned changes without writing files')",
          "    p.add_argument('--mode', choices=['all', 'only-up', 'only-down', 'only-new'], default='all', help='Apply only raise/lower/set actions')",
          '    args = p.parse_args()',
          '',
          '    repo_root = args.repo_root.resolve()',
          '    if not repo_root.exists() or not repo_root.is_dir():',
          "        raise SystemExit('Invalid --repo-root: ' + str(repo_root))",
          '',
          '    seen = set()',
          '    updates = []',
          '    for row in UPDATES:',
          "        suite = normalize_suite_path(str(row.get('suite_path', '')))",
          "        cpu = normalize_cpu_req(row.get('recommended_cpu', 1))",
          "        action = str(row.get('action', '') or '').lower()",
          '        if not _action_allowed(action, args.mode):',
          '            continue',
          '        key = (suite, cpu)',
          '        if not suite or key in seen:',
          '            continue',
          '        seen.add(key)',
          '        updates.append((suite, cpu, action))',
          '',
          "    print('Generated at: ' + str(GENERATED_AT))",
          "    print('Mode: ' + str(args.mode))",
          "    print('Total suites to update: ' + str(len(updates)))",
          '    changed = 0',
          '    for suite, cpu, action in updates:',
          '        suite_out, status = apply_one(repo_root, suite, cpu, args.dry_run)',
          "        print('- ' + str(suite_out) + ': action=' + str(action) + ' cpu:' + str(cpu) + ' -> ' + str(status))",
          "        if status == 'updated' or status == 'would update':",
          '            changed += 1',
          "    print('Completed. Changed entries: ' + str(changed))",
          '',
          "if __name__ == '__main__':",
          '    main()',
          '',
        ].join('\\n');
      }}

      function downloadCpuUpdateScript() {{
        const items = getSuggestionsForScript();
        const hintEl = document.getElementById('generateCpuScriptHint');
        if (!items.length) {{
          if (hintEl) hintEl.textContent = 'No actionable rows in current filter.';
          return;
        }}
        const content = buildCpuUpdateScript(items);
        const blob = new Blob([content], {{ type: 'text/x-python' }});
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        const ts = new Date().toISOString().replace(/[:.]/g, '-');
        a.href = url;
        a.download = 'apply_cpu_requirements_' + ts + '.py';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        if (hintEl) hintEl.textContent = 'Script downloaded for ' + items.length + ' suite(s).';
      }}

      function refreshCpuScriptHint() {{
        const items = getSuggestionsForScript();
        const hintEl = document.getElementById('generateCpuScriptHint');
        if (!hintEl) return;
        hintEl.textContent = items.length
          ? ('Will include ' + items.length + ' actionable suite(s) from current filter. Script supports --mode all|only-up|only-down|only-new.')
          : 'No actionable rows in current filter.';
      }}

      function sortableValue(raw) {{
        const s = String(raw ?? '').replace(/<[^>]*>/g, '').replace(/[\\s,]+/g, ' ').trim();
        const n = Number(s.replace(/[^\\d.+-]/g, ''));
        return Number.isFinite(n) && s.match(/[-+]?\\d/) ? n : s.toLowerCase();
      }}

      const suggestionsColumns = [
        'idx', 'suite_path', 'ya_ram_gb', 'ya_cpu_cores', 'ya_size',
        'chunks_count', 'median_cores', 'p95_cores', 'total_cpu_sec',
        'total_ram_gb', 'total_dur_sec', 'recommended_cpu', 'cpu_action',
        'test_errors', 'test_timeouts', 'test_muted', 'test_muted_timeouts', 'test_fails_total',
        'errors', 'timeouts', 'muted', 'muted_timeouts', 'fails_total'
      ];

      function actionCell(action) {{
        if (action === 'raise') return '<span style="color:#856404;background:#fff3cd;padding:2px 6px;border-radius:10px;">raise</span>';
        if (action === 'lower') return '<span style="color:#0c5460;background:#d1ecf1;padding:2px 6px;border-radius:10px;">lower</span>';
        if (action === 'set') return '<span style="color:#721c24;background:#f8d7da;padding:2px 6px;border-radius:10px;">set</span>';
        return '<span style="color:#155724;background:#d4edda;padding:2px 6px;border-radius:10px;">ok</span>';
      }}

      function renderSuggestionsTable() {{
        const q = (document.getElementById('suiteSearch')?.value || '').trim().toLowerCase();
        const filtered = data.cpu_suggestions.filter(s => !q || String(s.suite_path || '').toLowerCase().includes(q));
        const rowsData = filtered.map((s, i) => {{
          const cpuExplain = String(s.recommended_cpu_explain || '').replace(/"/g, '&quot;');
          const cpuReqText = String(s.recommended_cpu ?? 1);
          const cpuBadge = cpuReqText === 'all'
            ? '<span title="' + cpuExplain + '" style="display:inline-block;padding:2px 8px;border-radius:10px;background:#ffe8cc;color:#7c2d12;border:1px solid #fdba74;font-weight:700;">cpu:all</span>'
            : '<span title="' + cpuExplain + '"><b>cpu:' + cpuReqText + '</b></span>';
          const cpuCell = cpuBadge + (s.has_synthetic ? ' <span style="color:#b8860b;">(estimated from ya.make)</span>' : '');
          return [
            String(i + 1),
            '<span style="max-width:400px;display:inline-block;overflow:hidden;text-overflow:ellipsis;vertical-align:bottom;" title="' + (s.suite_path || '').replace(/"/g, '&quot;') + '">' + (s.suite_path || '') + '</span>',
            String(s.ya_ram_gb ?? ''),
            String(s.ya_cpu_cores ?? ''),
            String(s.ya_size ?? ''),
            String(s.chunks_count || 0),
            (s.median_cores != null ? Number(s.median_cores).toFixed(3) : ''),
            (s.p95_cores != null ? Number(s.p95_cores).toFixed(3) : ''),
            (s.total_cpu_sec != null ? Number(s.total_cpu_sec).toFixed(1) : ''),
            (s.total_ram_gb != null ? Number(s.total_ram_gb).toFixed(3) : ''),
            (s.total_dur_sec != null ? Number(s.total_dur_sec).toFixed(1) : '') + ' s',
            cpuCell,
            actionCell(s.cpu_action || 'ok'),
            String(s.test_errors || 0),
            String(s.test_timeouts || 0),
            String(s.test_muted || 0),
            String(s.test_muted_timeouts || 0),
            String(s.test_fails_total || 0),
            String(s.chunk_errors || 0),
            String(s.chunk_timeouts || 0),
            String(s.chunk_muted || 0),
            String(s.chunk_muted_timeouts || 0),
            String(s.chunk_fails_total || 0),
          ];
        }});

        rowsData.sort((a, b) => {{
          const av = sortableValue(a[suggestionsSortCol]);
          const bv = sortableValue(b[suggestionsSortCol]);
          if (av < bv) return suggestionsSortAsc ? -1 : 1;
          if (av > bv) return suggestionsSortAsc ? 1 : -1;
          return 0;
        }});

        const marker = (idx) => (idx === suggestionsSortCol ? (suggestionsSortAsc ? ' ▲' : ' ▼') : '');
        const topHeader =
          '<tr>' +
          '<th data-col="0" rowspan="2" style="cursor:pointer;user-select:none;">#' + marker(0) + '</th>' +
          '<th data-col="1" rowspan="2" style="cursor:pointer;user-select:none;">suite_path' + marker(1) + '</th>' +
          '<th colspan="3">ya.make</th>' +
          '<th colspan="1">runtime</th>' +
          '<th colspan="3">cpu usage</th>' +
          '<th colspan="1">ram usage</th>' +
          '<th colspan="1">runtime</th>' +
          '<th colspan="2">decision</th>' +
          '<th colspan="5">status tests</th>' +
          '<th colspan="5">status chunks</th>' +
          '</tr>';
        const subHeader =
          '<tr>' +
          '<th data-col="2" style="cursor:pointer;user-select:none;">ya_ram_gb' + marker(2) + '</th>' +
          '<th data-col="3" style="cursor:pointer;user-select:none;">ya_cpu' + marker(3) + '</th>' +
          '<th data-col="4" class="group-end" style="cursor:pointer;user-select:none;">ya_size' + marker(4) + '</th>' +
          '<th data-col="5" class="group-end" style="cursor:pointer;user-select:none;">chunks' + marker(5) + '</th>' +
          '<th data-col="6" style="cursor:pointer;user-select:none;">median_cores' + marker(6) + '</th>' +
          '<th data-col="7" style="cursor:pointer;user-select:none;">p95_cores' + marker(7) + '</th>' +
          '<th data-col="8" class="group-end" style="cursor:pointer;user-select:none;">total_cpu_sec' + marker(8) + '</th>' +
          '<th data-col="9" class="group-end" style="cursor:pointer;user-select:none;">total_ram_gb' + marker(9) + '</th>' +
          '<th data-col="10" class="group-end" style="cursor:pointer;user-select:none;">total_dur_sec' + marker(10) + '</th>' +
          '<th data-col="11" style="cursor:pointer;user-select:none;">recommended_cpu' + marker(11) + '</th>' +
          '<th data-col="12" class="group-end" style="cursor:pointer;user-select:none;">cpu_action' + marker(12) + '</th>' +
          '<th data-col="13" style="cursor:pointer;user-select:none;">errors' + marker(13) + '</th>' +
          '<th data-col="14" style="cursor:pointer;user-select:none;">timeouts' + marker(14) + '</th>' +
          '<th data-col="15" style="cursor:pointer;user-select:none;">muted' + marker(15) + '</th>' +
          '<th data-col="16" style="cursor:pointer;user-select:none;">muted_timeouts' + marker(16) + '</th>' +
          '<th data-col="17" class="group-end" style="cursor:pointer;user-select:none;">fails_total' + marker(17) + '</th>' +
          '<th data-col="18" style="cursor:pointer;user-select:none;">errors' + marker(18) + '</th>' +
          '<th data-col="19" style="cursor:pointer;user-select:none;">timeouts' + marker(19) + '</th>' +
          '<th data-col="20" style="cursor:pointer;user-select:none;">muted' + marker(20) + '</th>' +
          '<th data-col="21" style="cursor:pointer;user-select:none;">muted_timeouts' + marker(21) + '</th>' +
          '<th data-col="22" class="group-end" style="cursor:pointer;user-select:none;">fails_total' + marker(22) + '</th>' +
          '</tr>';

        const groupEnds = new Set([4, 5, 8, 9, 10, 12, 17, 22]);
        const bodyHtml = rowsData.map(cols => (
          '<tr>' + cols.map((c, i) => '<td' + (groupEnds.has(i) ? ' class="group-end"' : '') + '>' + c + '</td>').join('') + '</tr>'
        )).join('');
        document.getElementById('cpuSuggestionsTable').innerHTML =
          '<table id=\"cpuSuggestionsInner\" style=\"width:100%;border-collapse:collapse;\"><thead>' + topHeader + subHeader + '</thead><tbody>' + bodyHtml + '</tbody></table>';

        const ths = document.querySelectorAll('#cpuSuggestionsInner thead th');
        ths.forEach(th => th.addEventListener('click', () => {{
          const col = Number(th.getAttribute('data-col'));
          if (suggestionsSortCol === col) suggestionsSortAsc = !suggestionsSortAsc;
          else {{
            suggestionsSortCol = col;
            suggestionsSortAsc = true;
          }}
          renderSuggestionsTable();
        }}));
        refreshCpuScriptHint();
      }}

      const helpBtn = document.getElementById('cpuHelpToggle');
      const helpBox = document.getElementById('cpuHelpText');
      if (helpBtn && helpBox) {{
        helpBtn.addEventListener('click', () => {{
          helpBox.style.display = helpBox.style.display === 'block' ? 'none' : 'block';
        }});
      }}
      const genBtn = document.getElementById('generateCpuScriptBtn');
      if (genBtn) {{
        genBtn.addEventListener('click', downloadCpuUpdateScript);
      }}
      window.renderSuggestionsTable = renderSuggestionsTable;
      renderSuggestionsTable();
    }}

    function colorForTrack(trackName) {{
      const s = data.track_suite[trackName];
      if (!s || s === 'other') return '#bdbdbd';
      return data.suite_color[s] || '#999';
    }}

    function suiteFromLabel(label) {{
      if (!label || label === 'other') return '';
      if (label.includes('::')) return label.split('::', 1)[0];
      return label;
    }}

    function hexToRgba(hex, alpha) {{
      if (!hex || !hex.startsWith('#') || (hex.length !== 7 && hex.length !== 4)) {{
        return `rgba(180,180,180,${{alpha}})`;
      }}
      let r, g, b;
      if (hex.length === 4) {{
        r = parseInt(hex[1] + hex[1], 16);
        g = parseInt(hex[2] + hex[2], 16);
        b = parseInt(hex[3] + hex[3], 16);
      }} else {{
        r = parseInt(hex.slice(1, 3), 16);
        g = parseInt(hex.slice(3, 5), 16);
        b = parseInt(hex.slice(5, 7), 16);
      }}
      return `rgba(${{r}},${{g}},${{b}},${{alpha}})`;
    }}

    function isMatch(label, q) {{
      if (!q) return true;
      const s = suiteFromLabel(label).toLowerCase();
      const l = String(label || '').toLowerCase();
      return s.includes(q) || l.includes(q);
    }}

    function updateStackedPlotColors(plotId, q) {{
      const el = document.getElementById(plotId);
      if (!el || !el.data || !window.Plotly) return;
      const lineColors = [];
      const lineWidths = [];
      const fillColors = [];
      const opacities = [];
      const idx = [];
      el.data.forEach((tr, i) => {{
        const name = tr.name || '';
        const base = colorForTrack(name);
        const matched = name === 'other' ? true : isMatch(name, q);
        lineColors.push(matched ? base : '#cfcfcf');
        lineWidths.push(matched ? 1 : 0);
        fillColors.push(matched ? base : '#cfcfcf');
        opacities.push(matched ? (name === 'other' ? 0.35 : 0.75) : 0.12);
        idx.push(i);
      }});
      Plotly.restyle(el, {{
        'line.color': lineColors,
        'line.width': lineWidths,
        'fillcolor': fillColors,
        'opacity': opacities,
      }}, idx);
    }}

    function updatePieColors(plotId, q) {{
      const el = document.getElementById(plotId);
      if (!el || !el.data || !el.data[0] || !window.Plotly) return;
      const labels = el.data[0].labels || [];
      const colors = labels.map(lb => {{
        const base = lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999');
        const matched = isMatch(lb, q);
        return hexToRgba(base, matched ? 0.92 : 0.18);
      }});
      Plotly.restyle(el, {{'marker.colors': [colors]}}, [0]);
    }}

    function collapseTracksByQuery(tracks, q) {{
      if (!tracks) return tracks;
      if (!q) return tracks;
      const out = {{}};
      let refLen = 0;
      for (const vals of Object.values(tracks)) {{
        refLen = Math.max(refLen, Array.isArray(vals) ? vals.length : 0);
      }}
      const other = new Array(refLen).fill(0);
      for (const [name, valsRaw] of Object.entries(tracks)) {{
        const vals = Array.isArray(valsRaw) ? valsRaw : [];
        const matched = name !== 'other' && isMatch(name, q);
        if (matched) {{
          out[name] = vals;
          continue;
        }}
        for (let i = 0; i < vals.length; i += 1) {{
          other[i] += Number(vals[i] || 0);
        }}
      }}
      out.other = other;
      return out;
    }}

    function applySuiteSearch() {{
      const q = (document.getElementById('suiteSearch')?.value || '').trim().toLowerCase();
      const inc = document.getElementById('includeSyntheticCb')?.checked ?? true;
      const cpuSuiteBase = inc ? data.cpu_tracks_suite : data.cpu_tracks_suite_no_synthetic;
      const ramSuiteBase = inc ? data.ram_tracks_suite : data.ram_tracks_suite_no_synthetic;
      const cpuChunkBase = inc ? data.cpu_tracks : data.cpu_tracks_no_synthetic;
      const ramChunkBase = inc ? data.ram_tracks : data.ram_tracks_no_synthetic;

      if (data.by_chunk && data.xs_cpu && data.xs_cpu.length > 0) {{
        updateStackedPlotTracks('activeLayerChunk', collapseTracksByQuery(data.active_tracks_chunk, q));
        updateStackedPlotTracks('cpuLayer', collapseTracksByQuery(cpuChunkBase, q));
        updateStackedPlotTracks('ramLayer', collapseTracksByQuery(ramChunkBase, q));
      }}
      updateStackedPlotTracks('activeLayerSuite', collapseTracksByQuery(data.active_tracks_suite, q));
      updateStackedPlotTracks('cpuLayerSuite', collapseTracksByQuery(cpuSuiteBase, q));
      updateStackedPlotTracks('ramLayerSuite', collapseTracksByQuery(ramSuiteBase, q));

      ['activeLayerChunk', 'cpuLayer', 'ramLayer', 'activeLayerSuite', 'cpuLayerSuite', 'ramLayerSuite']
        .forEach(id => updateStackedPlotColors(id, q));
      ['cpuPie', 'ramPie', 'activePie', 'cpuPieSuite', 'ramPieSuite', 'activePieSuite']
        .forEach(id => updatePieColors(id, q));
      if (window.renderSuggestionsTable) {{
        window.renderSuggestionsTable();
      }}
    }}

    function clearSuiteSearch() {{
      const el = document.getElementById('suiteSearch');
      if (el) el.value = '';
      applySuiteSearch();
    }}

    function updateStackedPlotTracks(divId, tracks) {{
      const el = document.getElementById(divId);
      if (!el || !el.data || !window.Plotly) return;
      const names = el.data.map(t => t.name);
      const yArrays = names.map(n => (tracks[n] != null ? tracks[n] : []));
      Plotly.restyle(el, {{y: yArrays}}, names.map((_, i) => i));
    }}

    function applySyntheticToCharts() {{
      const inc = document.getElementById('includeSyntheticCb')?.checked ?? true;
      updateStackedPlotTracks('cpuLayerSuite', inc ? data.cpu_tracks_suite : data.cpu_tracks_suite_no_synthetic);
      updateStackedPlotTracks('ramLayerSuite', inc ? data.ram_tracks_suite : data.ram_tracks_suite_no_synthetic);
      if (data.by_chunk && data.xs_cpu && data.xs_cpu.length > 0) {{
        updateStackedPlotTracks('cpuLayer', inc ? data.cpu_tracks : data.cpu_tracks_no_synthetic);
        updateStackedPlotTracks('ramLayer', inc ? data.ram_tracks : data.ram_tracks_no_synthetic);
      }}
      applySuiteSearch();
    }}

    const cb = document.getElementById('includeSyntheticCb');
    const cbWrap = document.getElementById('syntheticCheckboxWrap');
    if (cb) {{
      if (!data.has_synthetic_metrics) {{
        cb.checked = false;
        cb.disabled = true;
        if (cbWrap) {{
          cbWrap.title = 'No runs with estimated metrics from ya.make in this report.';
        }}
      }}
      cb.addEventListener('change', applySyntheticToCharts);
    }}

    function humanFromKb(kb) {{
      const mb = kb / 1024.0;
      if (mb < 1024) return mb.toFixed(2) + ' MB';
      const gb = mb / 1024.0;
      return gb.toFixed(2) + ' GB';
    }}

    function humanFromSec(sec) {{
      if (sec < 60) return sec.toFixed(2) + ' s';
      if (sec < 3600) return (sec / 60).toFixed(2) + ' min';
      return (sec / 3600).toFixed(2) + ' h';
    }}

    function stackedArea(divId, xs, tracks, title, yTitle) {{
      const names = Object.keys(tracks);
      const traces = names.map((n) => {{
        const c = colorForTrack(n);
        return {{
          x: xs,
          y: tracks[n],
          mode: 'lines',
          line: {{width: 1, color: c}},
          fillcolor: c,
          opacity: n === 'other' ? 0.45 : 0.75,
          name: n,
          stackgroup: 'one',
          hoverinfo: 'none',
        }};
      }});
      Plotly.newPlot(divId, traces, {{
        title,
        xaxis: {{title: 'time (sec)'}},
        yaxis: {{title: yTitle}},
        hovermode: 'x unified',
        showlegend: false,
        margin: {{l: 60, r: 20, t: 50, b: 50}},
      }}, {{responsive: true}});
    }}

    function formatValue(y, unit) {{
      if (unit === 'active') return String(Math.round(y));
      if (unit === 'GB') return y.toFixed(3);
      return y.toFixed(3);
    }}

    function attachSortedHover(plotId, panelId, unit) {{
      const plot = document.getElementById(plotId);
      const panel = document.getElementById(panelId);
      if (!plot || !panel) return;
      plot.on('plotly_hover', (ev) => {{
        if (!ev || !ev.points || !ev.points.length) return;
        const t = ev.points[0].x;
        const rows = ev.points
          .map(p => ({{name: p.data.name, y: Number(p.y || 0)}}))
          .filter(p => p.y > 0)
          .sort((a, b) => b.y - a.y);
        const top = rows.slice(0, 40);
        const lines = top.map(r => `${{r.name}}: ${{formatValue(r.y, unit)}} ${{unit}}`);
        panel.textContent = `t=${{Number(t).toFixed(2)}}s\\n` + lines.join('\\n');
      }});
      plot.on('plotly_unhover', () => {{
        panel.textContent = 'Move cursor over chart to see sorted contributors';
      }});
    }}

    function renderClickTable(panelId, rows, t, unit) {{
      const panel = document.getElementById(panelId);
      if (!panel) return;
      const top = rows.slice(0, 100);
      const includeSynthetic = document.getElementById('includeSyntheticCb')?.checked ?? true;
      const htmlRows = top.map((r, i) => {{
        const isSynthetic = includeSynthetic && data.has_synthetic_metrics && data.track_has_synthetic && data.track_has_synthetic[r.name];
        const syntheticBadge = isSynthetic ? ' <span style="color:#b8860b;">(estimated from ya.make)</span>' : '';
        return (
        '<tr>' +
          '<td>' + (i + 1) + '</td>' +
          '<td><span style="display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:6px;vertical-align:middle;background:' + colorForTrack(r.name) + ';"></span>' + r.name + syntheticBadge + '</td>' +
          '<td>' + formatValue(r.y, unit) + ' ' + unit + '</td>' +
        '</tr>'
        );
      }}).join('');
      panel.innerHTML =
        '<div><b>t=' + Number(t).toFixed(2) + 's</b> | rows: ' + top.length + '</div>' +
        '<table><thead><tr><th>#</th><th>suite+chunk</th><th>value</th></tr></thead><tbody>' + htmlRows + '</tbody></table>';
    }}

    function attachSortedClick(plotId, panelId, unit) {{
      const plot = document.getElementById(plotId);
      if (!plot) return;
      plot.on('plotly_click', (ev) => {{
        if (!ev || !ev.points || !ev.points.length) return;
        const t = ev.points[0].x;
        const rows = ev.points
          .map(p => ({{name: p.data.name, y: Number(p.y || 0)}}))
          .filter(p => p.y > 0)
          .sort((a, b) => b.y - a.y);
        renderClickTable(panelId, rows, t, unit);
      }});
    }}

    Plotly.newPlot('activeChunks', [{{
      x: data.xs_active,
      y: data.ys_active,
      mode: 'lines',
      name: 'active_chunks',
      line: {{color: '#444', width: 2}},
      hoverinfo: 'none',
    }}], {{
      title: 'Concurrent running chunks',
      xaxis: {{title: 'time (sec)'}},
      yaxis: {{title: 'count'}},
      hovermode: 'x unified',
    }}, {{responsive: true}});

    if (data.by_chunk && data.xs_active_chunk && data.xs_active_chunk.length > 0) {{
      stackedArea('activeLayerChunk', data.xs_active_chunk, data.active_tracks_chunk, 'Layered active chunks by suite+chunk', 'active chunks');
      attachSortedHover('activeLayerChunk', 'activeHoverChunk', 'active');
      attachSortedClick('activeLayerChunk', 'activeClickChunk', 'active');
      stackedArea('cpuLayer', data.xs_cpu, data.cpu_tracks, 'Layered CPU (estimated cores) by suite+chunk', 'cores');
      stackedArea('ramLayer', data.xs_ram, data.ram_tracks, 'Layered RAM by suite+chunk', 'GB');
      attachSortedHover('cpuLayer', 'cpuHover', 'cores');
      attachSortedHover('ramLayer', 'ramHover', 'GB');
      attachSortedClick('cpuLayer', 'cpuClick', 'cores');
      attachSortedClick('ramLayer', 'ramClick', 'GB');
    }}

    stackedArea('activeLayerSuite', data.xs_active_suite, data.active_tracks_suite, 'Layered active chunks by suite', 'active chunks');
    attachSortedHover('activeLayerSuite', 'activeHoverSuite', 'active');
    attachSortedClick('activeLayerSuite', 'activeClickSuite', 'active');

    stackedArea('cpuLayerSuite', data.xs_cpu_suite, data.cpu_tracks_suite, 'Layered CPU (estimated cores) by suite', 'cores');
    stackedArea('ramLayerSuite', data.xs_ram_suite, data.ram_tracks_suite, 'Layered RAM by suite', 'GB');
    attachSortedHover('cpuLayerSuite', 'cpuHoverSuite', 'cores');
    attachSortedHover('ramLayerSuite', 'ramHoverSuite', 'GB');
    attachSortedClick('cpuLayerSuite', 'cpuClickSuite', 'cores');
    attachSortedClick('ramLayerSuite', 'ramClickSuite', 'GB');

    if (data.by_chunk && data.pie_cpu_labels && data.pie_cpu_labels.length > 0) {{
      Plotly.newPlot('cpuPie', [{{
        type: 'pie',
        labels: data.pie_cpu_labels,
        values: data.pie_cpu_vals,
        customdata: data.pie_cpu_vals.map(v => humanFromSec(v)),
        marker: {{colors: data.pie_cpu_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
        textinfo: 'percent',
        textposition: 'inside',
        automargin: true,
        sort: false,
        hovertemplate: '%{{label}}<br>cpu_sec=%{{value:.2f}}<br>cpu_human=%{{customdata}}<extra></extra>',
      }}], {{title: 'CPU consumers (top suite+chunk + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});
      Plotly.newPlot('ramPie', [{{
        type: 'pie',
        labels: data.pie_ram_labels,
        values: data.pie_ram_vals,
        customdata: data.pie_ram_vals.map(v => humanFromKb(v)),
        marker: {{colors: data.pie_ram_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
        textinfo: 'percent',
        textposition: 'inside',
        automargin: true,
        sort: false,
        hovertemplate: '%{{label}}<br>ram_kb_sum=%{{value:.0f}} KB<br>ram_human=%{{customdata}}<extra></extra>',
      }}], {{title: 'RAM consumers (top suite+chunk + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});
      Plotly.newPlot('activePie', [{{
        type: 'pie',
        labels: data.pie_active_labels,
        values: data.pie_active_vals,
        customdata: data.pie_active_vals.map(v => humanFromSec(v)),
        marker: {{colors: data.pie_active_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
        textinfo: 'percent',
        textposition: 'inside',
        automargin: true,
        sort: false,
        hovertemplate: '%{{label}}<br>active_time_sec=%{{value:.2f}}<br>active_human=%{{customdata}}<extra></extra>',
      }}], {{title: 'Active time share (suite+chunk, top + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});
    }}

    Plotly.newPlot('cpuPieSuite', [{{
      type: 'pie',
      labels: data.pie_cpu_suite_labels,
      values: data.pie_cpu_suite_vals,
      customdata: data.pie_cpu_suite_vals.map(v => humanFromSec(v)),
      marker: {{colors: data.pie_cpu_suite_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
      textinfo: 'percent',
      textposition: 'inside',
      automargin: true,
      sort: false,
      hovertemplate: '%{{label}}<br>cpu_sec=%{{value:.2f}}<br>cpu_human=%{{customdata}}<extra></extra>',
    }}], {{title: 'CPU consumers by suite (top + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});

    Plotly.newPlot('ramPieSuite', [{{
      type: 'pie',
      labels: data.pie_ram_suite_labels,
      values: data.pie_ram_suite_vals,
      customdata: data.pie_ram_suite_vals.map(v => humanFromKb(v)),
      marker: {{colors: data.pie_ram_suite_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
      textinfo: 'percent',
      textposition: 'inside',
      automargin: true,
      sort: false,
      hovertemplate: '%{{label}}<br>ram_kb_sum=%{{value:.0f}} KB<br>ram_human=%{{customdata}}<extra></extra>',
    }}], {{title: 'RAM consumers by suite (top + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});

    Plotly.newPlot('activePieSuite', [{{
      type: 'pie',
      labels: data.pie_active_suite_labels,
      values: data.pie_active_suite_vals,
      customdata: data.pie_active_suite_vals.map(v => humanFromSec(v)),
      marker: {{colors: data.pie_active_suite_labels.map(lb => lb === 'other' ? '#bdbdbd' : (data.suite_color[suiteFromLabel(lb)] || '#999'))}},
      textinfo: 'percent',
      textposition: 'inside',
      automargin: true,
      sort: false,
      hovertemplate: '%{{label}}<br>active_time_sec=%{{value:.2f}}<br>active_human=%{{customdata}}<extra></extra>',
    }}], {{title: 'Active time share by suite (top + other)', showlegend: false, margin: {{l: 20, r: 20, t: 50, b: 20}}}}, {{responsive: true}});

    const suiteSearchEl = document.getElementById('suiteSearch');
    if (suiteSearchEl) {{
      suiteSearchEl.addEventListener('input', applySuiteSearch);
    }}
    applySuiteSearch();
  </script>
</body>
</html>
"""
    out_html.write_text(html, encoding="utf-8")


def build_report_table_html(report_path: Path, out_html: Path, suite_filter: Optional[str]) -> None:
    report = json.loads(report_path.read_text(encoding="utf-8", errors="replace"))
    results = report.get("results", []) if isinstance(report, dict) else []
    rows: list[dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "test":
            continue
        suite_raw = str(item.get("path", "") or "")
        if not suite_raw:
            continue
        suite = normalize_suite_path(suite_raw)
        if suite_filter and suite != suite_filter:
            continue

        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        sub = str(item.get("subtest_name", "") or "")
        chunk_idx: Optional[int] = None
        m_idx = CHUNK_FROM_SUBTEST_RE.search(sub)
        if m_idx:
            chunk_idx = int(m_idx.group(1))
        elif CHUNK_SOLE_RE.search(sub) or CHUNK_BRACKET_ONLY_RE.search(sub):
            chunk_idx = 0
        chunk_group = chunk_group_from_subtest(sub)

        rows.append(
            {
                "suite_path": suite,
                "suite_path_raw": suite_raw,
                "test_name": str(item.get("name", "") or ""),
                "subtest_name": sub,
                "status": str(item.get("status", "") or ""),
                "error_type": str(item.get("error_type", "") or ""),
                "is_muted": bool(item.get("is_muted") or item.get("muted")),
                "chunk": bool(item.get("chunk")),
                "chunk_idx": chunk_idx,
                "chunk_group": chunk_group,
                "duration_sec": float(item.get("duration") or 0.0),
                "cpu_sec": cpu_seconds(metrics),
                "ram_kb": ram_kb(metrics),
                "ru_utime": metrics.get("ru_utime"),
                "ru_stime": metrics.get("ru_stime"),
                "ru_maxrss": metrics.get("ru_maxrss"),
                "ru_rss": metrics.get("ru_rss"),
                "wall_time": metrics.get("wall_time"),
                "id": item.get("id"),
                "hid": item.get("hid"),
                "size": item.get("size"),
                "tags": ", ".join(item.get("tags", [])) if isinstance(item.get("tags"), list) else "",
                "metrics_json": json.dumps(metrics, ensure_ascii=False, sort_keys=True),
            }
        )

    payload = {
        "suite_filter": suite_filter,
        "report_path": str(report_path),
        "rows_count": len(rows),
        "rows": rows,
    }
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Report table: suite/chunk/test</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Arial, sans-serif; margin: 12px; }}
    .toolbar {{ position: sticky; top: 0; background: #fff; z-index: 3; border-bottom: 1px solid #eee; padding: 8px 0; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
    .toolbar input, .toolbar select {{ padding: 4px 8px; }}
    .table-wrap {{ overflow-x: auto; border: 1px solid #eee; border-radius: 6px; }}
    table {{ width: 100%; min-width: 2650px; border-collapse: collapse; font-size: 12px; table-layout: fixed; }}
    th, td {{ border: 1px solid #eee; padding: 4px 6px; vertical-align: top; }}
    th {{ position: sticky; top: 48px; background: #fafafa; z-index: 2; cursor: pointer; user-select: none; }}
    td pre {{ margin: 0; white-space: pre-wrap; word-break: break-word; max-width: 620px; }}
    #tbody tr {{ content-visibility: auto; contain-intrinsic-size: 28px; }}
    th:nth-child(1), td:nth-child(1) {{ width: 320px; }}
    th:nth-child(2), td:nth-child(2) {{ width: 70px; }}
    th:nth-child(3), td:nth-child(3) {{ width: 130px; }}
    th:nth-child(4), td:nth-child(4) {{ width: 70px; }}
    th:nth-child(5), td:nth-child(5) {{ width: 220px; }}
    th:nth-child(6), td:nth-child(6) {{ width: 260px; }}
    th:nth-child(22), td:nth-child(22) {{ width: 130px; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
    .muted {{ color: #6a737d; }}
    .ok {{ color: #155724; }}
    .fail {{ color: #721c24; }}
    .sidepanel {{ position: fixed; top: 0; right: 0; width: min(52vw, 820px); height: 100vh; background: #fff; border-left: 1px solid #ddd; box-shadow: -8px 0 24px rgba(0,0,0,0.08); z-index: 20; display: none; }}
    .sidepanel .head {{ display: flex; justify-content: space-between; align-items: center; padding: 10px 12px; border-bottom: 1px solid #eee; }}
    .sidepanel .body {{ padding: 10px 12px; height: calc(100vh - 56px); overflow: auto; }}
  </style>
</head>
<body>
  <h2>Report table: suite/chunk/test</h2>
  <div class="muted">suite filter: {suite_filter or 'ALL SUITES'} | rows: <span id="rowsCount"></span></div>
  <div class="toolbar">
    <label>Search:</label>
    <input id="q" type="text" placeholder="suite/test/subtest/status/tags" style="min-width: 360px;" />
    <label>Status:</label>
    <select id="statusSel">
      <option value="">all</option>
    </select>
    <label>Mode:</label>
    <select id="chunkSel">
      <option value="">all</option>
      <option value="chunk">chunk=true</option>
      <option value="test">chunk=false</option>
    </select>
    <button type="button" onclick="clearFilters()">Clear</button>
  </div>
  <div class="table-wrap">
    <table id="tbl">
      <thead id="thead"></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
  <div id="metricsPanel" class="sidepanel" role="dialog" aria-modal="true">
    <div class="head">
      <b id="metricsPanelTitle">metrics</b>
      <button type="button" onclick="closeMetricsPanel()">Close</button>
    </div>
    <div class="body">
      <pre id="metricsPanelBody" class="mono"></pre>
    </div>
  </div>
  <script>
    const data = {json.dumps(payload, ensure_ascii=False)};
    const cols = [
      ['suite_path', 'suite_path'],
      ['chunk', 'chunk'],
      ['chunk_group', 'chunk_group'],
      ['chunk_idx', 'chunk_idx'],
      ['test_name', 'test_name'],
      ['subtest_name', 'subtest_name'],
      ['status', 'status'],
      ['error_type', 'error_type'],
      ['is_muted', 'is_muted'],
      ['duration_sec', 'duration_sec'],
      ['cpu_sec', 'cpu_sec'],
      ['ram_kb', 'ram_kb'],
      ['ru_utime', 'ru_utime'],
      ['ru_stime', 'ru_stime'],
      ['ru_maxrss', 'ru_maxrss'],
      ['ru_rss', 'ru_rss'],
      ['wall_time', 'wall_time'],
      ['size', 'size'],
      ['id', 'id'],
      ['hid', 'hid'],
      ['tags', 'tags'],
      ['metrics_json', 'metrics'],
    ];

    let sortCol = 'suite_path';
    let sortAsc = true;

    function valueForSort(v) {{
      if (v === null || v === undefined) return '';
      if (typeof v === 'boolean') return v ? 1 : 0;
      const n = Number(v);
      if (!Number.isNaN(n) && String(v).trim() !== '') return n;
      return String(v).toLowerCase();
    }}

    function esc(s) {{
      return String(s ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;');
    }}

    function render() {{
      const q = (document.getElementById('q').value || '').toLowerCase().trim();
      const st = document.getElementById('statusSel').value;
      const mode = document.getElementById('chunkSel').value;

      const filtered = data.rows.map((r, idx) => ({{r, idx}})).filter(({{r}}) => {{
        if (st && String(r.status || '') !== st) return false;
        if (mode === 'chunk' && !r.chunk) return false;
        if (mode === 'test' && r.chunk) return false;
        if (!q) return true;
        const hay = [
          r.suite_path, r.test_name, r.subtest_name, r.status, r.error_type, r.tags, r.chunk_group, r.id, r.hid
        ].map(x => String(x || '').toLowerCase()).join(' ');
        return hay.includes(q);
      }});

      filtered.sort((a, b) => {{
        const av = valueForSort(a.r[sortCol]);
        const bv = valueForSort(b.r[sortCol]);
        if (av < bv) return sortAsc ? -1 : 1;
        if (av > bv) return sortAsc ? 1 : -1;
        return 0;
      }});

      const head = '<tr>' + cols.map(([k, title]) => {{
        const marker = sortCol === k ? (sortAsc ? ' ▲' : ' ▼') : '';
        return '<th data-col=\"' + k + '\">' + esc(title) + marker + '</th>';
      }}).join('') + '</tr>';
      document.getElementById('thead').innerHTML = head;
      document.querySelectorAll('#thead th').forEach(th => th.addEventListener('click', () => {{
        const c = th.getAttribute('data-col');
        if (sortCol === c) sortAsc = !sortAsc;
        else {{ sortCol = c; sortAsc = true; }}
        render();
      }}));

      const body = filtered.map(({{r, idx}}) => {{
        const statusCls = /^(OK|PASS)$/i.test(String(r.status || '')) ? 'ok' : (/^(FAILED|ERROR|TIMEOUT|INTERNAL|MUTE)$/i.test(String(r.status || '')) ? 'fail' : '');
        const metricsCell = '<button type=\"button\" class=\"metrics-btn\" data-idx=\"' + String(idx) + '\">open metrics</button>';
        const vals = {{
          suite_path: esc(r.suite_path),
          chunk: r.chunk ? 'true' : 'false',
          chunk_group: esc(r.chunk_group || ''),
          chunk_idx: r.chunk_idx == null ? '' : String(r.chunk_idx),
          test_name: esc(r.test_name || ''),
          subtest_name: esc(r.subtest_name || ''),
          status: '<span class=\"' + statusCls + '\">' + esc(r.status || '') + '</span>',
          error_type: esc(r.error_type || ''),
          is_muted: r.is_muted ? 'true' : 'false',
          duration_sec: Number(r.duration_sec || 0).toFixed(3),
          cpu_sec: Number(r.cpu_sec || 0).toFixed(6),
          ram_kb: Number(r.ram_kb || 0).toFixed(0),
          ru_utime: esc(r.ru_utime ?? ''),
          ru_stime: esc(r.ru_stime ?? ''),
          ru_maxrss: esc(r.ru_maxrss ?? ''),
          ru_rss: esc(r.ru_rss ?? ''),
          wall_time: esc(r.wall_time ?? ''),
          size: esc(r.size ?? ''),
          id: '<span class=\"mono\">' + esc(r.id ?? '') + '</span>',
          hid: '<span class=\"mono\">' + esc(r.hid ?? '') + '</span>',
          tags: esc(r.tags || ''),
          metrics_json: metricsCell,
        }};
        return '<tr>' + cols.map(([k]) => '<td>' + (vals[k] ?? '') + '</td>').join('') + '</tr>';
      }}).join('');

      document.getElementById('tbody').innerHTML = body;
      document.querySelectorAll('.metrics-btn').forEach(btn => btn.addEventListener('click', () => {{
        const idx = Number(btn.getAttribute('data-idx'));
        openMetricsPanel(idx);
      }}));
      document.getElementById('rowsCount').textContent = String(filtered.length);
    }}

    function openMetricsPanel(idx) {{
      const row = data.rows[idx];
      if (!row) return;
      const title = (row.suite_path || '') + ' | ' + (row.test_name || '') + (row.subtest_name ? (' | ' + row.subtest_name) : '');
      const body = row.metrics_json || '{{}}';
      const panel = document.getElementById('metricsPanel');
      document.getElementById('metricsPanelTitle').textContent = title;
      document.getElementById('metricsPanelBody').textContent = body;
      panel.style.display = 'block';
    }}

    function closeMetricsPanel() {{
      const panel = document.getElementById('metricsPanel');
      panel.style.display = 'none';
    }}

    function clearFilters() {{
      document.getElementById('q').value = '';
      document.getElementById('statusSel').value = '';
      document.getElementById('chunkSel').value = '';
      render();
    }}

    const statuses = Array.from(new Set(data.rows.map(r => String(r.status || '')).filter(Boolean))).sort();
    const stSel = document.getElementById('statusSel');
    statuses.forEach(s => {{
      const opt = document.createElement('option');
      opt.value = s;
      opt.textContent = s;
      stSel.appendChild(opt);
    }});
    document.getElementById('q').addEventListener('input', render);
    document.getElementById('statusSel').addEventListener('change', render);
    document.getElementById('chunkSel').addEventListener('change', render);
    render();
  </script>
</body>
</html>
"""
    out_html.write_text(html, encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser(description="Build tests resource dashboard from evlog + report")
    p.add_argument("--suite-path", default=None, help="Suite path to filter; omit for ALL suites")
    p.add_argument("--report", required=True, type=Path, help="Path to report.json")
    p.add_argument("--evlog", required=True, type=Path, help="Path to ya evlog json/jsonl")
    p.add_argument("--out-trace", required=True, type=Path, help="Output trace JSON for chrome://tracing / Perfetto")
    p.add_argument("--out-stats", type=Path, default=None, help="Optional output JSON with matching stats")
    p.add_argument("--out-html", type=Path, default=None, help="Optional HTML dashboard with layered CPU/RAM charts")
    p.add_argument("--top-n", type=int, default=12, help="Top suites to keep highlighted; suite+chunk mode keeps all chunks of these suites")
    p.add_argument("--max-points", type=int, default=1000, help="Max points per series in HTML dashboard (downsampling)")
    p.add_argument("--html-by-chunk", action="store_true", help="Include suite+chunk charts in HTML (default: only by suite)")
    p.add_argument("--full-table", action="store_true", help="Generate additional detailed *_table.html (disabled by default)")
    p.add_argument("--maximize-reqs-for-timeout-tests", action="store_true", help="For suites with test timeouts use size max: SMALL=1, MEDIUM=4, LARGE=all")
    p.add_argument("--out-cpu-suggestions", type=Path, default=None, help="Output JSON with per-suite recommended cpuN for runner")
    p.add_argument("--repo-root", type=Path, default=None, help="Repo root to read ya.make REQUIREMENTS for synthetic CPU/RAM when report has no metrics")
    p.add_argument("--sanitizer", type=str, default=None, help="Optional SANITIZER_TYPE value for ya.make IF branches")
    args = p.parse_args()

    # Prevent accidental overwrite of inputs by outputs.
    report_r = args.report.resolve()
    evlog_r = args.evlog.resolve()
    out_trace_r = args.out_trace.resolve()
    out_stats_r = args.out_stats.resolve() if args.out_stats else None
    out_html_r = args.out_html.resolve() if args.out_html else None
    out_cpu_sugg_r = args.out_cpu_suggestions.resolve() if args.out_cpu_suggestions else None

    if report_r == out_trace_r:
        raise SystemExit("Invalid args: --report and --out-trace must be different files")
    if out_stats_r and report_r == out_stats_r:
        raise SystemExit("Invalid args: --report and --out-stats must be different files")
    if out_html_r and report_r == out_html_r:
        raise SystemExit("Invalid args: --report and --out-html must be different files")
    if out_cpu_sugg_r and report_r == out_cpu_sugg_r:
        raise SystemExit("Invalid args: --report and --out-cpu-suggestions must be different files")
    if out_stats_r and out_trace_r == out_stats_r:
        raise SystemExit("Invalid args: --out-trace and --out-stats must be different files")
    if out_html_r and out_trace_r == out_html_r:
        raise SystemExit("Invalid args: --out-trace and --out-html must be different files")
    if out_cpu_sugg_r and out_trace_r == out_cpu_sugg_r:
        raise SystemExit("Invalid args: --out-trace and --out-cpu-suggestions must be different files")

    chunks, report_status_by_suite = parse_report_chunks(args.report, args.suite_path)
    runs = parse_evlog_runs(args.evlog, args.suite_path)

    requirements_cache = None
    repo_root = args.repo_root
    if not repo_root or not repo_root.is_dir():
        if len(chunks) == 0 and runs:
            repo_root = Path.cwd()
        else:
            repo_root = None
    if repo_root and repo_root.is_dir():
        suite_paths = sorted(
            {normalize_suite_path(str(r["suite_path"])) for r in runs}
            | {normalize_suite_path(str(s)) for s in report_status_by_suite.keys()}
        )
        requirements_cache = ya_make_requirements.build_requirements_cache(repo_root, suite_paths, sanitizer=args.sanitizer)

    trace, stats, enriched_runs = build_trace(runs, chunks, args.suite_path, requirements_cache)

    cpu_suggestions = build_cpu_suggestions(
        enriched_runs,
        requirements_cache=requirements_cache,
        report_status_by_suite=report_status_by_suite,
        maximize_reqs_for_timeout_tests=args.maximize_reqs_for_timeout_tests,
    )
    if args.out_cpu_suggestions:
        args.out_cpu_suggestions.write_text(json.dumps(cpu_suggestions, ensure_ascii=False, indent=2), encoding="utf-8")

    args.out_trace.write_text(json.dumps(trace, ensure_ascii=False), encoding="utf-8")
    if args.out_stats:
        args.out_stats.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.out_html:
        run_config = {
            "suite_path": args.suite_path,
            "report": str(args.report),
            "evlog": str(args.evlog),
            "out_trace": str(args.out_trace),
            "out_stats": str(args.out_stats) if args.out_stats else None,
            "out_html": str(args.out_html),
            "top_n": args.top_n,
            "max_points": args.max_points,
            "html_by_chunk": args.html_by_chunk,
            "full_table": args.full_table,
            "maximize_reqs_for_timeout_tests": args.maximize_reqs_for_timeout_tests,
            "repo_root_for_synthetic": str(repo_root) if repo_root else None,
            "sanitizer": args.sanitizer,
        }
        build_html_dashboard(
            args.suite_path,
            enriched_runs,
            stats,
            args.out_html,
            args.top_n,
            args.max_points,
            by_chunk=args.html_by_chunk,
            cpu_suggestions=cpu_suggestions,
            run_config=run_config,
        )
        if args.full_table:
            out_table_html = args.out_html.with_name(args.out_html.stem + "_table.html")
            build_report_table_html(args.report, out_table_html, args.suite_path)

    print(json.dumps(stats, ensure_ascii=False, indent=2))
    print(f"Trace written: {args.out_trace}")
    if args.out_stats:
        print(f"Stats written: {args.out_stats}")
    if args.out_cpu_suggestions:
        print(f"CPU suggestions written: {args.out_cpu_suggestions}")
    if args.out_html:
        print(f"HTML written: {args.out_html}")
        if args.full_table:
            print(f"Table HTML written: {args.out_html.with_name(args.out_html.stem + '_table.html')}")


if __name__ == "__main__":
    main()
