from __future__ import annotations

from collections import defaultdict
from statistics import median
from typing import Any, Optional

try:
    from .dashboard_html_series import (
        build_active_series,
        build_step_series,
        downsample_active_series,
        downsample_step_series,
        normalize_chunk_group,
        palette,
        topn_other_map,
    )
except ImportError:
    from dashboard_html_series import (
        build_active_series,
        build_step_series,
        downsample_active_series,
        downsample_step_series,
        normalize_chunk_group,
        palette,
        topn_other_map,
    )


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    q = max(0.0, min(1.0, float(p)))
    sorted_vals = sorted(float(v) for v in values)
    pos = (len(sorted_vals) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    w = pos - lo
    return float(sorted_vals[lo] * (1.0 - w) + sorted_vals[hi] * w)


def _series_total_values(tracks: dict[str, list[float]]) -> list[float]:
    if not tracks:
        return []
    max_len = max((len(vals) for vals in tracks.values()), default=0)
    totals = [0.0] * max_len
    for vals in tracks.values():
        for i, v in enumerate(vals):
            totals[i] += float(v or 0.0)
    return totals


def _describe_distribution(values: list[float]) -> dict[str, float]:
    vals = [float(v or 0.0) for v in values]
    if not vals:
        return {"max": 0.0, "p95": 0.0, "median": 0.0}
    return {
        "max": float(max(vals)),
        "p95": _percentile(vals, 0.95),
        "median": _percentile(vals, 0.5),
    }


def _build_headline_stats(
    cpu_tracks_suite: dict[str, list[float]],
    ram_tracks_suite: dict[str, list[float]],
    ys_active: list[float],
    tests_tracks_suite: dict[str, list[float]],
    runs: list[dict[str, Any]],
    tests_per_suite: Optional[dict[str, int]],
    issues_summary: Optional[dict[str, int]],
    suite_chunk_issues_summary: Optional[dict[str, int]],
) -> dict[str, Any]:
    starts = [float(r.get("start_us", 0.0) or 0.0) for r in runs if float(r.get("start_us", 0.0) or 0.0) > 0.0]
    ends = [float(r.get("end_us", 0.0) or 0.0) for r in runs if float(r.get("end_us", 0.0) or 0.0) > 0.0]
    duration_sec = 0.0
    if starts and ends:
        duration_sec = max(0.0, (max(ends) - min(starts)) / 1_000_000.0)

    suite_total = len(set(str(r.get("suite_path") or "") for r in runs if str(r.get("suite_path") or "")))
    chunk_labels = set()
    for r in runs:
        suite = str(r.get("suite_path") or "")
        if not suite:
            continue
        group = normalize_chunk_group(r.get("chunk_group"))
        chunk = r.get("chunk")
        lb = f"{suite}::{group}/chunk{chunk}" if group else f"{suite}::chunk{chunk}"
        chunk_labels.add(lb)
    chunk_total = len(chunk_labels)
    tests_total = int(sum(int(v or 0) for v in (tests_per_suite or {}).values()))

    issues = {
        "failed_total": int((issues_summary or {}).get("failed_total", 0) or 0),
        "timeout": int((issues_summary or {}).get("timeout", 0) or 0),
        "timeout_muted": int((issues_summary or {}).get("timeout_muted", 0) or 0),
        "regular": int((issues_summary or {}).get("regular", 0) or 0),
        "regular_muted": int((issues_summary or {}).get("regular_muted", 0) or 0),
        "muted": int((issues_summary or {}).get("muted", 0) or 0),
        "skipped": int((issues_summary or {}).get("skipped", 0) or 0),
    }
    suite_chunk_issues = {
        "failed_total": int((suite_chunk_issues_summary or {}).get("failed_total", 0) or 0),
        "timeout": int((suite_chunk_issues_summary or {}).get("timeout", 0) or 0),
        "timeout_muted": int((suite_chunk_issues_summary or {}).get("timeout_muted", 0) or 0),
        "regular": int((suite_chunk_issues_summary or {}).get("regular", 0) or 0),
        "regular_muted": int((suite_chunk_issues_summary or {}).get("regular_muted", 0) or 0),
        "muted": int((suite_chunk_issues_summary or {}).get("muted", 0) or 0),
        "skipped": int((suite_chunk_issues_summary or {}).get("skipped", 0) or 0),
        "affected_suites": int((suite_chunk_issues_summary or {}).get("affected_suites", 0) or 0),
        "affected_chunks": int((suite_chunk_issues_summary or {}).get("affected_chunks", 0) or 0),
    }

    return {
        "total": {
            "suites": suite_total,
            "chunks": chunk_total,
            "tests": tests_total,
        },
        "issues": issues,
        "suite_chunk_issues": suite_chunk_issues,
        "cpu": _describe_distribution(_series_total_values(cpu_tracks_suite)),
        "ram": _describe_distribution(_series_total_values(ram_tracks_suite)),
        "active_chunks": _describe_distribution([float(v or 0.0) for v in ys_active]),
        "tests": _describe_distribution(_series_total_values(tests_tracks_suite)),
        "total_duration_sec": duration_sec,
    }


def build_dashboard_payload(
    suite_filter: Optional[str],
    runs: list[dict[str, Any]],
    stats: dict[str, Any],
    top_n: int,
    max_points: int,
    by_chunk: bool,
    cpu_suggestions: Optional[list[dict[str, Any]]] = None,
    run_config: Optional[dict[str, Any]] = None,
    suite_event_times: Optional[dict[str, dict[str, list[float]]]] = None,
    resources_overlay: Optional[dict[str, Any]] = None,
    tests_per_suite: Optional[dict[str, int]] = None,
    tests_per_chunk_by_label: Optional[dict[str, int]] = None,
    issues_summary: Optional[dict[str, int]] = None,
    suite_chunk_issues_summary: Optional[dict[str, int]] = None,
) -> dict[str, Any]:
    cpu_by_suite: dict[str, float] = defaultdict(float)
    ram_by_suite: dict[str, float] = defaultdict(float)
    active_time_by_suite: dict[str, float] = defaultdict(float)
    tests_count_by_suite: dict[str, float] = defaultdict(float)
    suite_set = sorted({str(r["suite_path"]) for r in runs})

    for r in runs:
        cpu_by_suite[str(r["suite_path"])] += float(r.get("cpu_sec_report", 0.0) or 0.0)
        ram_by_suite[str(r["suite_path"])] += float(r.get("ram_kb_report", 0.0) or 0.0)
        active_time_by_suite[str(r["suite_path"])] += float(r.get("dur_us", 0.0) or 0.0) / 1_000_000.0

    top_cpu_suite = topn_other_map(cpu_by_suite, top_n)
    top_ram_suite = topn_other_map(ram_by_suite, top_n)
    top_active_suite = topn_other_map(active_time_by_suite, top_n)
    for s, c in (tests_per_suite or {}).items():
        tests_count_by_suite[str(s)] = float(c or 0.0)
    top_tests_suite_pie = topn_other_map(tests_count_by_suite, top_n)

    enriched = []
    for r in runs:
        rr = dict(r)
        dur_used = float(rr.get("duration_used_sec", 0) or 0)
        if dur_used <= 0:
            dur_used = float(rr.get("dur_us", 0) or 0) / 1_000_000.0
        rr["cpu_cores_est"] = (float(rr.get("cpu_sec_report", 0.0) or 0.0) / dur_used) if dur_used > 0 else 0.0
        rr["ram_gb"] = float(rr.get("ram_kb_report", 0.0) or 0.0) / (1024.0 * 1024.0)
        rr["active_one"] = 1.0
        group = normalize_chunk_group(rr.get("chunk_group"))
        chunk_label = f"{rr['suite_path']}::{group}/chunk{rr['chunk']}" if group else f"{rr['suite_path']}::chunk{rr['chunk']}"
        rr["tests_in_chunk"] = int((tests_per_chunk_by_label or {}).get(chunk_label, 0) or 0)
        enriched.append(rr)

    xs_cpu_suite, cpu_tracks_suite = build_step_series(enriched, "cpu_cores_est", set(top_cpu_suite), label_mode="suite")
    xs_ram_suite, ram_tracks_suite = build_step_series(enriched, "ram_gb", set(top_ram_suite), label_mode="suite")
    xs_active_suite, active_tracks_suite = build_step_series(enriched, "active_one", set(top_active_suite), label_mode="suite")
    tests_time_by_suite: dict[str, float] = defaultdict(float)
    for rr in enriched:
        tests_time_by_suite[str(rr["suite_path"])] += float(rr.get("tests_in_chunk", 0) or 0) * (
            float(rr.get("dur_us", 0) or 0) / 1_000_000.0
        )
    top_tests_suite = topn_other_map(tests_time_by_suite, top_n)
    xs_tests_suite, tests_tracks_suite = build_step_series(enriched, "tests_in_chunk", set(top_tests_suite), label_mode="suite")
    xs_active, ys_active = build_active_series(enriched)

    headline_stats = _build_headline_stats(
        cpu_tracks_suite=cpu_tracks_suite,
        ram_tracks_suite=ram_tracks_suite,
        ys_active=ys_active,
        tests_tracks_suite=tests_tracks_suite,
        runs=runs,
        tests_per_suite=tests_per_suite,
        issues_summary=issues_summary,
        suite_chunk_issues_summary=suite_chunk_issues_summary,
    )

    xs_cpu_suite, cpu_tracks_suite = downsample_step_series(xs_cpu_suite, cpu_tracks_suite, max_points)
    xs_ram_suite, ram_tracks_suite = downsample_step_series(xs_ram_suite, ram_tracks_suite, max_points)
    xs_active_suite, active_tracks_suite = downsample_step_series(xs_active_suite, active_tracks_suite, max_points)
    xs_tests_suite, tests_tracks_suite = downsample_step_series(xs_tests_suite, tests_tracks_suite, max_points)
    xs_active, ys_active = downsample_active_series(xs_active, ys_active, max_points)

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

        xs_cpu, cpu_tracks = build_step_series(enriched, "cpu_cores_est", set(top_cpu), label_mode="suite_chunk")
        xs_ram, ram_tracks = build_step_series(enriched, "ram_gb", set(top_ram), label_mode="suite_chunk")
        xs_active_chunk, active_tracks_chunk = build_step_series(enriched, "active_one", set(top_active), label_mode="suite_chunk")
        xs_cpu, cpu_tracks = downsample_step_series(xs_cpu, cpu_tracks, max_points)
        xs_ram, ram_tracks = downsample_step_series(xs_ram, ram_tracks, max_points)
        xs_active_chunk, active_tracks_chunk = downsample_step_series(xs_active_chunk, active_tracks_chunk, max_points)
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

    cpu_tracks_suite_no_synthetic: dict[str, list[float]] = {}
    ram_tracks_suite_no_synthetic: dict[str, list[float]] = {}
    cpu_tracks_no_synthetic: dict[str, list[float]] = {}
    ram_tracks_no_synthetic: dict[str, list[float]] = {}
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
        _xs, cpu_tracks_suite_no_synthetic = build_step_series(
            enriched_no_synth, "cpu_cores_est", set(top_cpu_suite), label_mode="suite"
        )
        _, cpu_tracks_suite_no_synthetic = downsample_step_series(
            _xs, cpu_tracks_suite_no_synthetic, max_points
        )
        _xs, ram_tracks_suite_no_synthetic = build_step_series(
            enriched_no_synth, "ram_gb", set(top_ram_suite), label_mode="suite"
        )
        _, ram_tracks_suite_no_synthetic = downsample_step_series(
            _xs, ram_tracks_suite_no_synthetic, max_points
        )
        if by_chunk:
            _xs, cpu_tracks_no_synthetic = build_step_series(
                enriched_no_synth, "cpu_cores_est", set(top_cpu), label_mode="suite_chunk"
            )
            _, cpu_tracks_no_synthetic = downsample_step_series(
                _xs, cpu_tracks_no_synthetic, max_points
            )
            _xs, ram_tracks_no_synthetic = build_step_series(
                enriched_no_synth, "ram_gb", set(top_ram), label_mode="suite_chunk"
            )
            _, ram_tracks_no_synthetic = downsample_step_series(
                _xs, ram_tracks_no_synthetic, max_points
            )

    pal = palette()
    suite_color = {s: pal[i % len(pal)] for i, s in enumerate(suite_set)}

    track_suite = {}
    for lb in set(
        list(cpu_tracks.keys())
        + list(ram_tracks.keys())
        + list(active_tracks_chunk.keys())
        + list(cpu_tracks_suite.keys())
        + list(ram_tracks_suite.keys())
        + list(active_tracks_suite.keys())
        + list(tests_tracks_suite.keys())
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
    chunk_runs_for_tests: list[dict[str, Any]] = []
    tests_per_chunk_map = tests_per_chunk_by_label or {}
    for r in runs:
        group = normalize_chunk_group(r.get("chunk_group"))
        chunk_label = f"{r['suite_path']}::{group}/chunk{r['chunk']}" if group else f"{r['suite_path']}::chunk{r['chunk']}"
        chunk_runs_for_tests.append(
            {
                "suite": str(r["suite_path"]),
                "chunk_label": chunk_label,
                "start_sec": float(r.get("start_us", 0.0) or 0.0) / 1_000_000.0,
                "end_sec": float(r.get("end_us", 0.0) or 0.0) / 1_000_000.0,
                "tests": int(tests_per_chunk_map.get(chunk_label, 0) or 0),
            }
        )
    # Always build chunk-level events as fallback, then merge with provided test-level events.
    timeout_times_by_suite: dict[str, list[float]] = defaultdict(list)
    error_times_by_suite: dict[str, list[float]] = defaultdict(list)
    for rr in runs:
        suite = str(rr.get("suite_path") or "")
        if not suite:
            continue
        status = str(rr.get("status", "") or "").upper()
        error_type = str(rr.get("error_type", "") or "").upper()
        is_timeout = error_type == "TIMEOUT" or ("TIMEOUT" in status)
        is_muted = bool(rr.get("is_muted")) or status == "MUTE"
        is_failedish = status in {"FAILED", "ERROR", "INTERNAL"}
        t_sec = float(rr.get("end_us", 0.0) or 0.0) / 1_000_000.0
        if is_timeout:
            timeout_times_by_suite[suite].append(round(t_sec, 1))
        elif is_failedish and not is_muted:
            error_times_by_suite[suite].append(round(t_sec, 1))
    chunk_fallback_events: dict[str, dict[str, list[float]]] = {}
    for s in sorted(set(timeout_times_by_suite.keys()) | set(error_times_by_suite.keys())):
        chunk_fallback_events[s] = {
            "timeout_sec": sorted(set(timeout_times_by_suite.get(s, []))),
            "error_sec": sorted(set(error_times_by_suite.get(s, []))),
        }

    if suite_event_times is None:
        suite_event_times = chunk_fallback_events
    else:
        merged: dict[str, dict[str, list[float]]] = {}
        for s in set(chunk_fallback_events.keys()) | set(suite_event_times.keys()):
            provided = suite_event_times.get(s, {})
            fallback = chunk_fallback_events.get(s, {})
            p_timeout = list(provided.get("timeout_sec", [])) if isinstance(provided, dict) else []
            p_error = list(provided.get("error_sec", [])) if isinstance(provided, dict) else []
            # If test-level mapping produced nothing for a suite, fall back to chunk-level times.
            merged[s] = {
                "timeout_sec": sorted(set(p_timeout if p_timeout else fallback.get("timeout_sec", []))),
                "error_sec": sorted(set(p_error if p_error else fallback.get("error_sec", []))),
            }
        suite_event_times = merged

    # Derive UTC offset between evlog monotonic timeline (start_us/end_us) and
    # report wall-clock timestamps when available.
    utc_offsets: list[float] = []
    for rr in runs:
        start_ts = rr.get("report_suite_start_ts")
        finish_ts = rr.get("report_suite_finish_ts")
        try:
            start_ev = float(rr.get("start_us", 0.0) or 0.0) / 1_000_000.0
        except (TypeError, ValueError):
            start_ev = 0.0
        try:
            end_ev = float(rr.get("end_us", 0.0) or 0.0) / 1_000_000.0
        except (TypeError, ValueError):
            end_ev = 0.0
        if isinstance(start_ts, (int, float)) and start_ev > 0:
            utc_offsets.append(float(start_ts) - start_ev)
        if isinstance(finish_ts, (int, float)) and end_ev > 0:
            utc_offsets.append(float(finish_ts) - end_ev)
    utc_offset_sec = float(median(utc_offsets)) if utc_offsets else 0.0

    return {
        "suite_filter": suite_filter,
        "stats": stats,
        "headline_stats": headline_stats,
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
        "xs_tests_suite": xs_tests_suite,
        "tests_tracks_suite": tests_tracks_suite,
        "xs_active": xs_active,
        "ys_active": ys_active,
        "pie_cpu_labels": top_cpu + (["other"] if len(cpu_by_label) > len(top_cpu) else []),
        "pie_cpu_vals": [cpu_by_label.get(lb, 0.0) for lb in top_cpu]
        + (
            [sum(v for k, v in cpu_by_label.items() if k not in set(top_cpu))]
            if len(cpu_by_label) > len(top_cpu)
            else []
        ),
        "pie_ram_labels": top_ram + (["other"] if len(ram_by_label) > len(top_ram) else []),
        "pie_ram_vals": [ram_by_label.get(lb, 0.0) for lb in top_ram]
        + (
            [sum(v for k, v in ram_by_label.items() if k not in set(top_ram))]
            if len(ram_by_label) > len(top_ram)
            else []
        ),
        "pie_active_labels": top_active + (["other"] if len(active_time_by_label) > len(top_active) else []),
        "pie_active_vals": [active_time_by_label.get(lb, 0.0) for lb in top_active]
        + (
            [sum(v for k, v in active_time_by_label.items() if k not in set(top_active))]
            if len(active_time_by_label) > len(top_active)
            else []
        ),
        "pie_cpu_suite_labels": top_cpu_suite + (["other"] if len(cpu_by_suite) > len(top_cpu_suite) else []),
        "pie_cpu_suite_vals": [cpu_by_suite.get(lb, 0.0) for lb in top_cpu_suite]
        + (
            [sum(v for k, v in cpu_by_suite.items() if k not in set(top_cpu_suite))]
            if len(cpu_by_suite) > len(top_cpu_suite)
            else []
        ),
        "pie_ram_suite_labels": top_ram_suite + (["other"] if len(ram_by_suite) > len(top_ram_suite) else []),
        "pie_ram_suite_vals": [ram_by_suite.get(lb, 0.0) for lb in top_ram_suite]
        + (
            [sum(v for k, v in ram_by_suite.items() if k not in set(top_ram_suite))]
            if len(ram_by_suite) > len(top_ram_suite)
            else []
        ),
        "pie_active_suite_labels": top_active_suite + (["other"] if len(active_time_by_suite) > len(top_active_suite) else []),
        "pie_active_suite_vals": [active_time_by_suite.get(lb, 0.0) for lb in top_active_suite]
        + (
            [sum(v for k, v in active_time_by_suite.items() if k not in set(top_active_suite))]
            if len(active_time_by_suite) > len(top_active_suite)
            else []
        ),
        "pie_tests_suite_labels": top_tests_suite_pie + (["other"] if len(tests_count_by_suite) > len(top_tests_suite_pie) else []),
        "pie_tests_suite_vals": [tests_count_by_suite.get(lb, 0.0) for lb in top_tests_suite_pie]
        + (
            [sum(v for k, v in tests_count_by_suite.items() if k not in set(top_tests_suite_pie))]
            if len(tests_count_by_suite) > len(top_tests_suite_pie)
            else []
        ),
        "suite_color": suite_color,
        "track_suite": track_suite,
        "track_has_synthetic": track_has_synthetic,
        "suite_event_times": suite_event_times,
        "utc_offset_sec": utc_offset_sec,
        "utc_offset_samples": len(utc_offsets),
        "resources_overlay": resources_overlay,
        "cpu_suite_trace_count": len(cpu_tracks_suite),
        "ram_suite_trace_count": len(ram_tracks_suite),
        "cpu_chunk_trace_count": len(cpu_tracks) if by_chunk else 0,
        "ram_chunk_trace_count": len(ram_tracks) if by_chunk else 0,
        "chunk_runs_for_tests": chunk_runs_for_tests,
    }
