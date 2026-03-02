from __future__ import annotations

from collections import defaultdict
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
) -> dict[str, Any]:
    cpu_by_suite: dict[str, float] = defaultdict(float)
    ram_by_suite: dict[str, float] = defaultdict(float)
    active_time_by_suite: dict[str, float] = defaultdict(float)
    suite_set = sorted({str(r["suite_path"]) for r in runs})

    for r in runs:
        cpu_by_suite[str(r["suite_path"])] += float(r.get("cpu_sec_report", 0.0) or 0.0)
        ram_by_suite[str(r["suite_path"])] += float(r.get("ram_kb_report", 0.0) or 0.0)
        active_time_by_suite[str(r["suite_path"])] += float(r.get("dur_us", 0.0) or 0.0) / 1_000_000.0

    top_cpu_suite = topn_other_map(cpu_by_suite, top_n)
    top_ram_suite = topn_other_map(ram_by_suite, top_n)
    top_active_suite = topn_other_map(active_time_by_suite, top_n)

    enriched = []
    for r in runs:
        rr = dict(r)
        dur_s = float(rr["dur_us"]) / 1_000_000.0 if float(rr["dur_us"]) > 0 else 0.0
        rr["cpu_cores_est"] = (float(rr.get("cpu_sec_report", 0.0) or 0.0) / dur_s) if dur_s > 0 else 0.0
        rr["ram_gb"] = float(rr.get("ram_kb_report", 0.0) or 0.0) / (1024.0 * 1024.0)
        rr["active_one"] = 1.0
        enriched.append(rr)

    xs_cpu_suite, cpu_tracks_suite = build_step_series(enriched, "cpu_cores_est", set(top_cpu_suite), label_mode="suite")
    xs_ram_suite, ram_tracks_suite = build_step_series(enriched, "ram_gb", set(top_ram_suite), label_mode="suite")
    xs_active_suite, active_tracks_suite = build_step_series(enriched, "active_one", set(top_active_suite), label_mode="suite")
    xs_active, ys_active = build_active_series(enriched)

    xs_cpu_suite, cpu_tracks_suite = downsample_step_series(xs_cpu_suite, cpu_tracks_suite, max_points)
    xs_ram_suite, ram_tracks_suite = downsample_step_series(xs_ram_suite, ram_tracks_suite, max_points)
    xs_active_suite, active_tracks_suite = downsample_step_series(xs_active_suite, active_tracks_suite, max_points)
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

    return {
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
        "suite_color": suite_color,
        "track_suite": track_suite,
        "track_has_synthetic": track_has_synthetic,
        "suite_event_times": suite_event_times,
    }
