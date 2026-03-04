#!/usr/bin/env python3
"""
Analyze resource monitoring data and merge with report.json for layered dashboard.

Produces:
  - resources_report.html: interactive Plotly chart with CPU, RAM, disk over time,
    overlaid with test intervals from report.json (suite_start_timestamp, wall_time).
  - resources_trace.json: Chromium trace format with counter events for CPU/RAM/disk,
    can be merged with ya timeline trace for unified view in chrome://tracing.

Usage:
  python3 .github/scripts/utils/dashboard/analyze_resources.py \
    --resources-jsonl resources_monitor.jsonl \
    --report report.json \
    --output-html resources_report.html \
    [--output-trace resources_trace.json]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    print("plotly required: pip install plotly", file=sys.stderr)
    sys.exit(1)


def load_resources_jsonl(path: Path) -> list[dict[str, Any]]:
    records = []
    for line in path.read_text().strip().splitlines():
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def load_report_tests(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text())
    tests = []
    for r in data.get("results", []):
        if r.get("type") != "test" or not r.get("chunk"):
            continue
        metrics = r.get("metrics") or {}
        start = metrics.get("suite_start_timestamp")
        wall = metrics.get("wall_time", 0)
        if start is None:
            continue
        path_str = r.get("path", "") + " " + r.get("subtest_name", "")
        tests.append({
            "path": path_str.strip(),
            "start": float(start),
            "end": float(start) + float(wall),
            "status": r.get("status", ""),
        })
    return tests


def load_ram_usage_legacy(path: Path) -> list[tuple[int, float]]:
    """Parse ram_usage_legacy.txt (ts used_kb per line). Returns [(ts, gb), ...]."""
    out = []
    for line in path.read_text().strip().splitlines():
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) >= 2:
            try:
                ts = int(parts[0])
                kb = float(parts[1])
                out.append((ts, kb / (1024 * 1024)))
            except (ValueError, TypeError):
                continue
    return out


def build_html_dashboard(
    resources: list[dict],
    tests: list[dict],
    output_path: Path,
    interval_sec: float | None = None,
    try_num: int | None = None,
    try_boundaries: list[dict] | None = None,
    ram_usage_legacy_path: Path | None = None,
) -> None:
    if not resources:
        output_path.write_text("<html><body>No resource data</body></html>")
        return

    # Machine specs from first record (monitor adds cpu_cores, ram_total_gb)
    first = resources[0]
    cpu_cores = first.get("cpu_cores") or 1
    ram_total_gb = first.get("ram_total_gb") or 64.0
    if interval_sec is None and len(resources) >= 2:
        interval_sec = round(resources[1]["ts"] - resources[0]["ts"], 1) or 1.0
    interval_sec = interval_sec or 1.0

    ts = [r["ts"] for r in resources]
    cpu_total = [r.get("cpu_total_pct", 0) for r in resources]
    cpu_ya = [r.get("cpu_ya_pct", 0) for r in resources]
    ram_gb = [r.get("ram_used_kb", 0) / (1024 * 1024) for r in resources]
    ram_ya_gb = [r.get("ram_ya_kb", 0) / (1024 * 1024) for r in resources]

    # Peak comparison: new monitor vs legacy (ram_usage_legacy.txt)
    peak_ram_total_gb = max(ram_gb) if ram_gb else 0
    peak_ram_ya_gb = max(ram_ya_gb) if ram_ya_gb else 0
    peak_legacy_gb: float | None = None
    if ram_usage_legacy_path and ram_usage_legacy_path.exists():
        legacy_data = load_ram_usage_legacy(ram_usage_legacy_path)
        if legacy_data:
            peak_legacy_gb = max(gb for _, gb in legacy_data)
    disk_r = [r.get("disk_read_mb_delta", 0) for r in resources]
    disk_w = [r.get("disk_write_mb_delta", 0) for r in resources]
    disk_ya_r = [r.get("disk_ya_read_mb_delta", 0) for r in resources]
    disk_ya_w = [r.get("disk_ya_write_mb_delta", 0) for r in resources]

    # Active tests count per timestamp (replaces green bands)
    active_tests = []
    for t in ts:
        cnt = sum(1 for test in tests if test["start"] <= t < test["end"])
        active_tests.append(cnt)

    # CPU: convert % to cores (100% = 1 core)
    cpu_total_cores = [c / 100.0 * cpu_cores for c in cpu_total]
    cpu_ya_cores = [c / 100.0 * cpu_cores for c in cpu_ya]

    from datetime import datetime, timezone
    ts_str = [datetime.fromtimestamp(t, timezone.utc).strftime("%H:%M:%S") for t in ts]

    try_suffix = f" (try {try_num})" if try_num else ""
    try_boundaries = try_boundaries or []
    fig = make_subplots(
        rows=5,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=(
            f"CPU (ядра из {cpu_cores}, 100%=1 ядро)",
            f"RAM (GB из {ram_total_gb:.0f} GB всего)",
            f"Disk read (MB за {interval_sec:.0f} сек)",
            f"Disk write (MB за {interval_sec:.0f} сек)",
            "Количество тестов (параллельно)",
        ),
        row_heights=[0.24, 0.24, 0.17, 0.17, 0.18],
    )

    fig.add_trace(
        go.Scatter(x=ts_str, y=cpu_total_cores, mode="lines", name="CPU total (ядра)", line=dict(color="#2563eb")),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(x=ts_str, y=cpu_ya_cores, mode="lines", name="CPU ya make (ядра)", line=dict(color="#60a5fa", dash="dash")),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(x=ts_str, y=ram_gb, mode="lines", name="RAM total (GB)", line=dict(color="#16a34a")),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(x=ts_str, y=ram_ya_gb, mode="lines", name="RAM ya make (GB)", line=dict(color="#4ade80", dash="dash")),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(x=ts_str, y=disk_r, mode="lines", name="Disk read total (MB)", line=dict(color="#ea580c")),
        row=3, col=1,
    )
    fig.add_trace(
        go.Scatter(x=ts_str, y=disk_ya_r, mode="lines", name="Disk read ya make (MB)", line=dict(color="#fb923c", dash="dash")),
        row=3, col=1,
    )
    fig.add_trace(
        go.Scatter(x=ts_str, y=disk_w, mode="lines", name="Disk write total (MB)", line=dict(color="#7c3aed")),
        row=4, col=1,
    )
    fig.add_trace(
        go.Scatter(x=ts_str, y=disk_ya_w, mode="lines", name="Disk write ya make (MB)", line=dict(color="#a78bfa", dash="dash")),
        row=4, col=1,
    )
    fig.add_trace(
        go.Scatter(x=ts_str, y=active_tests, mode="lines", name="Кол-во тестов", line=dict(color="#0ea5e9", width=2, shape="hv")),
        row=5, col=1,
    )

    # Y-axis: 0 to max; scale max must be >= max displayed value
    cpu_data_max = max((cpu_total_cores or [0]) + (cpu_ya_cores or [0]))
    cpu_y_max = max(cpu_cores * 1.05, cpu_data_max * 1.05, 0.1)
    fig.update_yaxes(range=[0, cpu_y_max], row=1, col=1)

    ram_data_max = max((ram_gb or [0]) + (ram_ya_gb or [0]))
    ram_y_max = max(ram_total_gb * 1.05, ram_data_max * 1.05, 0.1)
    fig.update_yaxes(range=[0, ram_y_max], row=2, col=1)

    disk_all = (disk_r or [0]) + (disk_w or [0]) + (disk_ya_r or [0]) + (disk_ya_w or [0])
    disk_data_max = max(disk_all) if disk_all else 0
    disk_y_max = max(disk_data_max * 1.1, 1)
    fig.update_yaxes(range=[0, disk_y_max], row=3, col=1)
    fig.update_yaxes(range=[0, disk_y_max], row=4, col=1)

    active_data_max = max(active_tests) if active_tests else 0
    active_y_max = max(active_data_max * 1.1, 1)
    fig.update_yaxes(range=[0, active_y_max], row=5, col=1)

    # Try boundary markers (vertical lines when try ends / next starts)
    for b in try_boundaries:
        t_end = b.get("end")
        try_n = b.get("try")
        if t_end is None or try_n is None:
            continue
        idx = sum(1 for x in ts if x < t_end)
        if 0 <= idx < len(ts_str):
            fig.add_vline(
                x=ts_str[idx],
                line_dash="dash",
                line_color="rgba(100,116,139,0.7)",
                line_width=1,
                annotation_text=f"try {try_n} end",
                annotation_position="top",
            )

    boundary_note = " Метки: try N end = граница между попытками." if try_boundaries else ""
    peak_note = f" Пики: RAM total {peak_ram_total_gb:.1f} GB, RAM ya make {peak_ram_ya_gb:.1f} GB."
    if peak_legacy_gb is not None:
        peak_note += f" Legacy (grep/awk meminfo): {peak_legacy_gb:.1f} GB. Δ={peak_ram_total_gb - peak_legacy_gb:+.1f} GB."
    fig.update_layout(
        height=1105,
        title_text=f"Resource consumption during ya make{try_suffix}<br><sup>Общий отчёт по всем try.{boundary_note} RAM ya может быть > total (RSS считает shared memory на каждый процесс). Нижний график: кол-во тестов.{peak_note}</sup>",
        showlegend=True,
        template="plotly_white",
    )
    fig.update_xaxes(title_text="Time (UTC)", row=5, col=1)
    fig.write_html(str(output_path), include_plotlyjs="cdn")


def build_chromium_trace(resources: list[dict], output_path: Path) -> None:
    """Emit Chromium trace format with counter events (ph: C) for CPU, RAM, disk."""
    events = []
    for r in resources:
        ts_us = r.get("ts_us", int(r["ts"] * 1_000_000))
        events.append({"ph": "C", "ts": ts_us, "pid": 1, "tid": 0, "name": "cpu_total_pct", "args": {"value": r.get("cpu_total_pct", 0)}})
        events.append({"ph": "C", "ts": ts_us, "pid": 1, "tid": 1, "name": "cpu_ya_pct", "args": {"value": r.get("cpu_ya_pct", 0)}})
        events.append({"ph": "C", "ts": ts_us, "pid": 1, "tid": 2, "name": "ram_used_gb", "args": {"value": r.get("ram_used_kb", 0) / (1024 * 1024)}})
        events.append({"ph": "C", "ts": ts_us, "pid": 1, "tid": 3, "name": "ram_ya_gb", "args": {"value": r.get("ram_ya_kb", 0) / (1024 * 1024)}})
        events.append({"ph": "C", "ts": ts_us, "pid": 1, "tid": 4, "name": "disk_read_mb", "args": {"value": r.get("disk_read_mb_delta", 0)}})
        events.append({"ph": "C", "ts": ts_us, "pid": 1, "tid": 5, "name": "disk_ya_read_mb", "args": {"value": r.get("disk_ya_read_mb_delta", 0)}})
        events.append({"ph": "C", "ts": ts_us, "pid": 1, "tid": 6, "name": "disk_write_mb", "args": {"value": r.get("disk_write_mb_delta", 0)}})
        events.append({"ph": "C", "ts": ts_us, "pid": 1, "tid": 7, "name": "disk_ya_write_mb", "args": {"value": r.get("disk_ya_write_mb_delta", 0)}})
    output_path.write_text(json.dumps(events))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--resources-jsonl", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output-html", type=Path, required=True)
    parser.add_argument("--output-trace", type=Path, default=None)
    parser.add_argument("--try-num", type=int, default=None, help="Try number for report title (per-try mode)")
    parser.add_argument("--try-boundaries", type=Path, default=None, help="JSON file with try boundaries [{try,start,end}...] for combined report")
    parser.add_argument("--ram-usage-legacy", type=Path, default=None, help="ram_usage_legacy.txt for peak comparison (new vs legacy monitor)")
    args = parser.parse_args()

    if not args.resources_jsonl.exists():
        print(f"Resources file not found: {args.resources_jsonl}", file=sys.stderr)
        return 1
    if not args.report.exists():
        print(f"Report file not found: {args.report}", file=sys.stderr)
        return 1

    resources = load_resources_jsonl(args.resources_jsonl)
    if not resources:
        print("No resource samples in JSONL (ya make may have exited too quickly)", file=sys.stderr)
        args.output_html.write_text("<html><body>No resource data collected</body></html>")
        return 0
    tests = load_report_tests(args.report)

    try_boundaries = []
    if args.try_boundaries and args.try_boundaries.exists():
        try:
            try_boundaries = json.loads(args.try_boundaries.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    build_html_dashboard(
        resources, tests, args.output_html,
        try_num=args.try_num,
        try_boundaries=try_boundaries,
        ram_usage_legacy_path=args.ram_usage_legacy,
    )
    if args.output_trace:
        build_chromium_trace(resources, args.output_trace)

    return 0


if __name__ == "__main__":
    sys.exit(main())
