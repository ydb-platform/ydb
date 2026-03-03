#!/usr/bin/env python3
"""
Analyze resource monitoring data and merge with report.json for layered dashboard.

Produces:
  - resources_report.html: interactive Plotly chart with CPU, RAM, disk over time,
    overlaid with test intervals from report.json (suite_start_timestamp, wall_time).
  - resources_trace.json: Chromium trace format with counter events for CPU/RAM/disk,
    can be merged with ya timeline trace for unified view in chrome://tracing.

Usage:
  python3 analyze_resources.py \
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


def build_html_dashboard(
    resources: list[dict],
    tests: list[dict],
    output_path: Path,
) -> None:
    if not resources:
        output_path.write_text("<html><body>No resource data</body></html>")
        return

    ts = [r["ts"] for r in resources]
    cpu = [r.get("cpu_total_pct", 0) for r in resources]
    ram_gb = [r.get("ram_used_kb", 0) / (1024 * 1024) for r in resources]
    disk_r = [r.get("disk_read_mb_delta", 0) for r in resources]
    disk_w = [r.get("disk_write_mb_delta", 0) for r in resources]

    from datetime import datetime, timezone
    ts_str = [datetime.fromtimestamp(t, timezone.utc).strftime("%H:%M:%S") for t in ts]

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=("CPU %", "RAM (GB)", "Disk read (MB/sample)", "Disk write (MB/sample)"),
        row_heights=[0.3, 0.3, 0.2, 0.2],
    )

    fig.add_trace(
        go.Scatter(x=ts_str, y=cpu, mode="lines", name="CPU %", line=dict(color="#2563eb")),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(x=ts_str, y=ram_gb, mode="lines", name="RAM GB", line=dict(color="#16a34a")),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(x=ts_str, y=disk_r, mode="lines", name="Disk read", line=dict(color="#ea580c")),
        row=3, col=1,
    )
    fig.add_trace(
        go.Scatter(x=ts_str, y=disk_w, mode="lines", name="Disk write", line=dict(color="#7c3aed")),
        row=4, col=1,
    )

    # Add test intervals as vertical bands (limit to top by duration to avoid overload)
    if tests:
        t_min = min(ts)
        t_max = max(ts)
        by_duration = sorted(
            [t for t in tests if t["end"] > t_min and t["start"] < t_max],
            key=lambda x: x["end"] - x["start"],
            reverse=True,
        )[:80]
        for t in by_duration:
            start_idx = max(0, min(len(ts) - 1, sum(1 for x in ts if x < t["start"])))
            end_idx = max(0, min(len(ts), sum(1 for x in ts if x <= t["end"])))
            if start_idx >= end_idx:
                continue
            color = "rgba(34,197,94,0.12)" if t["status"] == "OK" else "rgba(239,68,68,0.12)"
            fig.add_vrect(
                x0=start_idx - 0.5, x1=end_idx - 0.5,
                fillcolor=color, line_width=0,
                row="all", col=1,
            )

    fig.update_layout(
        height=700,
        title_text="Resource consumption during ya make (layered with test intervals)",
        showlegend=True,
        template="plotly_white",
    )
    fig.update_xaxes(title_text="Time", row=4, col=1)
    fig.write_html(str(output_path), include_plotlyjs="cdn")


def build_chromium_trace(resources: list[dict], output_path: Path) -> None:
    """Emit Chromium trace format with counter events (ph: C) for CPU, RAM, disk."""
    events = []
    for r in resources:
        ts_us = r.get("ts_us", int(r["ts"] * 1_000_000))
        events.append({
            "ph": "C",
            "ts": ts_us,
            "pid": 1,
            "tid": 0,
            "name": "cpu_total_pct",
            "args": {"value": r.get("cpu_total_pct", 0)},
        })
        events.append({
            "ph": "C",
            "ts": ts_us,
            "pid": 1,
            "tid": 1,
            "name": "ram_used_gb",
            "args": {"value": r.get("ram_used_kb", 0) / (1024 * 1024)},
        })
        events.append({
            "ph": "C",
            "ts": ts_us,
            "pid": 1,
            "tid": 2,
            "name": "disk_read_mb",
            "args": {"value": r.get("disk_read_mb_delta", 0)},
        })
        events.append({
            "ph": "C",
            "ts": ts_us,
            "pid": 1,
            "tid": 3,
            "name": "disk_write_mb",
            "args": {"value": r.get("disk_write_mb_delta", 0)},
        })
    output_path.write_text(json.dumps(events))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--resources-jsonl", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output-html", type=Path, required=True)
    parser.add_argument("--output-trace", type=Path, default=None)
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

    build_html_dashboard(resources, tests, args.output_html)
    if args.output_trace:
        build_chromium_trace(resources, args.output_trace)

    return 0


if __name__ == "__main__":
    sys.exit(main())
