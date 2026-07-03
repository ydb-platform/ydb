from __future__ import annotations

import bisect
from collections import defaultdict
from typing import Any, Optional


def normalize_chunk_group(group: Optional[str]) -> Optional[str]:
    if not group:
        return None
    g = str(group).strip().strip("/")
    if not g:
        return None
    if g.endswith(".py"):
        g = g[:-3]
    if "/" in g:
        g = g.rsplit("/", 1)[-1]
    return g or None


def palette() -> list[str]:
    return [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
        "#4e79a7",
        "#f28e2b",
        "#59a14f",
        "#e15759",
        "#76b7b2",
        "#edc948",
        "#b07aa1",
        "#ff9da7",
        "#9c755f",
        "#bab0ab",
    ]


def topn_other_map(values: dict[str, float], top_n: int) -> list[str]:
    return [k for k, _ in sorted(values.items(), key=lambda x: x[1], reverse=True)[:top_n]]


def build_step_series(
    runs: list[dict[str, Any]],
    value_key: str,
    top_labels: set[str],
    label_mode: str = "suite_chunk",
) -> tuple[list[float], dict[str, list[float]]]:
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


def build_active_series(runs: list[dict[str, Any]]) -> tuple[list[float], list[float]]:
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


def downsample_step_series(
    xs: list[float], tracks: dict[str, list[float]], max_points: int
) -> tuple[list[float], dict[str, list[float]]]:
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


def downsample_active_series(
    xs: list[float], ys: list[float], max_points: int
) -> tuple[list[float], list[float]]:
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
