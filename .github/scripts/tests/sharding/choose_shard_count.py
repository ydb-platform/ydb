#!/usr/bin/env python3
"""Choose shard count from estimated test work and time of day.

Estimated single-job duration D (minutes) is derived from the total suite
weight (seconds of summed test runtime) divided by the test thread count:
    D = total_weight_sec / (60 * threads)

Profile (calibrated on PR-check duration distribution, see
.github/docs/pr-check-sharding-plan.md):
    D <  light_threshold  -> 1 shard (sharding overhead beats the gain)
    D <  120 min          -> 4 shards
    D <  200 min          -> 8 shards
    otherwise             -> 12 shards

Always keep enough shards so the ideal per-shard wall time stays within
``max_shard_wall_min`` (default 4h). Peak-hour and pool-capacity caps may
reduce the tier count, but never below that wall-time floor.

During peak pool hours (UTC) the shard count is capped so parallel checks do
not saturate the shared runner pool (unless the wall-time floor needs more).
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_THREADS = 52
DEFAULT_LIGHT_THRESHOLD_MIN = 60.0
DEFAULT_TIERS = ((120.0, 4), (200.0, 8), (float("inf"), 12))
DEFAULT_PEAK_HOURS_UTC = range(9, 17)
DEFAULT_PEAK_CAP = 4
# Slowest shard estimated wall time must stay within this budget.
DEFAULT_MAX_SHARD_WALL_MIN = 240.0


def estimate_single_job_minutes(total_weight_sec: float, threads: int) -> float:
    if threads <= 0:
        raise ValueError("threads must be positive")
    return total_weight_sec / 60.0 / threads


def estimate_critical_path_minutes(shard_weights_sec: list[float], threads: int) -> float:
    """Wall-clock lower bound for parallel shards: slowest shard / threads."""
    if not shard_weights_sec:
        return 0.0
    return estimate_single_job_minutes(max(shard_weights_sec), threads)


def min_shards_for_wall_budget(
    total_weight_sec: float,
    *,
    threads: int = DEFAULT_THREADS,
    max_shard_wall_min: float = DEFAULT_MAX_SHARD_WALL_MIN,
) -> int:
    """Lower bound on shard count for ideal wall time within the budget.

    Assumes perfect packing (critical path ~= single-job / N). Real packing
    can be slightly worse; callers should fail-fast if the planned critical
    path still exceeds the budget.
    """
    if max_shard_wall_min <= 0:
        raise ValueError("max_shard_wall_min must be positive")
    estimate_min = estimate_single_job_minutes(total_weight_sec, threads)
    if estimate_min <= 0:
        return 1
    return max(1, math.ceil(estimate_min / max_shard_wall_min))


def enrich_plan_timing_estimate(plan: dict, threads: int) -> dict:
    """Attach estimated_* timing fields from shard balance weights."""
    shards = plan.get("shards") or []
    loads = [float(shard.get("balance_weight") or 0) for shard in shards]
    total_weight = float(plan.get("total_weight") or sum(loads) or 0.0)
    max_load = max(loads) if loads else 0.0
    plan["estimate_threads"] = threads
    plan["estimated_max_shard_weight_sec"] = round(max_load, 1)
    plan["estimated_critical_path_min"] = round(
        estimate_critical_path_minutes(loads, threads), 1
    )
    plan["estimated_single_job_min"] = round(
        estimate_single_job_minutes(total_weight, threads), 1
    )
    return plan


def choose_shard_count(
    total_weight_sec: float,
    *,
    threads: int = DEFAULT_THREADS,
    light_threshold_min: float = DEFAULT_LIGHT_THRESHOLD_MIN,
    peak_cap: int = DEFAULT_PEAK_CAP,
    is_peak: bool = False,
    max_shards: int = 0,
    max_shard_wall_min: float = DEFAULT_MAX_SHARD_WALL_MIN,
) -> tuple[int, float]:
    """Return (shard_count, estimated_single_job_minutes)."""
    estimate_min = estimate_single_job_minutes(total_weight_sec, threads)
    wall_floor = min_shards_for_wall_budget(
        total_weight_sec,
        threads=threads,
        max_shard_wall_min=max_shard_wall_min,
    )
    if estimate_min < light_threshold_min:
        count = 1
    else:
        count = DEFAULT_TIERS[-1][1]
        for upper_min, tier_count in DEFAULT_TIERS:
            if estimate_min < upper_min:
                count = tier_count
                break
    count = max(count, wall_floor)
    if is_peak and count > peak_cap:
        count = peak_cap
    if max_shards > 0:
        count = min(count, max_shards)
    # Wall-time SLA wins over peak/capacity caps.
    count = max(count, wall_floor)
    return max(count, 1), estimate_min


def is_peak_hour_utc(hour: int) -> bool:
    return hour in DEFAULT_PEAK_HOURS_UTC


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "summary_json",
        type=Path,
        help="list_summary.json with total_weight (seconds) from apply_history_suite_weights.py",
    )
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument(
        "--light-threshold-min",
        type=float,
        default=DEFAULT_LIGHT_THRESHOLD_MIN,
        help="Estimated single-job minutes below which sharding is not worth it",
    )
    parser.add_argument("--peak-cap", type=int, default=DEFAULT_PEAK_CAP)
    parser.add_argument(
        "--no-peak-cap",
        action="store_true",
        help="Ignore the peak-hour cap (e.g. manual off-peak runs)",
    )
    parser.add_argument("--max-shards", type=int, default=0, help="Hard upper bound (0 = none)")
    parser.add_argument(
        "--max-shard-wall-min",
        type=float,
        default=DEFAULT_MAX_SHARD_WALL_MIN,
        help="Keep enough shards so ideal per-shard wall time stays within this many minutes",
    )
    parser.add_argument(
        "--now-utc-hour",
        type=int,
        default=None,
        help="Override current UTC hour (for tests)",
    )
    args = parser.parse_args()

    summary = json.loads(args.summary_json.read_text(encoding="utf-8"))
    total_weight_sec = float(summary.get("total_weight") or 0.0)

    hour = args.now_utc_hour if args.now_utc_hour is not None else datetime.now(timezone.utc).hour
    peak = (not args.no_peak_cap) and is_peak_hour_utc(hour)

    count, estimate_min = choose_shard_count(
        total_weight_sec,
        threads=args.threads,
        light_threshold_min=args.light_threshold_min,
        peak_cap=args.peak_cap,
        is_peak=peak,
        max_shards=args.max_shards,
        max_shard_wall_min=args.max_shard_wall_min,
    )
    wall_floor = min_shards_for_wall_budget(
        total_weight_sec,
        threads=args.threads,
        max_shard_wall_min=args.max_shard_wall_min,
    )
    print(
        f"estimated single-job duration: {estimate_min:.1f} min "
        f"(weight {total_weight_sec:.0f}s / {args.threads} threads), "
        f"wall_floor={wall_floor} (<= {args.max_shard_wall_min:.0f} min/shard), "
        f"peak={peak} (hour {hour} UTC) -> shard_count={count}",
        file=sys.stderr,
    )
    print(count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
