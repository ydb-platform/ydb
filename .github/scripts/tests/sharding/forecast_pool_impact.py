#!/usr/bin/env python3
"""PR-check sharding forecast: concrete answers for wait / exec / pool.

Questions answered:
  1. How long do PRs wait and run TODAY (by day / hour)?
  2. What changes if we turn sharding ON for BOTH presets?
  3. How do wait / exec / queue change at fixed shard counts and adaptive profiles
     (PR peak4 / peak6 / peak8 / fast tiers / soft 2-4-8)?

Requires: gh auth, network, PyYAML.

Example:
  python3 .github/scripts/tests/sharding/forecast_pool_impact.py \\
    --days 30 --out /tmp/sharding_forecast.json
"""
from __future__ import annotations

import argparse
import copy
import heapq
import json
import math
import random
import statistics
import subprocess
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

DEFAULT_REPO = "ydb-platform/ydb"
PR_CHECK_WORKFLOW = "PR-check"
REGRESSION_WORKFLOWS = [
    "Regression-run_Small_and_Medium",
    "Regression-run_Large",
    "Regression-run_compatibility",
    "Regression-run_stress",
    "Nightly-Build",
]

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CAPACITY_CONFIG = SCRIPT_DIR / "../../../config/runner_capacity.yml"

PREPARE_MIN = 8.0
MERGE_MIN = 2.0
LIGHT_THRESHOLD_MIN = 60.0
MAX_WALL_MIN = 240.0
PEAK_HOURS_UTC = range(9, 17)
PEAK_CAP = 4
HEAVY_RUN_FRAC = 0.45
SAMPLE_DAYS = 8
SAMPLE_RUNS_PER_DAY = 12
MC_WEEKS = 6
DOW_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# PR size buckets = adaptive sharding tiers (by original work minutes).
SIZE_BUCKET_IDS = ("light", "mid", "heavy", "xheavy")
SIZE_BUCKET_META: dict[str, dict[str, str]] = {
    "light": {"label": "<60м", "hint": "без шардов"},
    "mid": {"label": "60–120м", "hint": "adaptive → 4"},
    "heavy": {"label": "120–200м", "hint": "adaptive → 8"},
    "xheavy": {"label": "≥200м", "hint": "adaptive → 12"},
}


def size_bucket_for(work_min: float) -> str:
    if work_min < LIGHT_THRESHOLD_MIN:
        return "light"
    if work_min < 120:
        return "mid"
    if work_min < 200:
        return "heavy"
    return "xheavy"

# Adaptive tier tables: (upper_exclusive_minutes, shard_count), last catches the rest.
_TIERS_PR = ((120.0, 4), (200.0, 8), (1e9, 12))  # current choose_shard_count.py
_TIERS_FAST = ((90.0, 4), (150.0, 8), (1e9, 12))  # shard sooner (lower D thresholds)
_TIERS_SOFT = ((120.0, 2), (200.0, 4), (1e9, 8))  # cheaper concurrency

# Policies: OFF + fixed + several adaptive profiles (current PR + peak/tier variants).
POLICIES: list[dict[str, Any]] = [
    {"id": "off", "label": "OFF (сейчас)", "mode": "off"},
    {"id": "s2", "label": "2 шарда", "mode": "fixed", "shards": 2},
    {"id": "s4", "label": "4 шарда", "mode": "fixed", "shards": 4},
    {"id": "s8", "label": "8 шардов", "mode": "fixed", "shards": 8},
    {"id": "s12", "label": "12 шардов", "mode": "fixed", "shards": 12},
    {
        "id": "adaptive",
        "label": "adaptive PR (peak4)",
        "mode": "adaptive",
        "light_threshold_min": LIGHT_THRESHOLD_MIN,
        "tiers": _TIERS_PR,
        "peak_cap": 4,
    },
    {
        "id": "adap_p6",
        "label": "adaptive peak6",
        "mode": "adaptive",
        "light_threshold_min": LIGHT_THRESHOLD_MIN,
        "tiers": _TIERS_PR,
        "peak_cap": 6,
    },
    {
        "id": "adap_p8",
        "label": "adaptive peak8",
        "mode": "adaptive",
        "light_threshold_min": LIGHT_THRESHOLD_MIN,
        "tiers": _TIERS_PR,
        "peak_cap": 8,
    },
    {
        "id": "adap_fast",
        "label": "adaptive fast (45/90/150)",
        "mode": "adaptive",
        "light_threshold_min": 45.0,
        "tiers": _TIERS_FAST,
        "peak_cap": 4,
    },
    {
        "id": "adap_soft",
        "label": "adaptive soft (2/4/8)",
        "mode": "adaptive",
        "light_threshold_min": LIGHT_THRESHOLD_MIN,
        "tiers": _TIERS_SOFT,
        "peak_cap": 4,
    },
]

# Multipliers on (quota − reserved). 1.0 = current folder free capacity.
QUOTA_SCALES: list[float] = [1.0, 1.25, 1.5, 2.0, 2.5, 3.0]

# Yandex Cloud Ice Lake list-price → abstract "Units" (same numbers as ₽, no currency).
# Snapshot from console calculator (64 vCPU / 256 GB / 2.36 TB NRD SSD, 100% vCPU).
YC_HOURS_PER_MONTH = 720.0  # 30d continuous
YC_UNIT_VCPU_PER_MONTH = 57139.20 / 64.0  # ≈893.11 Units / vCPU / month
YC_UNIT_RAM_GB_PER_MONTH = 60825.60 / 256.0  # 237.6 Units / GB / month
YC_UNIT_SSD_GB_PER_MONTH = 25592.11 / 2360.0  # ≈10.84 Units / GB / month (2.36 TB)


def unit_rates_per_hour() -> dict[str, float]:
    return {
        "vcpu": YC_UNIT_VCPU_PER_MONTH / YC_HOURS_PER_MONTH,
        "ram_gb": YC_UNIT_RAM_GB_PER_MONTH / YC_HOURS_PER_MONTH,
        "ssd_gb": YC_UNIT_SSD_GB_PER_MONTH / YC_HOURS_PER_MONTH,
    }


def bill_units_from_hours(vcpu_h: float, ram_h: float, ssd_h: float) -> float:
    r = unit_rates_per_hour()
    return vcpu_h * r["vcpu"] + ram_h * r["ram_gb"] + ssd_h * r["ssd_gb"]


def billing_units_meta() -> dict[str, Any]:
    r = unit_rates_per_hour()
    return {
        "label": "Units",
        "note": "YC Ice Lake list price as abstract Units (not currency). "
        "Units = vCPU-h·rate + RAM-GB-h·rate + SSD-GB-h·rate.",
        "hours_per_month": YC_HOURS_PER_MONTH,
        "per_month": {
            "vcpu": YC_UNIT_VCPU_PER_MONTH,
            "ram_gb": YC_UNIT_RAM_GB_PER_MONTH,
            "ssd_gb": YC_UNIT_SSD_GB_PER_MONTH,
        },
        "per_hour": r,
        "source": "YC console calculator snapshot: Ice Lake 100% vCPU, "
        "64vCPU/256GB/2.36TB NRD SSD → 143556.91 Units/month continuous.",
    }
# "Good speedup": total p90 ≤ 60% of OFF@current quota (i.e. −40%+).
GOOD_TOTAL_RATIO = 0.60
# And wait p90 not worse than OFF@current (queue doesn't eat the win).
GOOD_WAIT_RATIO = 1.0


def scale_key(scale: float) -> str:
    return f"{scale:g}"


def with_quota_scale(config: dict[str, Any], scale: float) -> dict[str, Any]:
    """Scale free capacity: new_quota = reserved + (quota − reserved) * scale."""
    cfg = copy.deepcopy(config)
    reserved = cfg.get("reserved") or {}
    quotas = cfg["quotas"]
    cfg["quotas"] = {
        k: max(
            int(round(reserved.get(k, 0) + (quotas[k] - reserved.get(k, 0)) * scale)),
            int(reserved.get(k, 0)) + 1,
        )
        for k in quotas
    }
    return cfg


def slim_scenario(s: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "policy_id",
        "label",
        "wait_p50",
        "wait_p90",
        "exec_p50",
        "exec_p90",
        "total_p50",
        "total_p90",
        "demand_peak_runners",
        "pct_vcpu",
        "pct_ssd",
        "pct_instances",
        "pct_ram",
        "pr_runner_hours_demanded",
        "bill_instance_hours",
        "bill_vcpu_hours",
        "bill_ssd_gb_hours",
        "bill_pr_instance_hours",
        "bill_pr_vcpu_hours",
        "bill_pr_ram_gb_hours",
        "bill_pr_ssd_gb_hours",
        "bill_pr_units",
        "bill_pr_test_instance_hours",
        "bill_pr_overhead_instance_hours",
        "saturation_pct",
        "peak_concurrency",
        "quota_limiting",
        "quota_limiting_pct",
        "over_quota",
        "week_wait_p90",
        "week_exec_p90",
        "week_total_p90",
        "week_pr_count",
        "week_pr_jobs",
        "week_pr_bill_instance_hours",
        "week_pr_bill_ram_gb_hours",
        "week_pr_bill_units",
        "n_prs",
        "by_size",
    ]
    return {k: s[k] for k in keys if k in s}


def build_scenario_grid(
    *,
    capacity_cfg: dict[str, Any],
    pr_runs_by_dow: dict[int, float],
    hour_w: dict[int, float],
    rwdi: list[float],
    asan: list[float],
    reg: dict[str, dict[str, float]],
) -> dict[str, dict[str, dict[str, Any]]]:
    """policy_id → scale_key → slim scenario (for interactive HTML)."""
    grid: dict[str, dict[str, dict[str, Any]]] = {p["id"]: {} for p in POLICIES}
    for scale in QUOTA_SCALES:
        if abs(scale - 1.0) < 1e-9:
            continue  # filled later from detail @1.0× pass
        cfg = with_quota_scale(capacity_cfg, scale)
        sk = scale_key(scale)
        print(f"Grid scale {sk}x quotas={cfg['quotas']}...", file=sys.stderr)
        for policy in POLICIES:
            # Same PR/regression arrivals across quota scales — only capacity
            # changes. (Seeding by scale made Units jitter ±1–2% and looked like
            # «quota lowers bill», which is wrong: queue wait is not billed.)
            stable = sum(ord(c) for c in policy["id"])
            random.seed(42 + stable)
            full = avg_sims(
                policy=policy,
                pr_runs_by_dow=pr_runs_by_dow,
                hour_w=hour_w,
                rwdi=rwdi,
                asan=asan,
                reg=reg,
                capacity_cfg=cfg,
            )
            grid[policy["id"]][sk] = slim_scenario(full)
    return grid


def recommend_quota_for_speedup(
    grid: dict[str, dict[str, dict[str, Any]]],
    base_quotas: dict[str, Any],
    reserved: dict[str, Any],
) -> dict[str, Any]:
    """Minimal free-capacity multiplier for a 'good' total speedup vs OFF@1.0."""
    off0 = grid["off"][scale_key(1.0)]
    target_total = off0["total_p90"] * GOOD_TOTAL_RATIO
    wait_cap = off0["wait_p90"] * GOOD_WAIT_RATIO
    per_policy: list[dict[str, Any]] = []
    best: dict[str, Any] | None = None

    for policy in POLICIES:
        if policy["id"] == "off":
            continue
        hit = None
        for scale in QUOTA_SCALES:
            s = grid[policy["id"]][scale_key(scale)]
            if s["total_p90"] <= target_total and s["wait_p90"] <= wait_cap:
                hit = {"scale": scale, "scenario": s}
                break
        def bill_inst(sc: dict[str, Any]) -> float:
            return float(sc.get("bill_pr_instance_hours") or sc.get("pr_runner_hours_demanded") or 0)

        row = {
            "policy_id": policy["id"],
            "label": policy["label"],
            "min_scale": hit["scale"] if hit else None,
            "achieved": hit is not None,
            "total_p90_at_hit": hit["scenario"]["total_p90"] if hit else None,
            "wait_p90_at_hit": hit["scenario"]["wait_p90"] if hit else None,
            "exec_p90_at_hit": hit["scenario"]["exec_p90"] if hit else None,
            "bill_pr_instance_hours_at_hit": bill_inst(hit["scenario"]) if hit else None,
            "best_total_at_3x": grid[policy["id"]][scale_key(3.0)]["total_p90"],
            "bill_pr_instance_hours_at_3x": bill_inst(grid[policy["id"]][scale_key(3.0)]),
        }
        per_policy.append(row)
        if hit and (best is None or hit["scale"] < best["scale"]):
            best = {
                "policy_id": policy["id"],
                "label": policy["label"],
                "scale": hit["scale"],
                "total_p90": hit["scenario"]["total_p90"],
                "wait_p90": hit["scenario"]["wait_p90"],
                "exec_p90": hit["scenario"]["exec_p90"],
                "bill_pr_instance_hours": bill_inst(hit["scenario"]),
                "off_bill_pr_instance_hours": bill_inst(off0),
                "off_total_p90": off0["total_p90"],
                "off_wait_p90": off0["wait_p90"],
                "off_exec_p90": off0["exec_p90"],
            }

    def scaled_quota(scale: float) -> dict[str, int]:
        return {
            k: max(
                int(round(reserved.get(k, 0) + (base_quotas[k] - reserved.get(k, 0)) * scale)),
                int(reserved.get(k, 0)) + 1,
            )
            for k in base_quotas
        }

    if best and best.get("scale") is not None:
        sq = scaled_quota(best["scale"])
        bill0 = best.get("off_bill_pr_instance_hours") or bill_inst(off0)
        bill1 = best.get("bill_pr_instance_hours") or 0
        bill_pct = (bill1 - bill0) / max(bill0, 1e-9) * 100
        why = (
            f"Для total p90 ≤ {target_total:.0f}м (−40%+ vs OFF {off0['total_p90']:.0f}м) "
            f"и wait не хуже OFF: минимум ×{best['scale']:g} free capacity при "
            f"{best['label']} → total {best['total_p90']:.0f}м, wait {best['wait_p90']:.0f}м, "
            f"exec {best['exec_p90']:.0f}м. "
            f"Биллинг PR instance-hours/week {bill0:.0f}→{bill1:.0f} ({bill_pct:+.0f}%) — "
            f"платим за VM×время, не за размер квоты. "
            f"Folder quotas ≈ instances={sq['instances']}, "
            f"vcpu={sq['vcpu']}, ram_gb={sq['ram_gb']}, nrd_ssd_gb={sq['nrd_ssd_gb']} "
            f"(reserved без изменений)."
        )
    else:
        sq = scaled_quota(3.0)
        why = (
            f"Даже при ×3 free capacity ни один режим не дал total ≤ {target_total:.0f}м "
            f"при wait ≤ {wait_cap:.0f}м. Смотри таблицу по политикам — нужен ещё больший запас "
            f"или меньше фоновой нагрузки (regression)."
        )
        best = {
            "policy_id": "s4",
            "label": "4 шарда",
            "scale": None,
            "total_p90": grid["s4"][scale_key(3.0)]["total_p90"],
            "wait_p90": grid["s4"][scale_key(3.0)]["wait_p90"],
            "exec_p90": grid["s4"][scale_key(3.0)]["exec_p90"],
            "off_total_p90": off0["total_p90"],
            "off_wait_p90": off0["wait_p90"],
            "off_exec_p90": off0["exec_p90"],
        }

    return {
        "target_total_p90": target_total,
        "target_wait_p90_max": wait_cap,
        "good_total_ratio": GOOD_TOTAL_RATIO,
        "best": best,
        "recommended_quotas": sq,
        "why": why,
        "per_policy": per_policy,
        "scales": QUOTA_SCALES,
    }


def load_capacity_config(path: Path) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError as exc:
        raise SystemExit("PyYAML required (pip install pyyaml)") from exc
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise SystemExit(f"invalid capacity config: {path}")
    return data


def free_resource_budget(config: dict[str, Any]) -> dict[str, float]:
    """Folder capacity available to auto runners after reserved × headroom."""
    quotas = config["quotas"]
    reserved = config.get("reserved") or {}
    headroom = float(config.get("headroom_fraction", 1.0))
    return {
        "instances": (quotas["instances"] - reserved.get("instances", 0)) * headroom,
        "vcpu": (quotas["vcpu"] - reserved.get("vcpu", 0)) * headroom,
        "ram_gb": (quotas["ram_gb"] - reserved.get("ram_gb", 0)) * headroom,
        "nrd_ssd_gb": (quotas["nrd_ssd_gb"] - reserved.get("nrd_ssd_gb", 0)) * headroom,
    }


def pool_budget_for_footprint(
    free: dict[str, float], fp: dict[str, float], preset_label: str
) -> dict[str, Any]:
    fits_by_resource = {
        "instances": free["instances"],
        "vcpu": free["vcpu"] / fp["vcpu"],
        "ram_gb": free["ram_gb"] / fp["ram_gb"],
        "nrd_ssd_gb": free["nrd_ssd_gb"] / fp["nrd_ssd_gb"],
    }
    limiting = min(fits_by_resource, key=fits_by_resource.get)
    budget = max(int(math.floor(fits_by_resource[limiting])), 0)
    return {
        "preset_label": preset_label,
        "footprint": fp,
        "fits_runners_by_resource": {k: round(v, 2) for k, v in fits_by_resource.items()},
        "pool_budget_runners": budget,
        "limiting_resource": limiting,
    }


def pool_budget(config: dict[str, Any], preset_label: str = "build-preset-relwithdebinfo") -> dict[str, Any]:
    """Budget for one homogeneous preset + side-by-side rwdi/asan (they differ)."""
    quotas = config["quotas"]
    reserved = config.get("reserved") or {}
    headroom = float(config.get("headroom_fraction", 1.0))
    free = free_resource_budget(config)

    fp_main = resolve_footprint(config, preset_label)
    fp_rwdi = resolve_footprint(config, "build-preset-relwithdebinfo")
    fp_asan = resolve_footprint(config, "build-preset-release-asan")
    main = pool_budget_for_footprint(free, fp_main, preset_label)
    rwdi = pool_budget_for_footprint(free, fp_rwdi, "build-preset-relwithdebinfo")
    asan = pool_budget_for_footprint(free, fp_asan, "build-preset-release-asan")

    return {
        "pool_budget_runners": main["pool_budget_runners"],
        "preset_label": preset_label,
        "quotas": quotas,
        "reserved": reserved,
        "headroom_fraction": headroom,
        "footprint": fp_main,
        "fits_runners_by_resource": main["fits_runners_by_resource"],
        "free_after_reserved_headroom": {k: round(v, 1) for k, v in free.items()},
        "limiting_resource": main["limiting_resource"],
        "by_preset": {
            "relwithdebinfo": rwdi,
            "release-asan": asan,
        },
        "how_computed": (
            f"rwdi footprint {fp_rwdi['vcpu']}vCPU/{fp_rwdi['ram_gb']}GB → "
            f"{rwdi['pool_budget_runners']} runners (limit {rwdi['limiting_resource']}); "
            f"asan footprint {fp_asan['vcpu']}vCPU/{fp_asan['ram_gb']}GB → "
            f"{asan['pool_budget_runners']} runners (limit {asan['limiting_resource']}). "
            f"Scheduler uses resource pool (not homogeneous {main['pool_budget_runners']} slots)."
        ),
    }


def run_gh_json(args: list[str]) -> Any:
    return json.loads(subprocess.check_output(["gh", "api", *args], text=True))


def workflow_id(repo: str, name: str) -> str:
    data = subprocess.check_output(
        [
            "gh",
            "api",
            f"repos/{repo}/actions/workflows",
            "--paginate",
            "--jq",
            f'.workflows[] | select(.name=="{name}") | .id',
        ],
        text=True,
    ).strip().splitlines()
    if not data:
        raise SystemExit(f"workflow not found: {name}")
    return data[0]


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def pctile(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    return s[int(p * (len(s) - 1))]


def choose_shards_adaptive(
    duration_min: float, hour_utc: int, policy: dict[str, Any] | None = None
) -> int:
    """Adaptive shard count; policy may override light/tiers/peak_cap."""
    light = float((policy or {}).get("light_threshold_min", LIGHT_THRESHOLD_MIN))
    tiers = (policy or {}).get("tiers") or _TIERS_PR
    peak_cap = (policy or {}).get("peak_cap", PEAK_CAP)
    if duration_min < light:
        n = 1
    else:
        n = int(tiers[-1][1])
        for upper_min, tier_count in tiers:
            if duration_min < float(upper_min):
                n = int(tier_count)
                break
    wall_floor = max(1, math.ceil(duration_min / MAX_WALL_MIN))
    n = max(n, wall_floor)
    if peak_cap and hour_utc in PEAK_HOURS_UTC:
        n = min(n, int(peak_cap))
    return max(n, wall_floor)


def shards_for_policy(policy: dict[str, Any], duration_min: float, hour_utc: int) -> int:
    mode = policy["mode"]
    if mode == "off":
        return 1
    if mode == "fixed":
        light = float(policy.get("light_threshold_min", LIGHT_THRESHOLD_MIN))
        if duration_min < light:
            return 1
        return int(policy["shards"])
    return choose_shards_adaptive(duration_min, hour_utc, policy)


def sharded_wall(duration_min: float, shards: int) -> float:
    if shards <= 1:
        return duration_min
    return PREPARE_MIN + duration_min / shards + MERGE_MIN


def job_wall_runners(duration_min: float, hour_utc: int, policy: dict[str, Any]) -> tuple[float, int]:
    n = shards_for_policy(policy, duration_min, hour_utc)
    return sharded_wall(duration_min, n), n


def collect_pr_daily_totals(repo: str, start: date, end: date, wf_id: str) -> list[dict[str, Any]]:
    rows = []
    cur = start
    while cur <= end:
        total = run_gh_json(
            [f"repos/{repo}/actions/workflows/{wf_id}/runs?per_page=1&created={cur.isoformat()}"]
        )["total_count"]
        rows.append({"date": cur.isoformat(), "dow": cur.weekday(), "total_count": total})
        cur += timedelta(days=1)
    return rows


def sample_pr_observations(
    repo: str, days: list[date], wf_id: str
) -> tuple[list[dict[str, Any]], dict[str, list[float]], dict[int, float]]:
    """Empirical PR observations: wait / exec / total + duration pools + hour weights."""
    observations: list[dict[str, Any]] = []
    by_preset: dict[str, list[float]] = defaultdict(list)
    hour_counts: dict[int, int] = defaultdict(int)

    for day in days:
        data = run_gh_json(
            [f"repos/{repo}/actions/workflows/{wf_id}/runs?per_page=30&created={day.isoformat()}"]
        )
        runs = [
            r
            for r in data.get("workflow_runs", [])
            if r.get("conclusion") in ("success", "failure")
        ][:SAMPLE_RUNS_PER_DAY]
        for run in runs:
            created = parse_ts(run.get("created_at"))
            if created:
                hour_counts[created.hour] += 1
            jobs = run_gh_json(
                [f"repos/{repo}/actions/runs/{run['id']}/jobs?per_page=100"]
            ).get("jobs", [])
            heavy: list[tuple[datetime, datetime, float, str]] = []
            for job in jobs:
                labels = job.get("labels") or []
                preset = next((l for l in labels if l.startswith("build-preset-")), None)
                if not preset:
                    continue
                started, completed = parse_ts(job.get("started_at")), parse_ts(job.get("completed_at"))
                if not started or not completed:
                    continue
                dur = (completed - started).total_seconds() / 60.0
                if dur < 0.5:
                    continue
                by_preset[preset].append(dur)
                heavy.append((started, completed, dur, preset))
            if not created or not heavy:
                continue
            first_start = min(h[0] for h in heavy)
            last_end = max(h[1] for h in heavy)
            wait_min = max(0.0, (first_start - created).total_seconds() / 60.0)
            exec_min = max(h[2] for h in heavy)  # critical path ≈ slowest heavy preset
            # overlap-aware wall of heavy phase
            heavy_wall = (last_end - first_start).total_seconds() / 60.0
            total_min = (last_end - created).total_seconds() / 60.0
            observations.append(
                {
                    "dow": created.weekday(),
                    "hour": created.hour,
                    "wait_min": wait_min,
                    "exec_min": exec_min,
                    "heavy_wall_min": heavy_wall,
                    "total_min": total_min,
                    "rwdi": next((h[2] for h in heavy if "relwithdebinfo" in h[3]), None),
                    "asan": next((h[2] for h in heavy if "asan" in h[3]), None),
                }
            )

    total_h = sum(hour_counts.values())
    if total_h == 0:
        hour_w = {h: 1 / 24 for h in range(24)}
    else:
        hour_w = {h: (hour_counts.get(h, 0) + 0.5) / (total_h + 12.0) for h in range(24)}
        s = sum(hour_w.values())
        hour_w = {h: v / s for h, v in hour_w.items()}
    return observations, dict(by_preset), hour_w


def summarize_obs(obs: list[dict[str, Any]]) -> dict[str, Any]:
    if not obs:
        return {"n": 0}

    def pack(vals: list[float]) -> dict[str, float]:
        return {
            "p50": statistics.median(vals),
            "p90": pctile(vals, 0.9),
            "mean": statistics.mean(vals),
        }

    by_dow: list[dict[str, Any]] = []
    for dow in range(7):
        rows = [o for o in obs if o["dow"] == dow]
        if not rows:
            by_dow.append({"dow": dow, "name": DOW_NAMES[dow], "n": 0})
            continue
        by_dow.append(
            {
                "dow": dow,
                "name": DOW_NAMES[dow],
                "n": len(rows),
                "wait": pack([o["wait_min"] for o in rows]),
                "exec": pack([o["exec_min"] for o in rows]),
                "total": pack([o["total_min"] for o in rows]),
            }
        )

    by_hour: list[dict[str, Any]] = []
    for hour in range(24):
        rows = [o for o in obs if o["hour"] == hour]
        if not rows:
            by_hour.append({"hour": hour, "n": 0})
            continue
        by_hour.append(
            {
                "hour": hour,
                "n": len(rows),
                "wait": pack([o["wait_min"] for o in rows]),
                "exec": pack([o["exec_min"] for o in rows]),
                "total": pack([o["total_min"] for o in rows]),
            }
        )

    return {
        "n": len(obs),
        "wait": pack([o["wait_min"] for o in obs]),
        "exec": pack([o["exec_min"] for o in obs]),
        "total": pack([o["total_min"] for o in obs]),
        "by_dow": by_dow,
        "by_hour": by_hour,
    }


def collect_workflow_summary(repo: str, name: str, start: date, end: date) -> dict[str, Any]:
    wf = workflow_id(repo, name)
    created = f"{start.isoformat()}..{end.isoformat()}"
    durs: list[float] = []
    page = 1
    while page <= 15:
        data = run_gh_json(
            [f"repos/{repo}/actions/workflows/{wf}/runs?per_page=100&page={page}&created={created}"]
        )
        runs = data.get("workflow_runs") or []
        if not runs:
            break
        for run in runs:
            if not run.get("conclusion"):
                continue
            started = parse_ts(run.get("run_started_at") or run.get("created_at"))
            ended = parse_ts(run.get("updated_at"))
            if not started or not ended:
                continue
            dur = (ended - started).total_seconds() / 60.0
            if 0 <= dur <= 36 * 60:
                durs.append(dur)
        if len(runs) < 100:
            break
        page += 1
    if not durs:
        return {"name": name, "n": 0}
    durs_sorted = sorted(durs)
    return {
        "name": name,
        "n": len(durs),
        "runs_per_day": len(durs) / max((end - start).days, 1),
        "p50_min": statistics.median(durs),
        "p90_min": durs_sorted[int(0.9 * (len(durs_sorted) - 1))],
    }


@dataclass
class JobReq:
    ready_at: float
    wall: float
    runners: int
    vcpu: float
    ram_gb: float
    ssd_gb: float
    kind: str  # pr | reg
    slot: int
    pr_id: int | None = None
    # Original heavy-job work minutes (before /shards). Used to split bill into
    # conserved test-time vs prepare/merge overhead.
    work_min: float = 0.0


def resolve_footprint(capacity_cfg: dict[str, Any], label: str) -> dict[str, float]:
    fps = capacity_cfg.get("footprints") or {}
    fp = fps.get(label) or capacity_cfg.get("default_footprint") or {}
    return {
        "vcpu": float(fp["vcpu"]),
        "ram_gb": float(fp["ram_gb"]),
        "nrd_ssd_gb": float(fp["nrd_ssd_gb"]),
    }


def demand_resource_peaks(jobs: list[JobReq]) -> dict[str, float]:
    """Uncapped: every job starts at ready_at. Peak concurrent resources."""
    events: list[tuple[float, int, float, float, float, float]] = []
    for j in jobs:
        events.append((j.ready_at, 1, float(j.runners), j.vcpu, j.ram_gb, j.ssd_gb))
        events.append((j.ready_at + j.wall, 0, -float(j.runners), -j.vcpu, -j.ram_gb, -j.ssd_gb))
    events.sort(key=lambda x: (x[0], x[1]))  # releases before acquires at same t
    cur_r = cur_v = cur_m = cur_s = 0.0
    peak_r = peak_v = peak_m = peak_s = 0.0
    for _t, _kind, dr, dv, dm, ds in events:
        cur_r += dr
        cur_v += dv
        cur_m += dm
        cur_s += ds
        peak_r = max(peak_r, cur_r)
        peak_v = max(peak_v, cur_v)
        peak_m = max(peak_m, cur_m)
        peak_s = max(peak_s, cur_s)
    def bill(jobs_subset: list[JobReq]) -> dict[str, float]:
        # Charged only while VMs run: Σ (resource × wall_minutes) / 60.
        return {
            "instance_hours": sum(j.runners * j.wall for j in jobs_subset) / 60.0,
            "vcpu_hours": sum(j.vcpu * j.wall for j in jobs_subset) / 60.0,
            "ram_gb_hours": sum(j.ram_gb * j.wall for j in jobs_subset) / 60.0,
            "ssd_gb_hours": sum(j.ssd_gb * j.wall for j in jobs_subset) / 60.0,
        }

    all_bill = bill(jobs)
    pr_bill = bill([j for j in jobs if j.kind == "pr"])
    return {
        "demand_peak_runners": peak_r,
        "demand_peak_vcpu": peak_v,
        "demand_peak_ram_gb": peak_m,
        "demand_peak_ssd_gb": peak_s,
        "runner_hours_demanded": all_bill["instance_hours"],
        "pr_runner_hours_demanded": pr_bill["instance_hours"],
        "bill_instance_hours": all_bill["instance_hours"],
        "bill_vcpu_hours": all_bill["vcpu_hours"],
        "bill_ram_gb_hours": all_bill["ram_gb_hours"],
        "bill_ssd_gb_hours": all_bill["ssd_gb_hours"],
        "bill_pr_instance_hours": pr_bill["instance_hours"],
        "bill_pr_vcpu_hours": pr_bill["vcpu_hours"],
        "bill_pr_ram_gb_hours": pr_bill["ram_gb_hours"],
        "bill_pr_ssd_gb_hours": pr_bill["ssd_gb_hours"],
    }


def quota_pcts(
    demand: dict[str, float],
    quotas: dict[str, Any],
    reserved: dict[str, Any],
) -> dict[str, float]:
    """Peak demand + reserved as % of folder quotas."""
    inst = reserved.get("instances", 0) + demand["demand_peak_runners"]
    vcpu = reserved.get("vcpu", 0) + demand["demand_peak_vcpu"]
    ram = reserved.get("ram_gb", 0) + demand["demand_peak_ram_gb"]
    ssd = reserved.get("nrd_ssd_gb", 0) + demand["demand_peak_ssd_gb"]
    return {
        "peak_instances_total": inst,
        "peak_vcpu_total": vcpu,
        "peak_ram_gb_total": ram,
        "peak_ssd_gb_total": ssd,
        "pct_instances": 100.0 * inst / max(quotas["instances"], 1),
        "pct_vcpu": 100.0 * vcpu / max(quotas["vcpu"], 1),
        "pct_ram": 100.0 * ram / max(quotas["ram_gb"], 1),
        "pct_ssd": 100.0 * ssd / max(quotas["nrd_ssd_gb"], 1),
    }


def _scale_job_to_fit(j: JobReq, cap: dict[str, float]) -> None:
    """Shrink a single job so it can eventually start in an empty resource pool."""
    if j.runners <= 0:
        return
    scale = min(
        cap["instances"] / j.runners,
        cap["vcpu"] / max(j.vcpu, 1e-9),
        cap["ram_gb"] / max(j.ram_gb, 1e-9),
        cap["nrd_ssd_gb"] / max(j.ssd_gb, 1e-9),
        1.0,
    )
    if scale < 1.0 - 1e-12:
        j.runners = max(1, int(math.floor(j.runners * scale)))
        j.vcpu *= scale
        j.ram_gb *= scale
        j.ssd_gb *= scale


def simulate_week(
    *,
    policy: dict[str, Any],
    pr_runs_by_dow: dict[int, float],
    hour_w: dict[int, float],
    rwdi: list[float],
    asan: list[float],
    reg: dict[str, dict[str, float]],
    capacity_cfg: dict[str, Any],
    pool: int | None = None,  # unused; kept for call-site compat
) -> dict[str, Any]:
    """Simulate one week with resource-aware queue (rwdi ≠ asan footprints)."""
    del pool
    fp_rwdi = resolve_footprint(capacity_cfg, "build-preset-relwithdebinfo")
    fp_asan = resolve_footprint(capacity_cfg, "build-preset-release-asan")
    fp_default = resolve_footprint(capacity_cfg, "default")
    quotas = capacity_cfg["quotas"]
    reserved = capacity_cfg.get("reserved") or {}
    cap = free_resource_budget(capacity_cfg)

    jobs: list[JobReq] = []
    pr_meta: dict[int, dict[str, Any]] = {}
    pr_seq = 0

    for dow in range(7):
        n_runs = int(round(pr_runs_by_dow.get(dow, 0) * HEAVY_RUN_FRAC))
        for _ in range(n_runs):
            r = random.random()
            acc = 0.0
            hour = 12
            for h in range(24):
                acc += hour_w.get(h, 0.0)
                if r <= acc:
                    hour = h
                    break
            ready = float(dow * 24 * 60 + hour * 60 + random.randint(0, 59))
            slot = dow * 24 + hour
            walls: list[float] = []
            runners_list: list[int] = []
            works: list[float] = []
            for arr, fp in (
                (rwdi, fp_rwdi),
                (asan if asan else rwdi, fp_asan if asan else fp_rwdi),
            ):
                d = random.choice(arr) if arr else 30.0
                wall, runners = job_wall_runners(d, hour, policy)
                walls.append(wall)
                runners_list.append(runners)
                works.append(d)
                jobs.append(
                    JobReq(
                        ready,
                        wall,
                        runners,
                        runners * fp["vcpu"],
                        runners * fp["ram_gb"],
                        runners * fp["nrd_ssd_gb"],
                        "pr",
                        slot,
                        pr_seq,
                        work_min=d,
                    )
                )
            work_min = max(works) if works else 0.0
            pr_meta[pr_seq] = {
                "slot": slot,
                "dow": dow,
                "hour": hour,
                "exec_min": max(walls) if walls else 0.0,
                "runners_total": sum(runners_list),
                "work_min": work_min,
                "size": size_bucket_for(work_min),
            }
            pr_seq += 1

        for cfg in reg.values():
            expected = cfg["runs_per_day"]
            n_today = 1 if random.random() < min(1.0, expected) else 0
            if expected > 1 and random.random() < (expected - 1):
                n_today += 1
            for _ in range(n_today):
                hour = int(cfg["start_hour"])
                ready = float(dow * 24 * 60 + hour * 60 + random.randint(0, 30))
                d = cfg["p50_min"] * random.uniform(0.85, 1.15)
                n_reg = int(cfg["heavy_jobs"])
                jobs.append(
                    JobReq(
                        ready,
                        d,
                        n_reg,
                        n_reg * fp_default["vcpu"],
                        n_reg * fp_default["ram_gb"],
                        n_reg * fp_default["nrd_ssd_gb"],
                        "reg",
                        dow * 24 + hour,
                        None,
                        work_min=d,
                    )
                )

    demand = demand_resource_peaks(jobs)
    qpct = quota_pcts(demand, quotas, reserved)

    for j in jobs:
        _scale_job_to_fit(j, cap)

    jobs.sort(key=lambda j: (j.ready_at, 0 if j.kind == "reg" else 1))

    free = dict(cap)  # instances, vcpu, ram_gb, nrd_ssd_gb
    busy: list[tuple[float, int, JobReq]] = []  # (end, seq, job)
    pending: deque[JobReq] = deque()
    pr_start: dict[int, list[float]] = defaultdict(list)
    pr_ready: dict[int, float] = {}
    # (start_min, end_min, runners, vcpu, ram_gb, ssd_gb, size) for PR jobs.
    pr_run_intervals: list[tuple[float, float, float, float, float, float, str]] = []
    peak_running = 0.0
    peak_vcpu = 0.0
    queued_runner_hours = 0.0
    runner_hours_actual = 0.0
    saturated_min = 0.0
    prev_t = 0.0
    running = 0.0
    running_vcpu = 0.0
    busy_seq = 0
    instances_cap = max(cap["instances"], 1e-9)

    def fits(j: JobReq) -> bool:
        return (
            free["instances"] + 1e-9 >= j.runners
            and free["vcpu"] + 1e-9 >= j.vcpu
            and free["ram_gb"] + 1e-9 >= j.ram_gb
            and free["nrd_ssd_gb"] + 1e-9 >= j.ssd_gb
        )

    def release(t: float) -> None:
        nonlocal running, running_vcpu
        while busy and busy[0][0] <= t + 1e-9:
            _, _, j = heapq.heappop(busy)
            free["instances"] += j.runners
            free["vcpu"] += j.vcpu
            free["ram_gb"] += j.ram_gb
            free["nrd_ssd_gb"] += j.ssd_gb
            running -= j.runners
            running_vcpu -= j.vcpu

    def try_start(t: float) -> None:
        nonlocal peak_running, peak_vcpu, queued_runner_hours, running, running_vcpu, busy_seq
        while pending and fits(pending[0]):
            j = pending.popleft()
            wait = max(0.0, t - j.ready_at)
            if j.kind == "pr" and wait > 0:
                queued_runner_hours += wait * j.runners / 60.0
            free["instances"] -= j.runners
            free["vcpu"] -= j.vcpu
            free["ram_gb"] -= j.ram_gb
            free["nrd_ssd_gb"] -= j.ssd_gb
            running += j.runners
            running_vcpu += j.vcpu
            peak_running = max(peak_running, running)
            peak_vcpu = max(peak_vcpu, running_vcpu)
            busy_seq += 1
            heapq.heappush(busy, (t + j.wall, busy_seq, j))
            if j.kind == "pr":
                sz = "mid"
                if j.pr_id is not None and j.pr_id in pr_meta:
                    sz = str(pr_meta[j.pr_id].get("size") or "mid")
                elif j.work_min:
                    sz = size_bucket_for(j.work_min)
                pr_run_intervals.append(
                    (t, t + j.wall, float(j.runners), float(j.vcpu), float(j.ram_gb), float(j.ssd_gb), sz)
                )
            if j.pr_id is not None:
                pr_start[j.pr_id].append(t)
                pr_ready[j.pr_id] = j.ready_at

    def account_to(t: float) -> None:
        nonlocal runner_hours_actual, saturated_min, prev_t
        dt = t - prev_t
        if dt > 0:
            runner_hours_actual += running * dt / 60.0
            if pending or free["instances"] < 1:
                saturated_min += dt
        prev_t = t

    i = 0
    while i < len(jobs) or pending or busy:
        next_arrival = jobs[i].ready_at if i < len(jobs) else None
        next_done = busy[0][0] if busy else None
        if next_arrival is None and next_done is None:
            break
        if next_arrival is None:
            t = next_done
        elif next_done is None:
            t = next_arrival
        else:
            t = min(next_arrival, next_done)
        account_to(t)
        release(t)
        while i < len(jobs) and jobs[i].ready_at <= t + 1e-9:
            pending.append(jobs[i])
            i += 1
        try_start(t)

    week_min = 7 * 24 * 60.0
    wait_by_slot: dict[int, list[float]] = defaultdict(list)
    exec_by_slot: dict[int, list[float]] = defaultdict(list)
    total_by_slot: dict[int, list[float]] = defaultdict(list)
    wait_by_size_slot: dict[str, dict[int, list[float]]] = {
        s: defaultdict(list) for s in SIZE_BUCKET_IDS
    }
    exec_by_size_slot: dict[str, dict[int, list[float]]] = {
        s: defaultdict(list) for s in SIZE_BUCKET_IDS
    }
    total_by_size_slot: dict[str, dict[int, list[float]]] = {
        s: defaultdict(list) for s in SIZE_BUCKET_IDS
    }
    waits_by_size: dict[str, list[float]] = {s: [] for s in SIZE_BUCKET_IDS}
    execs_by_size: dict[str, list[float]] = {s: [] for s in SIZE_BUCKET_IDS}
    totals_by_size: dict[str, list[float]] = {s: [] for s in SIZE_BUCKET_IDS}
    waits: list[float] = []
    execs: list[float] = []
    totals: list[float] = []

    for pid, meta in pr_meta.items():
        starts = pr_start.get(pid, [])
        if not starts:
            continue
        ready = pr_ready.get(pid)
        if ready is None:
            continue
        wait = max(0.0, max(starts) - ready)
        exec_min = meta["exec_min"]
        total = wait + exec_min
        waits.append(wait)
        execs.append(exec_min)
        totals.append(total)
        slot = meta["slot"]
        wait_by_slot[slot].append(wait)
        exec_by_slot[slot].append(exec_min)
        total_by_slot[slot].append(total)
        sz = str(meta.get("size") or size_bucket_for(float(meta.get("work_min") or 0.0)))
        if sz not in waits_by_size:
            sz = "mid"
        waits_by_size[sz].append(wait)
        execs_by_size[sz].append(exec_min)
        totals_by_size[sz].append(total)
        wait_by_size_slot[sz][slot].append(wait)
        exec_by_size_slot[sz][slot].append(exec_min)
        total_by_size_slot[sz][slot].append(total)

    def slot_pack(src: dict[int, list[float]], key: str) -> list[float]:
        out = [0.0] * (7 * 24)
        for slot, vals in src.items():
            if vals:
                out[slot] = statistics.mean(vals) if key == "mean" else pctile(vals, 0.9)
        return out

    by_day = []
    for dow in range(7):
        slots = range(dow * 24, (dow + 1) * 24)
        w = [x for s in slots for x in wait_by_slot.get(s, [])]
        e = [x for s in slots for x in exec_by_slot.get(s, [])]
        tot = [x for s in slots for x in total_by_slot.get(s, [])]
        by_day.append(
            {
                "dow": dow,
                "name": DOW_NAMES[dow],
                "n": len(w),
                "wait_p50": statistics.median(w) if w else 0.0,
                "wait_p90": pctile(w, 0.9) if w else 0.0,
                "exec_p50": statistics.median(e) if e else 0.0,
                "exec_p90": pctile(e, 0.9) if e else 0.0,
                "total_p50": statistics.median(tot) if tot else 0.0,
                "total_p90": pctile(tot, 0.9) if tot else 0.0,
            }
        )

    by_hour = []
    for hour in range(24):
        w = [x for d in range(7) for x in wait_by_slot.get(d * 24 + hour, [])]
        e = [x for d in range(7) for x in exec_by_slot.get(d * 24 + hour, [])]
        tot = [x for d in range(7) for x in total_by_slot.get(d * 24 + hour, [])]
        by_hour.append(
            {
                "hour": hour,
                "n": len(w),
                "wait_p50": statistics.median(w) if w else 0.0,
                "wait_p90": pctile(w, 0.9) if w else 0.0,
                "exec_p50": statistics.median(e) if e else 0.0,
                "exec_p90": pctile(e, 0.9) if e else 0.0,
                "total_p50": statistics.median(tot) if tot else 0.0,
                "total_p90": pctile(tot, 0.9) if tot else 0.0,
            }
        )

    limiting = max(
        ("instances", qpct["pct_instances"]),
        ("vcpu", qpct["pct_vcpu"]),
        ("ram_gb", qpct["pct_ram"]),
        ("nrd_ssd_gb", qpct["pct_ssd"]),
        key=lambda x: x[1],
    )

    # Billable usage = time resources are actually running (not quota ceiling).
    # For shards: N * (prep + D/N + merge) = D + N*(prep+merge).
    # Rotation shortens each VM, but Σ(runners×wall) still grows by overhead.
    def bill_hours(subset: list[JobReq]) -> dict[str, float]:
        inst = sum(j.runners * j.wall for j in subset) / 60.0
        # Conserved test machine-time ≈ original work D (one "VM-hour of tests").
        test_inst = sum((j.work_min or j.wall) for j in subset) / 60.0
        overhead_inst = max(0.0, inst - test_inst)
        return {
            "instance_hours": inst,
            "vcpu_hours": sum(j.vcpu * j.wall for j in subset) / 60.0,
            "ram_gb_hours": sum(j.ram_gb * j.wall for j in subset) / 60.0,
            "ssd_gb_hours": sum(j.ssd_gb * j.wall for j in subset) / 60.0,
            "test_instance_hours": test_inst,
            "overhead_instance_hours": overhead_inst,
        }

    bill_all = bill_hours(jobs)
    bill_pr = bill_hours([j for j in jobs if j.kind == "pr"])

    week_pr_count = [0.0] * (7 * 24)
    week_pr_count_by_size = {s: [0.0] * (7 * 24) for s in SIZE_BUCKET_IDS}
    for meta in pr_meta.values():
        week_pr_count[meta["slot"]] += 1.0
        sz = str(meta.get("size") or "mid")
        if sz in week_pr_count_by_size:
            week_pr_count_by_size[sz][meta["slot"]] += 1.0
    week_pr_jobs = [0.0] * (7 * 24)
    week_pr_jobs_by_size = {s: [0.0] * (7 * 24) for s in SIZE_BUCKET_IDS}
    for j in jobs:
        if j.kind == "pr":
            week_pr_jobs[j.slot] += float(j.runners)
            sz = "mid"
            if j.pr_id is not None and j.pr_id in pr_meta:
                sz = str(pr_meta[j.pr_id].get("size") or "mid")
            elif j.work_min:
                sz = size_bucket_for(j.work_min)
            if sz in week_pr_jobs_by_size:
                week_pr_jobs_by_size[sz][j.slot] += float(j.runners)
    # Billable PR instance-hours / Units in each UTC hour (when VMs actually run).
    rates = unit_rates_per_hour()
    week_pr_bill_instance_hours = [0.0] * (7 * 24)
    week_pr_bill_ram_gb_hours = [0.0] * (7 * 24)
    week_pr_bill_units = [0.0] * (7 * 24)
    week_pr_bill_by_size = {s: [0.0] * (7 * 24) for s in SIZE_BUCKET_IDS}
    week_pr_bill_ram_by_size = {s: [0.0] * (7 * 24) for s in SIZE_BUCKET_IDS}
    week_pr_bill_units_by_size = {s: [0.0] * (7 * 24) for s in SIZE_BUCKET_IDS}
    week_end_min = 7 * 24 * 60.0
    for start, end, runners, vcpu, ram_gb, ssd_gb, sz in pr_run_intervals:
        t = max(0.0, start)
        end = min(end, week_end_min)
        while t < end - 1e-9:
            slot = min(int(t // 60.0), 7 * 24 - 1)
            slot_end = float((slot + 1) * 60)
            seg = min(end, slot_end) - t
            if seg > 0:
                hours = runners * seg / 60.0
                ram_h = ram_gb * seg / 60.0
                units = (
                    vcpu * (seg / 60.0) * rates["vcpu"]
                    + ram_h * rates["ram_gb"]
                    + ssd_gb * (seg / 60.0) * rates["ssd_gb"]
                )
                week_pr_bill_instance_hours[slot] += hours
                week_pr_bill_ram_gb_hours[slot] += ram_h
                week_pr_bill_units[slot] += units
                if sz in week_pr_bill_by_size:
                    week_pr_bill_by_size[sz][slot] += hours
                    week_pr_bill_ram_by_size[sz][slot] += ram_h
                    week_pr_bill_units_by_size[sz][slot] += units
            t = slot_end

    by_size: dict[str, dict[str, Any]] = {}
    for sz in SIZE_BUCKET_IDS:
        w = waits_by_size[sz]
        e = execs_by_size[sz]
        tot = totals_by_size[sz]
        by_size[sz] = {
            "id": sz,
            "label": SIZE_BUCKET_META[sz]["label"],
            "hint": SIZE_BUCKET_META[sz]["hint"],
            "n": float(len(w)),
            "wait_p90": pctile(w, 0.9) if w else 0.0,
            "exec_p90": pctile(e, 0.9) if e else 0.0,
            "total_p90": pctile(tot, 0.9) if tot else 0.0,
            "week_wait_p90": slot_pack(wait_by_size_slot[sz], "p90"),
            "week_exec_p90": slot_pack(exec_by_size_slot[sz], "p90"),
            "week_total_p90": slot_pack(total_by_size_slot[sz], "p90"),
            "week_pr_count": week_pr_count_by_size[sz],
            "week_pr_jobs": week_pr_jobs_by_size[sz],
            "week_pr_bill_instance_hours": week_pr_bill_by_size[sz],
            "week_pr_bill_ram_gb_hours": week_pr_bill_ram_by_size[sz],
            "week_pr_bill_units": week_pr_bill_units_by_size[sz],
        }

    return {
        "policy_id": policy["id"],
        "label": policy["label"],
        "peak_concurrency": peak_running,
        "demand_peak_runners": demand["demand_peak_runners"],
        "demand_peak_vcpu": demand["demand_peak_vcpu"],
        "demand_peak_ram_gb": demand["demand_peak_ram_gb"],
        "demand_peak_ssd_gb": demand["demand_peak_ssd_gb"],
        "peak_instances_total": qpct["peak_instances_total"],
        "peak_vcpu_total": qpct["peak_vcpu_total"],
        "peak_ram_gb_total": qpct["peak_ram_gb_total"],
        "peak_ssd_gb_total": qpct["peak_ssd_gb_total"],
        "pct_instances": qpct["pct_instances"],
        "pct_vcpu": qpct["pct_vcpu"],
        "pct_ram": qpct["pct_ram"],
        "pct_ssd": qpct["pct_ssd"],
        "quota_limiting": limiting[0],
        "quota_limiting_pct": limiting[1],
        "runner_hours_demanded": bill_all["instance_hours"],
        "pr_runner_hours_demanded": bill_pr["instance_hours"],
        "bill_instance_hours": bill_all["instance_hours"],
        "bill_vcpu_hours": bill_all["vcpu_hours"],
        "bill_ram_gb_hours": bill_all["ram_gb_hours"],
        "bill_ssd_gb_hours": bill_all["ssd_gb_hours"],
        "bill_pr_instance_hours": bill_pr["instance_hours"],
        "bill_pr_vcpu_hours": bill_pr["vcpu_hours"],
        "bill_pr_ram_gb_hours": bill_pr["ram_gb_hours"],
        "bill_pr_ssd_gb_hours": bill_pr["ssd_gb_hours"],
        "bill_pr_units": bill_units_from_hours(
            bill_pr["vcpu_hours"], bill_pr["ram_gb_hours"], bill_pr["ssd_gb_hours"]
        ),
        "bill_pr_test_instance_hours": bill_pr["test_instance_hours"],
        "bill_pr_overhead_instance_hours": bill_pr["overhead_instance_hours"],
        "runner_hours_actual": runner_hours_actual,
        "avg_runners": runner_hours_actual / (7 * 24),
        "pool_util_pct": 100.0 * runner_hours_actual / (instances_cap * 7 * 24),
        "saturation_pct": 100.0 * saturated_min / week_min,
        "peak_vcpu_running": peak_vcpu,
        "queued_runner_hours": queued_runner_hours,
        "over_quota": limiting[1] > 100.0,
        "wait_p50": statistics.median(waits) if waits else 0.0,
        "wait_p90": pctile(waits, 0.9) if waits else 0.0,
        "exec_p50": statistics.median(execs) if execs else 0.0,
        "exec_p90": pctile(execs, 0.9) if execs else 0.0,
        "total_p50": statistics.median(totals) if totals else 0.0,
        "total_p90": pctile(totals, 0.9) if totals else 0.0,
        "n_prs": len(waits),
        "by_day": by_day,
        "by_hour": by_hour,
        "week_wait_p90": slot_pack(wait_by_slot, "p90"),
        "week_exec_p90": slot_pack(exec_by_slot, "p90"),
        "week_total_p90": slot_pack(total_by_slot, "p90"),
        # Arrivals per hour-slot: PRs ready, and GitHub jobs they spawn
        # (rwdi+asan; each shard counts as a job → sum of runners).
        "week_pr_count": week_pr_count,
        "week_pr_jobs": week_pr_jobs,
        "week_pr_bill_instance_hours": week_pr_bill_instance_hours,
        "week_pr_bill_ram_gb_hours": week_pr_bill_ram_gb_hours,
        "week_pr_bill_units": week_pr_bill_units,
        "by_size": by_size,
    }


def avg_sims(**kwargs: Any) -> dict[str, Any]:
    rows = [simulate_week(**kwargs) for _ in range(MC_WEEKS)]
    keys = [
        "peak_concurrency",
        "peak_vcpu_running",
        "demand_peak_runners",
        "demand_peak_vcpu",
        "demand_peak_ram_gb",
        "demand_peak_ssd_gb",
        "peak_instances_total",
        "peak_vcpu_total",
        "peak_ram_gb_total",
        "peak_ssd_gb_total",
        "pct_instances",
        "pct_vcpu",
        "pct_ram",
        "pct_ssd",
        "quota_limiting_pct",
        "runner_hours_demanded",
        "pr_runner_hours_demanded",
        "bill_instance_hours",
        "bill_vcpu_hours",
        "bill_ram_gb_hours",
        "bill_ssd_gb_hours",
        "bill_pr_instance_hours",
        "bill_pr_vcpu_hours",
        "bill_pr_ram_gb_hours",
        "bill_pr_ssd_gb_hours",
        "bill_pr_units",
        "bill_pr_test_instance_hours",
        "bill_pr_overhead_instance_hours",
        "runner_hours_actual",
        "avg_runners",
        "pool_util_pct",
        "saturation_pct",
        "queued_runner_hours",
        "wait_p50",
        "wait_p90",
        "exec_p50",
        "exec_p90",
        "total_p50",
        "total_p90",
        "n_prs",
    ]
    out: dict[str, Any] = {
        "policy_id": kwargs["policy"]["id"],
        "label": kwargs["policy"]["label"],
    }
    for k in keys:
        out[k] = statistics.mean([r[k] for r in rows])
    # majority vote for limiting resource label
    lim_counts: dict[str, int] = defaultdict(int)
    for r in rows:
        lim_counts[r["quota_limiting"]] += 1
    out["quota_limiting"] = max(lim_counts, key=lim_counts.get)
    out["over_quota"] = out["quota_limiting_pct"] > 100.0

    by_day = []
    for dow in range(7):
        by_day.append(
            {
                "dow": dow,
                "name": DOW_NAMES[dow],
                "n": statistics.mean([r["by_day"][dow]["n"] for r in rows]),
                "wait_p50": statistics.mean([r["by_day"][dow]["wait_p50"] for r in rows]),
                "wait_p90": statistics.mean([r["by_day"][dow]["wait_p90"] for r in rows]),
                "exec_p50": statistics.mean([r["by_day"][dow]["exec_p50"] for r in rows]),
                "exec_p90": statistics.mean([r["by_day"][dow]["exec_p90"] for r in rows]),
                "total_p50": statistics.mean([r["by_day"][dow]["total_p50"] for r in rows]),
                "total_p90": statistics.mean([r["by_day"][dow]["total_p90"] for r in rows]),
            }
        )
    by_hour = []
    for hour in range(24):
        by_hour.append(
            {
                "hour": hour,
                "n": statistics.mean([r["by_hour"][hour]["n"] for r in rows]),
                "wait_p50": statistics.mean([r["by_hour"][hour]["wait_p50"] for r in rows]),
                "wait_p90": statistics.mean([r["by_hour"][hour]["wait_p90"] for r in rows]),
                "exec_p50": statistics.mean([r["by_hour"][hour]["exec_p50"] for r in rows]),
                "exec_p90": statistics.mean([r["by_hour"][hour]["exec_p90"] for r in rows]),
                "total_p50": statistics.mean([r["by_hour"][hour]["total_p50"] for r in rows]),
                "total_p90": statistics.mean([r["by_hour"][hour]["total_p90"] for r in rows]),
            }
        )
    out["by_day"] = by_day
    out["by_hour"] = by_hour
    for wk in (
        "week_wait_p90",
        "week_exec_p90",
        "week_total_p90",
        "week_pr_count",
        "week_pr_jobs",
        "week_pr_bill_instance_hours",
        "week_pr_bill_ram_gb_hours",
        "week_pr_bill_units",
    ):
        acc = [0.0] * (7 * 24)
        for r in rows:
            series = r.get(wk) or []
            for i, v in enumerate(series):
                acc[i] += v
        out[wk] = [v / len(rows) for v in acc]

    by_size: dict[str, dict[str, Any]] = {}
    n_rows = len(rows)
    for sz in SIZE_BUCKET_IDS:
        meta = SIZE_BUCKET_META[sz]
        entry: dict[str, Any] = {
            "id": sz,
            "label": meta["label"],
            "hint": meta["hint"],
        }
        for k in ("n", "wait_p90", "exec_p90", "total_p90"):
            entry[k] = statistics.mean([r["by_size"][sz][k] for r in rows])
        for wk in (
            "week_wait_p90",
            "week_exec_p90",
            "week_total_p90",
            "week_pr_count",
            "week_pr_jobs",
            "week_pr_bill_instance_hours",
            "week_pr_bill_ram_gb_hours",
            "week_pr_bill_units",
        ):
            acc = [0.0] * (7 * 24)
            for r in rows:
                series = r["by_size"][sz].get(wk) or []
                for i, v in enumerate(series):
                    acc[i] += v
            entry[wk] = [v / n_rows for v in acc]
        by_size[sz] = entry
    out["by_size"] = by_size
    return out


def _scenario_units(s: dict[str, Any]) -> float:
    if s.get("bill_pr_units") is not None:
        return float(s["bill_pr_units"])
    return bill_units_from_hours(
        float(s.get("bill_pr_vcpu_hours") or 0),
        float(s.get("bill_pr_ram_gb_hours") or 0),
        float(s.get("bill_pr_ssd_gb_hours") or 0),
    )


def pick_tradeoff(
    scenarios: list[dict[str, Any]],
    *,
    metric_name: str,
    metric_get: Any,
    weight: float = 1.0,
) -> dict[str, Any]:
    """Minimize score = (total/off_total) + weight*(metric/off_metric). Lower is better.

    Includes OFF as a candidate so «лучше не шардировать» can win.
    """
    off = next(s for s in scenarios if s["policy_id"] == "off")
    off_t = max(float(off["total_p90"]), 1e-9)
    off_m = max(float(metric_get(off)), 1e-9)
    ranked: list[dict[str, Any]] = []
    for s in scenarios:
        t_r = float(s["total_p90"]) / off_t
        m_r = float(metric_get(s)) / off_m
        score = t_r + weight * m_r
        ranked.append(
            {
                "policy_id": s["policy_id"],
                "label": s["label"],
                "total_p90": s["total_p90"],
                "metric": metric_get(s),
                "total_ratio": t_r,
                "metric_ratio": m_r,
                "score": score,
            }
        )
    ranked.sort(key=lambda r: (r["score"], r["total_p90"], r["metric"]))
    best = ranked[0]
    return {
        "metric_name": metric_name,
        "weight": weight,
        "policy_id": best["policy_id"],
        "label": best["label"],
        "score": best["score"],
        "total_p90": best["total_p90"],
        "metric": best["metric"],
        "off_total_p90": off["total_p90"],
        "off_metric": metric_get(off),
        "ranked": ranked,
        "why": (
            f"Минимум score = total/OFF + {weight:g}·{metric_name}/OFF: "
            f"{best['label']} (score {best['score']:.3f}). "
            f"total {off['total_p90']:.0f}→{best['total_p90']:.0f}м "
            f"({(best['total_ratio'] - 1) * 100:+.0f}%), "
            f"{metric_name} {metric_get(off):.0f}→{best['metric']:.0f} "
            f"({(best['metric_ratio'] - 1) * 100:+.0f}%)."
        ),
    }


def build_tradeoff_comparisons(scenarios: list[dict[str, Any]]) -> dict[str, Any]:
    """Extra decision views: total+Units and total+vCPU peak%."""
    by_units = pick_tradeoff(
        scenarios,
        metric_name="Units",
        metric_get=_scenario_units,
        weight=1.0,
    )
    by_cpu = pick_tradeoff(
        scenarios,
        metric_name="vCPU%",
        metric_get=lambda s: float(s.get("pct_vcpu") or 0),
        weight=1.0,
    )
    return {
        "by_total_units": by_units,
        "by_total_cpu": by_cpu,
        "note": (
            "score = (total p90 / OFF) + (метрика / OFF). Меньше = лучше. "
            "Equal weight: latency и стоимость/нагрузка одинаково важны."
        ),
    }


def pick_recommendation(scenarios: list[dict[str, Any]], pool: int) -> dict[str, str]:
    """Best total_p90 among policies that do not blow quota much worse than OFF.

    Uncapped peak often already exceeds folder quota (regression overlap). We
    treat OFF as the baseline tax and avoid policies that add >15pp (or >15%
    relative) on the limiting resource unless nothing else is left.
    """
    del pool  # kept for call-site compatibility
    off = next(s for s in scenarios if s["policy_id"] == "off")
    off_q = off["quota_limiting_pct"]
    soft_cap = max(100.0, off_q + 15.0, off_q * 1.15)
    on = [s for s in scenarios if s["policy_id"] != "off"]
    candidates = [s for s in on if s["quota_limiting_pct"] <= soft_cap]
    if not candidates:
        candidates = on
    best = min(
        candidates,
        key=lambda s: (
            s["total_p90"],
            s["quota_limiting_pct"] - off_q,
            s["pr_runner_hours_demanded"],
        ),
    )
    dq = best["quota_limiting_pct"] - off_q
    return {
        "policy_id": best["policy_id"],
        "label": best["label"],
        "why": (
            f"Лучший total p90 при квоте не сильно хуже OFF (soft cap {soft_cap:.0f}%): "
            f"total {off['total_p90']:.0f}→{best['total_p90']:.0f}m, "
            f"exec {off['exec_p90']:.0f}→{best['exec_p90']:.0f}m, "
            f"demand runners {off['demand_peak_runners']:.0f}→{best['demand_peak_runners']:.0f} "
            f"({best['demand_peak_runners'] - off['demand_peak_runners']:+.0f}), "
            f"{best['quota_limiting']} {off_q:.0f}%→{best['quota_limiting_pct']:.0f}% "
            f"({dq:+.0f}pp), "
            f"PR runner-h {off['pr_runner_hours_demanded']:.0f}→"
            f"{best['pr_runner_hours_demanded']:.0f}."
        ),
    }


def write_html_report(result: dict[str, Any], path: Path) -> None:
    payload = json.dumps(result, ensure_ascii=False)
    html = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>PR-check: wait / exec / sharding</title>
<style>
  :root { --bg:#fff; --fg:#111; --muted:#555; --line:#ddd; --ok:#0a7a3e; --bad:#a32020; --hl:#f6f8fa; }
  * { box-sizing:border-box; }
  body { margin:0; font:15px/1.45 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif; color:var(--fg); background:var(--bg); }
  main { max-width:none; width:100%; margin:0 auto; padding:24px 20px 64px; box-sizing:border-box; }
  .charts { max-width:none; }
  h1 { font-size:22px; margin:0 0 6px; }
  h2 { font-size:17px; margin:28px 0 10px; border-bottom:1px solid var(--line); padding-bottom:6px; }
  .sub { color:var(--muted); margin-bottom:18px; }
  .q { background:var(--hl); border:1px solid var(--line); border-radius:8px; padding:14px 16px; margin:12px 0; }
  .q .ask { font-weight:700; margin-bottom:6px; }
  .q .ans { font-size:16px; }
  .q .note { color:var(--muted); font-size:13px; margin-top:6px; }
  table { width:100%; border-collapse:collapse; font-size:13px; margin:8px 0 16px; }
  th, td { border:1px solid var(--line); padding:6px 8px; text-align:right; }
  th:first-child, td:first-child, td.left, th.left { text-align:left; }
  th { background:var(--hl); font-weight:600; }
  .good { color:var(--ok); font-weight:600; }
  .bad { color:var(--bad); font-weight:600; }
  .best { background:#eef8f0; }
  .best-units { background:#e8f1ff; }
  .best-cpu { background:#fff6e5; }
  .mark { font-size:11px; font-weight:700; margin-left:6px; white-space:nowrap; }
  .mark-units { color:#1a5fb4; }
  .mark-cpu { color:#b35c00; }
  .mark-total { color:#0a7a3e; }
  code { background:var(--hl); padding:1px 5px; border-radius:4px; font-size:12px; }
  .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:1px 8px; font-size:12px; font-weight:700; }
  .charts { background:#0f1115; color:#e8eaed; border-radius:10px; padding:16px; margin:16px 0 24px; }
  .charts h2, .charts h3 { color:#e8eaed; border:none; }
  .charts .sub { color:#9aa0a6; }
  .chart-card { background:#171a21; border:1px solid #2a2f3a; border-radius:10px; padding:12px 14px; margin:0; min-width:0; }
  .chart-card h3 { font-size:13px; color:#9aa0a6; margin:0 0 8px; font-weight:600; }
  .chart-wrap { position:relative; height:260px; }
  .chart-wrap.tall { height:280px; }
  .chart-row { display:grid; gap:12px; margin:12px 0; }
  .chart-row.cols-3 { grid-template-columns:repeat(3, minmax(0, 1fr)); }
  .chart-row.cols-2 { grid-template-columns:repeat(2, minmax(0, 1fr)); }
  .chart-row.cols-1 { grid-template-columns:1fr; }
  .chart-row-title { font-size:12px; color:#9aa0a6; margin:16px 0 0; font-weight:600; letter-spacing:0.02em; text-transform:uppercase; }
  @media (max-width:1100px) {
    .chart-row.cols-3 { grid-template-columns:1fr; }
    .chart-row.cols-2 { grid-template-columns:1fr; }
  }
  .controls { display:grid; grid-template-columns:1fr 1fr; gap:8px; margin:8px 0 10px; }
  .control { background:#171a21; border:1px solid #2a2f3a; border-radius:8px; padding:8px 10px; }
  .control.span-2 { grid-column:1 / -1; }
  .control > .lbl { display:flex; align-items:baseline; justify-content:space-between; gap:8px; font-size:12px; color:#9aa0a6; margin-bottom:4px; }
  .control > .lbl b { color:#e8eaed; font-weight:600; }
  .control .row { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .control input[type=range] { flex:1; min-width:120px; }
  .control .val { min-width:90px; font-weight:700; color:#e8eaed; font-size:13px; }
  .control .chip { display:inline-flex; align-items:center; gap:4px; cursor:pointer; font-weight:500; font-size:12px; color:#c4c7ce; white-space:nowrap; }
  .control .chip input { margin:0; }
  .control select { padding:2px 6px; border-radius:5px; border:1px solid #2a2f3a; background:#0f1115; color:#e8eaed; font-size:12px; }
  .control .hint { font-size:11px; color:#6b7280; margin-top:3px; }
  @media (max-width:900px) { .controls { grid-template-columns:1fr; } }
  .kpi { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:10px; margin:12px 0; }
  @media (max-width:900px) { .kpi { grid-template-columns:1fr 1fr; } }
  .kpi .card { background:#171a21; border:1px solid #2a2f3a; border-radius:10px; padding:10px 12px; }
  .kpi .v { font-size:20px; font-weight:700; }
  .kpi .l { font-size:12px; color:#9aa0a6; margin-top:4px; }
  .rec { background:#141821; border:1px solid #3d4a2a; border-radius:10px; padding:14px 16px; margin:12px 0; }
  .rec .ask { font-weight:700; color:#3dd68c; margin-bottom:6px; }
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
</head>
<body>
<main>
  <h1>Ответы: очередь, время выполнения, шардирование</h1>
  <div class="sub" id="sub"></div>

  <h2>1. Коротко</h2>
  <div id="answers"></div>

  <div class="charts">
    <h2>Интерактив: что будет при шардах и другой квоте</h2>
    <div class="rec" id="quotaRec"></div>
    <div class="controls">
      <div class="control">
        <div class="lbl"><b>1. Шарды</b><span id="policyVal">4 шарда</span></div>
        <div class="row"><input id="policyRange" type="range" min="0" max="9" step="1" value="5" /></div>
        <div class="hint" id="policyDetail"></div>
      </div>
      <div class="control">
        <div class="lbl"><b>2. Квота</b><span id="quotaVal">1.0×</span></div>
        <div class="row"><input id="quotaRange" type="range" min="0" max="5" step="1" value="0" /></div>
        <div class="hint" id="quotaDetail"></div>
      </div>
      <div class="control">
        <div class="lbl"><b>3. Окно</b></div>
        <div class="row">
          <label class="chip"><input type="radio" name="chartWindow" id="winWeek" value="week" checked /> неделя</label>
          <label class="chip"><input type="radio" name="chartWindow" id="winDay" value="day" /> день</label>
          <select id="daySelect" disabled>
            <option value="0">Пн</option>
            <option value="1">Вт</option>
            <option value="2">Ср</option>
            <option value="3">Чт</option>
            <option value="4">Пт</option>
            <option value="5">Сб</option>
            <option value="6">Вс</option>
          </select>
        </div>
      </div>
      <div class="control">
        <div class="lbl"><b>4. Часы UTC</b><span id="hourRangeVal">все 24ч</span></div>
        <div class="row">
          <label class="chip"><input type="radio" name="hourPreset" id="hourAll" value="all" checked /> сутки</label>
          <label class="chip"><input type="radio" name="hourPreset" id="hourWork" value="work" /> 09–18</label>
          <label class="chip"><input type="radio" name="hourPreset" id="hourNight" value="night" /> 20–06</label>
          <label class="chip"><input type="radio" name="hourPreset" id="hourCustom" value="custom" /> свой</label>
          <select id="hourFrom"></select>
          <span style="color:#6b7280;font-size:12px">–</span>
          <select id="hourTo"></select>
        </div>
      </div>
      <div class="control span-2">
        <div class="lbl"><b>5. Размер PR</b></div>
        <div class="row">
          <label class="chip"><input type="radio" name="sizeFilter" id="sizeAll" value="all" checked /> все</label>
          <label class="chip"><input type="radio" name="sizeFilter" id="sizeLight" value="light" /> &lt;60м</label>
          <label class="chip"><input type="radio" name="sizeFilter" id="sizeMid" value="mid" /> 60–120м</label>
          <label class="chip"><input type="radio" name="sizeFilter" id="sizeHeavy" value="heavy" /> 120–200м</label>
          <label class="chip"><input type="radio" name="sizeFilter" id="sizeXheavy" value="xheavy" /> ≥200м</label>
        </div>
      </div>
    </div>
    <div class="rec" id="viewSummary" style="border-color:#2a2f3a"></div>
    <div class="kpi" id="liveKpi"></div>
    <p class="sub" id="chartSub"></p>

    <div class="chart-row-title">1 · Latency по времени</div>
    <div class="chart-row cols-3">
      <div class="chart-card"><h3 id="titleWait">Wait p90 (мин)</h3><div class="chart-wrap tall"><canvas id="weekWaitChart"></canvas></div></div>
      <div class="chart-card"><h3 id="titleExec">Exec / run p90 (мин)</h3><div class="chart-wrap tall"><canvas id="weekExecChart"></canvas></div></div>
      <div class="chart-card"><h3 id="titleTotal">Total p90 (мин)</h3><div class="chart-wrap tall"><canvas id="weekTotalChart"></canvas></div></div>
    </div>

    <div class="chart-row-title" id="sizeRowTitle">2 · Разбивка по размеру PR</div>
    <div class="chart-row cols-3">
      <div class="chart-card"><h3 id="titleSizeWait">Wait p90 по размеру</h3><div class="chart-wrap"><canvas id="sizeWaitChart"></canvas></div></div>
      <div class="chart-card"><h3 id="titleSizeExec">Exec p90 по размеру</h3><div class="chart-wrap"><canvas id="sizeExecChart"></canvas></div></div>
      <div class="chart-card"><h3 id="titleSizeTotal">Total p90 по размеру</h3><div class="chart-wrap"><canvas id="sizeTotalChart"></canvas></div></div>
    </div>

    <div class="chart-row-title">3 · Биллинг PR</div>
    <div class="chart-row cols-1">
      <div class="chart-card">
        <h3 id="titleUnitsPerPr">Units на 1 PR — baseline vs selected</h3>
        <div class="chart-wrap" style="height:220px"><canvas id="unitsPerPrChart"></canvas></div>
        <p class="sub" id="unitsPerPrSub" style="margin:8px 0 0"></p>
      </div>
    </div>
    <div class="chart-row cols-3">
      <div class="chart-card"><h3 id="titlePrBill">Instance-hours по часам (когда VM бежит)</h3><div class="chart-wrap tall"><canvas id="weekPrBillChart"></canvas></div></div>
      <div class="chart-card"><h3 id="titlePrRam">RAM GB-hours по часам</h3><div class="chart-wrap tall"><canvas id="weekPrRamChart"></canvas></div></div>
      <div class="chart-card"><h3 id="titlePrUnits">Billing Units по часам (vCPU+RAM+SSD)</h3><div class="chart-wrap tall"><canvas id="weekPrUnitsChart"></canvas></div></div>
    </div>

    <div class="chart-row-title">4 · Workflow runs и jobs</div>
    <div class="chart-row cols-2">
      <div class="chart-card"><h3 id="titlePrCount">PR-check запуски по часам</h3><div class="chart-wrap tall"><canvas id="weekPrCountChart"></canvas></div></div>
      <div class="chart-card"><h3 id="titlePrJobs">PR jobs (rwdi+asan, шарды) по часам</h3><div class="chart-wrap tall"><canvas id="weekPrJobsChart"></canvas></div></div>
    </div>

    <div id="quotaRecTable"></div>
  </div>

  <h2>2. Сейчас (эмпирика GitHub)</h2>
  <div id="nowBox"></div>
  <h3>По дням недели (мин)</h3>
  <div id="nowDay"></div>
  <h3>По часам UTC (мин) — только часы с данными</h3>
  <div id="nowHour"></div>

  <h2>3. Лучшие срезы: шардирование × квота</h2>
  <p class="sub">Симуляция недели: wait = очередь, exec = wall critical path, total = wait + exec.
    Каждый срез сравниваем с <b>OFF на той же квоте</b>.</p>
  <div id="tradeoffComparisons"></div>
  <div id="scenarioTable"></div>

  <h2>4. Квота: baseline vs sharding (tradeoff)</h2>
  <div class="q">
    <div class="ask">Как читать эту таблицу (важно)</div>
    <div class="ans">
      Есть <b>три разных потолка</b>, их нельзя смешивать:<br/>
      1) <b>Folder quota в yaml</b> — например <code>quotas.instances: 140</code>. Это потолок облака, <b>не</b> число runners в симуляции.<br/>
      2) <b>Pool budget (расчёт)</b> — сколько ghrun-машин реально влезает после reserved + headroom и деления на footprint. Сейчас это <b>не 140</b>, а меньше — обычно упираемся в SSD.<br/>
      3) <b>Demand peak</b> — сколько runners <i>захотели бы</i> стартовать одновременно без очереди.<br/><br/>
      <b>Квота (потолок)</b> — сколько максимум можем поднять одновременно; за неё саму не платим.<br/>
      <b>Факт. потребление (биллинг)</b> — instance-hours / vCPU-hours / SSD·hours = ресурс × время, пока VM реально бежит.
      Чарджим только это. Шарды часто поднимают пик (нужен больший потолок), но machine-time может вырасти лишь на prepare/merge.
    </div>
  </div>
  <div id="poolBudgetBox"></div>
  <div id="quotaAnswers"></div>
  <div id="quotaTable"></div>
  <div id="quotaLimits"></div>

  <h2>5. Разбивка лучшего ON vs OFF</h2>
  <h3>По дням — total p90 (мин)</h3>
  <div id="cmpDay"></div>
  <h3>По часам UTC — total p90 (мин)</h3>
  <div id="cmpHour"></div>

  <h2>6. Что ещё важно</h2>
  <div id="extra"></div>

  <h2>Пересчёт</h2>
  <p><code>python3 .github/scripts/tests/sharding/forecast_pool_impact.py --days 30 --out /tmp/sharding_forecast.json</code></p>
</main>
<script>
const D = __PAYLOAD__;
const fmt = (n,d=0) => (n==null||Number.isNaN(n)) ? "—" : Number(n).toLocaleString("en-US",{maximumFractionDigits:d,minimumFractionDigits:d});
const delta = (a,b) => {
  if (!a) return "—";
  const p = (b-a)/Math.abs(a)*100;
  const cls = p < -5 ? "good" : (p > 5 ? "bad" : "");
  return `<span class="${cls}">${p>=0?"+":""}${fmt(p,0)}%</span>`;
};

const BILL_UNITS = D.billing_units || {
  hours_per_month: 720,
  per_hour: {
    vcpu: (57139.20 / 64) / 720,
    ram_gb: (60825.60 / 256) / 720,
    ssd_gb: (25592.11 / 2360) / 720,
  },
};
function billUnitsFromHours(vcpuH, ramH, ssdH) {
  const r = BILL_UNITS.per_hour || {};
  return (vcpuH || 0) * (r.vcpu || 0) + (ramH || 0) * (r.ram_gb || 0) + (ssdH || 0) * (r.ssd_gb || 0);
}
function scenarioBillUnits(s) {
  if (!s) return 0;
  if (s.bill_pr_units != null) return s.bill_pr_units;
  return billUnitsFromHours(s.bill_pr_vcpu_hours, s.bill_pr_ram_gb_hours, s.bill_pr_ssd_gb_hours);
}

const PB = D.pool_budget;
document.getElementById("sub").textContent =
  `Период ${D.period.start} → ${D.period.end} · folder instances quota=${PB.quotas.instances}, ` +
  `pool budget=${D.pool_budget_runners} runners (limited by ${PB.limiting_resource}) · шарды rwdi+asan вместе`;

const RW = PB.by_preset.relwithdebinfo;
const AS = PB.by_preset["release-asan"];
document.getElementById("poolBudgetBox").innerHTML = `
  <div class="q"><div class="ask">Footprint rwdi ≠ asan — и budget разный</div>
    <div class="ans">
      В yaml: <b>rwdi</b> ${RW.footprint.vcpu} vCPU / ${RW.footprint.ram_gb} GB RAM / ${RW.footprint.nrd_ssd_gb} SSD →
      влезает <b>${RW.pool_budget_runners}</b> runners (лимит ${RW.limiting_resource}).<br/>
      <b>asan</b> ${AS.footprint.vcpu} vCPU / ${AS.footprint.ram_gb} GB RAM / ${AS.footprint.nrd_ssd_gb} SSD →
      влезает <b>${AS.pool_budget_runners}</b> runners (лимит ${AS.limiting_resource}).<br/><br/>
      Folder <code>instances=${PB.quotas.instances}</code> — не budget.<br/>
      Симуляция режет не «105 одинаковых слотов», а <b>общий ресурсный пул</b>
      (instances/vCPU/RAM/SSD): asan-job ест больше vCPU/RAM, чем rwdi при том же числе машин.
    </div>
    <div class="note">${PB.how_computed}</div></div>
`;

const now = D.empirical_now;
const off = D.scenarios.find(s => s.policy_id === "off");
const rec = D.recommendation;
const best = D.scenarios.find(s => s.policy_id === rec.policy_id);
const adap = D.scenarios.find(s => s.policy_id === "adaptive");

document.getElementById("answers").innerHTML = `
  <div class="q"><div class="ask">Сколько PR ждут сейчас? <span class="pill">эмпирика GitHub</span></div>
    <div class="ans">wait p50 = <b>${fmt(now.wait.p50)}</b> мин, p90 = <b>${fmt(now.wait.p90)}</b> мин (n=${now.n})</div>
    <div class="note">Реальные runs: created_at → started_at первого heavy job. Это факт «как сейчас в GH», не модель.</div></div>
  <div class="q"><div class="ask">Сколько сейчас выполняется heavy critical path? <span class="pill">эмпирика</span></div>
    <div class="ans">exec p50 = <b>${fmt(now.exec.p50)}</b> мин, p90 = <b>${fmt(now.exec.p90)}</b> мин</div>
    <div class="note">max(длительность heavy preset jobs) в одном PR-check.</div></div>
  <div class="q"><div class="ask">Сколько total сейчас (wait + heavy)? <span class="pill">эмпирика</span></div>
    <div class="ans">total p50 = <b>${fmt(now.total.p50)}</b> мин, p90 = <b>${fmt(now.total.p90)}</b> мин</div></div>
  <div class="q"><div class="ask">Почему wait 3м «сейчас» ≠ ${fmt(off.wait_p90,0)}м в блоке про шардирование?</div>
    <div class="ans">Это <b>разные величины</b>. 3м — измеренный wait в sample. ${fmt(off.wait_p90,0)}м — <b>симулированный</b> wait в сценарии OFF при полной недельной нагрузке (все PR + regression на pool ${D.pool_budget_runners}).
      Модель специально жёстче: demand peak ~${fmt(off.demand_peak_runners,0)} runners vs budget ${D.pool_budget_runners} → очередь. Сравнивать OFF→ON можно только внутри симуляции (одинаковые правила), не «3м → 71м».</div>
    <div class="note">Эмпирика занижает хвост очереди (sample дней/часов, когда runners часто свободны). Симуляция оценивает что будет при устойчивой нагрузке недели.</div></div>
  <div class="q"><div class="ask">Что будет если включим шардирование? <span class="pill">только симуляция OFF→ON</span></div>
    <div class="ans">Рекомендация: <span class="pill">${rec.label}</span><br/>
      <b>Сравниваем модель OFF vs модель ON</b> (не эмпирику с моделью):<br/>
      total p90: <b>${fmt(off.total_p90)}</b> → <b>${fmt(best.total_p90)}</b> мин (${delta(off.total_p90,best.total_p90)})<br/>
      wait p90: <b>${fmt(off.wait_p90)}</b> → <b>${fmt(best.wait_p90)}</b> мин (${delta(off.wait_p90,best.wait_p90)}) &nbsp;<span style="color:var(--muted)">(эмпирика сейчас ${fmt(now.wait.p90)}м — см. выше)</span><br/>
      exec p90: <b>${fmt(off.exec_p90)}</b> → <b>${fmt(best.exec_p90)}</b> мин (${delta(off.exec_p90,best.exec_p90)}) &nbsp;<span style="color:var(--muted)">(эмпирика ${fmt(now.exec.p90)}м ≈ модель OFF ${fmt(off.exec_p90)}м)</span><br/>
      demand runners: <b>${fmt(off.demand_peak_runners,0)}</b> → <b>${fmt(best.demand_peak_runners,0)}</b> (${delta(off.demand_peak_runners,best.demand_peak_runners)})
    </div>
    <div class="note">${rec.why}</div></div>
  <div class="q"><div class="ask">Как зависит от числа шардов?</div>
    <div class="ans">Смотри таблицы latency + quota ниже: больше шардов → короче exec, выше peak demand квоты и часто выше wait.</div></div>
  <div class="q"><div class="ask">Tradeoff по квоте?</div>
    <div class="ans">Demand runners OFF→best: <b>${fmt(off.demand_peak_runners,0)}</b> → <b>${fmt(best.demand_peak_runners,0)}</b>
      (${delta(off.demand_peak_runners,best.demand_peak_runners)}). Лимитирующий ресурс best: <b>${best.quota_limiting}</b> ${fmt(best.quota_limiting_pct,0)}% квоты
      (OFF было ${fmt(off.quota_limiting_pct,0)}%). PR runner-hours/week: ${fmt(off.pr_runner_hours_demanded,0)} → ${fmt(best.pr_runner_hours_demanded,0)}
      (${delta(off.pr_runner_hours_demanded,best.pr_runner_hours_demanded)}).</div>
    <div class="note">Если % &gt; 100 — peak demand выше квоты: в проде будет очередь/кап шардов, а не «бесплатное» ускорение.</div></div>
`;

document.getElementById("nowBox").innerHTML = `
  <table>
    <tr><th class="left">Метрика</th><th>p50</th><th>p90</th><th>mean</th></tr>
    <tr><td class="left">Wait (очередь до heavy)</td><td>${fmt(now.wait.p50)}</td><td>${fmt(now.wait.p90)}</td><td>${fmt(now.wait.mean)}</td></tr>
    <tr><td class="left">Exec (heavy critical path)</td><td>${fmt(now.exec.p50)}</td><td>${fmt(now.exec.p90)}</td><td>${fmt(now.exec.mean)}</td></tr>
    <tr><td class="left">Total (wait+heavy end)</td><td>${fmt(now.total.p50)}</td><td>${fmt(now.total.p90)}</td><td>${fmt(now.total.mean)}</td></tr>
  </table>`;

function dayTable(rows, keys) {
  let h = "<table><tr><th class='left'>День</th><th>n</th>" + keys.map(k=>`<th>${k.label}</th>`).join("") + "</tr>";
  for (const r of rows) {
    if (!r.n) continue;
    h += `<tr><td class="left">${r.name||("UTC "+r.hour)}</td><td>${fmt(r.n,0)}</td>` +
      keys.map(k=>`<td>${fmt(k.get(r),0)}</td>`).join("") + "</tr>";
  }
  return h + "</table>";
}

document.getElementById("nowDay").innerHTML = dayTable(now.by_dow, [
  {label:"wait p50", get:r=>r.wait.p50}, {label:"wait p90", get:r=>r.wait.p90},
  {label:"exec p50", get:r=>r.exec.p50}, {label:"exec p90", get:r=>r.exec.p90},
  {label:"total p90", get:r=>r.total.p90},
]);
document.getElementById("nowHour").innerHTML = dayTable(now.by_hour.filter(r=>r.n>=2), [
  {label:"wait p50", get:r=>r.wait.p50}, {label:"wait p90", get:r=>r.wait.p90},
  {label:"exec p90", get:r=>r.exec.p90}, {label:"total p90", get:r=>r.total.p90},
]);

function scenarioUnits(s) { return scenarioBillUnits(s); }

/** All slices: sharding × quota. Each compared to OFF at the SAME quota. */
function buildSlices() {
  const grid = D.scenario_grid || {};
  const scales = ((D.quota_recommendation && D.quota_recommendation.scales) || [1, 1.25, 1.5, 2, 2.5, 3]).slice();
  const labels = Object.fromEntries((D.scenarios || []).map((s) => [s.policy_id, s.label]));
  const policyIds = (D.scenarios || []).map((s) => s.policy_id);
  const get = (pid, scale) => {
    const g = grid[pid];
    if (!g) return null;
    const k = String(Number(scale));
    return g[k] || g[String(scale)] || g[scale.toFixed ? scale.toFixed(2) : scale] || null;
  };
  const out = [];
  for (const scale of scales) {
    const offS = get("off", scale);
    if (!offS) continue;
    const offT = Math.max(offS.total_p90 || 0, 1e-9);
    const offU = Math.max(scenarioUnits(offS), 1e-9);
    for (const policyId of policyIds) {
      if (policyId === "off") continue;
      const s = get(policyId, scale);
      if (!s) continue;
      const total = s.total_p90 || 0;
      const units = scenarioUnits(s);
      const tR = total / offT;
      const uR = units / offU;
      const lab = labels[policyId] || s.label || policyId;
      out.push({
        policy_id: policyId,
        label: lab,
        scale,
        name: `${lab} @${scale}×`,
        total,
        units,
        wait: s.wait_p90 || 0,
        exec: s.exec_p90 || 0,
        pct_vcpu: s.pct_vcpu || 0,
        off_total: offS.total_p90 || 0,
        off_units: scenarioUnits(offS),
        off_wait: offS.wait_p90 || 0,
        d_total: (total / offT - 1) * 100,
        d_units: (units / offU - 1) * 100,
        score: tR + uR, // vs OFF@same quota
      });
    }
  }
  return out;
}
const SLICES = buildSlices();
const topByTotal = [...SLICES].sort((a, b) => a.total - b.total || a.units - b.units).slice(0, 3);
const topByUnits = [...SLICES].sort((a, b) => a.units - b.units || a.total - b.total).slice(0, 3);
const topByBoth = [...SLICES].sort((a, b) => a.score - b.score || a.total - b.total).slice(0, 3);

function topListHtml(rows, kind) {
  return rows.map((r, i) => {
    const extra = kind === "both"
      ? `score ${fmt(r.score, 3)} (= total/OFF@${r.scale}× + Units/OFF@${r.scale}×)`
      : kind === "total"
        ? `vs OFF@${r.scale}×: total ${fmt(r.off_total,0)}→${fmt(r.total,0)}м (${delta(r.off_total, r.total)}), Units ${delta(r.off_units, r.units)}`
        : `vs OFF@${r.scale}×: Units ${fmt(r.off_units,0)}→${fmt(r.units,0)} (${delta(r.off_units, r.units)}), total ${delta(r.off_total, r.total)}`;
    return `<div style="margin:6px 0"><b>${i + 1}. ${r.name}</b> — total <b>${fmt(r.total,0)}</b>м, Units <b>${fmt(r.units,0)}</b><br/>
      <span style="color:var(--muted)">${extra}</span></div>`;
  }).join("");
}

document.getElementById("tradeoffComparisons").innerHTML = `
  <div class="q">
    <div class="ask">Как читаем: срез = шардирование × квота</div>
    <div class="ans">Каждый кандидат сравниваем с <b>OFF на той же квоте</b> (не всегда с OFF@1×).
      Пример: «2 шарда @1.25×» vs «OFF @1.25×». Так видно, что даёт именно шардирование при выбранном потолке.</div>
  </div>
  <div class="q"><div class="ask">Топ‑3 по total (быстрее)</div>
    <div class="ans">${topListHtml(topByTotal, "total")}</div></div>
  <div class="q"><div class="ask">Топ‑3 по Units (дешевле)</div>
    <div class="ans">${topListHtml(topByUnits, "units")}</div>
    <div class="note">OFF на малых квотах часто самый дешёвый — его нет в списке срезов (только режимы с шардами). Если Units важнее скорости — смотри OFF в интерактиве.</div></div>
  <div class="q"><div class="ask">Топ‑3 по total + Units (баланс)</div>
    <div class="ans">${topListHtml(topByBoth, "both")}</div>
    <div class="note">score = (total / OFF@quota) + (Units / OFF@quota). Меньше = лучше. 2.0 = как OFF; &lt;2 значит лучше OFF на этой квоте.</div></div>
`;

const topTotalKeys = new Set(topByTotal.map(r => r.policy_id + "@" + r.scale));
const topUnitsKeys = new Set(topByUnits.map(r => r.policy_id + "@" + r.scale));
const topBothKeys = new Set(topByBoth.map(r => r.policy_id + "@" + r.scale));

let st = `<table><tr>
  <th class="left">Срез (шарды × квота)</th>
  <th>total</th><th>Δ vs OFF@quota</th>
  <th>Units</th><th>Δ vs OFF@quota</th>
  <th>wait</th><th>exec</th><th>vCPU%</th><th>score</th></tr>`;
// Show compact table: for each scale, OFF row + best few — or all slices sorted by score
const sliceView = [...SLICES].sort((a, b) => a.score - b.score || a.total - b.total);
for (const r of sliceView) {
  const key = r.policy_id + "@" + r.scale;
  const marks = [];
  let cls = "";
  if (topBothKeys.has(key) && topByBoth[0] && key === topByBoth[0].policy_id + "@" + topByBoth[0].scale) {
    cls = "best-units"; marks.push('<span class="mark mark-units">← #1 баланс</span>');
  } else if (topTotalKeys.has(key) && topByTotal[0] && key === topByTotal[0].policy_id + "@" + topByTotal[0].scale) {
    cls = "best"; marks.push('<span class="mark mark-total">← #1 total</span>');
  } else if (topUnitsKeys.has(key) && topByUnits[0] && key === topByUnits[0].policy_id + "@" + topByUnits[0].scale) {
    cls = "best-cpu"; marks.push('<span class="mark mark-cpu">← #1 Units</span>');
  }
  st += `<tr class="${cls}"><td class="left">${r.name}${marks.join("")}</td>
    <td>${fmt(r.total,0)}</td><td>${delta(r.off_total, r.total)}</td>
    <td>${fmt(r.units,0)}</td><td>${delta(r.off_units, r.units)}</td>
    <td>${fmt(r.wait,0)}</td><td>${fmt(r.exec,0)}</td>
    <td>${fmt(r.pct_vcpu,0)}%</td><td>${fmt(r.score,3)}</td></tr>`;
}
document.getElementById("scenarioTable").innerHTML = st + "</table>";
// Keep @1× policy table reference for old section below — rebuild short @1× view in a note
document.getElementById("scenarioTable").insertAdjacentHTML("beforebegin",
  `<p class="sub">Таблица: все срезы шардирование×квота, Δ всегда против <b>OFF на той же квоте</b>. Отсортировано по score (total+Units). Ниже в §4 — детали квоты.</p>`);

const Q = D.quotas, R = D.reserved;
document.getElementById("quotaAnswers").innerHTML = `
  <div class="q"><div class="ask">Что меняется на ${best.label}? (главное)</div>
    <div class="ans">
      <b>Выигрыш:</b> total p90 ${fmt(off.total_p90)}→${fmt(best.total_p90)}м (${delta(off.total_p90,best.total_p90)}).<br/>
      <b>Цена:</b> на пике хотим одновременно на ${fmt(best.demand_peak_runners - off.demand_peak_runners,0)} runners больше
      (${fmt(off.demand_peak_runners,0)}→${fmt(best.demand_peak_runners,0)}, ${delta(off.demand_peak_runners,best.demand_peak_runners)}),
      и на ${delta(off.pr_runner_hours_demanded,best.pr_runner_hours_demanded)} больше machine-time PR за неделю
      (${fmt(off.pr_runner_hours_demanded,0)}→${fmt(best.pr_runner_hours_demanded,0)} runner-hours).<br/>
      Пиковый «спрос к квоте» (vCPU): ${fmt(off.pct_vcpu,0)}%→${fmt(best.pct_vcpu,0)}% (${delta(off.pct_vcpu,best.pct_vcpu)}) —
      оба &gt;100%, потому что OFF уже на пике перегружает квоту; 4 шарда добавляют ещё ~${fmt(best.pct_vcpu - off.pct_vcpu,0)}pp сверху.
    </div>
    <div class="note">Красное «выше квоты» = пиковый спрос, не средняя утилизация. Средняя по неделе: pool util ~${fmt(best.pool_util_pct,0)}%.</div></div>
`;

let qt = `<table><tr>
  <th class="left">Политика</th>
  <th title="Потолок: сколько runners хотим одновременно на пике">пик спрос runners</th>
  <th title="Потолок: (reserved+спрос)/quota">пик vs квота vCPU</th>
  <th title="Биллинг: Σ (runners × wall) за неделю — платим за это">PR instance-h (билл)</th>
  <th title="Биллинг: Σ (vCPU × wall)">PR vCPU-h (билл)</th>
  <th title="Биллинг: Σ (SSD_GB × wall)">PR SSD·h (билл)</th>
  <th>Δ total p90</th>
  <th>Δ PR instance-h</th>
  <th>вердикт</th></tr>`;
for (const s of D.scenarios) {
  const cls = s.policy_id === rec.policy_id ? "best" : "";
  const bi = s.bill_pr_instance_hours ?? s.pr_runner_hours_demanded;
  const bv = s.bill_pr_vcpu_hours ?? 0;
  const bs = s.bill_pr_ssd_gb_hours ?? 0;
  const obi = off.bill_pr_instance_hours ?? off.pr_runner_hours_demanded;
  const bt = s.bill_pr_test_instance_hours;
  const bo = s.bill_pr_overhead_instance_hours;
  const billSplit = (bt!=null && bo!=null)
    ? `<br/><span style="color:var(--muted);font-size:11px">test ${fmt(bt,0)} + overhead ${fmt(bo,0)}</span>`
    : "";
  const verdict = s.policy_id === "off" ? "baseline: потолок vs факт. часы отдельно" :
    (s.policy_id === rec.policy_id ? "лучший latency; смотри Δ биллинга" :
    (s.total_p90 < off.total_p90 && (bi-obi)/Math.max(obi,1) > 0.35 ? "быстрее, но заметно дороже по machine-time" :
    (s.total_p90 >= best.total_p90 ? "хуже best" : "ok")));
  qt += `<tr class="${cls}"><td class="left">${s.label}</td>
    <td>${fmt(s.demand_peak_runners,0)}</td>
    <td class="${s.pct_vcpu>100?"bad":""}">${fmt(s.pct_vcpu,0)}%</td>
    <td>${fmt(bi,0)}${billSplit}</td>
    <td>${fmt(bv,0)}</td>
    <td>${fmt(bs,0)}</td>
    <td>${delta(off.total_p90,s.total_p90)}</td>
    <td>${delta(obi,bi)}</td>
    <td class="left">${verdict}</td></tr>`;
}
document.getElementById("quotaTable").innerHTML = qt + "</table>";
document.getElementById("quotaLimits").innerHTML = `
  <div class="q"><div class="ask">Квота ≠ счёт. Почему в модели биллинг растёт при шардах?</div>
    <div class="ans">
      Ротация (короче жизнь каждого runner) <b>уже учтена</b>: платим Σ (число_VM × время_VM).<br/>
      При N шардах: <code>N × (prepare + D/N + merge) = D + N×(prepare+merge)</code>.<br/>
      Кусок <b>D</b> (тесты) почти тот же, что без шардов. Растёт только overhead
      <b>N×(prepare+merge)</b> — в модели prepare=8м, merge=2м на шард.<br/>
      Поэтому «чаще ротация» сама по себе счёт не уменьшает: те же тестовые VM·часы + добавка на setup/merge.
      Выигрыш ротации — в latency/очереди (быстрее освобождаем пул), не в том, что D становится меньше.<br/>
      Если в проде prepare/merge дешевле 10м или шарятся между шардами — Δ биллинга будет меньше, чем в модели.
    </div></div>`;

let cd = `<table><tr><th class="left">День</th><th>OFF total p90</th><th>${best.label} total p90</th><th>Δ</th>
<th>OFF wait p90</th><th>ON wait p90</th><th>OFF exec p90</th><th>ON exec p90</th></tr>`;
for (let i=0;i<7;i++) {
  const a = off.by_day[i], b = best.by_day[i];
  cd += `<tr><td class="left">${a.name}</td><td>${fmt(a.total_p90,0)}</td><td>${fmt(b.total_p90,0)}</td>
    <td>${delta(a.total_p90,b.total_p90)}</td>
    <td>${fmt(a.wait_p90,0)}</td><td>${fmt(b.wait_p90,0)}</td>
    <td>${fmt(a.exec_p90,0)}</td><td>${fmt(b.exec_p90,0)}</td></tr>`;
}
document.getElementById("cmpDay").innerHTML = cd + "</table>";

let ch = `<table><tr><th class="left">UTC hour</th><th>OFF total p90</th><th>ON total p90</th><th>Δ</th>
<th>OFF wait p90</th><th>ON wait p90</th><th>OFF exec p90</th><th>ON exec p90</th></tr>`;
for (let h=0;h<24;h++) {
  const a = off.by_hour[h], b = best.by_hour[h];
  if ((a.n||0) < 1 && (b.n||0) < 1) continue;
  ch += `<tr><td class="left">${String(h).padStart(2,"0")}:00</td><td>${fmt(a.total_p90,0)}</td><td>${fmt(b.total_p90,0)}</td>
    <td>${delta(a.total_p90,b.total_p90)}</td>
    <td>${fmt(a.wait_p90,0)}</td><td>${fmt(b.wait_p90,0)}</td>
    <td>${fmt(a.exec_p90,0)}</td><td>${fmt(b.exec_p90,0)}</td></tr>`;
}
document.getElementById("cmpHour").innerHTML = ch + "</table>";

document.getElementById("extra").innerHTML = `
  <div class="q"><div class="ask">Почему нельзя шардить только rwdi?</div>
    <div class="ans">Critical path = max(rwdi, asan). Если шардить один пресет, второй остаётся bottleneck — total почти не падает, а квоту уже жжём.</div></div>
  <div class="q"><div class="ask">Только выигрыш или tradeoff?</div>
    <div class="ans">Latency выигрыш есть, но квота (особенно SSD/instances) на peak растёт с числом шардов.
      Смотри колонки % SSD и demand runners: если растут сильнее, чем падает total — плохой tradeoff.
      PR runner-hours ≈ объём machine-time; prepare/merge могут чуть поднять его при шардах.</div></div>
  <div class="q"><div class="ask">Adaptive</div>
    <div class="ans">total p90=${fmt(adap.total_p90,0)}м, demand runners=${fmt(adap.demand_peak_runners,0)},
      квота ${adap.quota_limiting} ${fmt(adap.quota_limiting_pct,0)}%. Peak-cap ${D.peak_cap} в UTC 9–16 режет demand ценой exec.</div></div>
  <div class="q"><div class="ask">Regression / nightly</div>
    <div class="ans">Этим пайплайном не шардируем; в demand/квоте они как фон (особенно ~23:00 UTC).</div></div>
`;

/* ---- Interactive week charts: policy + quota scale ---- */
const GRID = D.scenario_grid;
const QR = D.quota_recommendation;
const POLICY_IDS = (D.scenarios || []).map(s => s.policy_id);
const POLICY_LABELS = Object.fromEntries((D.scenarios || []).map(s => [s.policy_id, s.label]));
// Plain-language meaning of each slider position (no jargon).
const POLICY_HINTS = {
  off: "Без шардирования: каждый preset (rwdi и asan) — один большой job, как «раньше».",
  s2: "Всегда ровно 2 шарда на preset (лёгкие PR <60м всё равно без шардов).",
  s4: "Всегда ровно 4 шарда на preset (лёгкие <60м — без шардов).",
  s8: "Всегда ровно 8 шардов на preset (лёгкие <60м — без шардов).",
  s12: "Всегда ровно 12 шардов на preset (лёгкие <60м — без шардов).",
  adaptive: "Как сейчас в PR: число шардов от размера PR — "
    + "<60м→1, 60–120м→4, 120–200м→8, ≥200м→12. "
    + "В пик UTC 09–17 не больше 4 шардов (peak cap 4).",
  adap_p6: "Тот же adaptive, что в PR, но в пик UTC 09–17 можно до 6 шардов (мягче кап).",
  adap_p8: "Тот же adaptive, что в PR, но в пик UTC 09–17 можно до 8 шардов.",
  adap_fast: "Агрессивнее: шардирует раньше — "
    + "<45м→1, 45–90м→4, 90–150м→8, ≥150м→12. В пик всё ещё cap 4.",
  adap_soft: "Мягче / дешевле: mid→2, heavy→4, xheavy→8 "
    + "(вместо 4/8/12). В пик cap 4.",
};
function policyHint(policyId) {
  return POLICY_HINTS[policyId] || (POLICY_LABELS[policyId] || policyId);
}
const SCALES = (QR && QR.scales) ? QR.scales : [1,1.25,1.5,2,2.5,3];
const sk = (s) => String(Number(s)); // 1 → "1", 1.25 → "1.25"
const SIZE_ORDER = (D.size_buckets && D.size_buckets.order) || ["light","mid","heavy","xheavy"];
const SIZE_META = (D.size_buckets && D.size_buckets.meta) || {};

function sizeFilter() {
  const el = document.querySelector('input[name="sizeFilter"]:checked');
  return el ? el.value : "all";
}
function sizeHint() {
  const sz = sizeFilter();
  if (sz === "all") return "все размеры";
  const m = SIZE_META[sz] || {};
  return (m.label || sz) + (m.hint ? " · " + m.hint : "");
}
function seriesFrom(scenario, key) {
  if (!scenario) return [];
  const sz = sizeFilter();
  if (sz === "all") return scenario[key] || [];
  const b = scenario.by_size && scenario.by_size[sz];
  return (b && b[key]) || [];
}

if (QR) {
  const b = QR.best || {};
  const scaleTxt = b.scale == null ? "не найдено до ×3" : `×${b.scale} free capacity`;
  document.getElementById("quotaRec").innerHTML = `
    <div class="ask">Какая квота нужна для хорошего ускорения?</div>
    <div>Цель: total p90 ≤ <b>${fmt(QR.target_total_p90)}</b>м (−40%+ vs OFF@1.0× = ${fmt(b.off_total_p90)}м)
      и wait p90 ≤ <b>${fmt(QR.target_wait_p90_max)}</b>м.</div>
    <div style="margin-top:8px"><b>Рекомендация:</b> ${POLICY_LABELS[b.policy_id]||b.label||"—"} при <b>${scaleTxt}</b>.
      ${b.scale!=null ? `Тогда total ${fmt(b.total_p90)}м, wait ${fmt(b.wait_p90)}м, exec ${fmt(b.exec_p90)}м.
      Биллинг PR: ${fmt(b.off_bill_pr_instance_hours,0)}→${fmt(b.bill_pr_instance_hours,0)} instance-hours/week.` : ""}</div>
    <div style="margin-top:8px;color:#9aa0a6">${QR.why}</div>
    <div style="margin-top:8px;color:#9aa0a6">Квота = потолок одновременности. Счёт ≈ instance/vCPU-hours (ресурс×время работы).</div>`;
  let rt = `<table style="width:100%;margin-top:12px;font-size:13px;border-collapse:collapse">
    <tr><th class="left">Политика</th><th>мин. квота</th><th>total@hit</th><th>wait@hit</th><th>PR instance-h@hit</th><th>total@3×</th><th>PR instance-h@3×</th></tr>`;
  for (const r of QR.per_policy) {
    rt += `<tr><td class="left">${r.label}</td>
      <td>${r.min_scale==null ? "нет до ×3" : "×"+r.min_scale}</td>
      <td>${r.total_p90_at_hit==null?"—":fmt(r.total_p90_at_hit,0)}</td>
      <td>${r.wait_p90_at_hit==null?"—":fmt(r.wait_p90_at_hit,0)}</td>
      <td>${r.bill_pr_instance_hours_at_hit==null?"—":fmt(r.bill_pr_instance_hours_at_hit,0)}</td>
      <td>${fmt(r.best_total_at_3x,0)}</td>
      <td>${fmt(r.bill_pr_instance_hours_at_3x,0)}</td></tr>`;
  }
  document.getElementById("quotaRecTable").innerHTML = rt + "</table>";
}

const DOW = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"];
const DOW_RU = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"];
const weekLabels = [];
for (let d = 0; d < 7; d++) for (let h = 0; h < 24; h++)
  weekLabels.push(DOW[d] + " " + String(h).padStart(2, "0"));
const hourLabels = Array.from({length: 24}, (_, h) => String(h).padStart(2, "0") + ":00");

function chartWindowMode() {
  const day = document.getElementById("winDay");
  return day && day.checked ? "day" : "week";
}
function selectedDow() {
  return Math.max(0, Math.min(6, +document.getElementById("daySelect").value || 0));
}
function hourPreset() {
  const checked = document.querySelector('input[name="hourPreset"]:checked');
  return checked ? checked.value : "all";
}
function hoursInclusive(from, to) {
  const out = [];
  let h = ((from % 24) + 24) % 24;
  const end = ((to % 24) + 24) % 24;
  for (let i = 0; i < 24; i++) {
    out.push(h);
    if (h === end) break;
    h = (h + 1) % 24;
  }
  return out;
}
function selectedHours() {
  if (hourPreset() === "all") return Array.from({length: 24}, (_, h) => h);
  return hoursInclusive(+document.getElementById("hourFrom").value, +document.getElementById("hourTo").value);
}
function hourRangeText() {
  const hs = selectedHours();
  if (hs.length === 24) return "все 24ч UTC";
  const a = String(hs[0]).padStart(2, "0");
  const b = String(hs[hs.length - 1]).padStart(2, "0");
  const wrap = hs.some((h, i) => i > 0 && h < hs[i - 1]);
  return `${a}–${b} UTC (${hs.length}ч)` + (wrap ? ", через полночь" : "");
}
function applyHourPresetToSelects() {
  const p = hourPreset();
  const fromEl = document.getElementById("hourFrom");
  const toEl = document.getElementById("hourTo");
  const custom = p === "custom";
  fromEl.disabled = !custom && p !== "all";
  toEl.disabled = !custom && p !== "all";
  if (p === "all") {
    fromEl.value = "0";
    toEl.value = "23";
    fromEl.disabled = true;
    toEl.disabled = true;
  } else if (p === "work") {
    fromEl.value = "9";
    toEl.value = "18";
    fromEl.disabled = true;
    toEl.disabled = true;
  } else if (p === "night") {
    fromEl.value = "20";
    toEl.value = "6";
    fromEl.disabled = true;
    toEl.disabled = true;
  } else {
    fromEl.disabled = false;
    toEl.disabled = false;
  }
  document.getElementById("hourRangeVal").textContent = hourRangeText();
}
/** Slice to day (optional) then keep only selected UTC hours. */
function viewSeries(arr) {
  if (!arr || !arr.length) return { data: [], labels: [] };
  const hs = selectedHours();
  const hset = new Set(hs);
  if (chartWindowMode() === "day") {
    const d = selectedDow();
    const dayData = arr.slice(d * 24, d * 24 + 24);
    if (hs.length === 24) return { data: dayData, labels: hourLabels.slice() };
    return {
      data: hs.map((h) => dayData[h] ?? 0),
      labels: hs.map((h) => hourLabels[h]),
    };
  }
  if (hs.length === 24) return { data: arr.slice(), labels: weekLabels.slice() };
  const data = [], labels = [];
  for (let i = 0; i < arr.length; i++) {
    if (hset.has(i % 24)) {
      data.push(arr[i]);
      labels.push(weekLabels[i]);
    }
  }
  return { data, labels };
}
function syncDayControls() {
  const dayMode = chartWindowMode() === "day";
  document.getElementById("daySelect").disabled = !dayMode;
  applyHourPresetToSelects();
  const day = DOW_RU[selectedDow()];
  const hr = hourRangeText();
  const sz = sizeHint();
  const scope = dayMode ? `${day}, ${hr}, ${sz}` : `неделя, ${hr}, ${sz}`;
  document.getElementById("titleWait").textContent = `Очередь: wait p90 · ${scope} (мин)`;
  document.getElementById("titleExec").textContent = `Выполнение: exec p90 · ${scope} (мин)`;
  document.getElementById("titleTotal").textContent = `Итого total p90 · ${scope} (мин)`;
  document.getElementById("titlePrCount").textContent = `Объём: PR-check запуски · ${scope}`;
  document.getElementById("titlePrJobs").textContent = `Объём: PR jobs (rwdi+asan, шарды) · ${scope}`;
  document.getElementById("titlePrBill").textContent = `Биллинг PR: instance-hours · ${scope}`;
  document.getElementById("titlePrRam").textContent = `Биллинг PR: RAM GB-hours · ${scope}`;
  document.getElementById("titlePrUnits").textContent = `Биллинг PR: Units · ${scope}`;
}

const darkAxis = {
  responsive: true,
  maintainAspectRatio: false,
  interaction: { mode: "index", intersect: false },
  plugins: { legend: { labels: { color: "#c4c7ce" } } },
  scales: {
    x: { ticks: { color: "#9aa0a6", maxRotation: 0, autoSkip: true, maxTicksLimit: 28 }, grid: { color: "#242936" } },
    y: { beginAtZero: true, ticks: { color: "#9aa0a6" }, grid: { color: "#242936" } },
  },
};

function makeWeekChart(canvasId, onColor) {
  return new Chart(document.getElementById(canvasId), {
    type: "line",
    data: {
      labels: weekLabels,
      datasets: [
        { label: "без шардов", data: [], borderColor: "#c4c7ce", borderWidth: 1.5, pointRadius: 0, tension: 0.15, fill: false },
        { label: "выбранный режим", data: [], borderColor: onColor, borderWidth: 1.5, pointRadius: 0, tension: 0.15, fill: false },
      ],
    },
    options: darkAxis,
  });
}

const chartWait = makeWeekChart("weekWaitChart", "#f5a524");
const chartExec = makeWeekChart("weekExecChart", "#3dd68c");
const chartTotal = makeWeekChart("weekTotalChart", "#6aa7ff");
const chartPrCount = makeWeekChart("weekPrCountChart", "#c084fc");
const chartPrJobs = makeWeekChart("weekPrJobsChart", "#f472b6");
const chartPrBill = makeWeekChart("weekPrBillChart", "#fb923c");
const chartPrRam = makeWeekChart("weekPrRamChart", "#a78bfa");
const chartPrUnits = makeWeekChart("weekPrUnitsChart", "#38bdf8");

/** RAM GB-hours series; derive from instance-hours if JSON lacks the field. */
function ramSeriesFrom(scenario) {
  const direct = seriesFrom(scenario, "week_pr_bill_ram_gb_hours");
  if (direct && direct.length && direct.some((v) => v)) return direct;
  const inst = seriesFrom(scenario, "week_pr_bill_instance_hours") || [];
  const fullInst = scenario.bill_pr_instance_hours || 0;
  const fullRam = scenario.bill_pr_ram_gb_hours || 0;
  const rate = fullInst > 0 ? fullRam / fullInst : 0;
  return inst.map((v) => (Number(v) || 0) * rate);
}

function makeSizeBarChart(canvasId, accent, yTitle) {
  return new Chart(document.getElementById(canvasId), {
    type: "bar",
    data: {
      labels: SIZE_ORDER.map((id) => (SIZE_META[id] || {}).label || id),
      datasets: [
        { label: "baseline", data: [], backgroundColor: "rgba(196,199,206,0.45)", borderColor: "#c4c7ce", borderWidth: 1 },
        { label: "selected", data: [], backgroundColor: accent, borderColor: accent.replace("0.55", "1"), borderWidth: 1 },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: "#c4c7ce" } } },
      scales: {
        x: { ticks: { color: "#9aa0a6", maxRotation: 0 }, grid: { color: "#242936" } },
        y: { beginAtZero: true, ticks: { color: "#9aa0a6" }, grid: { color: "#242936" }, title: { display: true, text: yTitle, color: "#9aa0a6" } },
      },
    },
  });
}
const chartSizeWait = makeSizeBarChart("sizeWaitChart", "rgba(245,165,36,0.55)", "wait p90 (мин)");
const chartSizeExec = makeSizeBarChart("sizeExecChart", "rgba(61,214,140,0.55)", "exec p90 (мин)");
const chartSizeTotal = makeSizeBarChart("sizeTotalChart", "rgba(106,167,255,0.55)", "total p90 (мин)");

const chartUnitsPerPr = new Chart(document.getElementById("unitsPerPrChart"), {
  type: "bar",
  data: {
    labels: ["Units на 1 PR"],
    datasets: [
      {
        label: "baseline",
        data: [0],
        backgroundColor: "rgba(196,199,206,0.65)",
        borderColor: "#c4c7ce",
        borderWidth: 1,
        maxBarThickness: 120,
      },
      {
        label: "selected",
        data: [0],
        backgroundColor: "rgba(56,189,248,0.75)",
        borderColor: "#38bdf8",
        borderWidth: 1,
        maxBarThickness: 120,
      },
    ],
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { labels: { color: "#c4c7ce" } },
      tooltip: {
        callbacks: {
          label: (ctx) => ` ${ctx.dataset.label}: ${fmt(ctx.raw, 0)} Units / PR`,
        },
      },
    },
    scales: {
      x: { ticks: { color: "#e8eaed", font: { size: 13, weight: "600" } }, grid: { display: false } },
      y: {
        beginAtZero: true,
        ticks: { color: "#9aa0a6" },
        grid: { color: "#242936" },
        title: { display: true, text: "Units на 1 PR", color: "#9aa0a6" },
      },
    },
  },
});
function prCountInView(scenario) {
  if (!scenario) return 0;
  // Prefer sum of hourly PR arrivals in the current window (works for grid + filters).
  const fromSeries = viewSeriesSum(scenario, "week_pr_count");
  if (fromSeries > 0) return fromSeries;
  return scenario.n_prs || 0;
}
function unitsPerPr(scenario) {
  const bill = kpiBilling(scenario);
  const n = prCountInView(scenario);
  return n > 0 ? bill.units / n : 0;
}
function refreshUnitsPerPr(baseline, selected, labelA, labelB) {
  const a = unitsPerPr(baseline);
  const b = unitsPerPr(selected);
  const na = prCountInView(baseline);
  const nb = prCountInView(selected);
  const ua = kpiBilling(baseline).units;
  const ub = kpiBilling(selected).units;
  const scope = (chartWindowMode() === "day"
    ? `${DOW_RU[selectedDow()]}, ${hourRangeText()}, ${sizeHint()}`
    : `неделя, ${hourRangeText()}, ${sizeHint()}`);
  document.getElementById("titleUnitsPerPr").textContent =
    `Units на 1 PR · ${scope}`;
  chartUnitsPerPr.data.datasets[0].label = `${labelA} · ${fmt(a, 0)}`;
  chartUnitsPerPr.data.datasets[1].label = `${labelB} · ${fmt(b, 0)}`;
  chartUnitsPerPr.data.datasets[0].data = [a];
  chartUnitsPerPr.data.datasets[1].data = [b];
  chartUnitsPerPr.update();
  document.getElementById("unitsPerPrSub").innerHTML =
    `Сколько в среднем стоит <b>один</b> PR-check в Units (биллинг / число PR в окне). `
    + `baseline: ${fmt(ua,0)} Units / ${fmt(na,0)} PR = <b>${fmt(a,0)}</b> · `
    + `selected: ${fmt(ub,0)} Units / ${fmt(nb,0)} PR = <b>${fmt(b,0)}</b> `
    + `(${delta(a,b)}).`;
}

/** Latency for one size bucket in the current day/hour window (same filters as line charts). */
function sizeBucketLatency(scenario, sz, metric) {
  const b = scenario.by_size && scenario.by_size[sz];
  if (!b) return 0;
  const weekKey = "week_" + metric + "_p90";
  const hourWindowFull = selectedHours().length === 24 && chartWindowMode() === "week";
  if (hourWindowFull && b[metric + "_p90"] != null) return b[metric + "_p90"] || 0;
  const arr = b[weekKey] || [];
  if (!arr.length) return b[metric + "_p90"] || 0;
  return jsPctile(viewSeries(arr).data, 0.9);
}
function visibleSizeIds() {
  const sz = sizeFilter();
  return sz === "all" ? SIZE_ORDER.slice() : SIZE_ORDER.filter((id) => id === sz);
}
function refreshSizeBar(baseline, selected, labelA, labelB) {
  const ids = visibleSizeIds();
  const scope = (chartWindowMode() === "day"
    ? `${DOW_RU[selectedDow()]}, ${hourRangeText()}`
    : `неделя, ${hourRangeText()}`);
  const szHint = sizeFilter() === "all" ? "все размеры" : sizeHint();
  document.getElementById("sizeRowTitle").textContent =
    `2 · Разбивка по размеру PR · ${scope} · ${szHint}`;
  document.getElementById("titleSizeWait").textContent = `Wait p90 по размеру · ${scope}`;
  document.getElementById("titleSizeExec").textContent = `Exec p90 по размеру · ${scope}`;
  document.getElementById("titleSizeTotal").textContent = `Total p90 по размеру · ${scope}`;

  const labels = ids.map((id) => {
    const m = SIZE_META[id] || {};
    const n = selected.by_size && selected.by_size[id] ? fmt(selected.by_size[id].n, 0) : "—";
    return `${m.label || id}\nn≈${n}`;
  });
  const fill = (chart, metric) => {
    const a = ids.map((id) => sizeBucketLatency(baseline, id, metric));
    const b = ids.map((id) => sizeBucketLatency(selected, id, metric));
    chart.data.labels = labels;
    chart.data.datasets[0].label = labelA;
    chart.data.datasets[1].label = labelB;
    chart.data.datasets[0].data = a;
    chart.data.datasets[1].data = b;
    chart.update("none");
  };
  fill(chartSizeWait, "wait");
  fill(chartSizeExec, "exec");
  fill(chartSizeTotal, "total");
}

function scaledQuotaText(scale) {
  const R = D.reserved || {}, Q = D.quotas || {};
  const f = (k) => Math.max(Math.round((R[k]||0) + ((Q[k]||0)-(R[k]||0))*scale), (R[k]||0)+1);
  return `quotas ≈ inst=${f("instances")} · vcpu=${f("vcpu")} · ram=${f("ram_gb")} · ssd=${f("nrd_ssd_gb")}`;
}

function pick(policyId, scale) {
  const g = GRID[policyId];
  if (!g) return null;
  return g[sk(scale)] || g[String(scale)] || g[scale.toFixed(2)] || null;
}

function viewIsFull() {
  return chartWindowMode() === "week" && selectedHours().length === 24 && sizeFilter() === "all";
}
function jsPctile(vals, p) {
  const a = vals.filter((v) => v != null && !Number.isNaN(+v)).map(Number).sort((x, y) => x - y);
  if (!a.length) return 0;
  const i = Math.min(a.length - 1, Math.max(0, Math.floor(p * (a.length - 1))));
  return a[i];
}
function viewSeriesVals(scenario, key) {
  return viewSeries(seriesFrom(scenario, key)).data.map(Number);
}
function viewSeriesSum(scenario, key) {
  return viewSeriesVals(scenario, key).reduce((s, v) => s + (Number.isFinite(v) ? v : 0), 0);
}
function viewSeriesMax(scenario, key) {
  return viewSeriesVals(scenario, key).reduce((m, v) => Math.max(m, Number.isFinite(v) ? v : 0), 0);
}
/** Latency KPI for current window/size (same filters as charts). */
function kpiLatency(scenario, metric) {
  // metric: "wait" | "exec" | "total"
  if (viewIsFull()) return scenario[metric + "_p90"] || 0;
  const sz = sizeFilter();
  if (sz !== "all" && chartWindowMode() === "week" && selectedHours().length === 24) {
    const b = scenario.by_size && scenario.by_size[sz];
    if (b && b[metric + "_p90"] != null) return b[metric + "_p90"];
  }
  return jsPctile(viewSeriesVals(scenario, "week_" + metric + "_p90"), 0.9);
}
function kpiBilling(scenario) {
  const fullInst = scenario.bill_pr_instance_hours ?? scenario.bill_instance_hours ?? scenario.pr_runner_hours_demanded ?? 0;
  const fullVcpu = scenario.bill_pr_vcpu_hours ?? scenario.bill_vcpu_hours ?? 0;
  const fullUnits = scenarioBillUnits(scenario);
  if (viewIsFull()) return { inst: fullInst, vcpu: fullVcpu, units: fullUnits };
  const hasUnitsSeries = (scenario.week_pr_bill_units || []).length
    || (sizeFilter() !== "all" && scenario.by_size && scenario.by_size[sizeFilter()]
        && (scenario.by_size[sizeFilter()].week_pr_bill_units || []).length);
  if (hasUnitsSeries) {
    const units = viewSeriesSum(scenario, "week_pr_bill_units");
    const inst = viewSeriesSum(scenario, "week_pr_bill_instance_hours");
    const ratio = fullInst > 0 ? fullVcpu / fullInst : 0;
    return { inst, vcpu: inst * ratio, units };
  }
  const inst = viewSeriesSum(scenario, "week_pr_bill_instance_hours");
  const ratioV = fullInst > 0 ? fullVcpu / fullInst : 0;
  const ratioU = fullInst > 0 ? fullUnits / fullInst : 0;
  return { inst, vcpu: inst * ratioV, units: inst * ratioU };
}
/** Peak cards: true scenario peak when full view; else max jobs/h in window. */
function kpiPeak(scenario) {
  // Peak vCPU% is a scenario-level quota metric (policy × quota), not an
  // hourly series — always show it; window/size filters do not change it.
  return {
    pctVcpu: scenario.pct_vcpu || 0,
    demand: scenario.demand_peak_runners || 0,
  };
}

function refreshCharts() {
  const pi = +document.getElementById("policyRange").value;
  const si = +document.getElementById("quotaRange").value;
  const policyId = POLICY_IDS[pi];
  const scale = SCALES[si];
  const modeName = POLICY_LABELS[policyId];
  document.getElementById("policyVal").textContent = modeName;
  document.getElementById("policyDetail").textContent = policyHint(policyId);
  document.getElementById("quotaVal").textContent = scale + "× от сегодня";
  document.getElementById("quotaDetail").textContent = scaledQuotaText(scale);
  syncDayControls();
  const winHint = (chartWindowMode() === "day"
    ? ` Окно: ${DOW_RU[selectedDow()]}, ${hourRangeText()}, ${sizeHint()}.`
    : ` Окно: вся неделя, ${hourRangeText()}, ${sizeHint()}.`);
  const scopeShort = (chartWindowMode() === "day"
    ? `${DOW_RU[selectedDow()]}, ${hourRangeText()}, ${sizeHint()}`
    : `неделя, ${hourRangeText()}, ${sizeHint()}`);

  // baseline = always OFF @1.0×; selected = slider policy @ slider quota.
  const baseline = pick("off", 1.0);
  const selected = pick(policyId, scale);
  if (!baseline || !selected) return;

  const bBase = kpiBilling(baseline);
  const bSel = kpiBilling(selected);
  const pBase = kpiPeak(baseline);
  const pSel = kpiPeak(selected);
  const applyView = (chart, a, b, labelA, labelB) => {
    const va = viewSeries(a), vb = viewSeries(b);
    chart.data.labels = va.labels;
    chart.options.scales.x.ticks.maxTicksLimit = Math.min(48, Math.max(12, va.labels.length));
    chart.data.datasets[0].data = va.data;
    chart.data.datasets[0].label = labelA;
    chart.data.datasets[1].data = vb.data;
    chart.data.datasets[1].label = labelB;
    chart.data.datasets[1].hidden = false;
    chart.update("none");
  };
  const applyKey = (chart, sa, sb, key, labelA, labelB) =>
    applyView(chart, seriesFrom(sa, key), seriesFrom(sb, key), labelA, labelB);

  const la = "baseline (OFF @1×)";
  const lb = `selected (${modeName} @${scale}×)`;

  document.getElementById("viewSummary").innerHTML = `
      <div class="ask">baseline vs selected</div>
      <div><b style="color:#c4c7ce">baseline</b> = OFF @1× ·
        <b style="color:#6aa7ff">selected</b> = ${modeName} @${scale}× (ползунки выше).
        KPI и графики считают в окне: <b>${scopeShort}</b>.</div>
    `;
  const kpiPair = (a, b, unit, digits) => {
    const d = digits == null ? 0 : digits;
    const u = unit || "";
    if (a == null || b == null) {
      return `<div class="v" style="opacity:.55">— <span style="font-size:0.75em">(нет в окне)</span></div>`;
    }
    return `<div class="v">${fmt(a,d)}${u}→${fmt(b,d)}${u} <span style="font-size:0.85em">(${delta(a,b)})</span></div>`;
  };
  const uPerPrA = unitsPerPr(baseline);
  const uPerPrB = unitsPerPr(selected);
  const peakNote = viewIsFull()
    ? "peak vCPU% · baseline → selected"
    : "peak vCPU% сценария (не режется окном/размером)";
  document.getElementById("liveKpi").innerHTML = `
      <div class="card">${kpiPair(kpiLatency(baseline,"wait"), kpiLatency(selected,"wait"), "м")}<div class="l">wait p90 · ${scopeShort}<br/><span style="opacity:.8">${la} → ${lb}</span></div></div>
      <div class="card">${kpiPair(kpiLatency(baseline,"exec"), kpiLatency(selected,"exec"), "м")}<div class="l">exec p90 · ${scopeShort}<br/><span style="opacity:.8">${la} → ${lb}</span></div></div>
      <div class="card">${kpiPair(kpiLatency(baseline,"total"), kpiLatency(selected,"total"), "м")}<div class="l">total p90 · ${scopeShort}<br/><span style="opacity:.8">${la} → ${lb}</span></div></div>
      <div class="card">${kpiPair(bBase.units, bSel.units)}<div class="l">биллинг PR Units · ${scopeShort}<br/><span style="opacity:.8">${la} → ${lb}</span></div></div>
      <div class="card">${kpiPair(bBase.inst, bSel.inst)}<div class="l">биллинг PR instance-h · ${scopeShort}<br/><span style="opacity:.8">${la} → ${lb}</span></div></div>
      <div class="card">${kpiPair(bBase.vcpu, bSel.vcpu)}<div class="l">биллинг PR vCPU-h · ${scopeShort}<br/><span style="opacity:.8">${la} → ${lb}</span></div></div>
      <div class="card">${kpiPair(pBase.pctVcpu, pSel.pctVcpu, "%")}<div class="l">${peakNote}<br/><span style="opacity:.8">${la} → ${lb}</span></div></div>
      <div class="card">${kpiPair(uPerPrA, uPerPrB)}<div class="l">Units на 1 PR · ${scopeShort}<br/><span style="opacity:.8">${la} → ${lb}</span></div></div>`;
  document.getElementById("chartSub").textContent =
    `Серая = baseline, цветная = selected · ${la} → ${lb}.` + winHint
    + ` Units = vCPU+RAM+SSD по тарифу YC Ice Lake (абстрактные Units, не валюта).`;
  refreshSizeBar(baseline, selected, la, lb);
  refreshUnitsPerPr(baseline, selected, la, lb);
  applyKey(chartWait, baseline, selected, "week_wait_p90", la, lb);
  applyKey(chartExec, baseline, selected, "week_exec_p90", la, lb);
  applyKey(chartTotal, baseline, selected, "week_total_p90", la, lb);
  applyKey(chartPrCount, baseline, selected, "week_pr_count", la, lb);
  applyKey(chartPrJobs, baseline, selected, "week_pr_jobs", la, lb);
  applyKey(chartPrBill, baseline, selected, "week_pr_bill_instance_hours", la, lb);
  applyView(chartPrRam, ramSeriesFrom(baseline), ramSeriesFrom(selected), la, lb);
  applyKey(chartPrUnits, baseline, selected, "week_pr_bill_units", la, lb);
}

// Default: recommended policy/scale if present, else 4 shards @ 1.0×
(function initControls() {
  let pIdx = POLICY_IDS.indexOf("s4");
  let sIdx = 0;
  if (QR && QR.best) {
    const bi = POLICY_IDS.indexOf(QR.best.policy_id);
    if (bi >= 0) pIdx = bi;
    if (QR.best.scale != null) {
      const si = SCALES.indexOf(QR.best.scale);
      if (si >= 0) sIdx = si;
    }
  }
  const hourFrom = document.getElementById("hourFrom");
  const hourTo = document.getElementById("hourTo");
  for (let h = 0; h < 24; h++) {
    const t = String(h).padStart(2, "0") + ":00";
    hourFrom.appendChild(new Option(t, String(h)));
    hourTo.appendChild(new Option(t, String(h)));
  }
  hourFrom.value = "0";
  hourTo.value = "23";
  const policyRange = document.getElementById("policyRange");
  policyRange.max = String(Math.max(POLICY_IDS.length - 1, 0));
  policyRange.value = String(pIdx);
  document.getElementById("quotaRange").max = String(SCALES.length - 1);
  document.getElementById("quotaRange").value = String(sIdx);
  document.getElementById("policyRange").oninput = refreshCharts;
  document.getElementById("quotaRange").oninput = refreshCharts;
  document.getElementById("winWeek").onchange = refreshCharts;
  document.getElementById("winDay").onchange = refreshCharts;
  document.getElementById("daySelect").onchange = refreshCharts;
  for (const id of ["hourAll", "hourWork", "hourNight", "hourCustom"]) {
    document.getElementById(id).onchange = () => {
      refreshCharts();
    };
  }
  hourFrom.onchange = () => {
    document.getElementById("hourCustom").checked = true;
    refreshCharts();
  };
  hourTo.onchange = () => {
    document.getElementById("hourCustom").checked = true;
    refreshCharts();
  };
  for (const id of ["sizeAll", "sizeLight", "sizeMid", "sizeHeavy", "sizeXheavy"]) {
    document.getElementById(id).onchange = refreshCharts;
  }
  // default day = сегодня (UTC weekday Mon=0)
  const utcDow = (new Date().getUTCDay() + 6) % 7;
  document.getElementById("daySelect").value = String(utcDow);
  refreshCharts();
})();
</script>
</body>
</html>
"""
    path.write_text(html.replace("__PAYLOAD__", payload), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--capacity-config", type=Path, default=DEFAULT_CAPACITY_CONFIG)
    parser.add_argument("--preset-label", default="build-preset-relwithdebinfo")
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--html", type=Path, default=None)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    capacity_path = args.capacity_config.resolve()
    if not capacity_path.is_file():
        raise SystemExit(f"capacity config not found: {capacity_path}")
    capacity_cfg = load_capacity_config(capacity_path)
    budget_info = pool_budget(capacity_cfg, preset_label=args.preset_label)
    pool = int(budget_info["pool_budget_runners"])

    end = date.today()
    start = end - timedelta(days=args.days)
    print(f"Period: {start} .. {end}", file=sys.stderr)
    bp = budget_info.get("by_preset") or {}
    rw_b = bp.get("relwithdebinfo") or {}
    as_b = bp.get("release-asan") or {}
    print(
        f"Pool: resource scheduler; homogeneous equiv "
        f"rwdi={rw_b.get('pool_budget_runners')} ({rw_b.get('limiting_resource')}), "
        f"asan={as_b.get('pool_budget_runners')} ({as_b.get('limiting_resource')})",
        file=sys.stderr,
    )

    pr_id = workflow_id(args.repo, PR_CHECK_WORKFLOW)
    daily = collect_pr_daily_totals(args.repo, start, end, pr_id)
    span = max((end - start).days, 1)
    sample_dates = [
        start + timedelta(days=int(i * span / max(SAMPLE_DAYS - 1, 1)))
        for i in range(SAMPLE_DAYS)
    ]
    print(f"Sampling: {', '.join(d.isoformat() for d in sample_dates)}", file=sys.stderr)
    observations, presets, hour_w = sample_pr_observations(args.repo, sample_dates, pr_id)
    rwdi = presets.get("build-preset-relwithdebinfo", [])
    asan = presets.get("build-preset-release-asan", [])
    if len(rwdi) < 10:
        raise SystemExit(f"not enough rwdi samples: {len(rwdi)}")
    if len(observations) < 5:
        raise SystemExit(f"not enough PR observations: {len(observations)}")

    empirical_now = summarize_obs(observations)
    print(
        f"NOW n={empirical_now['n']} wait_p90={empirical_now['wait']['p90']:.0f}m "
        f"exec_p90={empirical_now['exec']['p90']:.0f}m total_p90={empirical_now['total']['p90']:.0f}m",
        file=sys.stderr,
    )

    reg_cfg = {
        "Regression-run_Small_and_Medium": {"heavy_jobs": 44, "start_hour": 23},
        "Regression-run_Large": {"heavy_jobs": 12, "start_hour": 23},
        "Regression-run_compatibility": {"heavy_jobs": 8, "start_hour": 23},
        "Regression-run_stress": {"heavy_jobs": 8, "start_hour": 23},
        "Nightly-Build": {"heavy_jobs": 2, "start_hour": 0},
    }
    reg: dict[str, dict[str, float]] = {}
    for name in REGRESSION_WORKFLOWS:
        summary = collect_workflow_summary(args.repo, name, start, end)
        print(f"{name}: n={summary.get('n', 0)} p50={summary.get('p50_min', 0):.1f}", file=sys.stderr)
        if summary.get("n", 0) == 0:
            continue
        reg[name] = {
            "runs_per_day": float(summary["runs_per_day"]),
            "p50_min": float(summary["p50_min"]),
            "heavy_jobs": float(reg_cfg[name]["heavy_jobs"]),
            "start_hour": float(reg_cfg[name]["start_hour"]),
        }

    dow_pr: dict[int, list[int]] = defaultdict(list)
    for row in daily:
        dow_pr[row["dow"]].append(row["total_count"])
    pr_runs_by_dow = {d: statistics.mean(v) for d, v in dow_pr.items()}

    print("Building policy × quota grid for interactive HTML...", file=sys.stderr)
    scenario_grid = build_scenario_grid(
        capacity_cfg=capacity_cfg,
        pr_runs_by_dow=pr_runs_by_dow,
        hour_w=hour_w,
        rwdi=rwdi,
        asan=asan,
        reg=reg,
    )
    # Full scenarios at current quota (1.0×) for tables / decision text.
    scenarios = []
    for policy in POLICIES:
        print(f"Simulating detail {policy['id']} @1.0x...", file=sys.stderr)
        random.seed(args.seed)
        scenarios.append(
            avg_sims(
                policy=policy,
                pr_runs_by_dow=pr_runs_by_dow,
                hour_w=hour_w,
                rwdi=rwdi,
                asan=asan,
                reg=reg,
                capacity_cfg=capacity_cfg,
            )
        )
        # Keep grid@1.0 in sync with detail week series.
        scenario_grid[policy["id"]][scale_key(1.0)] = slim_scenario(scenarios[-1])

    recommendation = pick_recommendation(scenarios, pool)
    tradeoff_comparisons = build_tradeoff_comparisons(scenarios)
    quota_recommendation = recommend_quota_for_speedup(
        scenario_grid,
        base_quotas=capacity_cfg["quotas"],
        reserved=capacity_cfg.get("reserved") or {},
    )
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "capacity_config_path": str(capacity_path),
        "pool_budget": budget_info,
        "pool_budget_runners": pool,
        "quotas": capacity_cfg["quotas"],
        "reserved": capacity_cfg.get("reserved") or {},
        "peak_cap": PEAK_CAP,
        "adaptive_profiles": [
            {
                "id": p["id"],
                "label": p["label"],
                "light_threshold_min": p.get("light_threshold_min"),
                "tiers": list(p.get("tiers") or []),
                "peak_cap": p.get("peak_cap"),
            }
            for p in POLICIES
            if p["mode"] == "adaptive"
        ],
        "size_buckets": {
            "order": list(SIZE_BUCKET_IDS),
            "meta": SIZE_BUCKET_META,
        },
        "billing_units": billing_units_meta(),
        "empirical_now": empirical_now,
        "scenarios": scenarios,
        "scenario_grid": scenario_grid,
        "quota_recommendation": quota_recommendation,
        "recommendation": recommendation,
        "tradeoff_comparisons": tradeoff_comparisons,
        "notes": [
            "Sharding applies to BOTH relwithdebinfo and release-asan, or neither.",
            "wait = minutes from PR ready until heavy jobs start (queue for runners).",
            "exec = heavy critical path wall (max of presets after sharding).",
            "total = wait + exec (approximation of time-to-green for heavy phase).",
            "demand_peak_* = uncapped concurrent resources if every job starts immediately.",
            "pct_* = (reserved + demand_peak) / folder quota — tradeoff vs latency.",
            "bill_pr_units = YC Ice Lake list-price meta-metric (labeled Units, not currency).",
            "scenario_grid: policy × free-capacity multiplier for interactive charts.",
            "Empirical 'now' is from sampled GitHub runs; scenarios are simulated weeks.",
        ],
    }

    print()
    print("=== СЕЙЧАС (эмпирика) ===")
    print(
        f"wait p50/p90: {empirical_now['wait']['p50']:.0f}/{empirical_now['wait']['p90']:.0f}m  |  "
        f"exec: {empirical_now['exec']['p50']:.0f}/{empirical_now['exec']['p90']:.0f}m  |  "
        f"total: {empirical_now['total']['p50']:.0f}/{empirical_now['total']['p90']:.0f}m"
    )
    print()
    print("=== LATENCY ===")
    print(f"{'policy':<28} {'wait90':>7} {'exec90':>7} {'total90':>8}")
    for s in scenarios:
        mark = " *" if s["policy_id"] == recommendation["policy_id"] else ""
        print(
            f"{s['label']:<28} {s['wait_p90']:7.0f} {s['exec_p90']:7.0f} "
            f"{s['total_p90']:8.0f}{mark}"
        )
    print()
    print("=== QUOTA CEILING (peak) vs BILLABLE USAGE (PR / week) ===")
    q = capacity_cfg["quotas"]
    print(
        f"quotas: instances={q['instances']} vcpu={q['vcpu']} "
        f"ram={q['ram_gb']} ssd={q['nrd_ssd_gb']}"
    )
    print(
        f"{'policy':<28} {'demR':>6} {'%vcpu':>6} {'Units':>10} "
        f"{'billInst':>9} {'ΔUnits':>7}"
    )
    off_units = scenarios[0]["bill_pr_units"]
    for s in scenarios:
        mark = " *" if s["policy_id"] == recommendation["policy_id"] else ""
        d_u = (s["bill_pr_units"] - off_units) / max(off_units, 1e-9) * 100
        print(
            f"{s['label']:<28} {s['demand_peak_runners']:6.0f} "
            f"{s['pct_vcpu']:5.0f}% {s['bill_pr_units']:10.0f} "
            f"{s['bill_pr_instance_hours']:9.0f} {d_u:+6.0f}%{mark}"
        )
    print()
    print(f"RECOMMENDATION (total|quota soft): {recommendation['label']}")
    print(f"  {recommendation['why']}")
    print()
    print("=== TRADEOFF: total + Units ===")
    print(f"  {tradeoff_comparisons['by_total_units']['why']}")
    print("=== TRADEOFF: total + vCPU% ===")
    print(f"  {tradeoff_comparisons['by_total_cpu']['why']}")
    print()
    print("=== QUOTA FOR GOOD SPEEDUP (−40% total, wait≤OFF) ===")
    print(quota_recommendation["why"])

    html_path = args.html
    if html_path is None and args.out is not None:
        html_path = args.out.with_suffix(".html")
    if html_path is None:
        html_path = Path("/tmp/sharding_forecast.html")

    if args.out:
        args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {args.out}", file=sys.stderr)
    else:
        print(json.dumps(result, indent=2))

    write_html_report(result, html_path)
    print(f"Wrote HTML: {html_path.resolve()}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
