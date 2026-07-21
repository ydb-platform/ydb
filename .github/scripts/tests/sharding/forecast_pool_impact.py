#!/usr/bin/env python3
"""Reproduce the PR-check sharding pool-impact forecast.

Collects last-N-days GitHub Actions stats and prints a decision table:
which workflows should enable sharding, and how the shared runner pool
is expected to move.

Requires: gh auth, network.

Example:
  python3 .github/scripts/tests/sharding/forecast_pool_impact.py
  python3 .github/scripts/tests/sharding/forecast_pool_impact.py --days 30 --out /tmp/sharding_forecast.json
"""
from __future__ import annotations

import argparse
import json
import math
import random
import statistics
import subprocess
import sys
from collections import defaultdict
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

# From runner_capacity.yml (keep in sync when quotas change).
QUOTA_INSTANCES = 140
RESERVED_INSTANCES = 22
HEADROOM = 0.9
VCPU_QUOTA = 8000
VCPU_RESERVED = 200
RWDI_VCPU = 64

PREPARE_MIN = 8.0
MERGE_MIN = 2.0
LIGHT_THRESHOLD_MIN = 60.0
MAX_WALL_MIN = 240.0
PEAK_HOURS_UTC = range(9, 17)
PEAK_CAP = 4
HEAVY_RUN_FRAC = 0.45
SAMPLE_DAYS = 8
SAMPLE_RUNS_PER_DAY = 12
MC_WEEKS = 12


def pool_budget() -> int:
    by_instances = int((QUOTA_INSTANCES - RESERVED_INSTANCES) * HEADROOM)
    by_vcpu = int((VCPU_QUOTA - VCPU_RESERVED) * HEADROOM / RWDI_VCPU)
    return min(by_instances, by_vcpu)


def run_gh_json(args: list[str]) -> Any:
    out = subprocess.check_output(["gh", "api", *args], text=True)
    return json.loads(out)


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


def choose_shards(duration_min: float, hour_utc: int) -> int:
    if duration_min < LIGHT_THRESHOLD_MIN:
        n = 1
    elif duration_min < 120:
        n = 4
    elif duration_min < 200:
        n = 8
    else:
        n = 12
    wall_floor = max(1, math.ceil(duration_min / MAX_WALL_MIN))
    n = max(n, wall_floor)
    if hour_utc in PEAK_HOURS_UTC:
        n = min(n, PEAK_CAP)
    return max(n, wall_floor)


def sharded_wall(duration_min: float, shards: int) -> float:
    if shards <= 1:
        return duration_min
    return PREPARE_MIN + duration_min / shards + MERGE_MIN


def collect_pr_daily_totals(repo: str, start: date, end: date, wf_id: str) -> list[dict[str, Any]]:
    rows = []
    cur = start
    while cur <= end:
        total = run_gh_json(
            [
                f"repos/{repo}/actions/workflows/{wf_id}/runs?per_page=1&created={cur.isoformat()}",
            ]
        )["total_count"]
        rows.append({"date": cur.isoformat(), "dow": cur.weekday(), "total_count": total})
        cur += timedelta(days=1)
    return rows


def sample_pr_job_durations(repo: str, days: list[date], wf_id: str) -> dict[str, list[float]]:
    by_preset: dict[str, list[float]] = defaultdict(list)
    for day in days:
        data = run_gh_json(
            [
                f"repos/{repo}/actions/workflows/{wf_id}/runs?per_page=30&created={day.isoformat()}",
            ]
        )
        runs = [
            r
            for r in data.get("workflow_runs", [])
            if r.get("conclusion") in ("success", "failure")
        ][:SAMPLE_RUNS_PER_DAY]
        for run in runs:
            jobs = run_gh_json(
                [f"repos/{repo}/actions/runs/{run['id']}/jobs?per_page=100"]
            ).get("jobs", [])
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
    return dict(by_preset)


def collect_workflow_summary(repo: str, name: str, start: date, end: date) -> dict[str, Any]:
    wf = workflow_id(repo, name)
    created = f"{start.isoformat()}..{end.isoformat()}"
    durs: list[float] = []
    conclusions: dict[str, int] = defaultdict(int)
    page = 1
    while page <= 15:
        data = run_gh_json(
            [
                f"repos/{repo}/actions/workflows/{wf}/runs?per_page=100&page={page}&created={created}",
            ]
        )
        runs = data.get("workflow_runs") or []
        if not runs:
            break
        for run in runs:
            if not run.get("conclusion"):
                continue
            conclusions[run["conclusion"]] += 1
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
        "conclusions": dict(conclusions),
    }


def empirical_duration_impact(rwdi: list[float]) -> dict[str, Any]:
    off = [sharded_wall(d, choose_shards(d, 3)) if d >= LIGHT_THRESHOLD_MIN else d for d in rwdi]
    peak = [sharded_wall(d, choose_shards(d, 12)) if d >= LIGHT_THRESHOLD_MIN else d for d in rwdi]
    base_p90 = sorted(rwdi)[int(0.9 * (len(rwdi) - 1))]
    off_p90 = sorted(off)[int(0.9 * (len(off) - 1))]
    peak_p90 = sorted(peak)[int(0.9 * (len(peak) - 1))]
    return {
        "n": len(rwdi),
        "frac_ge_60": sum(1 for d in rwdi if d >= LIGHT_THRESHOLD_MIN) / len(rwdi),
        "baseline_p50": statistics.median(rwdi),
        "baseline_p90": base_p90,
        "sharded_offpeak_p50": statistics.median(off),
        "sharded_offpeak_p90": off_p90,
        "sharded_peak_p90": peak_p90,
        "p90_delta_pct_offpeak": (off_p90 - base_p90) / base_p90 * 100,
    }


def simulate_week(
    *,
    scenario: str,
    pr_runs_by_dow: dict[int, float],
    hour_w: dict[int, float],
    rwdi: list[float],
    asan: list[float],
    reg: dict[str, dict[str, float]],
    pool: int,
) -> dict[str, float]:
    events: list[tuple[float, float, int]] = []
    for dow in range(7):
        n_runs = int(round(pr_runs_by_dow[dow] * HEAVY_RUN_FRAC))
        for _ in range(n_runs):
            r = random.random()
            acc = 0.0
            hour = 12
            for h in range(24):
                acc += hour_w.get(h, 0.0)
                if r <= acc:
                    hour = h
                    break
            start = dow * 24 * 60 + hour * 60 + random.randint(0, 59)
            for preset, arr, can_shard in (
                ("rwdi", rwdi, scenario in ("sharded_rwdi", "sharded_rwdi_asan")),
                ("asan", asan, scenario == "sharded_rwdi_asan"),
            ):
                d = random.choice(arr) if arr else 30.0
                if can_shard and d >= LIGHT_THRESHOLD_MIN:
                    n = choose_shards(d, hour)
                    wall = sharded_wall(d, n)
                    runners = n
                else:
                    wall = d
                    runners = 1
                events.append((float(start), float(start + wall), runners))

        for name, cfg in reg.items():
            expected = cfg["runs_per_day"]
            n_today = 1 if random.random() < min(1.0, expected) else 0
            if expected > 1 and random.random() < (expected - 1):
                n_today += 1
            for _ in range(n_today):
                start = dow * 24 * 60 + int(cfg["start_hour"]) * 60 + random.randint(0, 30)
                d = cfg["p50_min"] * random.uniform(0.85, 1.15)
                events.append((float(start), float(start + d), int(cfg["heavy_jobs"])))

    pts: list[tuple[float, int, int]] = []
    for start, end, runners in events:
        pts.append((start, 1, runners))
        pts.append((end, 0, -runners))
    pts.sort(key=lambda x: (x[0], x[1]))

    cur = peak = 0
    prev_t = 0.0
    queued = over = 0.0
    for t, _kind, delta in pts:
        dt = t - prev_t
        if dt > 0 and cur > pool:
            queued += (cur - pool) * dt
            over += dt
        cur += delta
        peak = max(peak, cur)
        prev_t = t
    return {
        "peak_concurrency": float(peak),
        "queued_runner_hours": queued / 60.0,
        "saturation_pct_time": over / (7 * 24 * 60) * 100,
    }


def avg_sims(**kwargs: Any) -> dict[str, float]:
    rows = [simulate_week(**kwargs) for _ in range(MC_WEEKS)]
    return {k: statistics.mean([r[k] for r in rows]) for k in rows[0]}


def decision_table(emp: dict[str, Any], delta_rwdi: dict[str, float], delta_both: dict[str, float]) -> list[dict[str, str]]:
    """Human-readable enable/disable decisions."""
    return [
        {
            "workflow": "PR-check / relwithdebinfo",
            "decision": "ENABLE",
            "why": (
                f"p90 wall {emp['baseline_p90']:.0f}m → {emp['sharded_offpeak_p90']:.0f}m "
                f"off-peak ({emp['p90_delta_pct_offpeak']:.0f}%). "
                f"Pool cost: peak +{delta_rwdi['peak_pct']:.0f}%, queue +{delta_rwdi['queued_pct']:.0f}%."
            ),
        },
        {
            "workflow": "PR-check / release-asan",
            "decision": "NOT YET",
            "why": (
                f"Same duration win pattern, but together with rwdi queue grows "
                f"+{delta_both['queued_pct']:.0f}% and peak +{delta_both['peak_pct']:.0f}%. "
                "Turn on after rwdi pilot looks stable."
            ),
        },
        {
            "workflow": "Regression-run_* / Nightly-Build",
            "decision": "DO NOT SHARD (N/A)",
            "why": (
                "This sharding pipeline is for PR-check graph replay only. "
                "Regression already fans out by branch×preset. Watch queue wait "
                "when PR-check sharding is on; do not apply PR-check shards here."
            ),
        },
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    repo = args.repo
    random.seed(args.seed)

    end = date.today()
    start = end - timedelta(days=args.days)
    pool = pool_budget()
    print(f"Period: {start} .. {end}", file=sys.stderr)
    print(f"Pool budget (approx): {pool} runners", file=sys.stderr)

    pr_id = workflow_id(repo, PR_CHECK_WORKFLOW)
    daily = collect_pr_daily_totals(repo, start, end, pr_id)
    print(f"PR-check daily totals: {len(daily)} days", file=sys.stderr)

    # Sample job durations from evenly spaced days in the window.
    span = max((end - start).days, 1)
    sample_dates = [
        start + timedelta(days=int(i * span / max(SAMPLE_DAYS - 1, 1)))
        for i in range(SAMPLE_DAYS)
    ]
    print(f"Sampling heavy jobs on: {', '.join(d.isoformat() for d in sample_dates)}", file=sys.stderr)
    presets = sample_pr_job_durations(repo, sample_dates, pr_id)
    rwdi = presets.get("build-preset-relwithdebinfo", [])
    asan = presets.get("build-preset-release-asan", [])
    if len(rwdi) < 10:
        raise SystemExit(f"not enough rwdi samples: {len(rwdi)}")

    emp = empirical_duration_impact(rwdi)
    print(
        f"rwdi samples={emp['n']} p50={emp['baseline_p50']:.1f}m p90={emp['baseline_p90']:.1f}m "
        f"sharded_p90_offpeak={emp['sharded_offpeak_p90']:.1f}m",
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
        summary = collect_workflow_summary(repo, name, start, end)
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

    # Flat hourly weights if we lack a better shape.
    hour_w = {h: 1 / 24 for h in range(24)}

    baseline = avg_sims(
        scenario="baseline",
        pr_runs_by_dow=pr_runs_by_dow,
        hour_w=hour_w,
        rwdi=rwdi,
        asan=asan,
        reg=reg,
        pool=pool,
    )
    sharded = avg_sims(
        scenario="sharded_rwdi",
        pr_runs_by_dow=pr_runs_by_dow,
        hour_w=hour_w,
        rwdi=rwdi,
        asan=asan,
        reg=reg,
        pool=pool,
    )
    both = avg_sims(
        scenario="sharded_rwdi_asan",
        pr_runs_by_dow=pr_runs_by_dow,
        hour_w=hour_w,
        rwdi=rwdi,
        asan=asan,
        reg=reg,
        pool=pool,
    )

    def delta(a: dict[str, float], b: dict[str, float]) -> dict[str, float]:
        return {
            "peak_pct": (b["peak_concurrency"] - a["peak_concurrency"]) / a["peak_concurrency"] * 100,
            "queued_pct": (b["queued_runner_hours"] - a["queued_runner_hours"])
            / max(a["queued_runner_hours"], 1e-9)
            * 100,
            "peak_concurrency": b["peak_concurrency"] - a["peak_concurrency"],
            "queued_runner_hours": b["queued_runner_hours"] - a["queued_runner_hours"],
        }

    d_rwdi = delta(baseline, sharded)
    d_both = delta(baseline, both)
    decisions = decision_table(emp, d_rwdi, d_both)

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "pool_budget_runners": pool,
        "empirical_pr_check_rwdi": emp,
        "week_baseline": baseline,
        "week_sharded_rwdi": sharded,
        "week_sharded_rwdi_asan": both,
        "delta_sharded_rwdi": d_rwdi,
        "delta_sharded_rwdi_asan": d_both,
        "decisions": decisions,
        "how_to_read": [
            "ENABLE = turn on PR-check sharding for that preset.",
            "NOT YET = same mechanism, wait until pool metrics after rwdi pilot are OK.",
            "DO NOT SHARD = this sharding feature does not apply; only watch side-effects on queue.",
        ],
    }

    print()
    print("=== DECISION TABLE ===")
    for row in decisions:
        print(f"[{row['decision']}] {row['workflow']}")
        print(f"  {row['why']}")
        print()
    print("=== POOL (synthetic week, rwdi sharding) ===")
    print(
        f"peak {baseline['peak_concurrency']:.0f} → {sharded['peak_concurrency']:.0f} "
        f"({d_rwdi['peak_pct']:+.0f}%)"
    )
    print(
        f"queued runner-hours/week {baseline['queued_runner_hours']:.0f} → "
        f"{sharded['queued_runner_hours']:.0f} ({d_rwdi['queued_pct']:+.0f}%)"
    )

    if args.out:
        args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {args.out}", file=sys.stderr)
    else:
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
