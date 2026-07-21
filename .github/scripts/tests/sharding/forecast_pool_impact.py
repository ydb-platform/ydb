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
MC_WEEKS = 12


def load_capacity_config(path: Path) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError as exc:
        raise SystemExit(
            "PyYAML is required to read runner_capacity.yml "
            "(pip install pyyaml)"
        ) from exc
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise SystemExit(f"invalid capacity config: {path}")
    return data


def pool_budget(config: dict[str, Any], preset_label: str = "build-preset-relwithdebinfo") -> dict[str, Any]:
    """Same budget idea as estimate_runner_capacity.py, but for an empty pool."""
    quotas = config["quotas"]
    reserved = config.get("reserved") or {}
    headroom = float(config.get("headroom_fraction", 1.0))
    footprints = config.get("footprints") or {}
    fp = footprints.get(preset_label) or config.get("default_footprint") or {}
    if "vcpu" not in fp:
        raise SystemExit(f"no footprint for {preset_label} in capacity config")

    free_instances = (quotas["instances"] - reserved.get("instances", 0)) * headroom
    free_vcpu = (quotas["vcpu"] - reserved.get("vcpu", 0)) * headroom
    free_ram = (quotas["ram_gb"] - reserved.get("ram_gb", 0)) * headroom
    free_ssd = (quotas["nrd_ssd_gb"] - reserved.get("nrd_ssd_gb", 0)) * headroom

    fits = [
        free_instances,
        free_vcpu / fp["vcpu"],
        free_ram / fp["ram_gb"],
        free_ssd / fp["nrd_ssd_gb"],
    ]
    budget = max(int(math.floor(min(fits))), 0)
    return {
        "pool_budget_runners": budget,
        "preset_label": preset_label,
        "quotas": quotas,
        "reserved": reserved,
        "headroom_fraction": headroom,
        "footprint": fp,
        "free_before_runners": {
            "instances": round(free_instances, 1),
            "vcpu": round(free_vcpu, 1),
            "ram_gb": round(free_ram, 1),
            "nrd_ssd_gb": round(free_ssd, 1),
        },
        "limiting_resource": ["instances", "vcpu", "ram_gb", "nrd_ssd_gb"][
            min(range(len(fits)), key=lambda i: fits[i])
        ],
    }


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
) -> dict[str, Any]:
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
    peak_by_hour: dict[int, int] = defaultdict(int)
    for t, _kind, delta in pts:
        dt = t - prev_t
        if dt > 0:
            if cur > pool:
                queued += (cur - pool) * dt
                over += dt
            h0 = int(prev_t // 60)
            h1 = int((t - 1e-9) // 60)
            for h in range(h0, h1 + 1):
                peak_by_hour[h] = max(peak_by_hour[h], cur)
        cur += delta
        peak = max(peak, cur)
        prev_t = t

    # Collapse synthetic week to average hour-of-day profile.
    hour_profile = []
    for hour in range(24):
        vals = [peak_by_hour.get(day * 24 + hour, 0) for day in range(7)]
        hour_profile.append(statistics.mean(vals) if vals else 0.0)

    return {
        "peak_concurrency": float(peak),
        "queued_runner_hours": queued / 60.0,
        "saturation_pct_time": over / (7 * 24 * 60) * 100,
        "hour_profile": hour_profile,
    }


def avg_sims(**kwargs: Any) -> dict[str, Any]:
    rows = [simulate_week(**kwargs) for _ in range(MC_WEEKS)]
    out: dict[str, Any] = {
        k: statistics.mean([r[k] for r in rows])
        for k in ("peak_concurrency", "queued_runner_hours", "saturation_pct_time")
    }
    out["hour_profile"] = [
        statistics.mean([r["hour_profile"][h] for r in rows]) for h in range(24)
    ]
    return out


def write_html_report(result: dict[str, Any], path: Path) -> None:
    """Self-contained HTML dashboard with Chart.js graphs."""
    payload = json.dumps(result, ensure_ascii=False)
    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>PR-check sharding forecast</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --bg: #0f1115;
      --panel: #171a21;
      --text: #e8eaed;
      --muted: #9aa0a6;
      --line: #2a2f3a;
      --ok: #3dd68c;
      --warn: #f5a524;
      --info: #6aa7ff;
      --bad: #f07178;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font: 14px/1.45 ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    main {{ max-width: 1100px; margin: 0 auto; padding: 28px 20px 64px; }}
    h1 {{ font-size: 28px; margin: 0 0 8px; font-weight: 650; }}
    h2 {{ font-size: 18px; margin: 28px 0 12px; font-weight: 600; }}
    .sub {{ color: var(--muted); margin-bottom: 20px; }}
    .grid {{ display: grid; gap: 12px; }}
    .grid.stats {{ grid-template-columns: repeat(4, minmax(0, 1fr)); }}
    .grid.cards {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
    .grid.charts {{ grid-template-columns: 1fr 1fr; }}
    @media (max-width: 900px) {{
      .grid.stats, .grid.cards, .grid.charts {{ grid-template-columns: 1fr; }}
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 14px 16px;
    }}
    .stat .value {{ font-size: 26px; font-weight: 700; }}
    .stat .label {{ color: var(--muted); margin-top: 4px; font-size: 12px; }}
    .ok {{ color: var(--ok); }} .warn {{ color: var(--warn); }} .info {{ color: var(--info); }}
    .pill {{
      display: inline-block;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 2px 10px;
      font-size: 12px;
      font-weight: 650;
      letter-spacing: 0.02em;
    }}
    .pill.ENABLE {{ background: rgba(61,214,140,.12); color: var(--ok); border-color: rgba(61,214,140,.35); }}
    .pill.NOT.YET, .pill.NOT_YET {{ background: rgba(245,165,36,.12); color: var(--warn); border-color: rgba(245,165,36,.35); }}
    .pill.N\\/A, .pill.DO {{ background: rgba(154,160,166,.12); color: var(--muted); }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--line); vertical-align: top; }}
    th {{ color: var(--muted); font-size: 12px; font-weight: 600; }}
    .chart-wrap {{ position: relative; height: 300px; }}
    .callout {{
      border: 1px solid var(--line);
      background: #141821;
      border-radius: 10px;
      padding: 12px 14px;
      margin: 16px 0;
      color: var(--muted);
    }}
    code {{ background: #11151c; padding: 1px 6px; border-radius: 4px; }}
  </style>
</head>
<body>
<main>
  <h1>Шардирование PR-check: прогноз</h1>
  <div class="sub" id="subtitle"></div>
  <div class="callout" id="headline"></div>

  <h2>1. Что включать</h2>
  <div class="grid cards" id="decisionCards"></div>
  <div class="card" style="margin-top:12px; overflow:auto">
    <table>
      <thead><tr><th>Workflow</th><th>Решение</th><th>Почему</th></tr></thead>
      <tbody id="decisionRows"></tbody>
    </table>
  </div>

  <h2>2. Длительность PR-check (rwdi)</h2>
  <div class="grid stats" id="durationStats"></div>
  <div class="card" style="margin-top:12px">
    <div class="chart-wrap"><canvas id="durationChart"></canvas></div>
  </div>

  <h2>3. Нагрузка на пул (синтетическая неделя)</h2>
  <div class="grid stats" id="poolStats"></div>
  <div class="grid charts" style="margin-top:12px">
    <div class="card"><div class="chart-wrap"><canvas id="poolChart"></canvas></div></div>
    <div class="card"><div class="chart-wrap"><canvas id="hourChart"></canvas></div></div>
  </div>

  <h2>4. Квоты из конфига</h2>
  <div class="card"><pre id="quotaBox" style="margin:0; white-space:pre-wrap; color:var(--muted)"></pre></div>

  <h2>5. Как пересчитать</h2>
  <div class="card">
    <code>python3 .github/scripts/tests/sharding/forecast_pool_impact.py --days 30 --out /tmp/sharding_forecast.json</code>
    <div class="sub" style="margin-top:8px">Рядом появится HTML: <code id="htmlPathHint"></code></div>
  </div>
</main>
<script>
const DATA = {payload};

function fmt(n, d=0) {{
  if (n === undefined || n === null || Number.isNaN(n)) return "—";
  return Number(n).toLocaleString("en-US", {{ maximumFractionDigits: d, minimumFractionDigits: d }});
}}
function pillClass(decision) {{
  if (decision === "ENABLE") return "ENABLE";
  if (decision === "NOT YET") return "NOT_YET";
  return "DO";
}}
function meaning(decision) {{
  if (decision === "ENABLE") return "Включать шардирование";
  if (decision === "NOT YET") return "Пока не включать";
  return "Не объект этого шардирования";
}}

const emp = DATA.empirical_pr_check_rwdi;
const base = DATA.week_baseline;
const shard = DATA.week_sharded_rwdi;
const both = DATA.week_sharded_rwdi_asan;
const dRwdi = DATA.delta_sharded_rwdi;
const pool = DATA.pool_budget_runners;

document.getElementById("subtitle").textContent =
  `Период ${{DATA.period.start}} → ${{DATA.period.end}} · бюджет пула ${{pool}} runners · снимок ${{DATA.generated_at}}`;

document.getElementById("headline").textContent =
  `Вывод: ENABLE для PR-check/relwithdebinfo (p90 ${{fmt(emp.baseline_p90)}}m → ${{fmt(emp.sharded_offpeak_p90)}}m). ` +
  `ASAN пока NOT YET. Regression/Nightly — N/A (только следить за очередью).`;

document.getElementById("decisionCards").innerHTML = DATA.decisions.map(d => `
  <div class="card">
    <div style="display:flex;justify-content:space-between;gap:8px;align-items:center">
      <strong>${{d.workflow}}</strong>
      <span class="pill ${{pillClass(d.decision)}}">${{d.decision}}</span>
    </div>
    <div style="margin-top:8px;font-weight:600">${{meaning(d.decision)}}</div>
    <div class="sub" style="margin:8px 0 0">${{d.why}}</div>
  </div>`).join("");

document.getElementById("decisionRows").innerHTML = DATA.decisions.map(d => `
  <tr>
    <td>${{d.workflow}}</td>
    <td><span class="pill ${{pillClass(d.decision)}}">${{d.decision}}</span></td>
    <td>${{d.why}}</td>
  </tr>`).join("");

document.getElementById("durationStats").innerHTML = `
  <div class="card stat"><div class="value ok">${{fmt(emp.sharded_offpeak_p90)}}m</div><div class="label">p90 после шардов (off-peak)</div></div>
  <div class="card stat"><div class="value">${{fmt(emp.baseline_p90)}}m</div><div class="label">p90 baseline</div></div>
  <div class="card stat"><div class="value">${{fmt(emp.baseline_p50)}}m</div><div class="label">p50 (почти не меняется)</div></div>
  <div class="card stat"><div class="value info">${{fmt(emp.frac_ge_60*100,1)}}%</div><div class="label">jobs ≥ 60m (будут шардиться)</div></div>`;

document.getElementById("poolStats").innerHTML = `
  <div class="card stat"><div class="value warn">${{fmt(dRwdi.peak_pct,0)}}%</div><div class="label">peak concurrency (rwdi shard)</div></div>
  <div class="card stat"><div class="value warn">${{fmt(dRwdi.queued_pct,0)}}%</div><div class="label">queued runner-hours / week</div></div>
  <div class="card stat"><div class="value">${{fmt(base.peak_concurrency,0)}} → ${{fmt(shard.peak_concurrency,0)}}</div><div class="label">peak runners baseline → shard</div></div>
  <div class="card stat"><div class="value">${{fmt(base.queued_runner_hours,0)}} → ${{fmt(shard.queued_runner_hours,0)}}</div><div class="label">queued runner-hours / week</div></div>`;

const pb = DATA.pool_budget;
document.getElementById("quotaBox").textContent =
  `config: ${{DATA.capacity_config_path}}\\n` +
  `quotas: instances=${{pb.quotas.instances}}, vcpu=${{pb.quotas.vcpu}}, ram_gb=${{pb.quotas.ram_gb}}, ssd=${{pb.quotas.nrd_ssd_gb}}\\n` +
  `budget: ${{pb.pool_budget_runners}} runners (limited by ${{pb.limiting_resource}})`;

document.getElementById("htmlPathHint").textContent = location.pathname.split("/").pop();

const commonOpts = {{
  responsive: true,
  maintainAspectRatio: false,
  plugins: {{ legend: {{ labels: {{ color: "#c4c7ce" }} }} }},
  scales: {{
    x: {{ ticks: {{ color: "#9aa0a6" }}, grid: {{ color: "#242936" }} }},
    y: {{ ticks: {{ color: "#9aa0a6" }}, grid: {{ color: "#242936" }}, beginAtZero: true }}
  }}
}};

new Chart(document.getElementById("durationChart"), {{
  type: "bar",
  data: {{
    labels: ["p50", "p90"],
    datasets: [
      {{ label: "Baseline", data: [emp.baseline_p50, emp.baseline_p90], backgroundColor: "#6b7280" }},
      {{ label: "Sharded off-peak", data: [emp.sharded_offpeak_p50, emp.sharded_offpeak_p90], backgroundColor: "#3dd68c" }},
      {{ label: "Sharded peak hours", data: [emp.sharded_offpeak_p50, emp.sharded_peak_p90], backgroundColor: "#6aa7ff" }}
    ]
  }},
  options: {{ ...commonOpts, plugins: {{ ...commonOpts.plugins, title: {{ display: true, text: "PR-check relwithdebinfo wall time (min)", color: "#e8eaed" }} }} }}
}});

new Chart(document.getElementById("poolChart"), {{
  type: "bar",
  data: {{
    labels: ["Baseline", "Shard rwdi", "Shard rwdi+asan"],
    datasets: [
      {{ label: "Peak concurrency", data: [base.peak_concurrency, shard.peak_concurrency, both.peak_concurrency], backgroundColor: "#6aa7ff" }},
      {{ label: "Queued runner-hours / week", data: [base.queued_runner_hours, shard.queued_runner_hours, both.queued_runner_hours], backgroundColor: "#f5a524" }}
    ]
  }},
  options: {{ ...commonOpts, plugins: {{ ...commonOpts.plugins, title: {{ display: true, text: "Pool pressure by scenario", color: "#e8eaed" }} }} }}
}});

new Chart(document.getElementById("hourChart"), {{
  type: "line",
  data: {{
    labels: [...Array(24).keys()].map(String),
    datasets: [
      {{ label: "Baseline", data: base.hour_profile, borderColor: "#9aa0a6", tension: 0.25, fill: false }},
      {{ label: "Shard rwdi", data: shard.hour_profile, borderColor: "#6aa7ff", tension: 0.25, fill: false }},
      {{ label: "Pool budget", data: Array(24).fill(pool), borderColor: "#f07178", borderDash: [6,4], pointRadius: 0, fill: false }}
    ]
  }},
  options: {{ ...commonOpts, plugins: {{ ...commonOpts.plugins, title: {{ display: true, text: "Avg runner demand by hour UTC", color: "#e8eaed" }} }} }}
}});
</script>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")


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
    parser.add_argument(
        "--capacity-config",
        type=Path,
        default=DEFAULT_CAPACITY_CONFIG,
        help="Path to runner_capacity.yml (quotas / footprints)",
    )
    parser.add_argument(
        "--preset-label",
        default="build-preset-relwithdebinfo",
        help="Runner footprint used for pool budget (default: rwdi)",
    )
    parser.add_argument("--out", type=Path, default=None, help="Write JSON result")
    parser.add_argument(
        "--html",
        type=Path,
        default=None,
        help="Write HTML dashboard (default: alongside --out as .html)",
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    repo = args.repo
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
    print(f"Capacity config: {capacity_path}", file=sys.stderr)
    print(
        f"Pool budget: {pool} runners "
        f"(limited by {budget_info['limiting_resource']}, "
        f"quotas instances={capacity_cfg['quotas']['instances']} "
        f"vcpu={capacity_cfg['quotas']['vcpu']})",
        file=sys.stderr,
    )

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
        "capacity_config_path": str(capacity_path),
        "pool_budget": budget_info,
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
    print(f"Wrote HTML dashboard: {html_path}", file=sys.stderr)
    print(f"Open: {html_path.resolve()}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
