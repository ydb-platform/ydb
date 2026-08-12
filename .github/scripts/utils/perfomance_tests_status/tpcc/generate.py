#!/usr/bin/env python3
"""Generate Now-first TPC-C HTML report from YDB query JSON.

Focus: last completed run vs previous 7 (lat↑ / tpmC↓ / broken cap), missing in
day-waves, stale clusters. Dive cards show last DISPLAY_RUNS for context.
History keeps the full --since window (day-grain; needed for compare-to-any-day).

Example:
  python3 generate.py --input out/raw.json --output out/tpcc-report.html --open
  # default --since from report_config.json (window_days, default 60)
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
import webbrowser
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TEMPLATE = ROOT / "template.html"
PTS = ROOT.parent
if str(PTS) not in sys.path:
    sys.path.insert(0, str(PTS))

from common.duty_decisions import (  # noqa: E402
    attach_duty_decisions_to_report,
    fetch_duty_decisions_index,
)
from common.duty_issues import (  # noqa: E402
    CLOSED_ISSUES_MAX_AGE_DAYS,
    attach_tickets_to_report,
    fetch_duty_issues,
)
from common.report_config import cfg_float, cfg_int, cfg_str, load_report_config  # noqa: E402

_CFG = load_report_config(ROOT)

LAT_TOL = cfg_float(_CFG, "lat_tol", 0.10)
LAT_WATCH = cfg_float(_CFG, "lat_watch", 0.07)
TPMC_TOL = cfg_float(_CFG, "tpmc_tol", 0.10)
TPMC_WATCH = cfg_float(_CFG, "tpmc_watch", 0.05)
# Slow drift vs older lookback (before alert prev-N window) → soft watch.
DRIFT_LOOKBACK_RUNS = cfg_int(_CFG, "drift_lookback_runs", 21)
TPMC_DRIFT_TOL = cfg_float(_CFG, "tpmc_drift_tol", 0.035)
OUTLIER_MULT = cfg_float(_CFG, "outlier_mult", 3.0)
LAT_CAP = cfg_float(_CFG, "lat_cap", 30000.0)

DEFAULT_WINDOW_DAYS = cfg_int(_CFG, "window_days", 60)  # ~2 months
NOW_RUNS = cfg_int(_CFG, "now_runs", 1)
DISPLAY_RUNS = cfg_int(_CFG, "display_runs", 3)
BASELINE_RUNS = cfg_int(_CFG, "baseline_runs", 7)
EXPECTED_LOOKBACK_DAYS = cfg_int(_CFG, "expected_lookback_days", 14)
EXPECTED_MIN_SHARE = cfg_float(_CFG, "expected_min_share", 0.50)
WAVE_COMPLETE_HOURS = cfg_float(_CFG, "wave_complete_hours", 6)
WAVE_COVERAGE_DONE = cfg_float(_CFG, "wave_coverage_done", 0.85)
STALE_HOURS = cfg_float(_CFG, "stale_hours", 36)
# 0 = keep full --since window (day-grain; OLAP caps run-points instead).
HISTORY_MAX_POINTS = cfg_int(_CFG, "history_max_points", 0)
INBOX_LIMIT = cfg_int(_CFG, "inbox_limit", 80)
INBOX_PER_BRANCH = cfg_int(_CFG, "inbox_per_branch", 45)
INBOX_PER_KIND = dict(
    _CFG.get("inbox_per_kind")
    or {
        "missing": 20,
        "in_progress": 15,
        "broken": 25,
        "both": 15,
        "lat": 20,
        "tpmc": 20,
        "stale": 10,
    }
)
REPORT_MATCH_MAX_SEC = cfg_int(_CFG, "report_match_max_sec", 6 * 3600)

CORE_BRANCHES = tuple(_CFG.get("core_branches") or ("main",))
DATALENS_BASE = cfg_str(_CFG, "datalens_base", "https://datalens.yandex/wf5xdbbl923ok")
DATALENS_TAB = cfg_str(_CFG, "datalens_tab", "9l5")

STATUS_ORDER = {
    "nodata": -1,
    "ok": 0,
    "in_progress": 1,
    "stale": 2,
    "watch": 3,
    "missing": 4,
    "regression": 5,
    "broken": 6,
}


def branch_rank(branch: str) -> int:
    if branch == "main":
        return 0
    if branch.startswith("stable-"):
        return 2
    if branch.startswith("prestable-"):
        return 3
    if branch.startswith("26"):
        return 4
    if branch == "(empty)":
        return 8
    return 9


def select_branches(
    points: list[dict], now_utc: datetime | None = None
) -> list[str]:
    """All branches with points in the report window — no min-points / name gate."""
    del now_utc  # kept for call-site compatibility
    seen: set[str] = set()
    for p in points:
        br = (p.get("branch") or "").strip()
        if br:
            seen.add(br)
    for b in CORE_BRANCHES:
        seen.add(b)
    return sorted(seen, key=lambda b: (branch_rank(b), b))


def select_clusters(points: list[dict], branches: set[str]) -> list[str]:
    """All clusters with points on selected branches — no min-points gate."""
    seen: set[str] = set()
    for p in points:
        if p["branch"] not in branches:
            continue
        c = (p.get("cluster") or "").strip()
        if c:
            seen.add(c)
    return sorted(seen)

def parse_ts(s) -> datetime | None:
    if s is None or s == "":
        return None
    # YDB scan / wrapper often returns Timestamp as integer µs (or ms)
    if isinstance(s, (int, float)) or (isinstance(s, str) and s.isdigit()):
        n = int(s)
        if n >= 10**14:
            sec = n / 1_000_000
        elif n >= 10**11:
            sec = n / 1_000
        else:
            sec = float(n)
        return datetime.fromtimestamp(sec, tz=timezone.utc)
    s = str(s)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "." in s and "+" not in s[s.find("T") :]:
        main, frac = s.split(".", 1)
        frac = "".join(ch for ch in frac if ch.isdigit())[:6].ljust(6, "0")
        s = f"{main}.{frac}+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_rows(path: Path) -> list[dict]:
    raw = json.loads(path.read_text())
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and "result_sets" in raw:
        rs = raw["result_sets"][0]
        cols = rs["columns"]
        names = []
        for c in cols:
            if isinstance(c, str):
                names.append(c)
            elif isinstance(c, dict):
                names.append(c.get("name") or c.get("Name") or next(iter(c.values())))
            else:
                names.append(str(c))
        return [dict(zip(names, row)) for row in rs["rows"]]
    if isinstance(raw, dict) and "rows" in raw and "columns" in raw:
        return [dict(zip(raw["columns"], r)) for r in raw["rows"]]
    raise SystemExit(f"Unsupported JSON shape in {path}")


def norm_branch(raw: str | None) -> str:
    b = (raw or "").strip()
    if not b:
        return "(empty)"
    if b.startswith("origin/"):
        b = b[len("origin/") :]
    if b.startswith("refs/heads/"):
        b = b[len("refs/heads/") :]
    return b or "(empty)"


def run_family(run_type: str) -> str:
    s = run_type or "unknown"
    if s.startswith("ydb_cli_"):
        s = s[len("ydb_cli_") :]
    return s


def suite_name(run_type: str, warehouses: int) -> str:
    return f"{run_family(run_type)}@{warehouses}"


def mart_cluster_to_ci(cluster: str) -> str:
    """perf3 → oltp-perf-3 (tests_results Info.ci_cluster_name)."""
    c = (cluster or "").strip().lower()
    m = re.fullmatch(r"perf(\d+)", c)
    if m:
        return f"oltp-perf-{m.group(1)}"
    return c


def ci_cluster_to_mart(ci_cluster: str) -> str:
    """oltp-perf-3 → perf3."""
    c = (ci_cluster or "").strip().lower()
    m = re.fullmatch(r"oltp-perf-(\d+)", c)
    if m:
        return f"perf{m.group(1)}"
    return c


def allure_suite_for(run_type: str, warehouses: int) -> str | None:
    """Map mart run_type@WH → tests_results Suite (TpccW{WH}T0Snapshot|Serializable)."""
    fam = run_family(run_type).lower()
    if "snapshot" in fam:
        mode = "Snapshot"
    elif "serializable" in fam:
        mode = "Serializable"
    else:
        return None
    try:
        wh = int(warehouses)
    except (TypeError, ValueError):
        return None
    if wh <= 0:
        return None
    return f"TpccW{wh}T0{mode}"


def attach_reports(
    points: list[dict],
    report_rows: list[dict],
    *,
    max_delta_sec: int = REPORT_MATCH_MAX_SEC,
) -> int:
    """Set point['report'] from nearest tests_results Allure URL. Returns match count."""
    by_key: dict[tuple[str, str], list[tuple[datetime, str]]] = defaultdict(list)
    for d in report_rows:
        url = (d.get("report_url") or d.get("Report") or "").strip()
        if not url:
            continue
        suite = str(d.get("Suite") or d.get("suite") or "")
        ci = str(d.get("ci_cluster_name") or d.get("ci_cluster") or "")
        if not suite or not ci:
            continue
        ts = parse_ts(d.get("timestamp") or d.get("Timestamp") or d.get("ts"))
        if ts is None:
            continue
        by_key[(ci.lower(), suite)].append((ts, url))
    for lst in by_key.values():
        lst.sort(key=lambda x: x[0])

    matched = 0
    for p in points:
        p.setdefault("report", None)
        suite = allure_suite_for(p.get("run_type") or "", p.get("warehouses") or 0)
        if not suite:
            continue
        ci = mart_cluster_to_ci(str(p.get("cluster") or ""))
        cand = by_key.get((ci, suite))
        if not cand:
            continue
        ts = p["ts"]
        best_url = None
        best_delta = None
        for rts, url in cand:
            delta = abs((rts - ts).total_seconds())
            if delta > max_delta_sec:
                continue
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_url = url
        if best_url:
            p["report"] = best_url
            matched += 1
    return matched


def wh_label(wh: int) -> str:
    if wh >= 1000 and wh % 1000 == 0:
        return f"{wh // 1000}k"
    return str(wh)


def median(xs):
    vs = [v for v in xs if v is not None]
    return statistics.median(vs) if vs else None


def avg(xs):
    vs = [v for v in xs if v is not None]
    return sum(vs) / len(vs) if vs else None


def percentile(xs, p: float):
    """Nearest-rank percentile; p in [0, 100]."""
    vs = sorted(v for v in xs if v is not None)
    if not vs:
        return None
    if len(vs) == 1:
        return vs[0]
    p = max(0.0, min(100.0, float(p)))
    k = int(round((p / 100.0) * (len(vs) - 1)))
    return vs[k]


def pct(a, b):
    if a is None or b is None or a == 0:
        return None
    return (b - a) / a * 100


def worse(a, b):
    return a if STATUS_ORDER.get(a, 0) >= STATUS_ORDER.get(b, 0) else b


def safe_id(*parts):
    s = "_".join(str(p) for p in parts)
    s = re.sub(r"[^A-Za-z0-9_]+", "_", s)
    return s[:90]


def normalize_points(rows: list[dict], since: datetime) -> list[dict]:
    points = []
    for d in rows:
        ts = parse_ts(d.get("timestamp") or d.get("ts"))
        if ts is None or ts < since:
            continue
        run_type = d.get("run_type") or ""
        if not str(run_type).startswith("ydb_cli_"):
            continue
        lat = d.get("lat90")
        if lat is None:
            lat = d.get("newOrderLatency90")
        lat_f = None if lat is None else float(lat)
        capped = lat_f is not None and lat_f >= LAT_CAP
        tpmc = d.get("tpmC")
        if tpmc is None:
            tpmc = d.get("tpmc")
        version = str(d.get("version") or "")[:12]
        commit_ts = parse_ts(
            d.get("git_commit_timestamp") or d.get("commit_timestamp") or d.get("commit_ts")
        )
        cluster = d.get("cluster") or "unknown"
        wh = int(d.get("warehouses") or 0)
        branch = norm_branch(d.get("git_branch") or d.get("branch"))
        fam = run_family(str(run_type))
        suite = suite_name(str(run_type), wh)
        commit_iso = commit_ts.isoformat().replace("+00:00", "") if commit_ts else None
        points.append(
            {
                "branch": branch,
                "cluster": cluster,
                "db": cluster,  # alias for olap-shaped UI
                "run_type": str(run_type),
                "warehouses": wh,
                "suite": suite,
                "family": fam,
                "ts": ts,
                "ts_iso": ts.isoformat().replace("+00:00", ""),
                "commit_ts": commit_ts,
                "commit_iso": commit_iso,
                "tpmc": None if tpmc is None else float(tpmc),
                "lat90": None if capped or lat_f is None else lat_f,
                "lat_capped": capped,
                "lat_raw": lat_f,
                "version": version,
                "report": d.get("report") or d.get("Report") or None,
                "label": f"{ts.date().isoformat()}_{version[:7] or '—'}",
                "commit_label": (
                    f"{commit_ts.strftime('%Y-%m-%d %H:%M')}_{version[:7] or '—'}"
                    if commit_ts
                    else f"no-commit_{version[:7] or '—'}"
                ),
            }
        )
    return points


def run_view(p: dict) -> dict:
    return {
        "ts": p["ts_iso"][:19],
        "day": p["ts"].date().isoformat(),
        "label": p["label"],
        "tpmc": p["tpmc"],
        "lat90": p["lat90"],
        "lat_capped": p["lat_capped"],
        "lat_raw": p["lat_raw"],
        "version": p["version"],
        "commit_iso": p.get("commit_iso"),
        "report": p.get("report"),
    }


def _history_tail(ordered: list[dict]) -> list[dict]:
    if HISTORY_MAX_POINTS and HISTORY_MAX_POINTS > 0:
        return ordered[-HISTORY_MAX_POINTS:]
    return ordered


def history_view(pts: list[dict]) -> dict:
    """History keyed by run timestamp (default order)."""
    ordered = sorted(pts, key=lambda p: p["ts"])
    tail = _history_tail(ordered)
    return {
        "labels": [p["label"] for p in tail],
        "days": [p["ts_iso"][:10] for p in tail],
        "tpmc": [p["tpmc"] for p in tail],
        "lat90": [p["lat_raw"] if p["lat_capped"] else p["lat90"] for p in tail],
        "markers": ["capped" if p["lat_capped"] else "ok" for p in tail],
        "versions": [p["version"] for p in tail],
        "reports": [p.get("report") for p in tail],
    }


def history_by_commit_view(pts: list[dict]) -> dict:
    """Same points ordered by git commit timestamp (fallback: run ts)."""
    ordered = sorted(
        pts,
        key=lambda p: (p["commit_ts"] or p["ts"], p["ts"]),
    )
    tail = _history_tail(ordered)
    return {
        "labels": [p["commit_label"] for p in tail],
        "days": [
            (p["commit_iso"] or p["ts_iso"] or "")[:10] for p in tail
        ],
        "tpmc": [p["tpmc"] for p in tail],
        "lat90": [p["lat_raw"] if p["lat_capped"] else p["lat90"] for p in tail],
        "markers": ["capped" if p["lat_capped"] else "ok" for p in tail],
        "versions": [p["version"] for p in tail],
        "reports": [p.get("report") for p in tail],
    }


def empty_history() -> dict:
    return {
        "labels": [],
        "days": [],
        "tpmc": [],
        "lat90": [],
        "markers": [],
        "versions": [],
        "reports": [],
    }


def append_synthetic_history(hist: dict, *, day: str, kind: str) -> dict:
    out = {k: list(v) if isinstance(v, list) else v for k, v in hist.items()}
    n = len(out.get("labels") or [])
    markers = list(out.get("markers") or (["ok"] * n))
    while len(markers) < n:
        markers.append("ok")
    ref_lat = next((v for v in reversed(out.get("lat90") or []) if v is not None), None)
    ref_tpmc = next((v for v in reversed(out.get("tpmc") or []) if v is not None), None)
    out.setdefault("labels", []).append(f"{day}_{kind.upper()}")
    out.setdefault("days", []).append(day)
    out.setdefault("lat90", []).append(ref_lat)
    out.setdefault("tpmc", []).append(ref_tpmc)
    out.setdefault("versions", []).append(kind)
    out.setdefault("reports", []).append(None)
    markers.append(kind)
    out["markers"] = markers
    return out


def wave_is_in_progress(age_h: float, present: set[str], expected: set[str]) -> bool:
    if age_h < WAVE_COMPLETE_HOURS:
        return True
    if not expected:
        return age_h < WAVE_COMPLETE_HOURS
    coverage = len(present & expected) / max(1, len(expected))
    return coverage < WAVE_COVERAGE_DONE


def classify_slice(pts: list[dict]) -> dict:
    """Now = last completed run; alert baseline = previous BASELINE_RUNS (median).

    Slow tpmC drift uses a second baseline: p90 of runs before the alert window
    (capped to the oldest DRIFT_LOOKBACK_RUNS). p90 keeps a high anchor so a
    gradual walk-down cannot hide a ~250k→240k slide the way median-prev7 does.
    """
    pts = sorted(pts, key=lambda p: p["ts"])
    now = pts[-NOW_RUNS:]
    base = pts[-(NOW_RUNS + BASELINE_RUNS) : -NOW_RUNS] or pts[: max(1, len(pts) // 2)]
    display = pts[-DISPLAY_RUNS:]
    alert_start = max(0, len(pts) - NOW_RUNS - BASELINE_RUNS)
    older = pts[:alert_start]
    # Oldest chunk of the pre-alert history (not the slice immediately before prev7).
    drift = older[:DRIFT_LOOKBACK_RUNS] if older else []

    lat_now = median([p["lat90"] for p in now if not p["lat_capped"]])
    lat_base = median([p["lat90"] for p in base if not p["lat_capped"]])
    lat_pct = pct(lat_base, lat_now)

    tpmc_now = median([p["tpmc"] for p in now])
    tpmc_base = median([p["tpmc"] for p in base])
    tpmc_pct = pct(tpmc_base, tpmc_now)
    tpmc_drift_base = percentile([p["tpmc"] for p in drift], 90) if drift else None
    tpmc_drift_pct = pct(tpmc_drift_base, tpmc_now)

    capped_now = sum(1 for p in now if p["lat_capped"])

    lat_status = "ok"
    lat_reasons: list[str] = []
    if capped_now > 0:
        lat_status = "broken"
        lat_reasons.append(f"last run lat capped (≥{int(LAT_CAP)})")
    elif lat_base is not None and lat_now is not None and lat_now > lat_base * OUTLIER_MULT:
        lat_status = "broken"
        lat_reasons.append(f"lat outlier >{OUTLIER_MULT:.0f}× (last run)")
    elif lat_pct is not None and lat_pct >= LAT_TOL * 100:
        lat_status = "regression"
        lat_reasons.append(f"lat +{lat_pct:.0f}% vs prev {len(base)} (last run)")
    elif lat_pct is not None and lat_pct >= LAT_WATCH * 100:
        lat_status = "watch"
        lat_reasons.append(f"lat +{lat_pct:.0f}% (watch, last run)")

    tpmc_status = "ok"
    tpmc_reasons: list[str] = []
    if tpmc_pct is not None and tpmc_pct <= -TPMC_TOL * 100:
        tpmc_status = "regression"
        tpmc_reasons.append(f"tpmC {tpmc_pct:.0f}% vs prev {len(base)} (last run)")
    elif tpmc_drift_pct is not None and tpmc_drift_pct <= -TPMC_DRIFT_TOL * 100:
        tpmc_status = "watch"
        tpmc_reasons.append(
            f"tpmC drift {tpmc_drift_pct:.0f}% vs p90 lookback {len(drift)} (before prev{len(base)})"
        )
    elif tpmc_pct is not None and tpmc_pct <= -TPMC_WATCH * 100:
        tpmc_status = "watch"
        tpmc_reasons.append(f"tpmC {tpmc_pct:.0f}% (watch, last run)")

    # watch does not escalate overall above regression from the other metric
    status = worse(
        lat_status if lat_status != "watch" else "ok",
        tpmc_status if tpmc_status != "watch" else "ok",
    )
    if (lat_status == "watch" or tpmc_status == "watch") and status == "ok":
        status = "watch"
    if lat_status == "broken":
        status = "broken"

    kind = "ok"
    lat_hot = lat_status in ("broken", "regression")
    tpmc_hot = tpmc_status == "regression"
    if lat_status == "broken":
        kind = "broken"
        if tpmc_hot:
            kind = "both"
    elif lat_hot and tpmc_hot:
        kind = "both"
    elif lat_hot:
        kind = "lat"
    elif tpmc_hot:
        kind = "tpmc"
    elif lat_status == "watch":
        kind = "lat"
    elif tpmc_status == "watch":
        kind = "tpmc"

    return {
        "status": status,
        "kind": kind,
        "reasons": lat_reasons + tpmc_reasons,
        "lat_base": lat_base,
        "lat_now": lat_now,
        "lat_pct": lat_pct,
        "tpmc_base": tpmc_base,
        "tpmc_now": tpmc_now,
        "tpmc_pct": tpmc_pct,
        "tpmc_drift_base": tpmc_drift_base,
        "tpmc_drift_pct": tpmc_drift_pct,
        "capped_now": capped_now,
        "now_runs": [run_view(p) for p in display],
        "n": len(pts),
        "last_ts": now[-1]["ts_iso"][:19] if now else None,
        "version": now[-1]["version"] if now else "",
        "report": next((p.get("report") for p in reversed(now) if p.get("report")), None),
    }


def build_waves(points: list[dict], lookback_start: datetime, branches: set[str], clusters: set[str]):
    """Day × Branch × Cluster waves; suites = run_type@wh keys present that day."""
    waves: dict[tuple[str, str, str], dict] = {}
    for p in points:
        if p["branch"] not in branches or p["cluster"] not in clusters:
            continue
        if p["ts"] < lookback_start:
            continue
        day = p["ts"].date().isoformat()
        key = (p["branch"], p["cluster"], day)
        w = waves.setdefault(
            key,
            {
                "branch": p["branch"],
                "cluster": p["cluster"],
                "day": day,
                "suites": set(),
                "max_ts": p["ts"],
                "min_ts": p["ts"],
            },
        )
        w["suites"].add(p["suite"])
        w["max_ts"] = max(w["max_ts"], p["ts"])
        w["min_ts"] = min(w["min_ts"], p["ts"])
    by_br_cl: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for w in waves.values():
        by_br_cl[(w["branch"], w["cluster"])].append(w)
    for lst in by_br_cl.values():
        lst.sort(key=lambda x: x["max_ts"])
    return by_br_cl


def collapse_in_progress_suite_dupes(
    inbox_by_id: dict[str, dict], ok_slices: list[dict]
) -> tuple[dict[str, dict], list[dict]]:
    """Drop standalone hot/ok rows when the same suite has in_progress(+finished).

    Otherwise Wave=finished unwraps the twin and inbox shows two identical LAT↑ rows
    (one with wh_label, one blank after unwrap).
    """

    def suite_triple(r: dict) -> tuple[str, str, str]:
        return (r.get("branch") or "", r.get("db") or "", r.get("suite") or "")

    in_prog = {
        suite_triple(r)
        for r in inbox_by_id.values()
        if r.get("issue") == "in_progress" and r.get("suite") and r.get("suite") != "—"
    }
    if not in_prog:
        return inbox_by_id, ok_slices
    collapsed = {
        i: r
        for i, r in inbox_by_id.items()
        if r.get("issue") == "in_progress" or suite_triple(r) not in in_prog
    }
    ok_kept = [r for r in ok_slices if suite_triple(r) not in in_prog]
    return collapsed, ok_kept


def expected_suites(by_br_cl: dict[tuple[str, str], list[dict]]) -> dict[tuple[str, str], set[str]]:
    out: dict[tuple[str, str], set[str]] = {}
    for key, waves in by_br_cl.items():
        if not waves:
            out[key] = set()
            continue
        counts: dict[str, int] = defaultdict(int)
        for w in waves:
            for s in w["suites"]:
                counts[s] += 1
        n = len(waves)
        out[key] = {s for s, c in counts.items() if c / n >= EXPECTED_MIN_SHARE}
    return out


def build_now_report(points: list[dict], since: datetime) -> dict:
    until = max((p["ts"] for p in points), default=since)
    # Wall-clock: if the whole pipeline stops, stale must still fire.
    now_utc = datetime.now(timezone.utc)
    lookback = now_utc - timedelta(days=EXPECTED_LOOKBACK_DAYS)
    branches = select_branches(points, now_utc=now_utc)
    branch_set = set(branches)
    clusters = select_clusters(points, branch_set)
    cluster_set = set(clusters)

    families = sorted({p["family"] for p in points if p["cluster"] in cluster_set and p["branch"] in branch_set})

    slices: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for p in points:
        slices[(p["branch"], p["cluster"], p["suite"])].append(p)

    by_br_cl_waves = build_waves(points, lookback, branch_set, cluster_set)
    expected = expected_suites(by_br_cl_waves)

    inbox: list[dict] = []
    ok_slices: list[dict] = []
    slice_status: dict[tuple[str, str, str], dict] = {}

    for (branch, cluster, suite), pts in slices.items():
        if branch not in branch_set or cluster not in cluster_set:
            continue
        if len(pts) < 1:
            continue
        info = classify_slice(pts)
        fam = pts[0]["family"]
        wh = pts[0]["warehouses"]
        run_type = pts[0]["run_type"]
        info.update(
            {
                "id": safe_id(branch, cluster, suite),
                "branch": branch,
                "db": cluster,
                "cluster": cluster,
                "suite": suite,
                "family": fam,
                "run_type": run_type,
                "warehouses": wh,
                "wh_label": wh_label(wh),
            }
        )
        slice_status[(branch, cluster, suite)] = info
        hist = history_view(pts)
        hist_commit = history_by_commit_view(pts)
        if info["status"] in ("broken", "regression"):
            item = dict(info)
            item["issue"] = info["kind"]
            item["history"] = hist
            item["history_by_commit"] = hist_commit
            inbox.append(item)
        elif info["status"] in ("ok", "watch"):
            item = dict(info)
            item["issue"] = "watch" if info["status"] == "watch" else "ok"
            item["history"] = hist
            item["history_by_commit"] = hist_commit
            ok_slices.append(item)

    for (branch, cluster), waves in by_br_cl_waves.items():
        if not waves:
            continue
        last = waves[-1]
        age_h = (now_utc - last["max_ts"]).total_seconds() / 3600.0
        exp = expected.get((branch, cluster), set())
        present = last["suites"]
        if age_h >= STALE_HOURS:
            inbox.append(
                {
                    "id": safe_id("stale", branch, cluster),
                    "issue": "stale",
                    "status": "stale",
                    "kind": "stale",
                    "branch": branch,
                    "db": cluster,
                    "cluster": cluster,
                    "suite": "—",
                    "family": "—",
                    "warehouses": None,
                    "wh_label": "—",
                    "reasons": [f"no fresh day-wave ≥{STALE_HOURS:.0f}h (last {last['day']})"],
                    "lat_base": None,
                    "lat_now": None,
                    "lat_pct": None,
                    "tpmc_base": None,
                    "tpmc_now": None,
                    "tpmc_pct": None,
                    "capped_now": 0,
                    "now_runs": [],
                    "n": 0,
                    "last_ts": last["max_ts"].isoformat().replace("+00:00", "")[:19],
                    "last_seen": None,
                    "version": "",
                    "history": empty_history(),
                    "history_by_commit": empty_history(),
                    "wave": last["day"],
                }
            )
            continue

        in_prog = wave_is_in_progress(age_h, present, exp)
        prev = waves[-2]["suites"] if len(waves) >= 2 else set()
        if in_prog:
            absent = sorted((prev & exp) - present)
            issue_kind = "in_progress"
        else:
            absent = sorted(exp - present)
            issue_kind = "missing"

        for suite in absent:
            pts = slices.get((branch, cluster, suite), [])
            hist = history_view(pts) if pts else empty_history()
            hist_commit = history_by_commit_view(pts) if pts else empty_history()
            hist = append_synthetic_history(hist, day=last["day"], kind=issue_kind)
            hist_commit = append_synthetic_history(
                hist_commit, day=last["day"], kind=issue_kind
            )
            last_seen = pts[-1]["ts_iso"][:19] if pts else None
            fam = suite.split("@", 1)[0] if "@" in suite else suite
            wh_s = suite.split("@", 1)[1] if "@" in suite else ""
            try:
                wh_i = int(wh_s) if wh_s else None
            except ValueError:
                wh_i = None
            if issue_kind == "in_progress":
                reason = (
                    f"day-wave {last['day']} in progress — {suite} ещё не доехал "
                    f"(last seen {last_seen or 'never'}; не алерт)"
                )
            else:
                reason = (
                    f"absent in completed day-wave {last['day']} "
                    f"(last seen {last_seen or 'never'})"
                )
            finished = None
            fin = slice_status.get((branch, cluster, suite))
            if issue_kind == "in_progress" and fin:
                if fin["status"] in ("broken", "regression"):
                    fin_issue = fin["kind"]
                elif fin["status"] == "watch":
                    fin_issue = "watch"
                else:
                    fin_issue = "ok"
                finished = {
                    "issue": fin_issue,
                    "status": fin["status"],
                    "kind": fin["kind"],
                    "reasons": fin.get("reasons") or [],
                    "lat_base": fin.get("lat_base"),
                    "lat_now": fin.get("lat_now"),
                    "lat_pct": fin.get("lat_pct"),
                    "tpmc_base": fin.get("tpmc_base"),
                    "tpmc_now": fin.get("tpmc_now"),
                    "tpmc_pct": fin.get("tpmc_pct"),
                    "tpmc_drift_base": fin.get("tpmc_drift_base"),
                    "tpmc_drift_pct": fin.get("tpmc_drift_pct"),
                    "capped_now": fin.get("capped_now") or 0,
                    "now_runs": fin.get("now_runs") or [],
                    "n": fin.get("n") or 0,
                    "last_ts": fin.get("last_ts"),
                    "version": fin.get("version") or "",
                    "report": fin.get("report"),
                    "warehouses": fin.get("warehouses") if fin.get("warehouses") is not None else wh_i,
                    "wh_label": fin.get("wh_label") or (wh_label(wh_i) if wh_i is not None else "—"),
                    "wave": fin.get("wave") or "",
                    "history": history_view(pts) if pts else None,
                    "history_by_commit": history_by_commit_view(pts) if pts else None,
                }
            inbox.append(
                {
                    "id": safe_id(issue_kind, branch, cluster, suite),
                    "issue": issue_kind,
                    "status": issue_kind,
                    "kind": issue_kind,
                    "branch": branch,
                    "db": cluster,
                    "cluster": cluster,
                    "suite": suite,
                    "family": fam,
                    "run_type": f"ydb_cli_{fam}" if fam and fam != "—" else "",
                    "warehouses": wh_i,
                    "wh_label": wh_label(wh_i) if wh_i is not None else "—",
                    "reasons": [reason],
                    "lat_base": None,
                    "lat_now": None,
                    "lat_pct": None,
                    "tpmc_base": None,
                    "tpmc_now": None,
                    "tpmc_pct": None,
                    "capped_now": 0,
                    "now_runs": [],
                    "n": len(pts),
                    "last_ts": last["max_ts"].isoformat().replace("+00:00", "")[:19],
                    "last_seen": last_seen,
                    "version": "",
                    "history": hist,
                    "history_by_commit": hist_commit,
                    "wave": last["day"],
                    "wave_in_progress": in_prog,
                    "expected": True,
                    "finished": finished,
                }
            )

    def inbox_key(r):
        return (
            branch_rank(r.get("branch") or ""),
            {
                "missing": 0,
                "broken": 1,
                "both": 1,
                "lat": 2,
                "tpmc": 2,
                "in_progress": 3,
                "stale": 4,
            }.get(r.get("issue"), 9),
            -STATUS_ORDER.get(r["status"], 0),
            -(r.get("lat_pct") or 0),
            (r.get("tpmc_pct") or 0),  # more negative first
            r.get("db") or "",
            r.get("suite") or "",
        )

    by_id: dict[str, dict] = {}
    for r in inbox:
        prev = by_id.get(r["id"])
        if prev is None or STATUS_ORDER.get(r["status"], 0) > STATUS_ORDER.get(prev["status"], 0):
            by_id[r["id"]] = r

    by_id, ok_slices = collapse_in_progress_suite_dupes(by_id, ok_slices)

    all_hot = list(by_id.values())

    cells = {}
    for br in branches:
        for cl in clusters:
            for fam in families:
                cells[f"{br}|{cl}|{fam}"] = {"status": "nodata", "n_hot": 0, "n": 0}

    for (branch, cluster, suite), info in slice_status.items():
        fam = info["family"]
        key = f"{branch}|{cluster}|{fam}"
        if key not in cells:
            continue
        cells[key]["n"] += 1
        if info["status"] in ("broken", "regression"):
            cells[key]["n_hot"] += 1
            cells[key]["status"] = worse(cells[key]["status"], info["status"])
        elif info["status"] in ("ok", "watch"):
            cells[key]["status"] = worse(cells[key]["status"], info["status"])

    for r in all_hot:
        br = r.get("branch") or ""
        fam = r.get("family")
        if r.get("issue") == "missing" and fam in families:
            key = f"{br}|{r['db']}|{fam}"
            if key in cells:
                cells[key]["n_hot"] += 1
                cells[key]["status"] = worse(cells[key]["status"], "missing")
        if r.get("issue") == "in_progress" and fam in families:
            key = f"{br}|{r['db']}|{fam}"
            if key in cells and cells[key]["status"] in ("ok", "nodata", "watch"):
                cells[key]["status"] = "in_progress"
        if r.get("issue") == "stale":
            for fam in families:
                key = f"{br}|{r['db']}|{fam}"
                if key in cells:
                    cells[key]["status"] = worse(cells[key]["status"], "stale")

    def summarize(rows, ok_n=None, slices_n=None):
        hot_rows = [r for r in rows if r.get("issue") != "in_progress"]
        return {
            "missing": sum(1 for r in rows if r.get("issue") == "missing"),
            "in_progress": sum(1 for r in rows if r.get("issue") == "in_progress"),
            "broken": sum(1 for r in rows if r.get("issue") in ("broken", "both") and r.get("status") == "broken"),
            "lat": sum(1 for r in rows if r.get("issue") in ("lat", "both")),
            "tpmc": sum(1 for r in rows if r.get("issue") in ("tpmc", "both")),
            "stale": sum(1 for r in rows if r.get("issue") == "stale"),
            "ok_slices": ok_n if ok_n is not None else sum(
                1 for info in slice_status.values() if info["status"] == "ok"
            ),
            "hot": len(hot_rows),
            "slices": slices_n if slices_n is not None else len(slice_status),
        }

    summary = summarize(all_hot)
    by_branch_summary = {}
    for br in branches:
        br_hot = [r for r in all_hot if r.get("branch") == br]
        br_ok = sum(
            1 for (b, _, _), info in slice_status.items() if b == br and info["status"] == "ok"
        )
        br_slices = sum(1 for (b, _, _) in slice_status if b == br)
        by_branch_summary[br] = summarize(br_hot, ok_n=br_ok, slices_n=br_slices)

    picked: list[dict] = []
    by_branch_rows: dict[str, list[dict]] = defaultdict(list)
    for r in all_hot:
        by_branch_rows[r.get("branch") or "unknown"].append(r)
    for br in branches:
        rows = by_branch_rows.get(br, [])
        by_kind: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            by_kind[r.get("issue") or "other"].append(r)
        br_picked: list[dict] = []
        for kind, limit in INBOX_PER_KIND.items():
            lim = limit if br == "main" else max(5, limit // max(1, len(branches) - 1))
            br_picked.extend(sorted(by_kind.get(kind, []), key=inbox_key)[:lim])
        picked.extend(sorted(br_picked, key=inbox_key)[:INBOX_PER_BRANCH])
    inbox = sorted(picked, key=inbox_key)[: INBOX_LIMIT * max(1, len(branches))]

    display_waves = build_waves(points, since, branch_set, cluster_set)
    display_expected = expected_suites(display_waves)
    # Best sha8 per (branch, cluster, day) for compare dropdown labels.
    wave_sha: dict[tuple[str, str, str], str] = {}
    for p in points:
        if p["cluster"] not in cluster_set or p["branch"] not in branch_set:
            continue
        ver = (p.get("version") or "").strip()
        if not ver or ver == "—":
            continue
        key3 = (p["branch"], p["cluster"], p["ts"].date().isoformat())
        prev = wave_sha.get(key3)
        if prev is None or (len(ver) >= 8 and len(prev) < 8):
            wave_sha[key3] = ver[:8]

    waves_meta = {}
    wave_list: dict[str, list[dict]] = {}
    for (br, cl), waves in display_waves.items():
        if not waves:
            continue
        last = waves[-1]
        max_ts = last["max_ts"]
        exp_n = len(display_expected.get((br, cl), ()))
        waves_meta[f"{br}|{cl}"] = {
            "branch": br,
            "db": cl,
            "cluster": cl,
            "day": last["day"],
            "ci_version": last["day"],
            "suites": len(last["suites"]),
            "expected": exp_n,
            "max_ts": max_ts.isoformat().replace("+00:00", "")[:19],
            "age_hours": round((now_utc - max_ts).total_seconds() / 3600.0, 1),
        }
        entries = []
        for w in reversed(waves):  # newest first — full --since window
            w_max = w["max_ts"]
            w_min = w["min_ts"]
            day = w["day"]
            entries.append(
                {
                    "id": day,
                    "ci_version": day,
                    "day": day,
                    "max_ts": w_max.isoformat().replace("+00:00", "")[:19],
                    "min_ts": w_min.isoformat().replace("+00:00", "")[:19],
                    "sha8": wave_sha.get((br, cl, day), "—"),
                    "suites": len(w["suites"]),
                    "expected": exp_n,
                    "suite_names": sorted(w["suites"]),
                    "current": w is last,
                }
            )
        wave_list[f"{br}|{cl}"] = entries

    waves_by_db: dict[str, dict] = {}
    for meta in waves_meta.values():
        db = meta["db"]
        prev = waves_by_db.get(db)
        if prev is None or meta["max_ts"] > prev["max_ts"]:
            waves_by_db[db] = dict(meta)

    last_activity: dict[str, dict] = {}
    for p in points:
        if p["cluster"] not in cluster_set or p["branch"] not in branch_set:
            continue
        for key in (f"{p['branch']}|{p['cluster']}", p["cluster"]):
            prev = last_activity.get(key)
            if prev is None or p["ts"] > prev["_ts"]:
                last_activity[key] = {
                    "branch": p["branch"],
                    "db": p["cluster"],
                    "cluster": p["cluster"],
                    "ci_version": p["ts"].date().isoformat(),
                    "day": p["ts"].date().isoformat(),
                    "suites": None,
                    "expected": None,
                    "max_ts": p["ts_iso"][:19],
                    "age_hours": round((now_utc - p["ts"]).total_seconds() / 3600.0, 1),
                    "activity_only": True,
                    "_ts": p["ts"],
                }
    for v in last_activity.values():
        v.pop("_ts", None)

    default_branch = "main" if "main" in branches else (branches[0] if branches else "main")

    dbs_by_branch: dict[str, list[str]] = {}
    tmp_dbs: dict[str, set[str]] = defaultdict(set)
    for p in points:
        if p["cluster"] in cluster_set and p["branch"] in branch_set:
            tmp_dbs[p["branch"]].add(p["cluster"])
    for br in branches:
        dbs_by_branch[br] = sorted(tmp_dbs.get(br, ()))

    return {
        "mode": "now",
        "window": f"{since.date().isoformat()}..{until.date().isoformat()}",
        "generated_at": now_utc.isoformat().replace("+00:00", "Z"),
        "source": "perfomance/tpcc + perfomance/olap/tests_results.report_url",
        "ui": {
            "now_runs": NOW_RUNS,
            "display_runs": DISPLAY_RUNS,
            # null = compare dropdown lists every day-wave in the window (no top-N cut).
            "compare_runs": None,
            "baseline_runs": BASELINE_RUNS,
            "drift_lookback_runs": DRIFT_LOOKBACK_RUNS,
            "tpmc_drift_tol": TPMC_DRIFT_TOL,
            "tpmc_watch": TPMC_WATCH,
            "stale_hours": STALE_HOURS,
            "wave_complete_hours": WAVE_COMPLETE_HOURS,
            "focus_branches": branches,
            "default_branch": default_branch,
            "focus_dbs": clusters,
            "dbs_by_branch": dbs_by_branch,
            "default_from": since.date().isoformat(),
            "default_to": until.date().isoformat(),
            "datalens_base": DATALENS_BASE,
            "datalens_tab": DATALENS_TAB,
        },
        "summary": summary,
        "by_branch": by_branch_summary,
        "heatmap": {
            "branches": branches,
            "dbs": clusters,
            "dbs_by_branch": dbs_by_branch,
            "families": families,
            "cells": cells,
        },
        "waves": waves_meta,
        "wave_list": wave_list,
        "waves_by_db": waves_by_db,
        "last_activity": last_activity,
        "inbox": inbox,
        "ok": ok_slices,
        "rules": {
            "now": "last completed run",
            "baseline": f"previous {BASELINE_RUNS} runs (median)",
            "display": f"last {DISPLAY_RUNS} runs in dive",
            "lat": f"+{int(LAT_TOL*100)}%",
            "lat_watch": f"+{int(LAT_WATCH*100)}%",
            "tpmc": f"-{int(TPMC_TOL*100)}%",
            "tpmc_watch": f"-{int(TPMC_WATCH*100)}%",
            "tpmc_drift": (
                f"-{TPMC_DRIFT_TOL*100:.1f}% vs p90 of oldest lookback {DRIFT_LOOKBACK_RUNS} "
                f"(before prev{BASELINE_RUNS}) → watch"
            ),
            "cap": f"lat ≥ {int(LAT_CAP)} → broken",
            "expected": f"suites in ≥{int(EXPECTED_MIN_SHARE*100)}% of day-waves / {EXPECTED_LOOKBACK_DAYS}d",
            "wave": "calendar day × Branch × Cluster",
            "wave_view": "finished (default) = last completed run; all = latest incl. in_progress",
            "compare": "per-cluster day-wave select; full history in window (no top-7 cut)",
        },
    }


def main():
    ap = argparse.ArgumentParser(description="TPC-C Now report generator")
    ap.add_argument("--input", required=True, type=Path)
    ap.add_argument(
        "--reports-input",
        type=Path,
        default=None,
        help="tests_results Allure URLs JSON (default: <input-dir>/reports.json if present)",
    )
    ap.add_argument(
        "--since",
        default=None,
        help=f"YYYY-MM-DD (default: today − {DEFAULT_WINDOW_DAYS}d from report_config.json)",
    )
    ap.add_argument("--until", default=None, help="YYYY-MM-DD optional upper bound")
    ap.add_argument("--output", type=Path, default=ROOT / "out" / "tpcc-report.html")
    ap.add_argument("--open", action="store_true")
    args = ap.parse_args()

    if args.since:
        since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
    else:
        since = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=DEFAULT_WINDOW_DAYS)
    until = None
    if args.until:
        until = datetime.fromisoformat(args.until).replace(tzinfo=timezone.utc) + timedelta(days=1)

    rows = load_rows(args.input)
    points = normalize_points(rows, since)
    if until:
        points = [p for p in points if p["ts"] < until]
    if not points:
        raise SystemExit("No points after filters")

    reports_path = args.reports_input
    if reports_path is None:
        cand = args.input.parent / "reports.json"
        if cand.is_file():
            reports_path = cand
    reports_matched = 0
    if reports_path and reports_path.is_file():
        report_rows = load_rows(reports_path)
        reports_matched = attach_reports(points, report_rows)
        print(f"reports: matched {reports_matched}/{len(points)} from {reports_path}", flush=True)
    else:
        print("reports: skipped (no --reports-input / out/reports.json)", flush=True)

    data = build_now_report(points, since)
    data["meta"] = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "points": len(points),
        "input": str(args.input),
        "reports_input": str(reports_path) if reports_path else None,
        "reports_matched": reports_matched,
    }
    issues, iss_warn = fetch_duty_issues(kind="tpcc", include_closed=True)
    if iss_warn:
        print(f"known_issues: {iss_warn}", flush=True)
    n_tick = attach_tickets_to_report(data, issues, kind="tpcc")
    print(
        f"known_issues: total={len(data.get('known_issues') or [])} "
        f"open={sum(1 for i in (data.get('known_issues') or []) if i.get('state') != 'closed')} "
        f"closed≤{CLOSED_ISSUES_MAX_AGE_DAYS}d="
        f"{sum(1 for i in (data.get('known_issues') or []) if i.get('state') == 'closed')} "
        f"suites_with_tickets={n_tick}",
        flush=True,
    )
    decisions, dec_warn = fetch_duty_decisions_index()
    if dec_warn:
        print(f"duty_decisions: {dec_warn}", flush=True)
    n_dec = attach_duty_decisions_to_report(data, decisions, kind="tpcc")
    print(
        f"duty_decisions: index_items={len((decisions.get('items') or {}))} "
        f"suites_with_decision={n_dec}",
        flush=True,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    json_path = args.output.with_suffix(".json")
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))

    tpl = TEMPLATE.read_text()
    if "__TPCC_REPORT_DATA__" not in tpl:
        raise SystemExit("template.html missing __TPCC_REPORT_DATA__ placeholder")
    payload = (
        json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        .replace("<", "\\u003c")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )
    html = tpl.replace("__TPCC_REPORT_DATA__", payload)
    args.output.write_text(html)

    s = data["summary"]
    print(
        f"points={len(points)} slices={s['slices']} hot={s['hot']} "
        f"broken={s['broken']} lat={s['lat']} tpmc={s['tpmc']} "
        f"missing={s['missing']} stale={s['stale']}"
    )
    print(f"wrote {args.output}")
    print(f"wrote {json_path}")
    if args.open:
        webbrowser.open(args.output.resolve().as_uri())


if __name__ == "__main__":
    main()
