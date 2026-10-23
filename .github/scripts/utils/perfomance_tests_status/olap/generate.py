#!/usr/bin/env python3
"""Generate Now-first OLAP suites HTML report from YDB query JSON.

Focus: last 2–3 runs, missing suites in CiVersion waves, fail/slow alerts.
History is attached only for hot slices (deep dive), not as the alert driver.

Example:
  python3 generate.py --input out/raw.json --output out/olap-report.html --open
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import webbrowser
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from classify_rules import (
    DUR_HARD,
    DUR_TOL,
    FAIL_BROKEN,
    FAIL_HOT,
    FAIL_RISE,
    NOISE_K,
    OUTLIER_MULT,
    SLOW_PERSIST_MIN,
    avg,
    classify_duration,
    fail_status_from_last,
    is_fail_rate_hot,
    median,
    pct,
)

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
from common.report_config import cfg_float, cfg_int, load_report_config  # noqa: E402

_CFG = load_report_config(ROOT)

DEFAULT_WINDOW_DAYS = cfg_int(_CFG, "window_days", 30)

NOW_RUNS = cfg_int(_CFG, "now_runs", 1)
DISPLAY_RUNS = cfg_int(_CFG, "display_runs", 7)
BASELINE_RUNS = cfg_int(_CFG, "baseline_runs", 7)
EXPECTED_LOOKBACK_DAYS = cfg_int(_CFG, "expected_lookback_days", 14)
EXPECTED_MIN_SHARE = cfg_float(_CFG, "expected_min_share", 0.50)
WAVE_COMPLETE_HOURS = cfg_float(_CFG, "wave_complete_hours", 6)
WAVE_COVERAGE_DONE = cfg_float(_CFG, "wave_coverage_done", 0.85)
STALE_HOURS = cfg_float(_CFG, "stale_hours", 36)
HISTORY_MAX_POINTS = cfg_int(_CFG, "history_max_points", 100)
INBOX_LIMIT = cfg_int(_CFG, "inbox_limit", 80)
INBOX_PER_KIND = dict(
    _CFG.get("inbox_per_kind")
    or {
        "missing": 20,
        "in_progress": 15,
        "failing": 30,
        "both": 15,
        "slower": 20,
        "stale": 10,
    }
)

FOCUS_PREFIXES = tuple(
    _CFG.get("focus_prefixes")
    or (
        "Clickbench",
        "Tpch",
        "Tpcds",
        "UploadTpch",
        "WorkloadManager",
    )
)
FAMILIES = list(FOCUS_PREFIXES)

FOCUS_DBS = set(
    _CFG.get("focus_dbs")
    or {
        "sas_big_column",
        "sas_small_column",
        "cloud_slonnn_64_column",
        "cloud_slonnn_128_column",
        "vla_big_column",
        "vla_small_column",
        "vla_3_node_column",
    }
)
# Prefer these first in UI; every branch with points in the window is included.
CORE_BRANCHES = ("main", "trunk")

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
INBOX_PER_BRANCH = 45


def branch_rank(branch: str) -> int:
    if branch == "main":
        return 0
    if branch == "trunk":
        return 1
    if branch.startswith("stable-"):
        return 2
    if branch.startswith("prestable-"):
        return 3
    return 9


def select_branches(
    points: list[dict], now_utc: datetime | None = None
) -> list[str]:
    """All branches that have focus-DB points in the report window — no min-points / name gate."""
    del now_utc  # kept for call-site compatibility
    seen: set[str] = set()
    for p in points:
        if p["db"] not in FOCUS_DBS:
            continue
        br = (p.get("branch") or "").strip()
        if br:
            seen.add(br)
    for b in CORE_BRANCHES:
        seen.add(b)
    return sorted(seen, key=lambda b: (branch_rank(b), b))


def parse_ts(s) -> datetime | None:
    if s is None or s == "":
        return None
    # YDB scan / wrapper often returns Timestamp as integer µs (or ms)
    if isinstance(s, (int, float)) or (isinstance(s, str) and s.isdigit()):
        n = int(s)
        # heuristics: ≥1e14 → µs, ≥1e11 → ms, else seconds
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


def norm_branch(d: dict) -> str:
    for key in ("Branch", "Version", "CiBranch", "CiVersion", "branch", "version"):
        v = d.get(key)
        v = str(v).strip() if v is not None else ""
        if not v or v in ("unknown",) or v.startswith("."):
            continue
        if key.lower() in ("version", "civersion") and "." in v:
            return v.rsplit(".", 1)[0]
        if key.lower() in ("branch", "cibranch"):
            return v
    db = d.get("DbAlias") or d.get("db") or ""
    if "cloud_" in str(db):
        return "trunk"
    return "unknown"


def error_class_of(color, diff_response=None, *, nodata: bool = False) -> str | None:
    """Map mart Color / diff_response → timeout|diff|other|warning (None if n/a)."""
    if nodata:
        return None
    try:
        if diff_response is not None and int(diff_response) > 0:
            return "diff"
    except (TypeError, ValueError):
        pass
    c = (str(color).strip().lower() if color is not None else "")
    if c == "blue":
        return "timeout"
    if c == "yellow":
        return "warning"
    if c == "red":
        return "other"
    return None


def commit_of(d: dict) -> str:
    for key in ("Version", "CiVersion", "version"):
        v = str(d.get(key) or "").strip()
        if v and "." in v and not v.startswith("."):
            return v.rsplit(".", 1)[-1][:12]
        if v and not v.startswith("."):
            return v[:12]
    return "—"


def ci_version_of(d: dict) -> str:
    for key in ("CiVersion", "ci_version", "Version", "version"):
        v = str(d.get(key) or "").strip()
        if v and not v.startswith("."):
            return v
    return ""


def suite_family(suite: str) -> str:
    for p in FOCUS_PREFIXES:
        if suite.startswith(p):
            return p
    return "Other"


def worse(a, b):
    return a if STATUS_ORDER.get(a, 0) >= STATUS_ORDER.get(b, 0) else b


def safe_id(*parts):
    s = "_".join(str(p) for p in parts)
    s = re.sub(r"[^A-Za-z0-9_]+", "_", s)
    return s[:90]


def parse_fail_tests(raw: str) -> list[str]:
    if not raw:
        return []
    out = []
    for part in str(raw).split(","):
        t = part.strip()
        if not t:
            continue
        if t.isdigit() or (len(t) <= 3 and t.replace(".", "").isdigit()):
            t = f"Query{t.zfill(2)}" if t.isdigit() else t
        out.append(t)
    return out[:40]


def normalize_points(rows: list[dict], since: datetime) -> list[dict]:
    points = []
    for d in rows:
        ts = parse_ts(d.get("RunTs") or d.get("run_ts") or d.get("ts"))
        if ts is None or ts < since:
            continue
        suite = d.get("Suite") or d.get("suite") or "unknown"
        if not suite.startswith(FOCUS_PREFIXES):
            continue
        db = d.get("DbAlias") or d.get("db") or "unknown"
        ydb = d.get("YdbSumMeans")
        if ydb is None:
            ydb = d.get("ydb")
        gross = d.get("GrossTime")
        if gross is None:
            gross = d.get("gross")
        success = int(d.get("SuccessCount") or d.get("success") or 0)
        fail = int(d.get("FailCount") or d.get("fail") or 0)
        if ydb is None and gross is None and (success + fail) == 0:
            continue
        total = success + fail
        points.append(
            {
                "branch": norm_branch(d),
                "db": db,
                "suite": suite,
                "family": suite_family(suite),
                "ts": ts,
                "ts_iso": ts.isoformat().replace("+00:00", ""),
                "ydb": None if ydb is None else float(ydb),
                "gross": None if gross is None else float(gross),
                "success": success,
                "fail": fail,
                "fail_rate": (fail / total) if total > 0 else 0.0,
                "fail_tests": str(d.get("FailTests") or d.get("fail_tests") or "")[:500],
                "version": commit_of(d),
                "ci_version": ci_version_of(d),
                "report": d.get("Report") or d.get("report"),
                "label": f"{ts.date().isoformat()}_{commit_of(d)}",
            }
        )
    return points


def run_view(p: dict) -> dict:
    return {
        "ts": p["ts_iso"][:19],
        "day": p["ts"].date().isoformat(),
        "label": p["label"],
        "ydb": p["ydb"],
        "gross": p["gross"],
        "fail": p["fail"],
        "success": p["success"],
        "fail_rate": round(p["fail_rate"], 4),
        "fail_tests": p["fail_tests"],
        "version": p["version"],
        "ci_version": p["ci_version"],
        "report": p["report"],
    }


def history_view(pts: list[dict]) -> dict:
    tail = pts[-HISTORY_MAX_POINTS:]
    return {
        "labels": [p["label"] for p in tail],
        "ydb": [p["ydb"] for p in tail],
        "fail_rate": [round(p["fail_rate"] * 100, 2) for p in tail],
        "reports": [p["report"] for p in tail],
        "versions": [p["version"] for p in tail],
        "ci_versions": [p["ci_version"] for p in tail],
        "markers": ["ok"] * len(tail),
    }


def append_synthetic_history(
    hist: dict,
    *,
    day: str,
    ci_version: str,
    kind: str,
) -> dict:
    """Add a visible ghost point for missing / in_progress wave."""
    out = {k: list(v) if isinstance(v, list) else v for k, v in hist.items()}
    n = len(out.get("labels") or [])
    markers = list(out.get("markers") or (["ok"] * n))
    while len(markers) < n:
        markers.append("ok")
    # ghost y = last known ydb so the bar is visible
    ref = None
    for v in reversed(out.get("ydb") or []):
        if v is not None:
            ref = v
            break
    label = f"{day}_{kind.upper()}"
    out.setdefault("labels", []).append(label)
    out.setdefault("ydb", []).append(ref)
    out.setdefault("fail_rate", []).append(None)
    out.setdefault("reports", []).append(None)
    out.setdefault("versions", []).append(kind)
    out.setdefault("ci_versions", []).append(ci_version)
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
    """Now = last completed run; baseline = previous BASELINE_RUNS."""
    pts = sorted(pts, key=lambda p: p["ts"])
    now = pts[-NOW_RUNS:]
    base = pts[-(NOW_RUNS + BASELINE_RUNS) : -NOW_RUNS] or pts[: max(1, len(pts) // 2)]
    display = pts[-DISPLAY_RUNS:]

    now_ydbs = [p["ydb"] for p in now]
    base_ydbs = [p["ydb"] for p in base]
    ydb_now = median(now_ydbs)
    ydb_base = median(base_ydbs)
    ydb_pct = pct(ydb_base, ydb_now)

    fr_now = avg([p["fail_rate"] for p in now]) or 0.0
    fr_base = avg([p["fail_rate"] for p in base]) or 0.0
    last_fr = now[-1]["fail_rate"] if now else 0.0

    fail_status, fail_reasons = fail_status_from_last(last_fr, fr_base)

    dur = classify_duration(ydb_pct, ydb_now, ydb_base, now_ydbs, base_ydbs)
    dur_status = dur["status"]
    dur_reasons = list(dur["reasons"])

    status = worse(fail_status, dur_status)
    # soft duration alone stays watch (not hot slower); fail + watch → failing
    kind = "ok"
    dur_hot = dur_status in ("regression", "broken")
    fail_hot = fail_status != "ok"
    if fail_hot and dur_hot:
        kind = "both"
    elif fail_hot:
        kind = "failing"
    elif dur_hot:
        kind = "slower"
    elif dur_status == "watch":
        kind = "watch"

    fail_tests = ""
    bad_queries = []
    seen = set()
    # fail names from the last completed run only
    for p in reversed(now):
        if p["fail"] > 0 and p.get("fail_tests"):
            if not fail_tests:
                fail_tests = p["fail_tests"]
            for q in parse_fail_tests(p["fail_tests"]):
                if q not in seen:
                    seen.add(q)
                    bad_queries.append({"test": q, "kind": "fail"})
    bad_queries = bad_queries[:25]

    return {
        "status": status,
        "kind": kind,
        "reasons": fail_reasons + dur_reasons,
        "ydb_base": ydb_base,
        "ydb_now": ydb_now,
        "ydb_pct": ydb_pct,
        "dur_thr_pct": dur.get("thr_pct"),
        "dur_noise_pct": dur.get("noise_pct"),
        "dur_level": dur.get("level"),
        "fail_rate_base": fr_base,
        "fail_rate_now": fr_now,
        "fail_tests": fail_tests,
        "bad_queries": bad_queries,
        "now_runs": [run_view(p) for p in display],
        "n": len(pts),
        "last_ts": now[-1]["ts_iso"][:19] if now else None,
        "report": next((p["report"] for p in reversed(now) if p.get("report")), None),
        "ci_version": now[-1]["ci_version"] if now else "",
    }


def build_waves(points: list[dict], lookback_start: datetime, branches: set[str]):
    """CiVersion × Branch × DbAlias waves."""
    waves: dict[tuple[str, str, str], dict] = {}
    for p in points:
        if p["branch"] not in branches or p["db"] not in FOCUS_DBS:
            continue
        if p["ts"] < lookback_start:
            continue
        civ = p["ci_version"] or f"unknown@{p['ts'].date().isoformat()}"
        key = (p["branch"], p["db"], civ)
        w = waves.setdefault(
            key,
            {
                "branch": p["branch"],
                "db": p["db"],
                "ci_version": civ,
                "suites": set(),
                "max_ts": p["ts"],
                "min_ts": p["ts"],
            },
        )
        w["suites"].add(p["suite"])
        w["max_ts"] = max(w["max_ts"], p["ts"])
        w["min_ts"] = min(w["min_ts"], p["ts"])
    by_br_db: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for w in waves.values():
        by_br_db[(w["branch"], w["db"])].append(w)
    for lst in by_br_db.values():
        lst.sort(key=lambda x: x["max_ts"])
    return by_br_db


def expected_suites(by_br_db_waves: dict[tuple[str, str], list[dict]]) -> dict[tuple[str, str], set[str]]:
    out: dict[tuple[str, str], set[str]] = {}
    for key, waves in by_br_db_waves.items():
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


def balance_inbox(
    all_hot: list[dict],
    branches: list[str],
    inbox_key,
) -> list[dict]:
    """Per-branch × kind cap so promote/query-hot rows stay visible."""
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
            lim = max(5, limit // max(1, len(branches) - 1)) if br != "main" else limit
            if br == "main":
                lim = limit
            br_picked.extend(sorted(by_kind.get(kind, []), key=inbox_key)[:lim])
        picked.extend(sorted(br_picked, key=inbox_key)[:INBOX_PER_BRANCH])
    return sorted(picked, key=inbox_key)[: INBOX_LIMIT * max(1, len(branches))]


def build_now_report(points: list[dict], since: datetime) -> dict:
    until = max((p["ts"] for p in points), default=since)
    # Wall-clock: if the whole pipeline stops, stale must still fire.
    now_utc = datetime.now(timezone.utc)
    lookback = now_utc - timedelta(days=EXPECTED_LOOKBACK_DAYS)
    branches = select_branches(points, now_utc=now_utc)
    branch_set = set(branches)

    slices: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for p in points:
        slices[(p["branch"], p["db"], p["suite"])].append(p)

    by_br_db_waves = build_waves(points, lookback, branch_set)
    expected = expected_suites(by_br_db_waves)

    inbox: list[dict] = []
    ok_slices: list[dict] = []
    slice_status: dict[tuple[str, str, str], dict] = {}

    # Per-slice Now classification (focus scope)
    for (branch, db, suite), pts in slices.items():
        if branch not in branch_set or db not in FOCUS_DBS:
            continue
        if len(pts) < 1:
            continue
        info = classify_slice(pts)
        info.update(
            {
                "id": safe_id(branch, db, suite),
                "branch": branch,
                "db": db,
                "suite": suite,
                "family": suite_family(suite),
            }
        )
        slice_status[(branch, db, suite)] = info
        if info["status"] in ("broken", "regression"):
            item = dict(info)
            item["issue"] = info["kind"]  # failing | slower | both
            item["history"] = history_view(sorted(pts, key=lambda p: p["ts"]))
            inbox.append(item)
        elif info["status"] in ("ok", "watch"):
            item = dict(info)
            item["issue"] = "watch" if info["status"] == "watch" else "ok"
            item["history"] = history_view(sorted(pts, key=lambda p: p["ts"]))
            ok_slices.append(item)

    # Missing / in_progress / stale from waves (per branch × db)
    for (branch, db), waves in by_br_db_waves.items():
        if not waves:
            continue
        last = waves[-1]
        age_h = (now_utc - last["max_ts"]).total_seconds() / 3600.0
        exp = expected.get((branch, db), set())
        present = last["suites"]
        if age_h >= STALE_HOURS:
            inbox.append(
                {
                    "id": safe_id("stale", branch, db),
                    "issue": "stale",
                    "status": "stale",
                    "kind": "stale",
                    "branch": branch,
                    "db": db,
                    "suite": "—",
                    "family": "—",
                    "reasons": [f"no fresh wave ≥{STALE_HOURS:.0f}h (last {last['ci_version']})"],
                    "ydb_base": None,
                    "ydb_now": None,
                    "ydb_pct": None,
                    "fail_rate_base": None,
                    "fail_rate_now": None,
                    "fail_tests": "",
                    "bad_queries": [],
                    "now_runs": [],
                    "n": 0,
                    "last_ts": last["max_ts"].isoformat().replace("+00:00", "")[:19],
                    "last_seen": None,
                    "report": None,
                    "ci_version": last["ci_version"],
                    "history": {
                        "labels": [], "ydb": [], "fail_rate": [], "reports": [],
                        "versions": [], "ci_versions": [], "markers": [],
                    },
                }
            )
            continue

        in_prog = wave_is_in_progress(age_h, present, exp)
        prev = waves[-2]["suites"] if len(waves) >= 2 else set()
        # During in-progress wave: only suites that already appeared in previous wave
        # (expected soon). When wave is done: full expected − present = real missing.
        if in_prog:
            absent = sorted((prev & exp) - present)
            issue_kind = "in_progress"
        else:
            absent = sorted(exp - present)
            issue_kind = "missing"

        wave_day = last["max_ts"].date().isoformat()
        for suite in absent:
            pts = slices.get((branch, db, suite), [])
            hist = history_view(sorted(pts, key=lambda p: p["ts"])) if pts else {
                "labels": [], "ydb": [], "fail_rate": [], "reports": [],
                "versions": [], "ci_versions": [], "markers": [],
            }
            hist = append_synthetic_history(
                hist,
                day=wave_day,
                ci_version=last["ci_version"],
                kind=issue_kind,
            )
            last_seen = pts[-1]["ts_iso"][:19] if pts else None
            if issue_kind == "in_progress":
                reason = (
                    f"wave {last['ci_version']} in progress — {suite} ещё не доехал "
                    f"(last seen {last_seen or 'never'}; не алерт)"
                )
            else:
                reason = (
                    f"absent in completed wave {last['ci_version']} "
                    f"(last seen {last_seen or 'never'})"
                )
            inbox.append(
                {
                    "id": safe_id(issue_kind, branch, db, suite),
                    "issue": issue_kind,
                    "status": issue_kind,
                    "kind": issue_kind,
                    "branch": branch,
                    "db": db,
                    "suite": suite,
                    "family": suite_family(suite),
                    "reasons": [reason],
                    "ydb_base": None,
                    "ydb_now": None,
                    "ydb_pct": None,
                    "fail_rate_base": None,
                    "fail_rate_now": None,
                    "fail_tests": "",
                    "bad_queries": [],
                    "now_runs": [],
                    "n": len(pts),
                    "last_ts": last["max_ts"].isoformat().replace("+00:00", "")[:19],
                    "last_seen": last_seen,
                    "report": None,
                    "ci_version": last["ci_version"],
                    "history": hist,
                    "wave": last["ci_version"],
                    "wave_in_progress": in_prog,
                    "expected": True,
                }
            )

    def inbox_key(r):
        return (
            branch_rank(r.get("branch") or ""),
            {
                "missing": 0,
                "failing": 1,
                "both": 1,
                "slower": 2,
                "in_progress": 3,
                "stale": 4,
            }.get(r.get("issue"), 9),
            -STATUS_ORDER.get(r["status"], 0),
            -(r.get("fail_rate_now") or 0),
            -(r.get("ydb_pct") or 0),
            r.get("db") or "",
            r.get("suite") or "",
        )

    # Deduplicate by id (prefer worse)
    by_id: dict[str, dict] = {}
    for r in inbox:
        prev = by_id.get(r["id"])
        if prev is None or STATUS_ORDER.get(r["status"], 0) > STATUS_ORDER.get(prev["status"], 0):
            by_id[r["id"]] = r
    all_hot = list(by_id.values())

    # Heatmap cells: branch|db|family (empty = nodata, not fake ok)
    dbs = sorted(FOCUS_DBS)
    cells = {}
    for br in branches:
        for db in dbs:
            for fam in FAMILIES:
                cells[f"{br}|{db}|{fam}"] = {"status": "nodata", "n_hot": 0, "n_queries": 0, "n": 0}
    cell_queries: dict[str, set[str]] = defaultdict(set)

    for (branch, db, suite), info in slice_status.items():
        fam = info["family"]
        key = f"{branch}|{db}|{fam}"
        if key not in cells:
            continue
        cells[key]["n"] += 1
        if info["status"] in ("broken", "regression"):
            cells[key]["n_hot"] += 1
            cells[key]["status"] = worse(cells[key]["status"], info["status"])
            for q in info.get("bad_queries") or []:
                if q.get("test"):
                    cell_queries[key].add(str(q["test"]))
            ft = info.get("fail_tests") or ""
            if ft and not str(ft).startswith("Infrastructure"):
                for part in re.split(r"[,;]", str(ft)):
                    t = part.strip()
                    if t:
                        cell_queries[key].add(t)
        elif info["status"] in ("ok", "watch"):
            cells[key]["status"] = worse(cells[key]["status"], info["status"])

    for r in all_hot:
        br = r.get("branch") or ""
        if r.get("issue") == "missing" and r.get("family") in FAMILIES:
            key = f"{br}|{r['db']}|{r['family']}"
            if key in cells:
                cells[key]["n_hot"] += 1
                cells[key]["status"] = worse(cells[key]["status"], "missing")
        if r.get("issue") == "in_progress" and r.get("family") in FAMILIES:
            key = f"{br}|{r['db']}|{r['family']}"
            if key in cells and cells[key]["status"] in ("ok", "nodata", "watch"):
                cells[key]["status"] = "in_progress"
        if r.get("issue") == "stale":
            for fam in FAMILIES:
                key = f"{br}|{r['db']}|{fam}"
                if key in cells:
                    cells[key]["status"] = worse(cells[key]["status"], "stale")

    for key, names in cell_queries.items():
        if key in cells:
            cells[key]["n_queries"] = len(names)

    def summarize(rows, ok_n=None, slices_n=None, watch_n=None, branch=None):
        # hot = actionable alerts only (not in_progress)
        hot_rows = [r for r in rows if r.get("issue") != "in_progress"]

        def _status_n(statuses):
            return sum(
                1
                for (b, _, _), info in slice_status.items()
                if info["status"] in statuses and (branch is None or b == branch)
            )

        return {
            "missing": sum(1 for r in rows if r.get("issue") == "missing"),
            "in_progress": sum(1 for r in rows if r.get("issue") == "in_progress"),
            "failing": sum(1 for r in rows if r.get("issue") in ("failing", "both")),
            "slower": sum(1 for r in rows if r.get("issue") in ("slower", "both")),
            "stale": sum(1 for r in rows if r.get("issue") == "stale"),
            "ok_slices": ok_n if ok_n is not None else _status_n(("ok", "watch")),
            "watch_slices": watch_n if watch_n is not None else _status_n(("watch",)),
            "hot": len(hot_rows),
            "slices": slices_n if slices_n is not None else (
                sum(1 for (b, _, _) in slice_status if branch is None or b == branch)
            ),
        }

    summary = summarize(all_hot)
    by_branch_summary = {}
    for br in branches:
        br_hot = [r for r in all_hot if r.get("branch") == br]
        br_ok = sum(
            1
            for (b, _, _), info in slice_status.items()
            if b == br and info["status"] in ("ok", "watch")
        )
        br_slices = sum(1 for (b, _, _) in slice_status if b == br)
        by_branch_summary[br] = summarize(
            br_hot, ok_n=br_ok, slices_n=br_slices, branch=br
        )

    inbox = balance_inbox(all_hot, branches, inbox_key)
    # OK catalog for drill-down (not in hot inbox by default)
    ok_slices = sorted(
        ok_slices,
        key=lambda r: (r.get("branch") or "", r.get("db") or "", r.get("suite") or ""),
    )

    def wave_meta_entry(br: str, db: str, last: dict, exp_n: int) -> dict:
        max_ts = last["max_ts"]
        return {
            "branch": br,
            "db": db,
            "ci_version": last["ci_version"],
            "suites": len(last["suites"]),
            "expected": exp_n,
            "max_ts": max_ts.isoformat().replace("+00:00", "")[:19],
            "day": max_ts.date().isoformat(),
            "age_hours": round((now_utc - max_ts).total_seconds() / 3600.0, 1),
        }

    # Display waves: full --since window (alerts still use 14d lookback above)
    display_waves = build_waves(points, since, branch_set)
    display_expected = expected_suites(display_waves)
    # Best sha8 per (branch, db, ci_version) from suite points
    wave_sha: dict[tuple[str, str, str], str] = {}
    for p in points:
        civ = p.get("ci_version") or ""
        if not civ or p["db"] not in FOCUS_DBS or p["branch"] not in branch_set:
            continue
        ver = (p.get("version") or "").strip()
        if not ver or ver == "—":
            continue
        key3 = (p["branch"], p["db"], civ)
        prev = wave_sha.get(key3)
        if prev is None or (len(ver) >= 8 and len(prev) < 8):
            wave_sha[key3] = ver[:8]

    waves_meta = {}
    wave_list: dict[str, list[dict]] = {}
    for (br, db), waves in display_waves.items():
        if not waves:
            continue
        last = waves[-1]
        exp_n = len(display_expected.get((br, db), ()))
        waves_meta[f"{br}|{db}"] = wave_meta_entry(br, db, last, exp_n)
        entries = []
        for w in reversed(waves):  # newest first
            max_ts = w["max_ts"]
            min_ts = w["min_ts"]
            civ = w["ci_version"]
            entries.append(
                {
                    "id": civ,
                    "ci_version": civ,
                    "max_ts": max_ts.isoformat().replace("+00:00", "")[:19],
                    "min_ts": min_ts.isoformat().replace("+00:00", "")[:19],
                    "day": max_ts.date().isoformat(),
                    "sha8": wave_sha.get((br, db, civ), "—"),
                    "suites": len(w["suites"]),
                    "expected": exp_n,
                    "suite_names": sorted(w["suites"]),
                    "current": w is last,
                }
            )
        wave_list[f"{br}|{db}"] = entries

    # Fallback: freshest wave per DbAlias across branches (cloud=trunk, etc.)
    waves_by_db: dict[str, dict] = {}
    for meta in waves_meta.values():
        db = meta["db"]
        prev = waves_by_db.get(db)
        if prev is None or meta["max_ts"] > prev["max_ts"]:
            waves_by_db[db] = dict(meta)

    # Ultimate fallback: last suite point timestamp per db / db×branch (no CiVersion wave)
    last_activity: dict[str, dict] = {}
    for p in points:
        if p["db"] not in FOCUS_DBS or p["branch"] not in branch_set:
            continue
        for key in (f"{p['branch']}|{p['db']}", p["db"]):
            prev = last_activity.get(key)
            if prev is None or p["ts"] > prev["_ts"]:
                last_activity[key] = {
                    "branch": p["branch"],
                    "db": p["db"],
                    "ci_version": p["ci_version"] or "—",
                    "suites": None,
                    "expected": None,
                    "max_ts": p["ts_iso"][:19],
                    "day": p["ts"].date().isoformat(),
                    "age_hours": round((now_utc - p["ts"]).total_seconds() / 3600.0, 1),
                    "activity_only": True,
                    "_ts": p["ts"],
                }
    for v in last_activity.values():
        v.pop("_ts", None)

    default_branch = "main" if "main" in branches else (branches[0] if branches else "main")

    # DbAliases that actually have runs on each branch (for heatmap filtering)
    dbs_by_branch: dict[str, list[str]] = {}
    tmp_dbs: dict[str, set[str]] = defaultdict(set)
    for p in points:
        if p["db"] in FOCUS_DBS and p["branch"] in branch_set:
            tmp_dbs[p["branch"]].add(p["db"])
    for br in branches:
        dbs_by_branch[br] = sorted(tmp_dbs.get(br, ()))

    return {
        "mode": "now",
        "window": f"{since.date().isoformat()}..{until.date().isoformat()}",
        "generated_at": now_utc.isoformat().replace("+00:00", "Z"),
        "source": "perfomance/olap/fast_results_siutes",
        "ui": {
            "now_runs": NOW_RUNS,
            "display_runs": DISPLAY_RUNS,
            "baseline_runs": BASELINE_RUNS,
            "stale_hours": STALE_HOURS,
            "wave_complete_hours": WAVE_COMPLETE_HOURS,
            "focus_branches": branches,
            "default_branch": default_branch,
            "focus_dbs": dbs,
            "dbs_by_branch": dbs_by_branch,
            "default_from": since.date().isoformat(),
            "default_to": until.date().isoformat(),
        },
        "summary": summary,
        "by_branch": by_branch_summary,
        "heatmap": {
            "branches": branches,
            "dbs": dbs,
            "dbs_by_branch": dbs_by_branch,
            "families": FAMILIES,
            "cells": cells,
        },
        "waves": waves_meta,
        "wave_list": wave_list,
        "waves_by_db": waves_by_db,
        "last_activity": last_activity,
        "inbox": inbox,
        "ok": ok_slices,
        "rules": {
            "now": f"last completed run",
            "baseline": f"previous {BASELINE_RUNS} runs",
            "baseline_runs": BASELINE_RUNS,
            "dur_soft": f"+{int(DUR_TOL*100)}% floor · thr=max(soft, {NOISE_K:g}×noise) · last run > base",
            "dur_hard": f"+{int(DUR_HARD*100)}% (or thr if noisier) on last run → hard slow; soft → watch",
            "dur": f"hard +{int(DUR_HARD*100)}% / soft +{int(DUR_TOL*100)}% · noise×{NOISE_K:g} · last run",
            "fail_broken": FAIL_BROKEN,
            "fail_hot": FAIL_HOT,
            "fail_rise": FAIL_RISE,
            "dur_tol": DUR_TOL,
            "dur_hard": DUR_HARD,
            "noise_k": NOISE_K,
            "outlier_mult": OUTLIER_MULT,
            "slow_persist_min": SLOW_PERSIST_MIN,
            "expected": f"suites in ≥{int(EXPECTED_MIN_SHARE*100)}% of CiVersion waves / {EXPECTED_LOOKBACK_DAYS}d",
            "wave": "CiVersion × Branch × DbAlias",
        },
    }


def _suite_reports_by_day(item: dict) -> dict[str, str]:
    """Map YYYY-MM-DD → sandbox report URL from suite-level history / now_runs."""
    out: dict[str, str] = {}
    hist = item.get("history") or {}
    for lab, url in zip(hist.get("labels") or [], hist.get("reports") or []):
        if not url:
            continue
        day = str(lab)[:10]
        if len(day) == 10:
            out[day] = url
    for run in item.get("now_runs") or []:
        url = run.get("report")
        if not url:
            continue
        day = run.get("day") or str(run.get("ts") or run.get("label") or "")[:10]
        if len(day) == 10:
            out[day] = url
    if item.get("report") and item.get("last_ts"):
        day = str(item["last_ts"])[:10]
        if len(day) == 10:
            out.setdefault(day, item["report"])
    return out


def _query_history(
    rows: list[dict],
    report_by_day: dict[str, str] | None = None,
    *,
    max_points: int | None = None,
) -> dict:
    tail = rows[-(max_points or HISTORY_MAX_POINTS) :]
    report_by_day = report_by_day or {}
    labels = [r.get("ts") or r.get("day") for r in tail]
    return {
        "labels": labels,
        "ydb": [r.get("ydb") for r in tail],
        "fail_rate": [
            None
            if r.get("nodata") or r.get("fr") is None
            else round(float(r["fr"]) * 100, 2)
            for r in tail
        ],
        "reports": [
            r.get("report") or report_by_day.get(str(r.get("day") or "")[:10])
            for r in tail
        ],
        "error_classes": [
            None
            if r.get("nodata")
            else (
                r.get("error_class")
                or error_class_of(r.get("color"), r.get("diff_response"))
            )
            for r in tail
        ],
        "nodata": [bool(r.get("nodata")) for r in tail],
        "versions": [r.get("version") for r in tail],
        "ci_versions": [r.get("ci_version") for r in tail],
        "mode": "runs" if any(r.get("ts") and "T" in str(r.get("ts")) for r in tail) else "daily",
    }


def _merge_query(item: dict, q: dict, limit: int = 25) -> None:
    bq = item.setdefault("bad_queries", [])
    seen = {x["test"] for x in bq}
    if q["test"] in seen:
        # upgrade fail→keep fail; fill slow pct if missing
        for x in bq:
            if x["test"] != q["test"]:
                continue
            if q.get("kind") in ("fail", "both"):
                x["kind"] = q["kind"] if x.get("kind") != "fail" else x["kind"]
                if q.get("kind") == "fail":
                    x["kind"] = "fail"
                x["fail_rate_late"] = q.get("fail_rate_late", x.get("fail_rate_late"))
            if q.get("ydb_pct") is not None:
                x["ydb_pct"] = q["ydb_pct"]
            if q.get("ydb_now") is not None:
                x["ydb_now"] = q["ydb_now"]
                x["ydb_base"] = q.get("ydb_base")
            if q.get("error_class"):
                x["error_class"] = q["error_class"]
            if q.get("history") and not x.get("history"):
                x["history"] = q["history"]
            elif q.get("history"):
                x["history"] = q["history"]
        return
    if len(bq) >= limit:
        return
    bq.append(q)


def attach_slow_queries_from_tests(data: dict, test_rows: list[dict]) -> None:
    """Fallback enrich from early/late issues dump (less precise than daily Now)."""
    if not test_rows:
        return
    by_key: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for d in test_rows:
        branch = d.get("Branch") or d.get("branch") or "unknown"
        db = d.get("DbAlias") or d.get("db") or "unknown"
        suite = d.get("Suite") or d.get("suite") or "unknown"
        test = d.get("Test") or d.get("test") or ""
        ydb_pct = d.get("ydb_pct")
        try:
            ydb_pct = float(ydb_pct) if ydb_pct is not None else None
        except (TypeError, ValueError):
            ydb_pct = None
        fr_l = float(d.get("fail_rate_late") or 0)
        if (ydb_pct is not None and ydb_pct >= 10) or fr_l >= 0.03:
            by_key[(branch, db, suite)].append(
                {
                    "test": test,
                    "kind": "fail" if fr_l >= 0.03 else "slow",
                    "ydb_pct": ydb_pct,
                    "fail_rate_late": fr_l,
                }
            )

    for item in data.get("inbox", []):
        if item.get("issue") not in ("failing", "slower", "both"):
            continue
        for q in by_key.get((item["branch"], item["db"], item["suite"]), []):
            _merge_query(item, q)


def load_daily_points(
    path: Path,
    suite_keys: set[tuple[str, str, str]],
    since: datetime | None = None,
) -> dict[tuple[str, str, str, str], list[dict]]:
    """Load json-lines query dump → (branch,db,suite,test) → [{ts,day,ydb,fr}, ...].

    Supports per-run rows (Ts/ydb/Success) and legacy daily buckets (Day/n/fails/ydb).
    If ``since`` is set, drops older points (default report window).
    """
    out: dict[tuple[str, str, str, str], list[dict]] = defaultdict(list)
    if not path.exists() or not suite_keys:
        return out
    since_day = since.date().isoformat() if since is not None else None
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line[0] != "{":
                continue
            try:
                o = json.loads(line)
            except json.JSONDecodeError:
                continue
            db = o.get("DbAlias") or "unknown"
            suite = o.get("Suite") or "unknown"
            # Align with suite-level norm_branch (cloud_* → trunk, Version→branch).
            branch = norm_branch(
                {
                    "Branch": o.get("Branch"),
                    "Version": o.get("Version"),
                    "CiBranch": o.get("CiBranch"),
                    "CiVersion": o.get("CiVersion"),
                    "DbAlias": db,
                }
            )
            if (branch, db, suite) not in suite_keys:
                continue
            test = o.get("Test") or "unknown"
            ydb = o.get("ydb") if "ydb" in o else o.get("YdbSumMeans")
            report = o.get("Report") or o.get("report")
            ts_raw = o.get("Ts") or o.get("ts") or o.get("Run_start_timestamp")
            if ts_raw is not None:
                dt = parse_ts(ts_raw)
                if dt is None:
                    continue
                if since is not None and dt < since:
                    continue
                ts = dt.isoformat().replace("+00:00", "Z")
                day = dt.date().isoformat()
                success = o.get("Success")
                color = o.get("Color")
                diff_response = o.get("DiffResponse")
                if diff_response is None:
                    diff_response = o.get("diff_response")
                # mart null-templates: Success=0 + Color NULL + no ydb → not in this run
                nodata = (
                    success is not None
                    and int(success) == 0
                    and "Color" in o
                    and not color
                    and ydb is None
                )
                if nodata:
                    fr = None
                elif success is None:
                    fr = float(o.get("fr") or 0.0)
                else:
                    fr = 0.0 if int(success) else 1.0
                out[(branch, db, suite, test)].append(
                    {
                        "ts": ts,
                        "day": day,
                        "ydb": None if ydb is None else float(ydb),
                        "fr": fr,
                        "report": report,
                        "nodata": nodata,
                        "color": color,
                        "diff_response": diff_response,
                        "error_class": error_class_of(
                            color, diff_response, nodata=bool(nodata)
                        ),
                        "version": commit_of(o),
                        "ci_version": ci_version_of(o),
                    }
                )
                continue
            # legacy day bucket
            n = int(o.get("n") or 0)
            fails = int(o.get("fails") or 0)
            day = str(o.get("Day") or "")
            if since_day and day and day < since_day:
                continue
            out[(branch, db, suite, test)].append(
                {
                    "ts": day,
                    "day": day,
                    "ydb": None if ydb is None else float(ydb),
                    "fr": (fails / n) if n else 0.0,
                    "report": report,
                    "version": commit_of(o),
                    "ci_version": ci_version_of(o),
                }
            )
    for key, rows in out.items():
        rows.sort(key=lambda r: r.get("ts") or r.get("day") or "")
    return out


def _sync_slow_query_lists(item: dict, slow_qs: list[dict] | None = None) -> None:
    """Keep slow_queries / badge Nq / dive pills on the same set."""
    item["bad_queries"] = sorted(
        item.get("bad_queries") or [],
        key=lambda q: (
            0 if q.get("kind") in ("fail", "both") else 1,
            -(q.get("fail_rate_late") or 0),
            -(q.get("ydb_pct") or 0),
        ),
    )[:25]
    slow_from_bad = [
        q
        for q in (item.get("bad_queries") or [])
        if q.get("kind") in ("slow", "both")
        or (q.get("ydb_pct") is not None and q.get("ydb_pct") >= 10)
    ]
    by_test: dict[str, dict] = {}
    for q in slow_qs or []:
        if q.get("test"):
            by_test[q["test"]] = q
    for q in slow_from_bad:
        if q.get("test"):
            by_test.setdefault(q["test"], q)
    item["slow_queries"] = sorted(
        by_test.values(),
        key=lambda q: -(q.get("ydb_pct") or 0),
    )[:25]
    item["query_map"] = {
        q["test"]: q
        for q in (item.get("bad_queries") or []) + (item.get("slow_queries") or [])
        if q.get("test") and q.get("history")
    }


def _query_metrics(rows: list[dict]) -> dict | None:
    if not rows:
        return None
    # last point is mart null-template → query absent in that suite run
    if rows[-1].get("nodata"):
        real = [r for r in rows if not r.get("nodata")]
        base = real[-BASELINE_RUNS:] if real else []
        return {
            "ydb_pct": None,
            "ydb_base": median([r["ydb"] for r in base]),
            "ydb_now": None,
            "fail_rate_late": None,
            "fail_rate_base": avg([r["fr"] for r in base]),
            "kind": "nodata",
            "is_fail": False,
            "is_slow": False,
            "is_watch": False,
            "dur_thr_pct": None,
            "dur_noise_pct": None,
            "dur_level": None,
            "error_class": None,
        }
    # One point is enough for fail (last_fr ≥ FAIL_HOT). Duration needs a base —
    # do not reuse the same point as baseline (would hide a solo fail as kind=ok).
    now = rows[-NOW_RUNS:]
    base_slice = rows[-(NOW_RUNS + BASELINE_RUNS) : -NOW_RUNS]
    if base_slice:
        base = base_slice
    elif len(rows) > len(now):
        base = rows[: max(1, len(rows) - len(now))]
    else:
        base = []
    now_ydbs = [r["ydb"] for r in now]
    base_ydbs = [r["ydb"] for r in base]
    ydb_now = median(now_ydbs)
    ydb_base = median(base_ydbs) if base else None
    ydb_pct = pct(ydb_base, ydb_now) if base else None
    fr_now = avg([r["fr"] for r in now]) or 0.0
    fr_base = avg([r["fr"] for r in base]) if base else None
    last = now[-1] if now else {}
    last_fr = (last.get("fr") or 0.0) if last else 0.0
    # last completed run only — same as classify_slice / JS
    is_fail = is_fail_rate_hot(last_fr)
    dur = classify_duration(ydb_pct, ydb_now, ydb_base, now_ydbs, base_ydbs)
    is_slow = dur["status"] in ("regression", "broken")  # hard only
    is_watch = dur["status"] == "watch"
    kind = "ok"
    if is_fail and is_slow:
        kind = "both"
    elif is_fail:
        kind = "fail"
    elif is_slow:
        kind = "slow"
    elif is_watch:
        kind = "watch"
    err = last.get("error_class") or error_class_of(
        last.get("color"), last.get("diff_response")
    )
    return {
        "ydb_pct": ydb_pct,
        "ydb_base": ydb_base,
        "ydb_now": ydb_now,
        "fail_rate_late": fr_now,
        "fail_rate_base": fr_base,
        "kind": kind,
        "is_fail": is_fail,
        "is_slow": is_slow,
        "is_watch": is_watch,
        "dur_thr_pct": dur.get("thr_pct"),
        "dur_noise_pct": dur.get("noise_pct"),
        "dur_level": dur.get("level"),
        "error_class": err if is_fail else None,
    }


def attach_now_query_regressions(
    data: dict, daily_path: Path, since: datetime | None = None
) -> int:
    """Now-based per-query slow/fail for hot suites (last run vs prev 7)."""
    suite_keys = {
        (r["branch"], r["db"], r["suite"])
        for r in data.get("inbox", [])
        if r.get("issue") in ("failing", "slower", "both")
    }
    series = load_daily_points(daily_path, suite_keys, since=since)
    by_suite: dict[tuple[str, str, str], list[tuple[str, list[dict]]]] = defaultdict(list)
    for (branch, db, suite, test), rows in series.items():
        by_suite[(branch, db, suite)].append((test, rows))

    n_slow = 0
    for item in data.get("inbox", []):
        if item.get("issue") not in ("failing", "slower", "both"):
            continue
        key = (item["branch"], item["db"], item["suite"])
        report_by_day = _suite_reports_by_day(item)
        slow_qs: list[dict] = []
        for test, rows in by_suite.get(key, []):
            m = _query_metrics(rows)
            if not m or (not m["is_fail"] and not m["is_slow"]):
                continue
            q = {
                "test": test,
                "kind": m["kind"],
                "ydb_pct": m["ydb_pct"],
                "ydb_base": m["ydb_base"],
                "ydb_now": m["ydb_now"],
                "fail_rate_late": m["fail_rate_late"],
                "fail_rate_base": m["fail_rate_base"],
                "dur_thr_pct": m.get("dur_thr_pct"),
                "dur_noise_pct": m.get("dur_noise_pct"),
                "dur_level": m.get("dur_level"),
                "error_class": m.get("error_class"),
                "history": _query_history(rows, report_by_day),
            }
            if m["is_slow"]:
                n_slow += 1
                slow_qs.append(q)
            _merge_query(item, q)

        _sync_slow_query_lists(item, slow_qs)
    return n_slow


def attach_suite_query_catalogs(
    data: dict, daily_path: Path, since: datetime | None = None
) -> tuple[int, int]:
    """Full per-query catalog (+ history) for OK and hot suites (incl. green queries)."""
    hot = [
        r
        for r in (data.get("inbox") or [])
        if r.get("issue") in ("failing", "slower", "both")
    ]
    ok = list(data.get("ok") or [])
    items = hot + ok
    suite_keys = {(r["branch"], r["db"], r["suite"]) for r in items}
    series = load_daily_points(daily_path, suite_keys, since=since)
    if not series:
        return 0, 0
    by_suite: dict[tuple[str, str, str], list[tuple[str, list[dict]]]] = defaultdict(list)
    for (branch, db, suite, test), rows in series.items():
        by_suite[(branch, db, suite)].append((test, rows))

    n_hot_q = 0
    n_ok_q = 0
    hot_keys = {(r["branch"], r["db"], r["suite"]) for r in hot}
    for item in items:
        key = (item["branch"], item["db"], item["suite"])
        report_by_day = _suite_reports_by_day(item)
        qs: list[dict] = []
        for test, rows in sorted(by_suite.get(key, []), key=lambda t: t[0]):
            if len(rows) < 1:
                continue
            m = _query_metrics(rows) or {
                "ydb_pct": None,
                "ydb_base": None,
                "ydb_now": None if rows[-1].get("nodata") else rows[-1].get("ydb"),
                "fail_rate_late": None if rows[-1].get("nodata") else (rows[-1].get("fr") or 0.0),
                "fail_rate_base": None,
                "kind": "nodata" if rows[-1].get("nodata") else "ok",
            }
            qs.append(
                {
                    "test": test,
                    "kind": m["kind"],
                    "ydb_pct": m.get("ydb_pct"),
                    "ydb_base": m.get("ydb_base"),
                    "ydb_now": m.get("ydb_now"),
                    "fail_rate_late": m.get("fail_rate_late"),
                    "fail_rate_base": m.get("fail_rate_base"),
                    "dur_thr_pct": m.get("dur_thr_pct"),
                    "dur_noise_pct": m.get("dur_noise_pct"),
                    "dur_level": m.get("dur_level"),
                    "error_class": m.get("error_class"),
                    "history": _query_history(rows, report_by_day),
                }
            )
        item["queries"] = qs
        # rebuild hot lists from catalog — drop stale enrichments (old +65% etc.)
        hot_qs = [q for q in qs if q.get("kind") in ("fail", "slow", "both")]
        item["bad_queries"] = sorted(
            hot_qs,
            key=lambda q: (
                0 if q.get("kind") in ("fail", "both") else 1,
                -(q.get("fail_rate_late") or 0),
                -(q.get("ydb_pct") or 0),
            ),
        )[:25]
        item["slow_queries"] = sorted(
            [q for q in hot_qs if q.get("kind") in ("slow", "both")],
            key=lambda q: -(q.get("ydb_pct") or 0),
        )[:25]
        item["query_map"] = {
            q["test"]: q for q in qs if q.get("test") and q.get("history")
        }
        if key in hot_keys:
            n_hot_q += len(qs)
        else:
            n_ok_q += len(qs)
    return n_hot_q, n_ok_q


def collapse_in_progress_suite_dupes(
    inbox_by_id: dict[str, dict], ok_slices: list[dict]
) -> tuple[dict[str, dict], list[dict]]:
    """Drop standalone hot/ok rows when the same suite has in_progress(+finished)."""

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


def attach_finished_snapshots(data: dict) -> int:
    """Attach last finished-run twin onto in_progress stubs (for All wave view dive)."""
    twins: dict[tuple[str, str, str], dict] = {}
    for r in list(data.get("ok") or []) + list(data.get("inbox") or []):
        if r.get("issue") == "in_progress":
            continue
        suite = r.get("suite")
        if not suite or suite == "—":
            continue
        twins[(r.get("branch") or "", r.get("db") or "", suite)] = r
    n = 0
    for r in data.get("inbox") or []:
        if r.get("issue") != "in_progress":
            continue
        twin = twins.get((r.get("branch") or "", r.get("db") or "", r.get("suite") or ""))
        if not twin:
            continue
        r["finished"] = {
            "issue": twin.get("issue"),
            "status": twin.get("status"),
            "kind": twin.get("kind"),
            "reasons": twin.get("reasons") or [],
            "ydb_base": twin.get("ydb_base"),
            "ydb_now": twin.get("ydb_now"),
            "ydb_pct": twin.get("ydb_pct"),
            "fail_rate_base": twin.get("fail_rate_base"),
            "fail_rate_now": twin.get("fail_rate_now"),
            "fail_tests": twin.get("fail_tests") or "",
            "bad_queries": twin.get("bad_queries") or [],
            "slow_queries": twin.get("slow_queries") or [],
            "queries": twin.get("queries") or [],
            "query_map": twin.get("query_map") or {},
            "now_runs": twin.get("now_runs") or [],
            "report": twin.get("report"),
            "last_ts": twin.get("last_ts"),
            "ci_version": twin.get("ci_version"),
            "history": twin.get("history"),
        }
        n += 1
    # Same suite must not stay as hot/ok + in_progress (Wave=finished unwrap → 2 inbox rows).
    by_id: dict[str, dict] = {}
    for r in data.get("inbox") or []:
        rid = r.get("id") or id(r)
        prev = by_id.get(rid)
        if prev is None or STATUS_ORDER.get(r.get("status") or "", 0) > STATUS_ORDER.get(
            prev.get("status") or "", 0
        ):
            by_id[rid] = r
    before = len(by_id) + len(data.get("ok") or [])
    by_id, ok_kept = collapse_in_progress_suite_dupes(by_id, list(data.get("ok") or []))
    data["inbox"] = list(by_id.values())
    data["ok"] = ok_kept
    if n or (before != len(by_id) + len(ok_kept)):
        refresh_summary_counts(data)
    return n


def _suite_key(r: dict) -> tuple[str, str, str]:
    return (r.get("branch") or "", r.get("db") or "", r.get("suite") or "")


def promote_ok_with_hot_queries(data: dict) -> int:
    """Suite sum can be OK while individual queries are hard-slow/fail — promote those.

    Inbox caps may drop a promotion. Those suites must return to ``ok`` so the
    heatmap does not show fake «no runs» for suites that are present and green
    at suite-sum level (query drill-down stays on the ok row).
    """
    stay: list[dict] = []
    moved_items: list[dict] = []
    for item in data.get("ok") or []:
        qs = item.get("queries") or []
        n_fail = sum(1 for q in qs if q.get("kind") in ("fail", "both"))
        n_slow = sum(1 for q in qs if q.get("kind") in ("slow", "both"))
        if not n_fail and not n_slow:
            stay.append(item)
            continue
        orig_issue = item.get("issue") or "ok"
        orig_status = item.get("status") or "ok"
        item = dict(item)
        item["_pre_promote_issue"] = orig_issue
        item["_pre_promote_status"] = orig_status
        if n_fail and n_slow:
            item["issue"] = "both"
            item["kind"] = "both"
        elif n_fail:
            item["issue"] = "failing"
            item["kind"] = "failing"
        else:
            item["issue"] = "slower"
            item["kind"] = "slower"
        item["status"] = "regression"
        reasons = list(item.get("reasons") or [])
        reasons.append(f"queries: fail {n_fail} · slow {n_slow} (suite sum not hot)")
        item["reasons"] = reasons
        data.setdefault("inbox", []).append(item)
        moved_items.append(item)
    data["ok"] = stay
    if moved_items:
        recap_inbox_after_promote(data)
        kept = {_suite_key(r) for r in (data.get("inbox") or [])}
        for item in moved_items:
            if _suite_key(item) in kept:
                item.pop("_pre_promote_issue", None)
                item.pop("_pre_promote_status", None)
                continue
            # Cap dropped this promotion — keep suite visible as suite-sum ok/watch.
            demoted = dict(item)
            demoted["issue"] = demoted.pop("_pre_promote_issue", None) or "ok"
            demoted["status"] = demoted.pop("_pre_promote_status", None) or "ok"
            demoted["kind"] = demoted["issue"] if demoted["issue"] in ("ok", "watch") else "ok"
            demoted["query_promote_capped"] = True
            data["ok"].append(demoted)
    return len(moved_items)


def recap_inbox_after_promote(data: dict) -> None:
    """Re-apply per-branch/kind caps after query-driven promotions."""
    ui = data.get("ui") or {}
    branches = list(ui.get("focus_branches") or [])
    if not branches:
        branches = sorted(
            {
                r.get("branch")
                for r in (data.get("inbox") or [])
                if r.get("branch")
            }
        )
    if not branches:
        return

    def inbox_key(r):
        return (
            branch_rank(r.get("branch") or ""),
            {
                "missing": 0,
                "failing": 1,
                "both": 1,
                "slower": 2,
                "in_progress": 3,
                "stale": 4,
            }.get(r.get("issue"), 9),
            -STATUS_ORDER.get(r.get("status") or "ok", 0),
            -(r.get("fail_rate_now") or 0),
            -(r.get("ydb_pct") or 0),
            r.get("db") or "",
            r.get("suite") or "",
        )

    data["inbox"] = balance_inbox(list(data.get("inbox") or []), branches, inbox_key)


def refresh_summary_counts(data: dict) -> None:
    """Recompute top-level summary after inbox/ok mutations."""
    inbox = data.get("inbox") or []
    ok = data.get("ok") or []
    hot_rows = [r for r in inbox if r.get("issue") != "in_progress"]
    all_rows = list(inbox) + list(ok)
    data["summary"] = {
        **(data.get("summary") or {}),
        "missing": sum(1 for r in inbox if r.get("issue") == "missing"),
        "in_progress": sum(1 for r in inbox if r.get("issue") == "in_progress"),
        "failing": sum(1 for r in inbox if r.get("issue") in ("failing", "both")),
        "slower": sum(1 for r in inbox if r.get("issue") in ("slower", "both")),
        "stale": sum(1 for r in inbox if r.get("issue") == "stale"),
        "ok_slices": len(ok),
        "watch_slices": sum(1 for r in ok if r.get("issue") == "watch" or r.get("status") == "watch"),
        "hot": len(hot_rows),
        "new_issues": sum(
            1
            for r in all_rows
            if (r.get("new_issue_count") or r.get("new_fail_count") or 0) > 0
            or (
                isinstance(r.get("finished"), dict)
                and (
                    r["finished"].get("new_issue_count")
                    or r["finished"].get("new_fail_count")
                    or 0
                )
                > 0
            )
        ),
        "new_fail": sum(
            1
            for r in all_rows
            if (r.get("new_issue_count") or r.get("new_fail_count") or 0) > 0
            or (
                isinstance(r.get("finished"), dict)
                and (
                    r["finished"].get("new_issue_count")
                    or r["finished"].get("new_fail_count")
                    or 0
                )
                > 0
            )
        ),
    }


def _embed_json_payload(data: dict) -> str:
    """JSON for <script type=application/json> — escape so </script> cannot break out."""
    return (
        json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        .replace("<", "\\u003c")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def render_html(data: dict, output: Path) -> None:
    tpl = TEMPLATE.read_text()
    if "__OLAP_REPORT_DATA__" not in tpl:
        raise SystemExit("template.html missing __OLAP_REPORT_DATA__ placeholder")
    payload = _embed_json_payload(data)
    title_bit = data.get("window", "")
    html = tpl.replace("__OLAP_REPORT_DATA__", payload)
    html = re.sub(
        r"<title>.*?</title>",
        f"<title>OLAP Now · {title_bit}</title>",
        html,
        count=1,
    )
    html = re.sub(
        r"<h1>.*?</h1>",
        f"<h1>OLAP Now · {title_bit}</h1>",
        html,
        count=1,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html)
    output.with_suffix(".json").write_text(json.dumps(data, ensure_ascii=False, indent=2))


def main():
    ap = argparse.ArgumentParser(description="Generate Now-first OLAP HTML report")
    ap.add_argument("--input", "-i", required=True, help="JSON suites dump (with Report, CiVersion)")
    ap.add_argument(
        "--tests-input",
        default=None,
        help="Optional per-query issues dump to enrich bad_queries on hot suites",
    )
    ap.add_argument(
        "--tests-daily-input",
        default=None,
        help="Optional json-lines per-query dump (per-run preferred; legacy daily ok)",
    )
    ap.add_argument(
        "--since",
        default=None,
        help=f"YYYY-MM-DD history lookback (default: {DEFAULT_WINDOW_DAYS}d from report_config.json)",
    )
    ap.add_argument("--output", "-o", default=str(ROOT / "out" / "olap-report.html"))
    ap.add_argument("--open", action="store_true")
    args = ap.parse_args()

    if args.since:
        since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
    else:
        since = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=DEFAULT_WINDOW_DAYS)

    rows = load_rows(Path(args.input))
    points = normalize_points(rows, since)
    if not points:
        raise SystemExit("No points after filtering — check --since and input data")

    data = build_now_report(points, since)

    tests_path = Path(args.tests_input) if args.tests_input else (ROOT / "out" / "raw_tests.json")
    if tests_path.exists():
        attach_slow_queries_from_tests(data, load_rows(tests_path))
        print(f"enriched bad_queries from {tests_path}")
    else:
        print(f"no tests dump at {tests_path} (optional)")

    daily_path = None
    if args.tests_daily_input:
        daily_path = Path(args.tests_daily_input)
    else:
        for cand in (ROOT / "out" / "raw_test_runs.json", ROOT / "out" / "raw_test_daily.json"):
            if cand.exists():
                daily_path = cand
                break
    if daily_path and daily_path.exists():
        n_slow = attach_now_query_regressions(data, daily_path, since=since)
        print(f"now slow-queries from {daily_path}: {n_slow}")
        n_hot_q, n_ok_q = attach_suite_query_catalogs(data, daily_path, since=since)
        print(f"suite query catalogs from {daily_path}: hot={n_hot_q} ok={n_ok_q}")
        n_promo = promote_ok_with_hot_queries(data)
        if n_promo:
            refresh_summary_counts(data)
            print(f"promoted ok→hot by query signal: {n_promo}")
        n_fin = attach_finished_snapshots(data)
        if n_fin:
            print(f"finished snapshots on in_progress: {n_fin}")
    else:
        print("no per-query dump at out/raw_test_runs.json (optional — query drill-down limited)")
        n_fin = attach_finished_snapshots(data)
        if n_fin:
            print(f"finished snapshots on in_progress: {n_fin}")

    issues, iss_warn = fetch_duty_issues(kind="olap", include_closed=True)
    if iss_warn:
        print(f"known_issues: {iss_warn}", flush=True)
    n_tick = attach_tickets_to_report(data, issues, kind="olap")
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
    n_dec = attach_duty_decisions_to_report(data, decisions, kind="olap")
    print(
        f"duty_decisions: index_items={len((decisions.get('items') or {}))} "
        f"suites_with_decision={n_dec}",
        flush=True,
    )

    out = Path(args.output)
    render_html(data, out)
    s = data["summary"]
    print(
        f"points={len(points)} inbox={s['hot']} "
        f"missing={s['missing']} in_progress={s.get('in_progress', 0)} "
        f"failing={s['failing']} slower={s['slower']} stale={s['stale']} "
        f"ok_slices={s['ok_slices']} watch={s.get('watch_slices', 0)}"
    )
    print(f"window={data['window']}")
    print(f"wrote {out}")
    print(f"wrote {out.with_suffix('.json')}")
    if args.open:
        webbrowser.open(out.resolve().as_uri())


if __name__ == "__main__":
    main()
