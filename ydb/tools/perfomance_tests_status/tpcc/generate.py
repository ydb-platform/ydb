#!/usr/bin/env python3
"""Generate Now-first TPC-C HTML report from YDB query JSON.

Focus: last 3 runs vs previous 7 (lat↑ / tpmC↓ / broken cap), missing in day-waves,
stale clusters. History is deep-dive only.

Example:
  python3 generate.py --input out/raw.json --output out/tpcc-report.html --open
  # default --since = today − 30 days (~1 month)
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import webbrowser
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TEMPLATE = ROOT / "template.html"

LAT_TOL = 0.10
LAT_WATCH = 0.07
TPMC_TOL = 0.10
OUTLIER_MULT = 3.0
LAT_CAP = 30000.0

DEFAULT_WINDOW_DAYS = 30  # ~1 month lookback when --since omitted
NOW_RUNS = 3
BASELINE_RUNS = 7
EXPECTED_LOOKBACK_DAYS = 14
EXPECTED_MIN_SHARE = 0.50
WAVE_COMPLETE_HOURS = 6
WAVE_COVERAGE_DONE = 0.85
STALE_HOURS = 36
HISTORY_MAX_POINTS = 40
INBOX_LIMIT = 80
INBOX_PER_BRANCH = 45
INBOX_PER_KIND = {
    "missing": 20,
    "in_progress": 15,
    "broken": 25,
    "both": 15,
    "lat": 20,
    "tpmc": 20,
    "stale": 10,
}

CORE_BRANCHES = ("main",)
MIN_BRANCH_POINTS = 8
MIN_CLUSTER_POINTS = 5

STATUS_ORDER = {
    "ok": 0,
    "in_progress": 1,
    "stale": 2,
    "watch": 3,
    "missing": 4,
    "regression": 5,
    "broken": 6,
}


def is_report_branch(branch: str) -> bool:
    if branch in CORE_BRANCHES or branch == "(empty)":
        return True
    return branch.startswith("stable-") or branch.startswith("prestable-") or branch.startswith("26")


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


def select_branches(points: list[dict]) -> list[str]:
    counts: dict[str, int] = defaultdict(int)
    for p in points:
        if not is_report_branch(p["branch"]):
            continue
        counts[p["branch"]] += 1
    chosen = [b for b, n in counts.items() if b in CORE_BRANCHES or n >= MIN_BRANCH_POINTS]
    for b in CORE_BRANCHES:
        if b in counts and b not in chosen:
            chosen.append(b)
    return sorted(chosen, key=lambda b: (branch_rank(b), b))


def select_clusters(points: list[dict], branches: set[str]) -> list[str]:
    counts: dict[str, int] = defaultdict(int)
    for p in points:
        if p["branch"] not in branches:
            continue
        counts[p["cluster"]] += 1
    chosen = [c for c, n in counts.items() if n >= MIN_CLUSTER_POINTS]
    return sorted(chosen)


def parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
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
        cluster = d.get("cluster") or "unknown"
        wh = int(d.get("warehouses") or 0)
        branch = norm_branch(d.get("git_branch") or d.get("branch"))
        fam = run_family(str(run_type))
        suite = suite_name(str(run_type), wh)
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
                "tpmc": None if tpmc is None else float(tpmc),
                "lat90": None if capped or lat_f is None else lat_f,
                "lat_capped": capped,
                "lat_raw": lat_f,
                "version": version,
                "label": f"{ts.date().isoformat()}_{version[:7] or '—'}",
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
    }


def history_view(pts: list[dict]) -> dict:
    tail = pts[-HISTORY_MAX_POINTS:]
    return {
        "labels": [p["label"] for p in tail],
        "tpmc": [p["tpmc"] for p in tail],
        "lat90": [p["lat_raw"] if p["lat_capped"] else p["lat90"] for p in tail],
        "markers": ["capped" if p["lat_capped"] else "ok" for p in tail],
        "versions": [p["version"] for p in tail],
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
    out.setdefault("lat90", []).append(ref_lat)
    out.setdefault("tpmc", []).append(ref_tpmc)
    out.setdefault("versions", []).append(kind)
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
    """Run-based Now classification for one (branch, cluster, suite)."""
    pts = sorted(pts, key=lambda p: p["ts"])
    now = pts[-NOW_RUNS:]
    base = pts[-(NOW_RUNS + BASELINE_RUNS) : -NOW_RUNS] or pts[: max(1, len(pts) // 2)]

    lat_now = median([p["lat90"] for p in now if not p["lat_capped"]])
    lat_base = median([p["lat90"] for p in base if not p["lat_capped"]])
    lat_pct = pct(lat_base, lat_now)

    tpmc_now = median([p["tpmc"] for p in now])
    tpmc_base = median([p["tpmc"] for p in base])
    tpmc_pct = pct(tpmc_base, tpmc_now)

    capped_now = sum(1 for p in now if p["lat_capped"])

    lat_status = "ok"
    lat_reasons: list[str] = []
    if capped_now > 0:
        lat_status = "broken"
        lat_reasons.append(f"lat capped in {capped_now}/{len(now)} recent runs (≥{int(LAT_CAP)})")
    elif lat_base is not None and lat_now is not None and lat_now > lat_base * OUTLIER_MULT:
        lat_status = "broken"
        lat_reasons.append(f"lat outlier >{OUTLIER_MULT:.0f}×")
    elif lat_pct is not None and lat_pct >= LAT_TOL * 100:
        lat_status = "regression"
        lat_reasons.append(f"lat +{lat_pct:.0f}% vs last {len(base)} runs")
    elif lat_pct is not None and lat_pct >= LAT_WATCH * 100:
        lat_status = "watch"
        lat_reasons.append(f"lat +{lat_pct:.0f}% (watch)")

    tpmc_status = "ok"
    tpmc_reasons: list[str] = []
    if tpmc_pct is not None and tpmc_pct <= -TPMC_TOL * 100:
        tpmc_status = "regression"
        tpmc_reasons.append(f"tpmC {tpmc_pct:.0f}% vs last {len(base)} runs")

    # watch does not escalate overall above regression from the other metric
    status = worse(
        lat_status if lat_status != "watch" else "ok",
        tpmc_status,
    )
    if lat_status == "watch" and status == "ok":
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
        "capped_now": capped_now,
        "now_runs": [run_view(p) for p in now],
        "n": len(pts),
        "last_ts": now[-1]["ts_iso"][:19] if now else None,
        "version": now[-1]["version"] if now else "",
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
    now_utc = until
    lookback = until - timedelta(days=EXPECTED_LOOKBACK_DAYS)
    branches = select_branches(points)
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
        info.update(
            {
                "id": safe_id(branch, cluster, suite),
                "branch": branch,
                "db": cluster,
                "cluster": cluster,
                "suite": suite,
                "family": fam,
                "warehouses": wh,
                "wh_label": wh_label(wh),
            }
        )
        slice_status[(branch, cluster, suite)] = info
        hist = history_view(sorted(pts, key=lambda p: p["ts"]))
        if info["status"] in ("broken", "regression"):
            item = dict(info)
            item["issue"] = info["kind"]
            item["history"] = hist
            inbox.append(item)
        elif info["status"] in ("ok", "watch"):
            item = dict(info)
            item["issue"] = "watch" if info["status"] == "watch" else "ok"
            item["history"] = hist
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
                    "history": {
                        "labels": [],
                        "tpmc": [],
                        "lat90": [],
                        "markers": [],
                        "versions": [],
                    },
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
            hist = history_view(sorted(pts, key=lambda p: p["ts"])) if pts else {
                "labels": [],
                "tpmc": [],
                "lat90": [],
                "markers": [],
                "versions": [],
            }
            hist = append_synthetic_history(hist, day=last["day"], kind=issue_kind)
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
                    "capped_now": fin.get("capped_now") or 0,
                    "now_runs": fin.get("now_runs") or [],
                    "n": fin.get("n") or 0,
                    "last_ts": fin.get("last_ts"),
                    "version": fin.get("version") or "",
                    "history": history_view(sorted(pts, key=lambda p: p["ts"])) if pts else None,
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
    all_hot = list(by_id.values())

    cells = {}
    for br in branches:
        for cl in clusters:
            for fam in families:
                cells[f"{br}|{cl}|{fam}"] = {"status": "ok", "n_hot": 0, "n": 0}

    for (branch, cluster, suite), info in slice_status.items():
        fam = info["family"]
        key = f"{branch}|{cluster}|{fam}"
        if key not in cells:
            continue
        cells[key]["n"] += 1
        if info["status"] in ("broken", "regression"):
            cells[key]["n_hot"] += 1
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
            if key in cells and cells[key]["status"] == "ok":
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
    waves_meta = {}
    for (br, cl), waves in display_waves.items():
        if not waves:
            continue
        last = waves[-1]
        max_ts = last["max_ts"]
        waves_meta[f"{br}|{cl}"] = {
            "branch": br,
            "db": cl,
            "cluster": cl,
            "day": last["day"],
            "ci_version": last["day"],
            "suites": len(last["suites"]),
            "expected": len(display_expected.get((br, cl), ())),
            "max_ts": max_ts.isoformat().replace("+00:00", "")[:19],
            "age_hours": round((now_utc - max_ts).total_seconds() / 3600.0, 1),
        }

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
        "source": "perfomance/tpcc",
        "ui": {
            "now_runs": NOW_RUNS,
            "baseline_runs": BASELINE_RUNS,
            "stale_hours": STALE_HOURS,
            "wave_complete_hours": WAVE_COMPLETE_HOURS,
            "focus_branches": branches,
            "default_branch": default_branch,
            "focus_dbs": clusters,
            "dbs_by_branch": dbs_by_branch,
            "default_from": since.date().isoformat(),
            "default_to": until.date().isoformat(),
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
        "waves_by_db": waves_by_db,
        "last_activity": last_activity,
        "inbox": inbox,
        "ok": ok_slices,
        "rules": {
            "now": f"last {NOW_RUNS} runs",
            "baseline": f"previous {BASELINE_RUNS} runs",
            "lat": f"+{int(LAT_TOL*100)}%",
            "tpmc": f"-{int(TPMC_TOL*100)}%",
            "cap": f"lat ≥ {int(LAT_CAP)} → broken",
            "expected": f"suites in ≥{int(EXPECTED_MIN_SHARE*100)}% of day-waves / {EXPECTED_LOOKBACK_DAYS}d",
            "wave": "calendar day × Branch × Cluster",
            "wave_view": "finished (default) = last completed run; all = latest incl. in_progress",
        },
    }


def main():
    ap = argparse.ArgumentParser(description="TPC-C Now report generator")
    ap.add_argument("--input", required=True, type=Path)
    ap.add_argument(
        "--since",
        default=None,
        help=f"YYYY-MM-DD (default: today − {DEFAULT_WINDOW_DAYS}d ≈ 1 month)",
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

    data = build_now_report(points, since)
    data["meta"] = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "points": len(points),
        "input": str(args.input),
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    json_path = args.output.with_suffix(".json")
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))

    tpl = TEMPLATE.read_text()
    html = tpl.replace("__TPCC_REPORT_DATA__", json.dumps(data, ensure_ascii=False))
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
