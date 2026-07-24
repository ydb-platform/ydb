#!/usr/bin/env python3
"""Generate TPC-C HTML performance report from YDB query JSON.

Input formats:
  1) MCP/ydb tool dump: {"result_sets":[{"columns":[...],"rows":[...]}]}
  2) Flat list: [{"cluster":..., "run_type":..., ...}, ...]

Example:
  python3 generate.py --input out/raw.json --since 2026-07-13 --output out/report.html
  open out/report.html
"""

from __future__ import annotations

import argparse
import json
import statistics
import webbrowser
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TEMPLATE = ROOT / "template.html"

LAT_TOL = 0.10
LAT_WATCH = 0.07
TPMC_TOL_FLOOR = 0.03
TPMC_TOL_CEIL = 0.20


def parse_ts(s: str) -> datetime:
    if not s:
        raise ValueError("empty timestamp")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "." in s and "+" not in s[s.find("T") :]:
        # 2026-07-13T12:00:00.123456
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
        # columns may be list of names or list of {name: ...}
        names = []
        for c in cols:
            if isinstance(c, str):
                names.append(c)
            elif isinstance(c, dict):
                names.append(c.get("name") or c.get("Name") or next(iter(c.values())))
            else:
                names.append(str(c))
        out = []
        for row in rs["rows"]:
            d = dict(zip(names, row))
            out.append(d)
        return out
    if isinstance(raw, dict) and "rows" in raw and "columns" in raw:
        return [dict(zip(raw["columns"], r)) for r in raw["rows"]]
    raise SystemExit(f"Unsupported JSON shape in {path}")


def normalize_points(rows: list[dict], since: datetime) -> list[dict]:
    points = []
    for d in rows:
        ts_raw = d.get("timestamp") or d.get("ts")
        if ts_raw is None:
            continue
        ts = parse_ts(str(ts_raw))
        if ts < since:
            continue
        lat = d.get("lat90")
        if lat is None:
            lat = d.get("newOrderLatency90")
        capped = lat is not None and float(lat) >= 30000
        branch = d.get("git_branch") or d.get("branch") or ""
        if branch == "":
            branch = "(empty)"
        version = str(d.get("version") or "")[:11]
        tpmc = d.get("tpmC")
        if tpmc is None:
            tpmc = d.get("tpmc")
        points.append(
            {
                "cluster": d["cluster"],
                "run_type": d["run_type"],
                "warehouses": int(d["warehouses"]),
                "branch": branch,
                "ts": ts.isoformat().replace("+00:00", ""),
                "tpmc": None if tpmc is None else float(tpmc),
                "lat90": None if capped or lat is None else float(lat),
                "lat_capped": capped,
                "lat_raw": None if lat is None else float(lat),
                "version": version,
                "label": f"{ts.date().isoformat()}_{version[:7]}",
            }
        )
    return points


def avg(xs, field):
    vs = [p[field] for p in xs if p.get(field) is not None]
    return sum(vs) / len(vs) if vs else None


def pct(a, b):
    if a is None or b is None or a == 0:
        return None
    return (b - a) / a * 100


def safe_cv(vals):
    vals = [v for v in vals if v is not None]
    if len(vals) < 3:
        return None
    m = statistics.mean(vals)
    return None if m == 0 else statistics.pstdev(vals) / abs(m)


def successive_rel_mad(vals):
    vals = [v for v in vals if v is not None]
    if len(vals) < 3:
        return None
    rels = [abs(b - a) / (((abs(a) + abs(b)) / 2) or 1) for a, b in zip(vals, vals[1:])]
    rels.sort()
    return rels[len(rels) // 2]


def med(xs):
    return statistics.median(xs) if xs else None


def tpmc_tol(noise):
    if noise is None:
        return TPMC_TOL_FLOOR
    return min(TPMC_TOL_CEIL, max(TPMC_TOL_FLOOR, 2.5 * noise))


def bar_colors_tpmc(vals, base, tol_v):
    out = []
    for v in vals:
        if v is None or base is None:
            out.append("#5b8def")
        elif v < base * (1 - tol_v):
            out.append("#e5534b")
        elif v > base * (1 + tol_v * 0.5):
            out.append("#3fb950")
        else:
            out.append("#5b8def")
    return out


def bar_colors_lat(vals, base, tol_v, watch, capped_flags):
    out = []
    for v, cap in zip(vals, capped_flags):
        if cap:
            out.append("#8b0000")
        elif v is None or base is None:
            out.append("#d29922")
        elif v > base * (1 + tol_v):
            out.append("#e5534b")
        elif v > base * (1 + watch):
            out.append("#db6d28")
        elif v < base * (1 - watch):
            out.append("#3fb950")
        else:
            out.append("#d29922")
    return out


def is_lat_bad(v, base, capped, tol_v):
    if capped:
        return True
    if v is None or base is None:
        return False
    return v > base * (1 + tol_v)


def is_tpmc_bad(v, base, tol_v):
    if v is None or base is None:
        return False
    return v < base * (1 - tol_v)


def find_regression_start(pts, lat_base, tpmc_base, metric, lat_tol, tpmc_tol):
    flags = []
    for p in pts:
        if metric == "lat":
            flags.append(
                is_lat_bad(
                    p["lat_raw"] if p["lat_capped"] else p["lat90"],
                    lat_base,
                    p["lat_capped"],
                    lat_tol,
                )
            )
        else:
            flags.append(is_tpmc_bad(p["tpmc"], tpmc_base, tpmc_tol))
    start = None
    i = 0
    while i < len(flags):
        if flags[i]:
            j = i
            while j < len(flags) and flags[j]:
                j += 1
            start = i
            i = j
        else:
            i += 1
    if flags and flags[-1]:
        i = len(flags) - 1
        while i > 0 and flags[i - 1]:
            i -= 1
        start = i
    if start is None:
        return None, 0, 0
    streak = 0
    for k in range(start, len(flags)):
        if flags[k]:
            streak += 1
        else:
            break
    return pts[start], streak, sum(1 for f in flags if f)


def confidence(n, streak, bad_total, delta, status, n_base, n_recent, tol_v, noise):
    if status == "broken":
        return (
            ("high", "почти все прогоны capped")
            if n >= 10 and bad_total >= 0.8 * n
            else ("medium", "много capped")
        )
    score, reasons = 0, []
    if n >= 8:
        score += 2
        reasons.append(f"n={n}")
    elif n >= 5:
        score += 1
        reasons.append(f"n={n}")
    else:
        reasons.append(f"мало точек n={n}")
    if n_base >= 2 and n_recent >= 2:
        score += 1
    else:
        reasons.append("тонкий baseline/recent")
    if delta is not None and tol_v:
        ratio = abs(delta / 100) / tol_v
        if ratio >= 2.0:
            score += 2
            reasons.append(f"|Δ|/tol={ratio:.1f}")
        elif ratio >= 1.2:
            score += 1
            reasons.append(f"|Δ|/tol={ratio:.1f}")
    if streak >= 3:
        score += 2
        reasons.append(f"streak={streak}")
    elif streak >= 2:
        score += 1
        reasons.append(f"streak={streak}")
    else:
        reasons.append(f"streak={streak}")
    if noise is not None:
        reasons.append(f"noise={noise*100:.0f}%")
    level = "high" if score >= 6 else "medium" if score >= 3 else "low"
    return level, ", ".join(reasons[:4])


def build_report(points: list[dict], since: datetime, recent_start: datetime | None = None):
    # baseline: first 2 days of window
    base_end = since + timedelta(days=2)
    if recent_start is None:
        recent_start = since + timedelta(days=8)

    by_noise_key = defaultdict(lambda: defaultdict(list))
    for p in points:
        by_noise_key[(p["cluster"], p["run_type"], p["warehouses"])][p["branch"]].append(p)

    noise_table = {}
    for nk, by_br in by_noise_key.items():
        lat_cvs, tpmc_cvs, lat_jumps, tpmc_jumps = [], [], [], []
        n_branches = n_points = 0
        for br, pts in by_br.items():
            pts = sorted(pts, key=lambda x: x["ts"])
            n_points += len(pts)
            lats = [p["lat90"] for p in pts if p["lat90"] is not None]
            tpmcs = [p["tpmc"] for p in pts if p["tpmc"] is not None]
            if len(lats) >= 3 or len(tpmcs) >= 3:
                n_branches += 1
            for fn, bucket, arr in (
                (safe_cv, lat_cvs, lats),
                (safe_cv, tpmc_cvs, tpmcs),
                (successive_rel_mad, lat_jumps, lats),
                (successive_rel_mad, tpmc_jumps, tpmcs),
            ):
                v = fn(arr)
                if v is not None:
                    bucket.append(v)
        lat_noise = max([x for x in (med(lat_cvs), med(lat_jumps)) if x is not None], default=None)
        tpmc_noise = max([x for x in (med(tpmc_cvs), med(tpmc_jumps)) if x is not None], default=None)
        tt = tpmc_tol(tpmc_noise)
        noise_table[nk] = {
            "cluster": nk[0],
            "run_type": nk[1],
            "warehouses": nk[2],
            "lat_noise": lat_noise,
            "tpmc_noise": tpmc_noise,
            "lat_tol": LAT_TOL,
            "tpmc_tol": tt,
            "lat_watch": LAT_WATCH,
            "tpmc_watch": tt * 0.6,
            "n_branches": n_branches,
            "n_points": n_points,
        }

    slices = defaultdict(list)
    for p in points:
        slices[(p["cluster"], p["run_type"], p["warehouses"], p["branch"])].append(p)

    chart_specs, regs = [], []
    branch_problem = defaultdict(int)

    for key, pts in sorted(slices.items()):
        pts = sorted(pts, key=lambda x: x["ts"])
        if len(pts) < 2:
            continue
        cluster, run_type, wh, branch = key
        nz = noise_table[(cluster, run_type, wh)]
        lat_tol, tpmc_tol_v = nz["lat_tol"], nz["tpmc_tol"]
        lat_watch, tpmc_watch = nz["lat_watch"], nz["tpmc_watch"]
        early = [p for p in pts if parse_ts(p["ts"]) < base_end] or pts[: max(1, len(pts) // 2)]
        late = [p for p in pts if parse_ts(p["ts"]) >= recent_start] or pts[max(1, len(pts) // 2) :]
        tpmc_base, lat_base = avg(early, "tpmc"), avg(early, "lat90")
        tpmc_now, lat_now = avg(late, "tpmc"), avg(late, "lat90")
        tpmc_pct, lat_pct = pct(tpmc_base, tpmc_now), pct(lat_base, lat_now)
        n_cap = sum(1 for p in pts if p["lat_capped"])
        status, reasons, primary_metric = "ok", [], None
        if n_cap >= max(2, int(0.5 * len(pts))) and len(pts) >= 3:
            status = "broken"
            reasons.append(f"capped {n_cap}/{len(pts)}")
            primary_metric = "lat"
        else:
            if lat_pct is not None and lat_pct / 100 >= lat_tol and (lat_now or 0) > 0:
                status = "regression"
                reasons.append(f"lat +{lat_pct:.0f}% > +10%")
                primary_metric = "lat"
            elif lat_pct is not None and lat_pct / 100 >= lat_watch and (lat_now or 0) > 0:
                status = "watch"
                reasons.append(f"lat +{lat_pct:.0f}% (7–10%)")
                primary_metric = "lat"
            if tpmc_pct is not None and (-tpmc_pct / 100) >= tpmc_tol_v:
                status = "regression" if status != "broken" else status
                reasons.append(f"tpmC {tpmc_pct:.1f}% < -tol {tpmc_tol_v*100:.0f}%")
                if primary_metric is None:
                    primary_metric = "tpmc"
            elif tpmc_pct is not None and (-tpmc_pct / 100) >= tpmc_watch and status == "ok":
                status = "watch"
                reasons.append(f"tpmC {tpmc_pct:.1f}%")
                primary_metric = "tpmc"

        short_rt = run_type.replace("ydb_cli_", "")
        short_br = branch.replace("origin/", "")
        safe = (
            f"{cluster}_{short_rt}_{wh}_{short_br}"
            .replace("-", "_")
            .replace(".", "_")
            .replace("(", "")
            .replace(")", "")
        )
        labels = [p["label"] for p in pts]
        tpmc_vals = [p["tpmc"] for p in pts]
        lat_vals = [p["lat_raw"] if p["lat_capped"] else p["lat90"] for p in pts]
        capped_flags = [p["lat_capped"] for p in pts]
        versions = [p["version"] for p in pts]
        common = {
            "cluster": cluster,
            "branch": branch,
            "run_type": run_type,
            "warehouses": wh,
            "status": status,
            "n": len(pts),
            "labels": labels,
            "versions": versions,
            "short_rt": short_rt,
            "short_br": short_br,
            "lat_tol": lat_tol,
            "tpmc_tol": tpmc_tol_v,
            "lat_noise": nz["lat_noise"],
            "tpmc_noise": nz["tpmc_noise"],
        }
        chart_specs.append(
            {
                **common,
                "id": f"{safe}_tpmc",
                "kind": "tpmc",
                "title": f"tpmC ({wh//1000}k wh)",
                "subtitle": f"{cluster} · tol ±{tpmc_tol_v*100:.0f}%",
                "values": tpmc_vals,
                "colors": bar_colors_tpmc(tpmc_vals, tpmc_base, tpmc_tol_v),
                "baseline": tpmc_base,
                "higher_better": True,
                "delta": tpmc_pct,
            }
        )
        chart_specs.append(
            {
                **common,
                "id": f"{safe}_lat",
                "kind": "lat",
                "title": f"Latency 90p ({wh//1000}k wh)",
                "subtitle": f"{cluster} · tol +10%",
                "values": lat_vals,
                "colors": bar_colors_lat(lat_vals, lat_base, lat_tol, lat_watch, capped_flags),
                "baseline": lat_base,
                "higher_better": False,
                "delta": lat_pct,
                "capped": capped_flags,
            }
        )
        if status in ("regression", "broken", "watch") and len(pts) >= 3:
            metric = primary_metric or "lat"
            start_pt, streak, bad_total = find_regression_start(
                pts, lat_base, tpmc_base, metric, lat_tol, tpmc_tol_v
            )
            if status == "broken":
                start_pt = next((p for p in pts if p["lat_capped"]), pts[0])
                streak = bad_total = n_cap
            delta_main = lat_pct if metric == "lat" else tpmc_pct
            tol_main = lat_tol if metric == "lat" else tpmc_tol_v
            noise_main = nz["lat_noise"] if metric == "lat" else nz["tpmc_noise"]
            conf, conf_why = confidence(
                len(pts), streak, bad_total, delta_main, status, len(early), len(late), tol_main, noise_main
            )
            branch_problem[branch] += 1
            regs.append(
                {
                    "cluster": cluster,
                    "run_type": run_type,
                    "warehouses": wh,
                    "branch": branch,
                    "status": status,
                    "reasons": reasons,
                    "n": len(pts),
                    "tpmc_base": tpmc_base,
                    "tpmc_now": tpmc_now,
                    "tpmc_pct": tpmc_pct,
                    "lat_base": lat_base,
                    "lat_now": lat_now,
                    "lat_pct": lat_pct,
                    "last_ts": pts[-1]["ts"][:10],
                    "metric": metric,
                    "started_day": start_pt["ts"][:10] if start_pt else None,
                    "started_commit": start_pt["version"] if start_pt else None,
                    "streak": streak,
                    "bad_total": bad_total,
                    "confidence": conf,
                    "confidence_why": conf_why,
                    "short_rt": short_rt,
                    "short_br": short_br,
                    "lat_tol": lat_tol,
                    "tpmc_tol": tpmc_tol_v,
                    "lat_noise": nz["lat_noise"],
                    "tpmc_noise": nz["tpmc_noise"],
                }
            )

    regs.sort(
        key=lambda r: (
            0 if r["branch"] == "origin/main" else 1,
            -branch_problem[r["branch"]],
            r["branch"],
            r["cluster"],
            r["run_type"],
            r["warehouses"],
            {"regression": 0, "broken": 1, "watch": 2}[r["status"]],
        )
    )
    branches = sorted(
        {c["branch"] for c in chart_specs},
        key=lambda b: (0 if b == "origin/main" else 1, -branch_problem[b], b),
    )
    until = max((parse_ts(p["ts"]) for p in points), default=since)
    return {
        "charts": chart_specs,
        "regs": regs,
        "branches": branches,
        "noise": list(noise_table.values()),
        "window": f"{since.date().isoformat()}..{until.date().isoformat()}",
        "baseline": f"{since.date().isoformat()}..{(base_end - timedelta(days=1)).date().isoformat()}",
        "rules": {
            "lat_regression_pct": LAT_TOL * 100,
            "lat_watch_pct": LAT_WATCH * 100,
            "tpmc_tol": "from noise, typically ±3%",
            "compare_weight": "0.5^(age_days/2)",
        },
    }


def render_html(data: dict, output: Path):
    tpl = TEMPLATE.read_text()
    if "__TPCC_REPORT_DATA__" not in tpl:
        raise SystemExit("template.html missing __TPCC_REPORT_DATA__ placeholder")
    payload = json.dumps(data, ensure_ascii=False)
    html = tpl.replace("__TPCC_REPORT_DATA__", payload)
    # Patch visible window labels if present
    win = data.get("window", "")
    if ".." in win:
        a, b = win.split("..", 1)
        html = html.replace("2026-07-13 → 2026-07-24", f"{a} → {b}")
        html = html.replace("с 13.07", f"с {a[8:10]}.{a[5:7]}" if len(a) >= 10 else f"с {a}")
        html = html.replace("since 2026-07-13", f"since {a}")
    base = data.get("baseline", "")
    if ".." in base:
        a, b = base.split("..", 1)
        html = html.replace("13–14.07", f"{a[8:10]}–{b[8:10]}.{a[5:7]}" if len(a) >= 10 else base)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html)
    (output.with_suffix(".json")).write_text(json.dumps(data, ensure_ascii=False, indent=2))


def main():
    ap = argparse.ArgumentParser(description="Generate TPC-C HTML report")
    ap.add_argument("--input", "-i", required=True, help="JSON from YDB/MCP query")
    ap.add_argument("--since", default=None, help="YYYY-MM-DD (default: 11 days ago UTC)")
    ap.add_argument("--recent-from", default=None, help="YYYY-MM-DD start of 'recent' window")
    ap.add_argument("--output", "-o", default=str(ROOT / "out" / "tpcc-report.html"))
    ap.add_argument("--open", action="store_true", help="Open HTML in browser")
    args = ap.parse_args()

    if args.since:
        since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
    else:
        since = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=11)

    recent_start = None
    if args.recent_from:
        recent_start = datetime.fromisoformat(args.recent_from).replace(tzinfo=timezone.utc)

    rows = load_rows(Path(args.input))
    points = normalize_points(rows, since)
    if not points:
        raise SystemExit("No points after filtering — check --since and input data")
    data = build_report(points, since, recent_start)
    out = Path(args.output)
    render_html(data, out)
    print(f"points={len(points)} charts={len(data['charts'])} regs={len(data['regs'])}")
    print(f"wrote {out}")
    print(f"wrote {out.with_suffix('.json')}")
    if args.open:
        webbrowser.open(out.resolve().as_uri())


if __name__ == "__main__":
    main()
