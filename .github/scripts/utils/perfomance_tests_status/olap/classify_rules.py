"""Pure OLAP Now classify / compare rules (generate.py + unit tests). TPC-C lives in tpcc/."""

from __future__ import annotations

import statistics

DUR_TOL = 0.10
DUR_HARD = 0.25
NOISE_K = 2.0
SLOW_PERSIST_MIN = 1
OUTLIER_MULT = 3.0

FAIL_BROKEN = 0.50
FAIL_HOT = 0.10
FAIL_RISE = 0.05


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


def stdev(xs) -> float:
    vs = [v for v in xs if v is not None]
    if len(vs) < 2:
        return 0.0
    return float(statistics.pstdev(vs))


def noise_pct(base_vals, ydb_base) -> float:
    if ydb_base is None or ydb_base <= 0:
        return 0.0
    return stdev(base_vals) / ydb_base * 100.0


def dur_threshold_pct(base_vals, ydb_base) -> float:
    return max(DUR_TOL * 100.0, NOISE_K * noise_pct(base_vals, ydb_base))


def count_above_base(now_vals, ydb_base) -> int:
    if ydb_base is None:
        return 0
    return sum(1 for v in now_vals if v is not None and v > ydb_base)


def is_fail_rate_hot(last_fr: float) -> bool:
    """Suite + query + JS: last_fr ≥ FAIL_HOT (≥10%) is always hot."""
    return last_fr >= FAIL_HOT


def fail_status_from_last(last_fr: float, fr_base: float) -> tuple[str, list[str]]:
    """Mirror generate.classify_slice fail branch (status + reasons)."""
    if last_fr >= FAIL_BROKEN:
        return "broken", [f"last run fail_rate {last_fr:.0%}"]
    if last_fr >= FAIL_HOT and last_fr >= fr_base + FAIL_RISE:
        return "regression", [f"fail_rate {fr_base:.0%}→{last_fr:.0%} (last run)"]
    if last_fr >= FAIL_HOT:
        return "regression", [f"last run fail_rate {last_fr:.0%}"]
    return "ok", []


def classify_duration(
    ydb_pct: float | None,
    ydb_now,
    ydb_base,
    now_vals: list,
    base_vals: list,
) -> dict:
    thr = dur_threshold_pct(base_vals, ydb_base)
    hard_floor = max(DUR_HARD * 100.0, thr)
    n_above = count_above_base(now_vals, ydb_base)
    persist = n_above >= SLOW_PERSIST_MIN
    noise = noise_pct(base_vals, ydb_base)

    if ydb_base is not None and ydb_now is not None and ydb_now > ydb_base * OUTLIER_MULT:
        return {
            "status": "broken",
            "level": "hard",
            "reasons": [f"dur outlier >{OUTLIER_MULT:.0f}×"],
            "thr_pct": thr,
            "noise_pct": noise,
            "n_above": n_above,
            "persist": persist,
        }
    if ydb_pct is None:
        return {
            "status": "ok",
            "level": "ok",
            "reasons": [],
            "thr_pct": thr,
            "noise_pct": noise,
            "n_above": n_above,
            "persist": persist,
        }
    if persist and ydb_pct >= hard_floor:
        return {
            "status": "regression",
            "level": "hard",
            "reasons": [
                f"dur +{ydb_pct:.0f}% ≥ hard {hard_floor:.0f}% "
                f"(thr {thr:.0f}% · noise {noise:.0f}% · {n_above}/{len(now_vals)} above base)"
            ],
            "thr_pct": thr,
            "noise_pct": noise,
            "n_above": n_above,
            "persist": persist,
        }
    if persist and ydb_pct >= thr:
        return {
            "status": "watch",
            "level": "soft",
            "reasons": [
                f"dur +{ydb_pct:.0f}% soft watch "
                f"(thr {thr:.0f}% · < hard {hard_floor:.0f}% · {n_above}/{len(now_vals)} above base)"
            ],
            "thr_pct": thr,
            "noise_pct": noise,
            "n_above": n_above,
            "persist": persist,
        }
    return {
        "status": "ok",
        "level": "ok",
        "reasons": [],
        "thr_pct": thr,
        "noise_pct": noise,
        "n_above": n_above,
        "persist": persist,
    }


# Mirror olap/template.html reactState defaults (fail + nodata on; hard/soft off).
DEFAULT_REACT = {"fail": True, "hard": False, "soft": False, "nodata": True}
# Full hard paint (fail + hard slow) — used when UI React hard is on.
REACT_FAIL_HARD = {"fail": True, "hard": True, "soft": False, "nodata": False}


def _react(signals: dict | None) -> dict:
    return dict(DEFAULT_REACT if signals is None else signals)


def olap_hard_band(side: dict | None, react: dict | None = None) -> int:
    """Hard band for compare paint; same React toggles as template.html fmtFS."""
    if not side:
        return 0
    s = _react(react)
    st = side.get("status") or ""
    fails = (side.get("n_fail") or 0) if s.get("fail") else 0
    slows = (side.get("n_slow") or 0) if s.get("hard") else 0
    if s.get("fail") and st in ("broken", "failing", "fail", "both"):
        return 2
    if fails > 0:
        return 2
    if s.get("hard") and (slows > 0 or st in ("regression", "slower", "slow")):
        return 1
    return 0


def _nodata_count(side: dict | None, react: dict) -> int:
    """React-visible query-gap count (status=nodata with 0 counts counts as 1)."""
    if not side or not react.get("nodata"):
        return 0
    n = int(side.get("n_nodata") or 0)
    if n:
        return n
    if (side.get("status") or "") in ("nodata", "noruns"):
        return 1
    return 0


def compare_delta_olap(prev: dict, now: dict, react: dict | None = None) -> str:
    """Significant compare paint; opposing channels → mixed. React-filtered.

    Includes nodata count up/down when React.nodata is on (heatmap + query pills).
    """
    s = _react(react)
    pb = olap_hard_band(prev, s)
    nb = olap_hard_band(now, s)
    pf = (prev.get("n_fail") or 0) if s.get("fail") else 0
    ps = (prev.get("n_slow") or 0) if s.get("hard") else 0
    nf = (now.get("n_fail") or 0) if s.get("fail") else 0
    ns = (now.get("n_slow") or 0) if s.get("hard") else 0
    pnd = _nodata_count(prev, s)
    nnd = _nodata_count(now, s)
    fail_up, fail_down = nf > pf, nf < pf
    slow_up, slow_down = ns > ps, ns < ps
    nd_up, nd_down = nnd > pnd, nnd < pnd
    if (fail_up and slow_down) or (fail_down and slow_up):
        return "mixed"
    if (fail_up and nd_down) or (fail_down and nd_up) or (slow_up and nd_down) or (
        slow_down and nd_up
    ):
        return "mixed"
    if fail_up or slow_up or nd_up or nb > pb:
        return "worse"
    if fail_down or slow_down or nd_down or nb < pb:
        return "better"
    return "same"


def fmt_fs(
    n_fail: int = 0,
    n_slow: int = 0,
    n_soft: int = 0,
    n_nodata: int = 0,
    react: dict | None = None,
) -> str:
    """Mirror template.html fmtFS (order: slow, fail, soft, no data)."""
    s = _react(react)
    bits: list[str] = []
    if s.get("hard") and n_slow:
        bits.append(f"slow {n_slow}")
    if s.get("fail") and n_fail:
        bits.append(f"fail {n_fail}")
    if s.get("soft") and n_soft:
        bits.append(f"soft {n_soft}")
    if s.get("nodata") and n_nodata:
        bits.append(f"no data {n_nodata}")
    return " ".join(bits)


def short_cell_status(
    st: str,
    n_fail: int = 0,
    n_slow: int = 0,
    n_soft: int = 0,
    n_nodata: int = 0,
    react: dict | None = None,
) -> str:
    """Mirror template.html shortCellStatus for label/paint consistency checks.

    Prefer calling on a side already passed through normalize_side_for_react —
    otherwise status=failing with React.fail off still falls back to bare 'fail'.

    noruns = no suite runs in window (grey).
    nodata + n_nodata>0 = missing query results (purple, critical).
    """
    # Coverage beats query counts (same as JS).
    if st == "missing":
        return "missing"
    if st == "stale":
        return "stale"
    if st == "in_progress":
        return "in progress"
    if st == "noruns":
        return "no runs"
    fs = fmt_fs(n_fail, n_slow, n_soft, n_nodata, react)
    if fs:
        if st == "broken":
            return f"broken {fs}"
        return fs
    if st in ("broken", "failing", "fail"):
        return "fail"
    if st in ("regression", "slower", "slow"):
        return "hot"
    if st == "watch":
        return "soft"
    # Bare nodata without counts = empty cell → no runs (not query gaps).
    if st == "nodata":
        return "no runs" if not n_nodata else "no data"
    if st == "both":
        return "fail+slow"
    return "ok"


def paint_class_for_side(side: dict | None) -> str:
    """CSS class: query-nodata → purple 'nodata'; empty → grey 'noruns'."""
    if not side:
        return "noruns"
    st = side.get("status") or "ok"
    n_nd = int(side.get("n_nodata") or 0)
    if st == "noruns":
        return "noruns"
    if st == "nodata" or n_nd > 0:
        return "nodata" if n_nd > 0 else "noruns"
    if st.startswith("delta-"):
        return st
    return st


def normalize_side_for_react(side: dict | None, react: dict | None = None) -> dict:
    """Align status + counts with React toggles (single source for alert + compare).

    Critical: suite status=failing with React.fail off must become ok, otherwise
    compare delta=same paints red 'fail =' while alert (React-filtered) showed ok.
    """
    if not side:
        return {
            "status": "noruns",
            "n_fail": 0,
            "n_slow": 0,
            "n_soft": 0,
            "n_nodata": 0,
            "n_hot": 0,
            "n_queries": 0,
        }
    s = _react(react)
    st = side.get("status") or "ok"
    if st == "noruns":
        return {
            "status": "noruns",
            "n_fail": 0,
            "n_slow": 0,
            "n_soft": 0,
            "n_nodata": 0,
            "n_hot": 0,
            "n_queries": 0,
        }
    raw_fail = int(side.get("n_fail") or 0)
    raw_slow = int(side.get("n_slow") or 0)
    raw_soft = int(side.get("n_soft") or 0)
    raw_nd = int(side.get("n_nodata") or 0)
    n_fail = raw_fail if s.get("fail") else 0
    n_slow = raw_slow if s.get("hard") else 0
    n_soft = raw_soft if s.get("soft") else 0
    n_nodata = raw_nd if s.get("nodata") else 0

    if st in ("missing", "stale", "in_progress"):
        # missing always stays (ops). stale/in_progress with only React-visible
        # nodata counts → nodata so paint matches "no data N" (not yellow stale).
        out_st = st
        if (
            st in ("stale", "in_progress")
            and n_nodata
            and not n_fail
            and not n_slow
            and not n_soft
        ):
            out_st = "nodata"
        return {
            "status": out_st,
            "n_fail": n_fail,
            "n_slow": n_slow,
            "n_soft": n_soft,
            "n_nodata": n_nodata,
            "n_hot": int(side.get("n_hot") or 0),
            "n_queries": n_fail + n_slow + n_soft + n_nodata,
        }

    suite_fail = bool(s.get("fail") and st in ("broken", "failing", "fail", "both"))
    suite_slow = bool(s.get("hard") and st in ("regression", "slower", "slow", "both"))
    suite_soft = bool(s.get("soft") and st == "watch")
    suite_nd = bool(s.get("nodata") and st in ("nodata", "noruns"))

    if n_fail and n_slow:
        new_st = "broken" if st == "broken" else "both"
    elif n_fail or suite_fail:
        new_st = "broken" if st == "broken" else "failing"
    elif n_slow or suite_slow:
        new_st = "regression"
    elif n_soft or suite_soft:
        new_st = "watch"
    elif n_nodata or (suite_nd and raw_nd > 0):
        new_st = "nodata"
    elif st in ("nodata", "noruns") and not n_fail and not n_slow and not n_soft:
        # Empty / no-runs cell (no query gaps).
        new_st = "noruns"
    else:
        new_st = "ok"

    return {
        "status": new_st,
        "n_fail": n_fail,
        "n_slow": n_slow,
        "n_soft": n_soft,
        "n_nodata": n_nodata,
        "n_hot": int(side.get("n_hot") or 0),
        "n_queries": n_fail + n_slow + n_soft + n_nodata,
    }


def resolve_alert_cell(side: dict | None, react: dict | None = None) -> dict:
    """Alert (no compare): paint class + label from React-normalized side."""
    now = normalize_side_for_react(side, react)
    paint = paint_class_for_side(now)
    label = short_cell_status(
        now["status"],
        now["n_fail"],
        now["n_slow"],
        now["n_soft"],
        now["n_nodata"],
        react,
    )
    return {
        **now,
        "compare": False,
        "delta": None,
        "paint": paint,
        "label": label,
        "delta_status": now["status"],
    }


def _compare_delta_is_nodata_only(
    prev: dict, now: dict, react: dict | None = None
) -> bool:
    s = _react(react)
    if not s.get("nodata"):
        return False
    pf = (prev.get("n_fail") or 0) if s.get("fail") else 0
    ps = (prev.get("n_slow") or 0) if s.get("hard") else 0
    nf = (now.get("n_fail") or 0) if s.get("fail") else 0
    ns = (now.get("n_slow") or 0) if s.get("hard") else 0
    if nf != pf or ns != ps:
        return False
    if olap_hard_band(prev, s) != olap_hard_band(now, s):
        return False
    return _nodata_count(prev, s) != _nodata_count(now, s)


def resolve_compare_cell(prev: dict, now: dict, react: dict | None = None) -> dict:
    """Compare: one contract for delta / paint / label (mirror template liveCell).

    Query-nodata → query-nodata must stay purple 'no data N =', never green 'ok ='.
    ok → no data / no data → ok get delta-* (+ delta-nodata purple gradient).
    """
    prev_n = normalize_side_for_react(prev, react)
    now_n = normalize_side_for_react(now, react)
    delta = compare_delta_olap(prev_n, now_n, react)
    if delta == "same":
        paint = paint_class_for_side(now_n)
    else:
        paint = f"delta-{delta}"
        if _compare_delta_is_nodata_only(prev_n, now_n, react):
            paint = f"{paint} delta-nodata"
    a = short_cell_status(
        prev_n["status"],
        prev_n["n_fail"],
        prev_n["n_slow"],
        prev_n["n_soft"],
        prev_n["n_nodata"],
        react,
    )
    b = short_cell_status(
        now_n["status"],
        now_n["n_fail"],
        now_n["n_slow"],
        now_n["n_soft"],
        now_n["n_nodata"],
        react,
    )
    label = f"{b} =" if a == b else f"{a} → {b}"
    primary = paint.split()[0]
    return {
        **now_n,
        "compare": True,
        "prev": prev_n,
        "delta": delta,
        "paint": paint,
        "status": primary,
        "delta_status": now_n["status"],
        "label": label,
    }


def compare_cell_paint_status(prev: dict, now: dict, react: dict | None = None) -> str:
    """CSS/status class for compare cell (React-normalized)."""
    return resolve_compare_cell(prev, now, react)["paint"]


def unwrap_finished_twin(row: dict) -> dict:
    """Mirror template.html unwrapFinishedTwin."""
    fin = row.get("finished") or {}
    issue = fin.get("issue")
    if not issue or issue == "in_progress":
        st = fin.get("status")
        issue = st if st and st != "in_progress" else "ok"
    out = dict(fin)
    for k in ("branch", "db", "family", "suite"):
        out[k] = row.get(k) if row.get(k) is not None else fin.get(k)
    out["issue"] = issue
    return out


def resolve_compare_row(row: dict, wave_view: str) -> dict | None:
    """Mirror template.html rowForCompare: twin / skip / keep coverage stub."""
    if row.get("issue") != "in_progress":
        return row
    if row.get("finished"):
        return unwrap_finished_twin(row)
    if wave_view == "finished":
        return None
    return row


def include_row_in_compare(
    issue: str | None, wave_view: str, *, has_finished: bool = False
) -> bool:
    """True if row enters compare pool (after optional twin unwrap)."""
    if issue == "in_progress" and wave_view == "finished" and not has_finished:
        return False
    return True
