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


# Mirror olap/template.html reactState defaults (fail on, hard/soft/nodata off).
DEFAULT_REACT = {"fail": True, "hard": False, "soft": False, "nodata": False}
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


def compare_delta_olap(prev: dict, now: dict, react: dict | None = None) -> str:
    """Significant hard compare paint; opposing fail/slow → mixed. React-filtered."""
    s = _react(react)
    pb = olap_hard_band(prev, s)
    nb = olap_hard_band(now, s)
    pf = (prev.get("n_fail") or 0) if s.get("fail") else 0
    ps = (prev.get("n_slow") or 0) if s.get("hard") else 0
    nf = (now.get("n_fail") or 0) if s.get("fail") else 0
    ns = (now.get("n_slow") or 0) if s.get("hard") else 0
    fail_up, fail_down = nf > pf, nf < pf
    slow_up, slow_down = ns > ps, ns < ps
    if (fail_up and slow_down) or (fail_down and slow_up):
        return "mixed"
    if fail_up or slow_up or nb > pb:
        return "worse"
    if fail_down or slow_down or nb < pb:
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
    """Mirror template.html shortCellStatus for label/paint consistency checks."""
    # Coverage beats query counts (same as JS).
    if st == "missing":
        return "missing"
    if st == "stale":
        return "stale"
    if st == "in_progress":
        return "in progress"
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
    if st == "nodata":
        return "no data"
    if st == "both":
        return "fail+slow"
    return "ok"


def compare_cell_paint_status(prev: dict, now: dict, react: dict | None = None) -> str:
    """Mirror liveCell status class: delta-* or raw now.status when same."""
    delta = compare_delta_olap(prev, now, react)
    if delta == "same":
        return now.get("status") or "ok"
    return f"delta-{delta}"


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
