"""TPC-C compare-delta / paint rules (Python mirror of template.html).

Used by unit tests; tpcc/generate.py keeps its own Now classify_slice.
"""

from __future__ import annotations


def baseline_usable(lat_base, tpmc_base) -> bool:
    """False when prev7 before compare/index is empty — must not classify as ok."""
    return lat_base is not None or tpmc_base is not None


def pct_change(base, now) -> float | None:
    if base is None or now is None:
        return None
    try:
        b = float(base)
        n = float(now)
    except (TypeError, ValueError):
        return None
    if b == 0:
        return None
    return (n - b) / b * 100.0


def classify_pair_values(
    lat_base,
    tpmc_base,
    lat_now,
    tpmc_now,
    capped_now: bool = False,
) -> dict | None:
    """Mirror template.html classifyPairValues.

    Returns None when baseline is missing (unless lat capped) — callers must not
    paint green ok from a null prev7 (window-edge compare day).
    """
    if not capped_now and not baseline_usable(lat_base, tpmc_base):
        return None
    lat_pct = pct_change(lat_base, lat_now)
    tpmc_pct = pct_change(tpmc_base, tpmc_now)
    lat_status = "ok"
    reasons: list[str] = []
    if capped_now:
        lat_status = "broken"
        reasons.append("lat capped")
    elif lat_base is not None and lat_now is not None and float(lat_now) > float(lat_base) * 3:
        lat_status = "broken"
        reasons.append("lat outlier >3×")
    elif lat_pct is not None and lat_pct >= 10:
        lat_status = "regression"
        reasons.append(f"lat {lat_pct:+.0f}%")
    elif lat_pct is not None and lat_pct >= 7:
        lat_status = "watch"
        reasons.append(f"lat {lat_pct:+.0f}% watch")
    tpmc_status = "ok"
    if tpmc_pct is not None and tpmc_pct <= -10:
        tpmc_status = "regression"
        reasons.append(f"tpmC {tpmc_pct:+.0f}%")
    elif tpmc_pct is not None and tpmc_pct <= -5:
        tpmc_status = "watch"
        reasons.append(f"tpmC {tpmc_pct:+.0f}% watch")
    status = "ok"
    if lat_status == "broken":
        status = "broken"
    elif lat_status == "regression" or tpmc_status == "regression":
        status = "regression"
    elif lat_status == "watch" or tpmc_status == "watch":
        status = "watch"
    lat_hot = lat_status in ("broken", "regression")
    tpmc_hot = tpmc_status == "regression"
    if lat_status == "broken":
        kind = "both" if tpmc_hot else "broken"
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
    else:
        kind = "ok"
    heat = status
    if kind == "lat" and status == "regression":
        heat = "lat"
    if kind == "tpmc" and status == "regression":
        heat = "tpmc"
    if kind == "both":
        heat = "both"
    return {
        "status": heat,
        "kind": kind,
        "issue": kind if kind != "ok" else ("watch" if status == "watch" else "ok"),
        "reasons": reasons,
        "lat_pct": lat_pct,
        "tpmc_pct": tpmc_pct,
        "n_lat": 1 if (lat_hot or lat_status == "broken") else 0,
        "n_tpmc": 1 if tpmc_hot else 0,
        "n_broken": 1 if lat_status == "broken" else 0,
    }


def side_metric_label(side: dict | None) -> str:
    """Mirror template.html sideMetricLabel (simplified for tests)."""
    if not side:
        return "—"
    st = side.get("status") or "ok"
    if st == "nodata":
        return "no data"
    if st == "noruns":
        return "no runs"
    parts: list[str] = []
    if (side.get("n_broken") or 0) > 0 or st == "broken":
        parts.append("broken")
    lat_pct = side.get("lat_pct")
    if lat_pct is not None and (
        (side.get("n_lat") or 0) > 0
        or st in ("broken", "lat", "regression", "both", "watch")
        or abs(float(lat_pct)) >= 0.5
    ):
        parts.append(f"lat {float(lat_pct):+.0f}%")
    tpmc_pct = side.get("tpmc_pct")
    if tpmc_pct is not None and (
        (side.get("n_tpmc") or 0) > 0
        or st in ("tpmc", "both", "regression")
        or abs(float(tpmc_pct)) >= 0.5
    ):
        parts.append(f"tpmC {float(tpmc_pct):+.0f}%")
    if parts:
        return " · ".join(parts)
    if st in ("ok", "watch"):
        return "ok" if st == "ok" else "watch"
    return st


def resolve_compare_cell(
    prev: dict, now: dict, lat_tol: float = 10.0, tpmc_tol: float = 10.0
) -> dict:
    """One contract for TPC-C compare paint/label (mirror template liveCell)."""
    delta = compare_delta_tpcc(prev, now, lat_tol=lat_tol, tpmc_tol=tpmc_tol)
    now_st = now.get("status") or "ok"
    paint = now_st if delta == "same" else f"delta-{delta}"
    a = side_metric_label(prev)
    b = side_metric_label({**now, "status": now_st})
    label = f"{b} =" if a == b else f"{a} → {b}"
    return {
        **now,
        "compare": True,
        "prev": prev,
        "delta": delta,
        "paint": paint,
        "status": paint,
        "delta_status": now_st,
        "label": label,
    }


def issue_filter_from_live(live: dict | None) -> str:
    """Heatmap click → inbox Issue. Mirror template.html cell click mapping."""
    if not live:
        return ""
    st = live.get("delta_status") if live.get("compare") else live.get("status")
    st = st or live.get("status") or ""
    if st in ("ok", "watch"):
        return "ok"
    if st in (
        "in_progress",
        "missing",
        "stale",
        "broken",
        "lat",
        "tpmc",
        "both",
    ):
        return st
    if st == "nodata":
        return ""
    if st == "regression":
        return "lat" if (live.get("n_lat") or 0) else "tpmc"
    return ""


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
    """Mirror template.html rowForCompare."""
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
    if issue == "in_progress" and wave_view == "finished" and not has_finished:
        return False
    return True


def tpcc_hard_band(side: dict | None) -> int:
    if not side:
        return 0
    st = side.get("status") or ""
    if st == "broken" or (side.get("n_broken") or 0) > 0:
        return 2
    if st in ("lat", "tpmc", "both", "regression") or (side.get("n_lat") or 0) or (
        side.get("n_tpmc") or 0
    ):
        return 1
    return 0


def compare_delta_tpcc(
    prev: dict, now: dict, lat_tol: float = 10.0, tpmc_tol: float = 10.0
) -> str:
    pb = tpcc_hard_band(prev)
    nb = tpcc_hard_band(now)
    lat_step = 0.0
    tpmc_step = 0.0
    if prev.get("lat_pct") is not None and now.get("lat_pct") is not None:
        lat_step = float(now["lat_pct"]) - float(prev["lat_pct"])
    if prev.get("tpmc_pct") is not None and now.get("tpmc_pct") is not None:
        # positive step = tpmC got worse (lower)
        tpmc_step = float(prev["tpmc_pct"]) - float(now["tpmc_pct"])
    broken_up = (now.get("n_broken") or 0) > (prev.get("n_broken") or 0)
    broken_down = (now.get("n_broken") or 0) < (prev.get("n_broken") or 0)
    if nb > pb or broken_up:
        return "worse-hot" if pb >= 1 else "worse"
    if nb < pb or broken_down:
        return "better"
    if pb >= 1 and nb >= 1:
        lat_worse = lat_step >= lat_tol
        lat_better = lat_step <= -lat_tol
        tpmc_worse = tpmc_step >= tpmc_tol
        tpmc_better = tpmc_step <= -tpmc_tol
        if (lat_worse and tpmc_better) or (lat_better and tpmc_worse):
            return "mixed"
        if lat_worse or tpmc_worse:
            return "worse-hot"
        if lat_better or tpmc_better:
            return "better"
    return "same"
