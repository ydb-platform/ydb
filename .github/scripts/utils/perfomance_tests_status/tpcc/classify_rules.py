"""TPC-C compare-delta rules (Python mirror of template.html compareDeltaTpcc).

Used by unit tests; tpcc/generate.py keeps its own Now classify_slice.
"""

from __future__ import annotations


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
