"""Pick a good historical run (from dig-runs / pack history) and dig its Allure plans/logs."""

from __future__ import annotations

from typing import Any

from .sandbox import inspect_sandbox


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _report_of(run: dict[str, Any]) -> str | None:
    for k in ("Report", "report", "report_url"):
        u = str(run.get(k) or "").strip()
        if u.startswith("http"):
            return u
    return None


def select_baseline_from_slice_runs(
    slice_runs: list[dict[str, Any]],
    *,
    metric: str = "YdbSumMeans",
    jump: dict[str, Any] | None = None,
    focus_version: str | None = None,
) -> dict[str, Any] | None:
    """
    Choose a historical run with better metric + Allure Report.

    Preference:
      1) run at jump.from_ts (last good before largest step) if Report present
      2) latest earlier run with Report and metric << focus (≤85% of focus or ≤ median)
      3) min(metric) among runs with Report (excluding focus tip)
    """
    runs = [r for r in (slice_runs or []) if isinstance(r, dict)]
    if len(runs) < 2:
        return None

    def ts(r: dict[str, Any]) -> str:
        return str(r.get("RunTs") or r.get("timestamp") or "")

    runs = sorted(runs, key=ts)
    focus = runs[-1]
    if focus_version:
        fv = focus_version[:7]
        for r in reversed(runs):
            ver = str(r.get("Version") or r.get("version") or "")
            if ver.startswith(fv) or fv in ver:
                focus = r
                break
    focus_m = _num(focus.get(metric) if metric in focus else focus.get("lat90"))
    focus_ts = ts(focus)

    def pack(r: dict[str, Any], reason: str) -> dict[str, Any]:
        return {
            "reason": reason,
            "RunTs": r.get("RunTs") or r.get("timestamp"),
            "Version": r.get("Version") or r.get("version"),
            "metric": metric,
            "metric_value": _num(r.get(metric) if metric in r else r.get("lat90")),
            "FailCount": r.get("FailCount"),
            "Report": _report_of(r),
            "focus_RunTs": focus.get("RunTs") or focus.get("timestamp"),
            "focus_metric_value": focus_m,
        }

    # 1) jump.from side
    if jump and jump.get("from_ts"):
        for r in runs:
            if ts(r) == str(jump.get("from_ts")) and _report_of(r):
                mv = _num(r.get(metric) if metric in r else r.get("lat90"))
                if focus_m is None or mv is None or mv <= focus_m:
                    return pack(r, "largest_step_from")

    with_report = [r for r in runs if _report_of(r) and ts(r) < focus_ts]
    if not with_report:
        with_report = [r for r in runs if _report_of(r) and r is not focus]
    if not with_report:
        return None

    # 2) latest earlier run clearly better than focus
    if focus_m is not None and focus_m > 0:
        threshold = focus_m * 0.85
        better = [
            r
            for r in with_report
            if (_num(r.get(metric) if metric in r else r.get("lat90")) or 1e18) <= threshold
        ]
        if better:
            return pack(sorted(better, key=ts)[-1], "latest_better_than_focus")

    # 3) min metric with report
    best = min(
        with_report,
        key=lambda r: (_num(r.get(metric) if metric in r else r.get("lat90")) is None, _num(r.get(metric) if metric in r else r.get("lat90")) or 0),
    )
    return pack(best, "min_metric_with_report")


def select_baseline_from_pack_history(
    ctx: dict[str, Any],
) -> dict[str, Any] | None:
    """Fallback before dig-runs: suite_history arrays (ydb/lat90 + reports)."""
    hist = ctx.get("suite_history") or {}
    reports = list(hist.get("reports") or [])
    versions = list(hist.get("versions") or [])
    labels = list(hist.get("labels") or hist.get("days") or [])
    kind = str((ctx.get("report") or {}).get("kind") or "")
    if kind == "tpcc":
        series = list(hist.get("lat90") or [])
        metric = "lat90"
    else:
        series = list(hist.get("ydb") or hist.get("ydb_score") or [])
        metric = "YdbSumMeans"
    n = min(len(series), len(reports)) if reports else 0
    if n < 2:
        return None
    runs: list[dict[str, Any]] = []
    for i in range(n):
        runs.append(
            {
                "RunTs": labels[i] if i < len(labels) else str(i),
                "Version": versions[i] if i < len(versions) else None,
                "Report": reports[i],
                metric: series[i],
                "lat90": series[i] if metric == "lat90" else None,
                "YdbSumMeans": series[i] if metric == "YdbSumMeans" else None,
            }
        )
    return select_baseline_from_slice_runs(runs, metric=metric)


def select_baseline(
    *,
    dig: dict[str, Any] | None = None,
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Prefer dig_runs.summary.slice_runs; else pack suite_history."""
    if dig:
        summary = dig.get("summary") or dig
        kind = str(dig.get("kind") or (ctx or {}).get("report", {}).get("kind") or "")
        slice_runs = list(summary.get("slice_runs") or [])
        if kind == "tpcc" or (slice_runs and "lat90" in (slice_runs[0] or {})):
            jump = summary.get("largest_lat_step")
            # normalize jump keys for select (from_ts)
            if jump and "from_ts" not in jump and jump.get("from_version"):
                jump = {
                    **jump,
                    "from_ts": jump.get("from_ts"),
                }
            # tpcc jump uses different shape — find from by version
            if jump and not jump.get("from_ts"):
                fv = str(jump.get("from_version") or "")[:7]
                for r in slice_runs:
                    ver = str(r.get("version") or r.get("Version") or "")
                    if fv and (ver.startswith(fv) or fv in ver):
                        jump = {**jump, "from_ts": r.get("timestamp") or r.get("RunTs")}
                        break
            sel = (dig.get("selection") or (ctx or {}).get("selection") or {})
            return select_baseline_from_slice_runs(
                [
                    {
                        **r,
                        "YdbSumMeans": r.get("YdbSumMeans"),
                        "lat90": r.get("lat90"),
                        "Version": r.get("version") or r.get("Version"),
                        "RunTs": r.get("timestamp") or r.get("RunTs"),
                        "Report": r.get("Report") or r.get("report"),
                    }
                    for r in slice_runs
                ],
                metric="lat90",
                jump=jump,
                focus_version=str(sel.get("focus_sha") or "")[:7] or None,
            )
        jump = summary.get("largest_ydb_step")
        sel = (dig.get("selection") or (ctx or {}).get("selection") or {})
        return select_baseline_from_slice_runs(
            slice_runs,
            metric="YdbSumMeans",
            jump=jump,
            focus_version=str(sel.get("focus_sha") or "")[:7] or None,
        )
    if ctx:
        return select_baseline_from_pack_history(ctx)
    return None


def compare_plan_digs(
    focus_cases: list[dict[str, Any]],
    baseline_cases: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compare plan_dig hints / change flags for matching query names."""
    def by_query(cases: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for c in cases:
            name = str(c.get("name") or "")
            aa = c.get("attach_analysis") or {}
            pd = aa.get("plan_dig") or {}
            if not name:
                continue
            # index by bare QueryNN when present
            key = name
            for part in name.split("."):
                if part.lower().startswith("query"):
                    key = part
                    break
            out[key] = {
                "name": name,
                "hints": list(pd.get("hints") or []),
                "plan_changed_across_iterations": bool(pd.get("plan_changed_across_iterations")),
                "stats": pd.get("stats"),
                "iteration_count": len(pd.get("iterations") or []),
            }
        return out

    f_map = by_query(focus_cases)
    b_map = by_query(baseline_cases)
    comparisons: list[dict[str, Any]] = []
    for key in sorted(set(f_map) | set(b_map)):
        f = f_map.get(key) or {}
        b = b_map.get(key) or {}
        fh, bh = f.get("hints") or [], b.get("hints") or []
        comparisons.append(
            {
                "query": key,
                "focus_name": f.get("name"),
                "baseline_name": b.get("name"),
                "focus_hints": fh,
                "baseline_hints": bh,
                "hints_equal": fh == bh if (fh or bh) else None,
                "focus_only": sorted(set(fh) - set(bh)),
                "baseline_only": sorted(set(bh) - set(fh)),
                "verdict": (
                    "plan_regressed"
                    if fh and bh and fh != bh
                    else "plan_same"
                    if fh and bh and fh == bh
                    else "partial"
                    if (fh or bh)
                    else "no_plans"
                ),
            }
        )
    return {
        "comparisons": comparisons,
        "note": "hints_equal=false → plan_regressed; equal → dig server logs / runtime on same plan",
    }


def dig_baseline_allure(
    baseline: dict[str, Any],
    *,
    query_names: list[str] | None = None,
    offline: bool = False,
    include_plans: bool = True,
) -> dict[str, Any]:
    """Fetch Allure for baseline.Report (plans + logs for named queries)."""
    url = baseline.get("Report") or baseline.get("report")
    out: dict[str, Any] = {
        "baseline": baseline,
        "fetched": False,
        "url": url,
        "allure": None,
        "error": None,
        "query_names": list(query_names or []),
    }
    if not url:
        out["error"] = "baseline has no Report URL"
        return out
    sandbox = inspect_sandbox(
        str(url),
        offline=offline,
        extra_case_names=list(query_names or []) or None,
        include_plans=include_plans,
    )
    out["fetched"] = bool(sandbox.get("fetched"))
    out["error"] = sandbox.get("error")
    out["allure"] = sandbox.get("allure")
    out["source"] = sandbox.get("source")
    return out
