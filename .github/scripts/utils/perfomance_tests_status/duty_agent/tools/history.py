"""First-seen / chronic / appearance window from compact history in the pack."""

from __future__ import annotations

from typing import Any


def _as_list(hist: dict[str, Any] | None, key: str) -> list[Any]:
    if not hist or not isinstance(hist, dict):
        return []
    v = hist.get(key)
    return list(v) if isinstance(v, list) else []


def _norm_fail_rate(v: Any) -> float | None:
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    # History often stores 0..100 percent; sometimes 0..1.
    if x > 1.5:
        x = x / 100.0
    return x


def fail_first_seen(hist: dict[str, Any] | None, *, fail_threshold: float = 0.1) -> dict[str, Any]:
    """Find earliest point where fail_rate crosses threshold (OLAP)."""
    labels = _as_list(hist, "labels")
    fr = _as_list(hist, "fail_rate")
    versions = _as_list(hist, "versions")
    reports = _as_list(hist, "reports")
    n = min(len(labels), len(fr)) if labels and fr else 0
    first_i = None
    for i in range(n):
        x = _norm_fail_rate(fr[i])
        if x is None:
            continue
        if x >= fail_threshold:
            first_i = i
            break
    last_i = n - 1 if n else None
    last_fr = _norm_fail_rate(fr[last_i]) if last_i is not None and last_i < len(fr) else None
    chronic = False
    if first_i is not None and n >= 4 and first_i <= max(0, n - 4):
        chronic = True

    prev_green_i = None
    if first_i is not None and first_i > 0:
        for j in range(first_i - 1, -1, -1):
            x = _norm_fail_rate(fr[j])
            if x is not None and x < fail_threshold:
                prev_green_i = j
                break

    def _at(i: int | None, key_list: list[Any]) -> Any:
        if i is None or i < 0 or i >= len(key_list):
            return None
        return key_list[i]

    return {
        "metric": "fail_rate",
        "threshold": fail_threshold,
        "points": n,
        "first_index": first_i,
        "first_label": _at(first_i, labels),
        "first_version": _at(first_i, versions),
        "first_report": _at(first_i, reports),
        "prev_green_index": prev_green_i,
        "prev_green_label": _at(prev_green_i, labels),
        "prev_green_version": _at(prev_green_i, versions),
        "prev_green_report": _at(prev_green_i, reports),
        "last_label": _at(last_i, labels),
        "last_version": _at(last_i, versions),
        "last_fail_rate": last_fr,
        "chronic_in_window": chronic,
    }


def metric_regress_first_seen(
    hist: dict[str, Any] | None,
    *,
    key: str,
    worse: str,
    pct: float = 10.0,
) -> dict[str, Any]:
    """TPC-C: first index where metric worsens vs previous median of up-to-3 prior points."""
    labels = _as_list(hist, "labels")
    series = _as_list(hist, key)
    versions = _as_list(hist, "versions")
    n = min(len(labels), len(series))
    first_i = None
    for i in range(1, n):
        cur = series[i]
        if cur is None:
            continue
        prev = [float(x) for x in series[max(0, i - 3) : i] if x is not None]
        if not prev:
            continue
        base = sum(prev) / len(prev)
        if base == 0:
            continue
        try:
            cur_f = float(cur)
        except (TypeError, ValueError):
            continue
        delta_pct = (cur_f - base) / abs(base) * 100.0
        bad = (delta_pct >= pct) if worse == "up" else (delta_pct <= -pct)
        if bad:
            first_i = i
            break
    prev_green_i = first_i - 1 if first_i is not None and first_i > 0 else None
    return {
        "metric": key,
        "worse": worse,
        "pct": pct,
        "points": n,
        "first_label": labels[first_i] if first_i is not None else None,
        "first_version": versions[first_i] if first_i is not None and first_i < len(versions) else None,
        "prev_green_label": labels[prev_green_i] if prev_green_i is not None else None,
        "prev_green_version": (
            versions[prev_green_i]
            if prev_green_i is not None and prev_green_i < len(versions)
            else None
        ),
        "last_label": labels[-1] if n else None,
        "last_value": series[-1] if n else None,
    }


def appearance_summary(ctx: dict[str, Any], history: dict[str, Any]) -> dict[str, Any]:
    """When the problem showed up in suite / sticky history (+ focus sha)."""
    fr = (ctx.get("selection") or {}).get("focus_run") or {}
    sticky = history.get("sticky_query") or {}
    suite = history.get("suite") or {}
    # Prefer sticky (query-level) when present
    primary = sticky if sticky.get("first_version") or sticky.get("first_label") else suite
    scope = "sticky_query" if primary is sticky and sticky else "suite"
    if isinstance(suite, dict) and "lat90" in suite:
        # tpcc nested
        lat = suite.get("lat90") or {}
        tpmc = suite.get("tpmc") or {}
        primary = lat if lat.get("first_version") else tpmc
        scope = "tpcc_lat90" if primary is lat else "tpcc_tpmc"

    out = {
        "scope": scope,
        "focus_sha": fr.get("sha"),
        "focus_label": fr.get("label") or fr.get("day"),
        "focus_ts": fr.get("ts"),
        "first_fail_label": primary.get("first_label") if isinstance(primary, dict) else None,
        "first_fail_sha": primary.get("first_version") if isinstance(primary, dict) else None,
        "first_fail_report": primary.get("first_report") if isinstance(primary, dict) else None,
        "prev_green_label": primary.get("prev_green_label") if isinstance(primary, dict) else None,
        "prev_green_sha": primary.get("prev_green_version") if isinstance(primary, dict) else None,
        "prev_green_report": primary.get("prev_green_report") if isinstance(primary, dict) else None,
        "chronic_in_window": bool(
            isinstance(primary, dict) and primary.get("chronic_in_window")
        ),
        "fresh_on_focus": False,
    }
    fsha = out["first_fail_sha"]
    focus = out["focus_sha"]
    if fsha and focus and str(fsha).startswith(str(focus)[:7]):
        out["fresh_on_focus"] = True
    elif (
        out["first_fail_label"]
        and out["focus_label"]
        and str(out["first_fail_label"]) == str(out["focus_label"])
    ):
        out["fresh_on_focus"] = True
    return out


def analyze_history(ctx: dict[str, Any]) -> dict[str, Any]:
    kind = (ctx.get("report") or {}).get("kind")
    suite_hist = ctx.get("suite_history")
    sticky = ctx.get("sticky_detail") or {}
    sticky_hist = sticky.get("history") if isinstance(sticky, dict) else None
    out: dict[str, Any] = {"kind": kind, "suite": None, "sticky_query": None, "appeared": None}
    if kind == "olap":
        # Suite: treat ≥5% as "failing enough" for appearance (percent scale common).
        out["suite"] = fail_first_seen(
            suite_hist if isinstance(suite_hist, dict) else None, fail_threshold=0.05
        )
        if sticky_hist:
            # Sticky query often 0/100 — use 0.5 so sparse 4% suite noise does not pollute.
            out["sticky_query"] = fail_first_seen(sticky_hist, fail_threshold=0.5)
        out["appeared"] = appearance_summary(ctx, out)
    elif kind == "tpcc":
        out["suite"] = {
            "lat90": metric_regress_first_seen(
                suite_hist if isinstance(suite_hist, dict) else None, key="lat90", worse="up"
            ),
            "tpmc": metric_regress_first_seen(
                suite_hist if isinstance(suite_hist, dict) else None, key="tpmc", worse="down"
            ),
        }
        out["appeared"] = appearance_summary(ctx, out)
    return out
