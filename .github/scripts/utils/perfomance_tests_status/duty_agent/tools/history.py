"""First-seen / chronic signals from compact history in the context pack."""

from __future__ import annotations

from typing import Any


def _as_list(hist: dict[str, Any] | None, key: str) -> list[Any]:
    if not hist or not isinstance(hist, dict):
        return []
    v = hist.get(key)
    return list(v) if isinstance(v, list) else []


def fail_first_seen(hist: dict[str, Any] | None, *, fail_threshold: float = 0.1) -> dict[str, Any]:
    """Find earliest point where fail_rate crosses threshold (OLAP)."""
    labels = _as_list(hist, "labels")
    fr = _as_list(hist, "fail_rate")
    versions = _as_list(hist, "versions")
    n = min(len(labels), len(fr)) if labels and fr else 0
    first_i = None
    for i in range(n):
        v = fr[i]
        if v is None:
            continue
        try:
            # history may store 0..1 or 0..100
            x = float(v)
        except (TypeError, ValueError):
            continue
        if x > 1.5:
            x = x / 100.0
        if x >= fail_threshold:
            first_i = i
            break
    last_i = n - 1 if n else None
    last_fr = None
    if last_i is not None and last_i < len(fr) and fr[last_i] is not None:
        try:
            last_fr = float(fr[last_i])
            if last_fr > 1.5:
                last_fr = last_fr / 100.0
        except (TypeError, ValueError):
            last_fr = None
    chronic = False
    if first_i is not None and n >= 4 and first_i <= max(0, n - 4):
        # failing for most of the tail window
        chronic = True
    return {
        "metric": "fail_rate",
        "threshold": fail_threshold,
        "points": n,
        "first_label": labels[first_i] if first_i is not None else None,
        "first_version": versions[first_i] if first_i is not None and first_i < len(versions) else None,
        "last_label": labels[last_i] if last_i is not None else None,
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
    return {
        "metric": key,
        "worse": worse,
        "pct": pct,
        "points": n,
        "first_label": labels[first_i] if first_i is not None else None,
        "first_version": versions[first_i] if first_i is not None and first_i < len(versions) else None,
        "last_label": labels[-1] if n else None,
        "last_value": series[-1] if n else None,
    }


def analyze_history(ctx: dict[str, Any]) -> dict[str, Any]:
    kind = (ctx.get("report") or {}).get("kind")
    suite_hist = ctx.get("suite_history")
    sticky = ctx.get("sticky_detail") or {}
    sticky_hist = sticky.get("history") if isinstance(sticky, dict) else None
    out: dict[str, Any] = {"kind": kind, "suite": None, "sticky_query": None}
    if kind == "olap":
        out["suite"] = fail_first_seen(suite_hist if isinstance(suite_hist, dict) else None)
        if sticky_hist:
            out["sticky_query"] = fail_first_seen(sticky_hist)
    elif kind == "tpcc":
        out["suite"] = {
            "lat90": metric_regress_first_seen(
                suite_hist if isinstance(suite_hist, dict) else None, key="lat90", worse="up"
            ),
            "tpmc": metric_regress_first_seen(
                suite_hist if isinstance(suite_hist, dict) else None, key="tpmc", worse="down"
            ),
        }
    return out
