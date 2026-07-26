"""Metric deltas for olap_slow / tpcc from context suite_now + suite_history."""

from __future__ import annotations

from typing import Any


def _f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def metrics_delta(ctx: dict[str, Any]) -> dict[str, Any]:
    kind = str((ctx.get("report") or {}).get("kind") or "")
    suite_now = ctx.get("suite_now") or {}
    hist = ctx.get("suite_history") or {}
    fr = (ctx.get("selection") or {}).get("focus_run") or {}
    out: dict[str, Any] = {
        "kind": kind,
        "suite_now": {},
        "history_tail": {},
        "deltas": {},
        "flags": [],
        "note": "Facts only — agent interprets regression cause.",
    }

    if kind == "tpcc":
        lat_now = _f(suite_now.get("lat_now") if suite_now.get("lat_now") is not None else fr.get("lat90"))
        lat_base = _f(suite_now.get("lat_base"))
        tpmc_now = _f(suite_now.get("tpmc_now") if suite_now.get("tpmc_now") is not None else fr.get("tpmc"))
        tpmc_base = _f(suite_now.get("tpmc_base"))
        out["suite_now"] = {
            "lat_now": lat_now,
            "lat_base": lat_base,
            "lat_pct": _f(suite_now.get("lat_pct")),
            "tpmc_now": tpmc_now,
            "tpmc_base": tpmc_base,
            "tpmc_pct": _f(suite_now.get("tpmc_pct")),
            "capped_now": bool(suite_now.get("capped_now") or fr.get("lat_capped")),
            "reasons": list(suite_now.get("reasons") or []),
        }
        lat90 = list(hist.get("lat90") or [])
        tpmc = list(hist.get("tpmc") or [])
        versions = list(hist.get("versions") or [])
        labels = list(hist.get("labels") or hist.get("days") or [])
        out["history_tail"] = {
            "labels": labels[-6:],
            "versions": versions[-6:],
            "lat90": lat90[-6:],
            "tpmc": tpmc[-6:],
        }
        d: dict[str, Any] = {}
        if lat_now is not None and lat_base not in (None, 0):
            d["lat_abs"] = lat_now - float(lat_base)
            d["lat_pct_calc"] = (lat_now / float(lat_base) - 1.0) * 100.0
        if tpmc_now is not None and tpmc_base not in (None, 0):
            d["tpmc_abs"] = tpmc_now - float(tpmc_base)
            d["tpmc_pct_calc"] = (tpmc_now / float(tpmc_base) - 1.0) * 100.0
        out["deltas"] = d
        if out["suite_now"]["capped_now"]:
            out["flags"].append("lat_capped")
        if (out["suite_now"].get("lat_pct") or 0) > 5 or (d.get("lat_pct_calc") or 0) > 5:
            out["flags"].append("lat_regression")
        if (out["suite_now"].get("tpmc_pct") or 0) < -5 or (d.get("tpmc_pct_calc") or 0) < -5:
            out["flags"].append("tpmc_regression")

    elif kind == "olap":
        out["suite_now"] = {
            "fail_rate_now": _f(suite_now.get("fail_rate_now")),
            "fail_rate_base": _f(suite_now.get("fail_rate_base")),
            "ydb_now": _f(suite_now.get("ydb_now")),
            "ydb_base": _f(suite_now.get("ydb_base")),
            "ydb_pct": _f(suite_now.get("ydb_pct")),
            "status": suite_now.get("status") or suite_now.get("issue"),
            "reasons": list(suite_now.get("reasons") or []),
        }
        frates = list(hist.get("fail_rate") or [])
        versions = list(hist.get("versions") or [])
        labels = list(hist.get("labels") or [])
        ydb = list(hist.get("ydb") or hist.get("ydb_score") or [])
        out["history_tail"] = {
            "labels": labels[-6:],
            "versions": versions[-6:],
            "fail_rate": frates[-6:],
            "ydb": ydb[-6:] if ydb else None,
        }
        d = {}
        yn = out["suite_now"]["ydb_now"]
        yb = out["suite_now"]["ydb_base"]
        if yn is not None and yb not in (None, 0):
            d["ydb_pct_calc"] = (yn / float(yb) - 1.0) * 100.0
        out["deltas"] = d
        # ydb_* is wall time: positive pct = slower = regression
        if (out["suite_now"].get("ydb_pct") or 0) > 5 or (d.get("ydb_pct_calc") or 0) > 5:
            out["flags"].append("ydb_regression")
        elif (out["suite_now"].get("ydb_pct") or 0) < -5 or (d.get("ydb_pct_calc") or 0) < -5:
            out["flags"].append("ydb_faster")
        fn = out["suite_now"]["fail_rate_now"]
        if fn is not None and fn >= 0.1:
            out["flags"].append("failing")

        queries = [q for q in (ctx.get("queries") or []) if isinstance(q, dict)]
        out["queries"] = [
            {
                "test": q.get("test"),
                "kind": q.get("kind"),
                "error_class": q.get("error_class"),
                "ydb_pct": q.get("ydb_pct"),
            }
            for q in queries[:20]
        ]
    else:
        out["flags"].append("unknown_kind")

    links = ctx.get("links") or {}
    if links.get("datalens"):
        out["datalens"] = links.get("datalens")
    return out
