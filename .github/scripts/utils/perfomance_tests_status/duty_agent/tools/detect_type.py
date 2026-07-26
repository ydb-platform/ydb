"""Seed analysis_types + problems from perf-duty-context/v1 (hints only)."""

from __future__ import annotations

import re
from typing import Any

from .context import kind_of


def _as_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


def _query_counts(ctx: dict[str, Any]) -> dict[str, int]:
    """Fail/slow/soft/nodata/ok/total from suite_now or queries list."""
    suite_now = ctx.get("suite_now") or {}
    qc = suite_now.get("query_counts")
    if isinstance(qc, dict) and any(qc.get(k) is not None for k in ("nodata", "fail", "total")):
        return {
            "fail": _as_int(qc.get("fail")) or 0,
            "slow": _as_int(qc.get("slow")) or 0,
            "soft": _as_int(qc.get("soft")) or 0,
            "nodata": _as_int(qc.get("nodata")) or 0,
            "ok": _as_int(qc.get("ok")) or 0,
            "total": _as_int(qc.get("total")) or 0,
        }
    out = {"fail": 0, "slow": 0, "soft": 0, "nodata": 0, "ok": 0, "total": 0}
    for key, dest in (
        ("n_fail", "fail"),
        ("n_slow", "slow"),
        ("n_soft", "soft"),
        ("n_nodata", "nodata"),
        ("n_ok", "ok"),
        ("n_queries", "total"),
    ):
        n = _as_int(suite_now.get(key))
        if n is not None:
            out[dest] = n
    queries = list(ctx.get("queries") or [])
    if queries and not out["total"]:
        for q in queries:
            if not isinstance(q, dict):
                continue
            out["total"] += 1
            k = str(q.get("kind") or "ok").lower()
            if k in ("fail", "both"):
                out["fail"] += 1
            if k in ("slow", "both"):
                out["slow"] += 1
            if k in ("watch", "soft"):
                out["soft"] += 1
            if k in ("nodata", "missing", "in_progress"):
                out["nodata"] += 1
            if k == "ok":
                out["ok"] += 1
    elif queries and out["nodata"] == 0:
        # Counts present but nodata may only be in the sample list
        for q in queries:
            if isinstance(q, dict) and str(q.get("kind") or "").lower() in (
                "nodata",
                "missing",
                "in_progress",
            ):
                out["nodata"] += 1
    return out


def _legacy_nodata_gap(ctx: dict[str, Any], counts: dict[str, int]) -> bool:
    """Old Save packs omitted nodata queries — infer incomplete coverage."""
    if counts.get("nodata"):
        return False
    suite_now = ctx.get("suite_now") or {}
    status = str(suite_now.get("status") or suite_now.get("issue") or "").lower()
    if status in ("failing", "fail", "broken", "error", "regression", "slower", "both"):
        return False
    fr = (ctx.get("selection") or {}).get("focus_run") or {}
    success = _as_int(fr.get("success"))
    fail = _as_int(fr.get("fail")) or 0
    # Explicit incomplete SuccessCount (Tpcds-style 26/100) with no fails.
    if success is not None and success > 0 and success < 40 and fail == 0:
        return True
    # Huge "improvement" on ok suite + empty query list → almost always partial means.
    if list(ctx.get("queries") or []):
        return False
    ydb_pct = suite_now.get("ydb_pct")
    try:
        if ydb_pct is not None and float(ydb_pct) < -40:
            return True
    except (TypeError, ValueError):
        pass
    return False


def detect_type(ctx: dict[str, Any]) -> dict[str, Any]:
    """Best-effort seed; agent may reclassify after fetch."""
    kind = kind_of(ctx)
    suite_now = ctx.get("suite_now") or {}
    queries = list(ctx.get("queries") or [])
    sticky = ctx.get("sticky_query")
    reasons = [str(r) for r in (suite_now.get("reasons") or [])]
    reasons_l = " ".join(reasons).lower()
    counts = _query_counts(ctx)

    analysis_types: list[str] = []
    problems_seed: list[dict[str, Any]] = []

    if kind == "olap":
        fail_queries = [
            q for q in queries if isinstance(q, dict) and str(q.get("kind") or "") in ("fail", "both")
        ]
        slow_queries = [
            q for q in queries if isinstance(q, dict) and str(q.get("kind") or "") in ("slow", "both")
        ]
        nodata_queries = [
            q
            for q in queries
            if isinstance(q, dict)
            and str(q.get("kind") or "") in ("nodata", "missing", "in_progress")
        ]
        status = str(suite_now.get("status") or suite_now.get("issue") or "").lower()
        sn_kind = str(suite_now.get("kind") or "").lower()

        has_fail = (
            bool(fail_queries)
            or counts["fail"] > 0
            or status in ("failing", "fail", "error", "broken", "both")
            or sn_kind in ("fail", "both")
        )
        # ydb_* is wall time: positive ydb_pct = slower (regression); negative = faster.
        # Do not match bare "slow" in reasons — pack text often says "slow 0".
        reasons_say_slow = bool(
            re.search(r"slow\s*[1-9]|kind[=:]\s*slow|\bslower\b", reasons_l)
        )
        has_slow = (
            bool(slow_queries)
            or counts["slow"] > 0
            or sn_kind == "slow"
            or reasons_say_slow
            or (
                suite_now.get("ydb_pct") is not None
                and isinstance(suite_now.get("ydb_pct"), (int, float))
                and float(suite_now["ydb_pct"]) > 5
            )
        )
        has_nodata = (
            bool(nodata_queries)
            or counts["nodata"] > 0
            or sn_kind == "nodata"
            or status == "nodata"
            or "nodata" in reasons_l
            or "no data" in reasons_l
            or _legacy_nodata_gap(ctx, counts)
        )

        if has_fail:
            analysis_types.append("olap_fail")
            if fail_queries:
                for q in fail_queries[:12]:
                    problems_seed.append(
                        {
                            "id": f"seed_{q.get('test')}",
                            "analysis_type": "olap_fail",
                            "title": str(q.get("test") or "fail"),
                            "test": q.get("test"),
                            "error_class": q.get("error_class"),
                            "status": "seed",
                        }
                    )
            elif sticky:
                problems_seed.append(
                    {
                        "id": f"seed_{sticky}",
                        "analysis_type": "olap_fail",
                        "title": str(sticky),
                        "test": sticky,
                        "status": "seed",
                    }
                )
            else:
                problems_seed.append(
                    {
                        "id": "seed_suite_fail",
                        "analysis_type": "olap_fail",
                        "title": "suite failing",
                        "status": "seed",
                    }
                )

        if has_slow:
            analysis_types.append("olap_slow")
            if slow_queries:
                for q in slow_queries[:8]:
                    problems_seed.append(
                        {
                            "id": f"seed_slow_{q.get('test')}",
                            "analysis_type": "olap_slow",
                            "title": f"slow {q.get('test')}",
                            "test": q.get("test"),
                            "status": "seed",
                        }
                    )
            else:
                problems_seed.append(
                    {
                        "id": "seed_suite_slow",
                        "analysis_type": "olap_slow",
                        "title": "suite slow / ydb regression",
                        "status": "seed",
                    }
                )

        if has_nodata:
            analysis_types.append("olap_nodata")
            n_nd = counts["nodata"] or len(nodata_queries)
            sample = [str(q.get("test")) for q in nodata_queries[:8] if q.get("test")]
            title = f"no data ×{n_nd}" if n_nd else "no data (coverage gap)"
            if sample:
                title += ": " + ", ".join(sample[:5])
            problems_seed.append(
                {
                    "id": "seed_suite_nodata",
                    "analysis_type": "olap_nodata",
                    "title": title,
                    "n_nodata": n_nd or None,
                    "sample": sample,
                    "query_counts": counts,
                    "status": "seed",
                }
            )

        if not analysis_types:
            analysis_types.append("olap_fail")
            problems_seed.append(
                {
                    "id": "seed_unknown_olap",
                    "analysis_type": "olap_fail",
                    "title": "olap (unclassified)",
                    "status": "seed",
                }
            )

    elif kind == "tpcc":
        lat_pct = suite_now.get("lat_pct")
        tpmc_pct = suite_now.get("tpmc_pct")
        capped = bool(
            suite_now.get("capped_now")
            or (ctx.get("selection") or {}).get("focus_run", {}).get("lat_capped")
        )
        sn_kind = str(suite_now.get("kind") or "").lower()

        lat_bad = False
        tpmc_bad = False
        try:
            if lat_pct is not None and float(lat_pct) > 5:
                lat_bad = True
        except (TypeError, ValueError):
            pass
        try:
            if tpmc_pct is not None and float(tpmc_pct) < -5:
                tpmc_bad = True
        except (TypeError, ValueError):
            pass
        if capped or "lat" in reasons_l or sn_kind in ("slow", "lat"):
            lat_bad = lat_bad or capped or "lat" in reasons_l
        if "tpmc" in reasons_l or "tpm" in reasons_l:
            tpmc_bad = True

        # If both weak, pick from which metric moved more
        if not lat_bad and not tpmc_bad:
            try:
                lp = abs(float(lat_pct)) if lat_pct is not None else 0.0
                tp = abs(float(tpmc_pct)) if tpmc_pct is not None else 0.0
                if lp >= tp and lp > 0:
                    lat_bad = True
                elif tp > 0:
                    tpmc_bad = True
                else:
                    lat_bad = True  # default tpcc seed
            except (TypeError, ValueError):
                lat_bad = True

        if tpmc_bad:
            analysis_types.append("tpcc_tpmc")
            problems_seed.append(
                {
                    "id": "seed_tpmc",
                    "analysis_type": "tpcc_tpmc",
                    "title": "tpmC regression",
                    "status": "seed",
                    "tpmc_pct": tpmc_pct,
                }
            )
        if lat_bad:
            analysis_types.append("tpcc_lat")
            problems_seed.append(
                {
                    "id": "seed_lat",
                    "analysis_type": "tpcc_lat",
                    "title": "latency regression" + (" (capped)" if capped else ""),
                    "status": "seed",
                    "lat_pct": lat_pct,
                    "lat_capped": capped,
                }
            )
    else:
        analysis_types.append("unknown")
        problems_seed.append(
            {"id": "seed_unknown", "analysis_type": "unknown", "title": "unknown kind", "status": "seed"}
        )

    if len(analysis_types) > 1:
        rollup = "mixed"
    else:
        rollup = analysis_types[0] if analysis_types else "unknown"

    return {
        "kind": kind,
        "rollup": rollup,
        "analysis_types": analysis_types,
        "problems_seed": problems_seed,
        "query_counts": counts if kind == "olap" else None,
        "suite_now_status": suite_now.get("status") or suite_now.get("issue"),
        "sticky_query": sticky,
        "reasons": reasons,
        "note": "Seed only — agent may reclassify after fetch-focus / metrics-delta.",
    }
