"""Seed analysis_types + problems from perf-duty-context/v1 (hints only)."""

from __future__ import annotations

import re
from typing import Any

from .context import kind_of


def detect_type(ctx: dict[str, Any]) -> dict[str, Any]:
    """Best-effort seed; agent may reclassify after fetch."""
    kind = kind_of(ctx)
    suite_now = ctx.get("suite_now") or {}
    queries = list(ctx.get("queries") or [])
    sticky = ctx.get("sticky_query")
    reasons = [str(r) for r in (suite_now.get("reasons") or [])]
    reasons_l = " ".join(reasons).lower()

    analysis_types: list[str] = []
    problems_seed: list[dict[str, Any]] = []

    if kind == "olap":
        fail_queries = [
            q for q in queries if isinstance(q, dict) and str(q.get("kind") or "") == "fail"
        ]
        slow_queries = [
            q for q in queries if isinstance(q, dict) and str(q.get("kind") or "") == "slow"
        ]
        status = str(suite_now.get("status") or suite_now.get("issue") or "").lower()
        sn_kind = str(suite_now.get("kind") or "").lower()

        has_fail = bool(fail_queries) or status in ("failing", "fail", "error") or sn_kind == "fail"
        # ydb_* is wall time: positive ydb_pct = slower (regression); negative = faster.
        # Do not match bare "slow" in reasons — pack text often says "slow 0".
        reasons_say_slow = bool(
            re.search(r"slow\s*[1-9]|kind[=:]\s*slow|\bslower\b", reasons_l)
        )
        has_slow = (
            bool(slow_queries)
            or sn_kind == "slow"
            or reasons_say_slow
            or (
                suite_now.get("ydb_pct") is not None
                and isinstance(suite_now.get("ydb_pct"), (int, float))
                and float(suite_now["ydb_pct"]) > 5
            )
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
        capped = bool(suite_now.get("capped_now") or (ctx.get("selection") or {}).get("focus_run", {}).get("lat_capped"))
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
        "suite_now_status": suite_now.get("status") or suite_now.get("issue"),
        "sticky_query": sticky,
        "reasons": reasons,
        "note": "Seed only — agent may reclassify after fetch-focus / metrics-delta.",
    }
