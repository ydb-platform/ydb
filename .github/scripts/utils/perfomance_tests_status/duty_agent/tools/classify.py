"""Rule-based pre-label for duty cards (olap + tpcc)."""

from __future__ import annotations

from typing import Any


INFRA_FPS = frozenset(
    {
        "disconnected_node",
        "node_disconnected",
        "transport_error",
        "unavailable",
        "oom",
        "sandbox_fail",
    }
)


def prelabel(ctx: dict[str, Any], sandbox: dict[str, Any], history: dict[str, Any]) -> dict[str, Any]:
    kind = (ctx.get("report") or {}).get("kind")
    suite_now = ctx.get("suite_now") or {}
    reasons = [str(x) for x in (suite_now.get("reasons") or [])]
    queries = ctx.get("queries") or []
    fps = list(sandbox.get("fingerprints") or [])
    primary = sandbox.get("primary")

    labels: list[str] = []
    confidence = 0.35
    hypothesis = "needs_manual_triage"

    if primary in INFRA_FPS or any(f in INFRA_FPS for f in fps):
        labels.append("infra")
        hypothesis = f"infra_event:{primary or fps[0]}"
        confidence = 0.75
    elif primary == "timeout" or any(f == "timeout" for f in fps):
        labels.append("timeout")
        hypothesis = "timeout_storm_or_slow"
        confidence = 0.6
    elif primary == "diff" or any(
        (q.get("error_class") == "diff") for q in queries if isinstance(q, dict)
    ):
        labels.append("result_diff")
        hypothesis = "result_diff"
        confidence = 0.65

    if kind == "olap":
        fr_now = suite_now.get("fail_rate_now")
        fr_base = suite_now.get("fail_rate_base")
        try:
            if fr_now is not None and fr_base is not None:
                a, b = float(fr_now), float(fr_base)
                if a > 1.5:
                    a /= 100.0
                if b > 1.5:
                    b /= 100.0
                if a >= 0.1 and abs(a - b) < 0.05:
                    labels.append("chronic_fail")
                    if hypothesis == "needs_manual_triage":
                        hypothesis = "chronic_fail_not_fresh_spike"
                        confidence = max(confidence, 0.55)
                elif a >= 0.1 and a > b + 0.1:
                    labels.append("fresh_fail_spike")
                    confidence = max(confidence, 0.55)
        except (TypeError, ValueError):
            pass
        suite_h = history.get("suite") or {}
        if suite_h.get("chronic_in_window"):
            labels.append("chronic_in_window")
    elif kind == "tpcc":
        if suite_now.get("capped_now"):
            labels.append("lat_capped")
            hypothesis = "lat_capped_broken"
            confidence = max(confidence, 0.7)
        lat_pct = suite_now.get("lat_pct")
        tpmc_pct = suite_now.get("tpmc_pct")
        try:
            if lat_pct is not None and float(lat_pct) >= 10:
                labels.append("lat_regression")
            if tpmc_pct is not None and float(tpmc_pct) <= -10:
                labels.append("tpmc_regression")
        except (TypeError, ValueError):
            pass
        if "lat_regression" in labels or "tpmc_regression" in labels:
            if hypothesis == "needs_manual_triage":
                hypothesis = "tpcc_metric_regression"
                confidence = max(confidence, 0.5)

    reason_l = " ".join(reasons).lower()
    if "outlier" in reason_l:
        labels.append("outlier")
    if "disconnected" in reason_l:
        labels.append("infra")
        hypothesis = "infra_event:disconnected"
        confidence = max(confidence, 0.7)

    # de-dupe labels preserving order
    seen: set[str] = set()
    uniq = []
    for x in labels:
        if x not in seen:
            seen.add(x)
            uniq.append(x)

    return {
        "labels": uniq,
        "hypothesis": hypothesis,
        "confidence": round(min(0.95, confidence), 2),
        "kind": kind,
    }
