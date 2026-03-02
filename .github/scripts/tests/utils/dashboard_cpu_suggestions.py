from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional


def _status_bucket() -> dict[str, int]:
    return {"errors": 0, "timeouts": 0, "muted": 0, "muted_timeouts": 0, "fails_total": 0}


def _round_cpu_tier(cores: float) -> int:
    """Round up to runner-friendly tier: 1, 2, 4, 8, 16."""
    if cores <= 1:
        return 1
    if cores <= 2:
        return 2
    if cores <= 4:
        return 4
    if cores <= 8:
        return 8
    return 16


def _next_cpu_tier(cpu: int) -> int:
    """Move to the next runner tier: 1->2->4->8->16."""
    if cpu <= 1:
        return 2
    if cpu <= 2:
        return 4
    if cpu <= 4:
        return 8
    if cpu <= 8:
        return 16
    return 16


def build_cpu_suggestions(
    runs: list[dict[str, Any]],
    requirements_cache: Optional[dict[str, dict[str, Any]]] = None,
    report_status_by_suite: Optional[dict[str, dict[str, dict[str, int]]]] = None,
    maximize_reqs_for_timeout_tests: bool = False,
) -> list[dict[str, Any]]:
    """
    Per-suite stats from runs (cpu_sec_report, dur_us) to suggest cpuN for the runner.
    Returns list of dicts: suite_path, chunks_count, median_cores, p95_cores,
    recommended_cpu (tier 1/2/4/8/16 or "all"), total_cpu_sec, total_ram_gb, total_dur_sec, has_synthetic.
    """
    by_suite: dict[str, list[float]] = defaultdict(list)
    by_suite_cpu: dict[str, float] = defaultdict(float)
    by_suite_ram_kb: dict[str, float] = defaultdict(float)
    by_suite_dur: dict[str, float] = defaultdict(float)
    by_suite_synthetic: dict[str, bool] = defaultdict(bool)
    by_suite_errors: dict[str, int] = defaultdict(int)
    by_suite_timeouts: dict[str, int] = defaultdict(int)
    by_suite_muted: dict[str, int] = defaultdict(int)
    by_suite_muted_timeouts: dict[str, int] = defaultdict(int)
    for r in runs:
        suite = str(r.get("suite_path", ""))
        if not suite:
            continue
        cpu = float(r.get("cpu_sec_report", 0.0) or 0.0)
        ram_kb = float(r.get("ram_kb_report", 0.0) or 0.0)
        dur_s = float(r.get("dur_us", 0) or 0) / 1_000_000.0
        by_suite_cpu[suite] += cpu
        by_suite_ram_kb[suite] += ram_kb
        by_suite_dur[suite] += dur_s
        if r.get("synthetic_metrics"):
            by_suite_synthetic[suite] = True
        status = str(r.get("status", "") or "").upper()
        error_type = str(r.get("error_type", "") or "").upper()
        is_timeout = error_type == "TIMEOUT" or ("TIMEOUT" in status)
        is_muted = bool(r.get("is_muted")) or status == "MUTE"
        is_failedish = status in {"FAILED", "ERROR", "INTERNAL"}
        if is_timeout:
            by_suite_timeouts[suite] += 1
        if is_muted:
            by_suite_muted[suite] += 1
        if is_timeout and is_muted:
            by_suite_muted_timeouts[suite] += 1
        if is_failedish and not is_timeout and not is_muted:
            by_suite_errors[suite] += 1
        if dur_s > 0:
            by_suite[suite].append(cpu / dur_s)
    all_suites = sorted(set(by_suite_cpu.keys()) | set((report_status_by_suite or {}).keys()) | set((requirements_cache or {}).keys()))
    out: list[dict[str, Any]] = []
    for suite in all_suites:
        cores_list = by_suite[suite]
        if not cores_list:
            median_c = 0.0
            p95_c = 0.0
        else:
            sorted_cores = sorted(cores_list)
            n = len(sorted_cores)
            median_c = sorted_cores[(n - 1) // 2] if n else 0.0
            idx95 = min(int(0.95 * n + 0.5), n - 1) if n else 0
            p95_c = sorted_cores[idx95] if n else 0.0
        if report_status_by_suite and suite in report_status_by_suite:
            chunk_status = report_status_by_suite[suite].get("chunks", _status_bucket())
            test_status = report_status_by_suite[suite].get("tests", _status_bucket())
        else:
            chunk_status = {
                "errors": by_suite_errors[suite],
                "timeouts": by_suite_timeouts[suite],
                "muted": by_suite_muted[suite],
                "muted_timeouts": by_suite_muted_timeouts[suite],
                "fails_total": by_suite_errors[suite] + by_suite_timeouts[suite],
            }
            test_status = _status_bucket()

        timeout_tests_count = int(test_status.get("timeouts", 0) or 0)
        base_recommended = _round_cpu_tier(p95_c)
        timeout_boost_applied = timeout_tests_count > 0
        recommended = _next_cpu_tier(base_recommended) if timeout_boost_applied else base_recommended
        req = (requirements_cache or {}).get(suite, {})
        ya_cpu = req.get("cpu_cores")
        ya_ram = req.get("ram_gb")
        ya_size = req.get("size")
        if not ya_size:
            ya_size = "SMALL"
        # Policy: MEDIUM suites must not request more than cpu:4.
        medium_cap_applied = str(ya_size or "").upper() == "MEDIUM" and recommended > 4
        if medium_cap_applied:
            recommended = 4
        timeout_max_policy_applied = bool(maximize_reqs_for_timeout_tests and timeout_tests_count > 0)
        size_u = str(ya_size or "").upper()
        timeout_max_value: Any = None
        if timeout_max_policy_applied:
            if size_u == "SMALL":
                timeout_max_value = 1
            elif size_u == "MEDIUM":
                timeout_max_value = 4
            elif size_u == "LARGE":
                timeout_max_value = "all"
            else:
                timeout_max_value = "all"
        recommended_req: Any = timeout_max_value if timeout_max_policy_applied else recommended
        explain_parts = [
            f"p95_cores={p95_c:.3f}",
            f"base_tier={base_recommended}",
        ]
        if timeout_boost_applied:
            explain_parts.append(f"test_timeouts={timeout_tests_count} -> next_tier={_next_cpu_tier(base_recommended)}")
        if timeout_max_policy_applied:
            explain_parts.append(f"maximize_reqs_for_timeout_tests(size={size_u or 'UNKNOWN'}) -> {timeout_max_value}")
        if medium_cap_applied:
            explain_parts.append("MEDIUM cap -> 4")
        explain_parts.append(f"final={recommended_req}")
        recommended_cpu_explain = "; ".join(explain_parts)
        if recommended_req == "all":
            if ya_cpu is None:
                cpu_action = "set"
            else:
                cpu_action = "raise"
        else:
            recommended_num = int(recommended_req)
            if ya_cpu is None:
                # In ya.make implicit default is cpu:1, so do not suggest explicit set(cpu:1).
                cpu_action = "ok" if recommended_num <= 1 else "set"
            else:
                ya_cpu_i = int(ya_cpu)
                if recommended_num > ya_cpu_i:
                    cpu_action = "raise"
                elif recommended_num < ya_cpu_i:
                    cpu_action = "lower"
                else:
                    cpu_action = "ok"
        out.append({
            "suite_path": suite,
            "chunks_count": len(cores_list),
            "median_cores": round(median_c, 3),
            "p95_cores": round(p95_c, 3),
            "recommended_cpu": recommended_req,
            "recommended_cpu_base": base_recommended,
            "recommended_cpu_timeout_boost": timeout_boost_applied,
            "recommended_cpu_timeout_max_policy_applied": timeout_max_policy_applied,
            "recommended_cpu_timeout_max_policy_value": timeout_max_value,
            "recommended_cpu_medium_cap_applied": medium_cap_applied,
            "recommended_cpu_explain": recommended_cpu_explain,
            "total_cpu_sec": round(by_suite_cpu[suite], 2),
            "total_ram_gb": round(by_suite_ram_kb[suite] / (1024.0 * 1024.0), 3),
            "total_dur_sec": round(by_suite_dur[suite], 2),
            "has_synthetic": by_suite_synthetic.get(suite, False),
            "ya_cpu_cores": ya_cpu,
            "ya_ram_gb": ya_ram,
            "ya_size": ya_size,
            "chunk_errors": chunk_status["errors"],
            "chunk_timeouts": chunk_status["timeouts"],
            "chunk_muted": chunk_status["muted"],
            "chunk_muted_timeouts": chunk_status["muted_timeouts"],
            "chunk_fails_total": chunk_status["fails_total"],
            "test_errors": test_status["errors"],
            "test_timeouts": test_status["timeouts"],
            "test_muted": test_status["muted"],
            "test_muted_timeouts": test_status["muted_timeouts"],
            "test_fails_total": test_status["fails_total"],
            # Legacy aliases kept in output schema for stability.
            "errors": chunk_status["errors"],
            "timeouts": chunk_status["timeouts"],
            "muted": chunk_status["muted"],
            "muted_timeouts": chunk_status["muted_timeouts"],
            "fails_total": chunk_status["fails_total"],
            "cpu_action": cpu_action,
        })
    return out
