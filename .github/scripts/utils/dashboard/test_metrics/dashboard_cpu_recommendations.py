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


def _compute_parallel_stats(runs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    if not runs:
        return {}

    events: list[tuple[float, int, str, float, float]] = []
    t0: Optional[float] = None
    suite_start_us: dict[str, float] = {}
    suite_end_us: dict[str, float] = {}
    for r in runs:
        suite = str(r.get("suite_path", ""))
        if not suite:
            continue
        start = float(r.get("start_us", 0))
        end = float(r.get("end_us", 0))
        if end <= start:
            continue
        dur_s = (end - start) / 1e6
        cpu_sec = float(r.get("cpu_sec_report", 0.0) or 0.0)
        ram_kb = float(r.get("ram_kb_report", 0.0) or 0.0)
        cores = cpu_sec / dur_s if dur_s > 0 else 0.0
        if t0 is None or start < t0:
            t0 = start
        if suite not in suite_start_us or start < suite_start_us[suite]:
            suite_start_us[suite] = start
        if suite not in suite_end_us or end > suite_end_us[suite]:
            suite_end_us[suite] = end
        events.append((start, 1, suite, +cores, +ram_kb))
        events.append((end, -1, suite, -cores, -ram_kb))

    if not events or t0 is None:
        return {}

    events.sort(key=lambda e: (e[0], -e[1]))

    suite_count: dict[str, int] = defaultdict(int)
    total_count = 0
    total_cpu = 0.0
    total_ram_kb = 0.0

    peak_self: dict[str, int] = defaultdict(int)
    peak_self_at: dict[str, float] = {}
    suite_cpu: dict[str, float] = defaultdict(float)
    suite_ram_kb: dict[str, float] = defaultdict(float)
    peak_self_cpu: dict[str, float] = defaultdict(float)
    peak_self_cpu_at: dict[str, float] = {}
    peak_self_ram_kb: dict[str, float] = defaultdict(float)
    peak_self_ram_at: dict[str, float] = {}
    peak_others: dict[str, int] = defaultdict(int)
    peak_others_at: dict[str, float] = {}
    peak_cpu: dict[str, float] = defaultdict(float)
    peak_cpu_at: dict[str, float] = {}
    peak_ram: dict[str, float] = defaultdict(float)
    peak_ram_at: dict[str, float] = {}

    for time_us, delta, suite, dcpu, dram in events:
        suite_count[suite] += delta
        suite_cpu[suite] += dcpu
        suite_ram_kb[suite] += dram
        total_count += delta
        total_cpu += dcpu
        total_ram_kb += dram

        if delta > 0 and suite_count[suite] > peak_self[suite]:
            peak_self[suite] = suite_count[suite]
            peak_self_at[suite] = time_us
        if suite_count[suite] > 0 and suite_cpu[suite] > peak_self_cpu[suite]:
            peak_self_cpu[suite] = suite_cpu[suite]
            peak_self_cpu_at[suite] = time_us
        if suite_count[suite] > 0 and suite_ram_kb[suite] > peak_self_ram_kb[suite]:
            peak_self_ram_kb[suite] = suite_ram_kb[suite]
            peak_self_ram_at[suite] = time_us

        for s, cnt in suite_count.items():
            if cnt <= 0:
                continue
            others = total_count - cnt
            if others > peak_others[s]:
                peak_others[s] = others
                peak_others_at[s] = time_us
            if total_cpu > peak_cpu[s]:
                peak_cpu[s] = total_cpu
                peak_cpu_at[s] = time_us
            if total_ram_kb > peak_ram[s]:
                peak_ram[s] = total_ram_kb
                peak_ram_at[s] = time_us

    def _at(ts_us: Optional[float]) -> Optional[float]:
        if ts_us is None:
            return None
        return round(ts_us / 1e6, 1)

    result: dict[str, dict[str, Any]] = {}
    all_suites = (
        set(peak_self)
        | set(peak_others)
        | set(peak_cpu)
        | set(peak_ram)
        | set(peak_self_cpu)
        | set(peak_self_ram_kb)
    )
    for s in all_suites:
        result[s] = {
            "suite_start_sec": _at(suite_start_us.get(s)),
            "suite_end_sec": _at(suite_end_us.get(s)),
            "max_parallel_self": peak_self[s],
            "max_parallel_self_at_sec": _at(peak_self_at.get(s)),
            "peak_others_during_suite": peak_others[s],
            "peak_others_during_suite_at_sec": _at(peak_others_at.get(s)),
            "peak_self_cpu_cores_during_suite": round(peak_self_cpu[s], 3),
            "peak_self_cpu_at_sec": _at(peak_self_cpu_at.get(s)),
            "peak_self_ram_gb_during_suite": round(peak_self_ram_kb[s] / (1024.0 * 1024.0), 3),
            "peak_self_ram_at_sec": _at(peak_self_ram_at.get(s)),
            "peak_total_cpu_cores_during_suite": round(peak_cpu[s], 3),
            "peak_total_cpu_at_sec": _at(peak_cpu_at.get(s)),
            "peak_total_ram_gb_during_suite": round(peak_ram[s] / (1024.0 * 1024.0), 3),
            "peak_total_ram_at_sec": _at(peak_ram_at.get(s)),
        }
    return result


def build_cpu_recommendations(
    runs: list[dict[str, Any]],
    requirements_cache: Optional[dict[str, dict[str, Any]]] = None,
    report_status_by_suite: Optional[dict[str, dict[str, dict[str, int]]]] = None,
    report_chunks_by_suite: Optional[dict[str, int]] = None,
    maximize_reqs_for_timeout_tests: bool = False,
) -> list[dict[str, Any]]:
    dedup_runs_by_chunk: dict[tuple[str, str], dict[str, Any]] = {}
    dedup_runs_fallback: list[dict[str, Any]] = []
    for r in runs:
        suite = str(r.get("suite_path", ""))
        if not suite:
            continue
        report_hid = r.get("report_hid")
        if report_hid is not None:
            key = (suite, "hid:" + str(report_hid))
        else:
            chunk = r.get("chunk")
            if chunk is None:
                dedup_runs_fallback.append(r)
                continue
            try:
                chunk_i = int(chunk)
            except (TypeError, ValueError):
                dedup_runs_fallback.append(r)
                continue
            chunk_group = str(r.get("chunk_group", "") or "")
            key = (suite, "idx:" + chunk_group + ":" + str(chunk_i))
        prev = dedup_runs_by_chunk.get(key)
        if prev is None:
            dedup_runs_by_chunk[key] = r
            continue
        prev_dur = float(prev.get("dur_us", 0) or 0)
        cur_dur = float(r.get("dur_us", 0) or 0)
        if cur_dur > prev_dur:
            dedup_runs_by_chunk[key] = r
    runs_for_recommendations = list(dedup_runs_by_chunk.values()) + dedup_runs_fallback
    parallel_stats = _compute_parallel_stats(runs_for_recommendations)

    by_suite: dict[str, list[float]] = defaultdict(list)
    by_suite_runs: dict[str, int] = defaultdict(int)
    by_suite_cpu: dict[str, float] = defaultdict(float)
    by_suite_ram_kb: dict[str, float] = defaultdict(float)
    by_suite_dur: dict[str, float] = defaultdict(float)
    by_suite_synthetic: dict[str, bool] = defaultdict(bool)
    by_suite_errors: dict[str, int] = defaultdict(int)
    by_suite_timeouts: dict[str, int] = defaultdict(int)
    by_suite_muted: dict[str, int] = defaultdict(int)
    by_suite_muted_timeouts: dict[str, int] = defaultdict(int)
    for r in runs_for_recommendations:
        suite = str(r.get("suite_path", ""))
        if not suite:
            continue
        cpu = float(r.get("cpu_sec_report", 0.0) or 0.0)
        ram_kb = float(r.get("ram_kb_report", 0.0) or 0.0)
        dur_s = float(r.get("dur_us", 0) or 0) / 1_000_000.0
        by_suite_runs[suite] += 1
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
    all_suites = sorted(set(by_suite_cpu.keys()))
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
        ya_split_factor = req.get("split_factor")
        if not ya_size:
            ya_size = "SMALL"
        size_u_cap = str(ya_size or "").upper()
        small_cap_applied = size_u_cap == "SMALL" and recommended > 1
        medium_cap_applied = size_u_cap == "MEDIUM" and recommended > 4
        if small_cap_applied:
            recommended = 1
        elif medium_cap_applied:
            recommended = 4
        timeout_max_policy_applied = bool(maximize_reqs_for_timeout_tests and timeout_tests_count > 0)
        size_u = size_u_cap
        timeout_max_value: Any = None
        if timeout_max_policy_applied:
            if size_u == "SMALL":
                timeout_max_value = 1
            elif size_u == "MEDIUM":
                timeout_max_value = 4
            elif size_u == "LARGE":
                timeout_max_value = 4
            else:
                timeout_max_value = 4
        recommended_req: Any = timeout_max_value if timeout_max_policy_applied else recommended
        explain_parts = [
            f"p95_cores={p95_c:.3f}",
            f"base_tier={base_recommended}",
        ]
        if timeout_boost_applied:
            explain_parts.append(f"test_timeouts={timeout_tests_count} -> next_tier={_next_cpu_tier(base_recommended)}")
        if timeout_max_policy_applied:
            explain_parts.append(f"maximize_reqs_for_timeout_tests(size={size_u or 'UNKNOWN'}) -> {timeout_max_value}")
        if small_cap_applied:
            explain_parts.append("SMALL cap -> 1")
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
                cpu_action = "ok" if recommended_num <= 1 else "set"
            else:
                ya_cpu_i = int(ya_cpu)
                if recommended_num > ya_cpu_i:
                    cpu_action = "raise"
                elif recommended_num < ya_cpu_i:
                    cpu_action = "lower"
                else:
                    cpu_action = "ok"
        chunks_real = by_suite_runs[suite]
        chunks_report = (report_chunks_by_suite or {}).get(suite)
        # Mismatch: real runtime chunks vs ya.make SPLIT_FACTOR(N) when set
        chunks_mismatch = ya_split_factor is not None and chunks_real != ya_split_factor
        out.append({
            "suite_path": suite,
            "chunks_count": chunks_real,
            "chunks_count_report": chunks_report,
            "chunks_count_mismatch": chunks_mismatch,
            "ya_split_factor": ya_split_factor,
            "median_cores": round(median_c, 3),
            "p95_cores": round(p95_c, 3),
            "recommended_cpu": recommended_req,
            "recommended_cpu_base": base_recommended,
            "recommended_cpu_timeout_boost": timeout_boost_applied,
            "recommended_cpu_timeout_max_policy_applied": timeout_max_policy_applied,
            "recommended_cpu_timeout_max_policy_value": timeout_max_value,
            "recommended_cpu_small_cap_applied": small_cap_applied,
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
            "errors": chunk_status["errors"],
            "timeouts": chunk_status["timeouts"],
            "muted": chunk_status["muted"],
            "muted_timeouts": chunk_status["muted_timeouts"],
            "fails_total": chunk_status["fails_total"],
            "cpu_action": cpu_action,
            **{k: parallel_stats.get(suite, {}).get(k, v) for k, v in {
                "suite_start_sec": None,
                "suite_end_sec": None,
                "max_parallel_self": 0,
                "max_parallel_self_at_sec": None,
                "peak_others_during_suite": 0,
                "peak_others_during_suite_at_sec": None,
                "peak_self_cpu_cores_during_suite": 0.0,
                "peak_self_cpu_at_sec": None,
                "peak_self_ram_gb_during_suite": 0.0,
                "peak_self_ram_at_sec": None,
                "peak_total_cpu_cores_during_suite": 0.0,
                "peak_total_cpu_at_sec": None,
                "peak_total_ram_gb_during_suite": 0.0,
                "peak_total_ram_at_sec": None,
            }.items()},
        })
    return out

