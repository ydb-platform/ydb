from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Optional


def _status_bucket() -> dict[str, int]:
    return {"total": 0, "passed": 0, "errors": 0, "timeouts": 0, "muted": 0, "muted_timeouts": 0, "fails_total": 0, "skipped": 0}


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


# When SPLIT_FACTOR is not set in ya.make, runner may pick 1..10. No need to suggest "set" if recommended is in that range.
DEFAULT_SPLIT_FACTOR_MAX = 10


def _size_duration_threshold_sec(size_u: str) -> int:
    if size_u == "SMALL":
        return 60
    if size_u == "MEDIUM":
        return 600
    if size_u == "LARGE":
        return 1800
    # Conservative default for unknown size.
    return 600


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
    max_test_duration_sec_by_suite: Optional[dict[str, float]] = None,
    test_duration_stats_by_suite: Optional[dict[str, dict[str, Any]]] = None,
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
    # Marker bounds: use all runs, but skip dependency "sole chunk" rows when suite has
    # indexed [i/N] chunks (recipe/deps at ya-make start must not shift suite start marker).
    suites_with_indexed = {
        str(r.get("suite_path", ""))
        for r in runs
        if r.get("chunk_group")
    }
    suite_start_us_all: dict[str, float] = {}
    suite_end_us_all: dict[str, float] = {}
    for r in runs:
        suite = str(r.get("suite_path", ""))
        if not suite:
            continue
        if (
            suite in suites_with_indexed
            and not r.get("chunk_group")
            and int(r.get("chunk", -1) or -1) == 0
        ):
            continue
        start = float(r.get("start_us", 0) or 0)
        end = float(r.get("end_us", 0) or 0)
        if end <= start:
            continue
        if suite not in suite_start_us_all or start < suite_start_us_all[suite]:
            suite_start_us_all[suite] = start
        if suite not in suite_end_us_all or end > suite_end_us_all[suite]:
            suite_end_us_all[suite] = end

    by_suite: dict[str, list[float]] = defaultdict(list)
    by_suite_runs_all: dict[str, int] = defaultdict(int)
    by_suite_runs_non_sole: dict[str, int] = defaultdict(int)
    by_suite_cpu: dict[str, float] = defaultdict(float)
    by_suite_ram_kb: dict[str, float] = defaultdict(float)
    by_suite_dur: dict[str, float] = defaultdict(float)
    by_suite_dur_report: dict[str, float] = defaultdict(float)
    by_suite_dur_evlog: dict[str, float] = defaultdict(float)
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
        dur_report = float(r.get("duration_report_sec", 0) or 0)
        dur_evlog = float(r.get("dur_us", 0) or 0) / 1_000_000.0
        dur_s = max(dur_report, dur_evlog)
        by_suite_runs_all[suite] += 1
        raw_name = str(r.get("raw_name", "") or "").strip().lower()
        is_sole_chunk = raw_name == "sole chunk"
        if not is_sole_chunk:
            by_suite_runs_non_sole[suite] += 1
        by_suite_cpu[suite] += cpu
        by_suite_ram_kb[suite] += ram_kb
        by_suite_dur[suite] += dur_s
        by_suite_dur_report[suite] += dur_report
        by_suite_dur_evlog[suite] += dur_evlog
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

        base_recommended = _round_cpu_tier(p95_c)
        req = (requirements_cache or {}).get(suite, {})
        ya_cpu = req.get("cpu_cores")
        ya_ram = req.get("ram_gb")
        ya_size = req.get("size")
        # When FORK_TEST_FILES(): effective = TEST_SRCS file count × SPLIT_FACTOR(N); use for comparison and display.
        ya_effective_split = req.get("effective_split_factor")
        ya_split_factor_raw = req.get("split_factor")
        ya_split_factor = int(ya_effective_split) if ya_effective_split is not None else ya_split_factor_raw
        ya_split_factor_tooltip = req.get("split_factor_tooltip")
        if not ya_size:
            ya_size = "SMALL"
        size_u_cap = str(ya_size or "").upper()
        long_test_threshold_sec = _size_duration_threshold_sec(size_u_cap)
        max_test_duration_sec = float((max_test_duration_sec_by_suite or {}).get(suite, 0.0) or 0.0)
        long_test_boost_applied = max_test_duration_sec >= float(long_test_threshold_sec)
        recommended = _next_cpu_tier(base_recommended) if long_test_boost_applied else base_recommended
        # When suite has any timeout(s): recommend at least 2x actual consumption (tier), capped by size limit.
        timeout_tests_count = int(test_status.get("timeouts", 0) or 0)
        timeout_2x_boost_applied = False
        if timeout_tests_count > 0 and p95_c > 0:
            recommended_2x_tier = _round_cpu_tier(2.0 * p95_c)
            if recommended_2x_tier > recommended:
                recommended = recommended_2x_tier
                timeout_2x_boost_applied = True
        small_cap_applied = size_u_cap == "SMALL" and recommended > 1
        medium_cap_applied = size_u_cap == "MEDIUM" and recommended > 4
        if small_cap_applied:
            recommended = 1
        elif medium_cap_applied:
            recommended = 4
        timeout_max_policy_applied = bool(maximize_reqs_for_timeout_tests and long_test_boost_applied)
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
        if long_test_boost_applied:
            explain_parts.append(
                f"max_test_duration_sec={max_test_duration_sec:.3f} >= threshold_sec={long_test_threshold_sec} -> next_tier={_next_cpu_tier(base_recommended)}"
            )
        if timeout_2x_boost_applied:
            explain_parts.append(
                f"timeouts={timeout_tests_count} -> 2x_p95_tier={_round_cpu_tier(2.0 * p95_c)}"
            )
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
        chunks_real = by_suite_runs_non_sole[suite] if by_suite_runs_non_sole[suite] > 0 else by_suite_runs_all[suite]
        chunks_report = (report_chunks_by_suite or {}).get(suite)
        timeout_sec = _size_duration_threshold_sec(size_u_cap)
        timeout_budget_sec = float(timeout_sec) * 0.98
        dur_stats = (test_duration_stats_by_suite or {}).get(suite, {})
        p96_test_duration_sec = float(dur_stats.get("p96_duration_sec", 0.0) or 0.0)
        total_test_duration_sec = float(dur_stats.get("total_duration_sec", 0.0) or 0.0)
        chunk_loads = list(dur_stats.get("chunk_loads", []) or [])
        overloaded_chunks = 0
        overloaded_total_duration_sec = 0.0
        overloaded_chunk_examples: list[str] = []
        if timeout_tests_count > 0 and timeout_budget_sec > 0:
            overloaded_items: list[tuple[float, str]] = []
            for cl in chunk_loads:
                load_sec = float(cl.get("sum_duration_sec", 0.0) or 0.0)
                if load_sec > timeout_budget_sec:
                    overloaded_chunks += 1
                    overloaded_total_duration_sec += load_sec
                    chunk_idx = int(cl.get("chunk_idx", 0) or 0)
                    chunk_group = cl.get("chunk_group")
                    chunk_name = f"{chunk_group}/chunk{chunk_idx}" if chunk_group else f"chunk{chunk_idx}"
                    overloaded_items.append((load_sec, f"{chunk_name}={load_sec:.1f}s"))
            overloaded_items.sort(key=lambda x: x[0], reverse=True)
            overloaded_chunk_examples = [x[1] for x in overloaded_items[:3]]
        target_chunk_load_sec = float(timeout_sec) * 0.5
        needed_chunks_for_overloaded = (
            int(math.ceil(overloaded_total_duration_sec / target_chunk_load_sec))
            if target_chunk_load_sec > 0 and overloaded_chunks > 0
            else 0
        )
        extra_chunks = max(0, needed_chunks_for_overloaded - overloaded_chunks)
        split_should_raise = extra_chunks > 0
        recommended_split = int(chunks_real) + int(extra_chunks) if split_should_raise else int(chunks_real)
        if ya_split_factor is None:
            # With FORK_TEST_FILES: default range is test_srcs_count × 1..10; if real chunks in that range → ok.
            # Without: default range 1..10.
            test_srcs_count = req.get("test_srcs_count")
            if test_srcs_count is not None and test_srcs_count > 0:
                default_max = test_srcs_count * DEFAULT_SPLIT_FACTOR_MAX
                if test_srcs_count <= chunks_real <= default_max:
                    recommended_split_action = "ok"
                elif int(recommended_split) > default_max:
                    recommended_split_action = "raise"
                else:
                    recommended_split_action = "ok"
            elif int(recommended_split) <= DEFAULT_SPLIT_FACTOR_MAX:
                recommended_split_action = "ok"
            else:
                recommended_split_action = "raise"
        else:
            try:
                ya_split_i = int(ya_split_factor)
            except (TypeError, ValueError):
                ya_split_i = None
            if ya_split_i is None:
                recommended_split_action = "set"
            elif int(recommended_split) > ya_split_i:
                recommended_split_action = "raise"
            elif int(recommended_split) < ya_split_i:
                recommended_split_action = "lower"
            else:
                recommended_split_action = "ok"
        heavy_test_threshold_sec = timeout_budget_sec * 0.97
        heavy_candidates = list(dur_stats.get("heavy_test_candidates", []) or [])
        heavy_tests_all = []
        if timeout_budget_sec > 0:
            for t in heavy_candidates:
                dur = float(t.get("duration_sec", 0.0) or 0.0)
                if dur < heavy_test_threshold_sec:
                    continue
                heavy_tests_all.append(
                    {
                        "name": str(t.get("name", "") or ""),
                        "duration_sec": round(dur, 3),
                        "chunk_idx": t.get("chunk_idx"),
                        "chunk_group": t.get("chunk_group"),
                    }
                )
        heavy_tests_count = len(heavy_tests_all)
        heavy_examples = []
        for t in heavy_tests_all[:3]:
            chunk_idx = t.get("chunk_idx")
            suffix = f" [chunk{chunk_idx}]" if chunk_idx is not None else ""
            heavy_examples.append(f"{str(t.get('name', '') or '')}{suffix}: {float(t.get('duration_sec', 0.0) or 0.0):.1f}s")
        recommended_split_explain = (
            f"timeouts={timeout_tests_count}; "
            f"chunk_timeout_budget_sec={timeout_budget_sec:.1f}; "
            f"overloaded_chunks(>budget)={overloaded_chunks}; "
            f"overloaded_total_duration_sec={overloaded_total_duration_sec:.1f}; "
            f"target_chunk_load_sec={target_chunk_load_sec:.1f}; "
            f"needed_chunks_for_overloaded={needed_chunks_for_overloaded}; "
            f"extra_chunks={extra_chunks}; "
            f"split={chunks_real}->{recommended_split}; "
            f"examples={', '.join(overloaded_chunk_examples) if overloaded_chunk_examples else 'none'}"
        )
        # Mismatch: real runtime chunks vs ya.make SPLIT_FACTOR(N) when set
        chunks_mismatch = ya_split_factor is not None and chunks_real != ya_split_factor
        test_srcs_count = req.get("test_srcs_count")
        tests_in_report = int(test_status.get("total", 0) or 0)
        avg_tests_per_chunk = (
            round(tests_in_report / chunks_real, 1) if tests_in_report > 0 and chunks_real > 0 else None
        )
        if ya_split_factor is not None and not ya_split_factor_tooltip:
            ya_split_factor_tooltip = f"Effective split from ya.make: {ya_split_factor}."
        chunks_count_tooltip_parts = [
            f"Runtime: {chunks_real} chunk process(es) actually started.",
        ]
        if ya_split_factor is not None:
            chunks_count_tooltip_parts.append(f"Expected from ya.make: {ya_split_factor}.")
            if chunks_mismatch:
                chunks_count_tooltip_parts.append("Mismatch with ya.make — check FORK_TEST_FILES / SPLIT_FACTOR.")
            else:
                chunks_count_tooltip_parts.append("Matches ya.make effective split.")
        if tests_in_report > 0:
            per_chunk_note = f" ~{avg_tests_per_chunk} tests/chunk on average." if avg_tests_per_chunk else ""
            chunks_count_tooltip_parts.append(f"{tests_in_report} tests in report.{per_chunk_note}")
        chunks_count_tooltip = " ".join(chunks_count_tooltip_parts)
        budget_min = timeout_budget_sec / 60.0
        target_min = target_chunk_load_sec / 60.0
        if extra_chunks > 0:
            recommended_split_tooltip = (
                f"Raise split {chunks_real} → {recommended_split} (+{extra_chunks} chunks). "
                f"{timeout_tests_count} test timeout(s); "
                f"{overloaded_chunks} chunk(s) exceed SIZE({size_u}) serial-time budget (~{budget_min:.0f} min per chunk). "
                f"Target ~{target_min:.0f} min serial test time per chunk."
            )
            if test_srcs_count and int(test_srcs_count) > 0:
                needed_sf = int(math.ceil(int(recommended_split) / int(test_srcs_count)))
                recommended_split_tooltip += (
                    f" With FORK_TEST_FILES ({test_srcs_count} files): set SPLIT_FACTOR({needed_sf}) "
                    f"→ {needed_sf * int(test_srcs_count)} effective chunks."
                )
            else:
                recommended_split_tooltip += f" Set SPLIT_FACTOR({recommended_split}) in ya.make."
            if overloaded_chunk_examples:
                recommended_split_tooltip += f" Examples: {', '.join(overloaded_chunk_examples[:2])}."
        elif recommended_split_action == "lower":
            recommended_split_tooltip = (
                f"Can lower split from {ya_split_factor} to {recommended_split}: "
                f"runtime uses fewer chunks than ya.make specifies."
            )
        elif recommended_split_action == "set":
            recommended_split_tooltip = (
                f"Set effective split to {recommended_split} in ya.make "
                f"(currently {ya_split_factor or 'unset'})."
            )
        else:
            recommended_split_tooltip = (
                f"Current {chunks_real} chunks OK: no overloaded chunks requiring more split "
                f"(or no timeouts in this run)."
            )
        split_action_tooltips = {
            "raise": (
                f"recommended_split ({recommended_split}) > ya_split_factor ({ya_split_factor}): "
                f"increase SPLIT_FACTOR in ya.make."
            ),
            "lower": (
                f"recommended_split ({recommended_split}) < ya_split_factor ({ya_split_factor}): "
                f"ya.make over-splits; can reduce SPLIT_FACTOR."
            ),
            "set": f"ya.make has no SPLIT_FACTOR; suggest setting effective split to {recommended_split}.",
            "ok": "recommended_split matches current setup; no SPLIT_FACTOR change needed.",
        }
        split_action_tooltip = split_action_tooltips.get(recommended_split_action, "")

        cpu_tip_parts = [
            f"Observed CPU load: median {median_c:.1f} cores, p95 {p95_c:.1f} cores.",
            f"Base tier from p95: cpu:{base_recommended}.",
        ]
        if long_test_boost_applied:
            cpu_tip_parts.append(
                f"Raised one tier: longest test {max_test_duration_sec / 60:.1f} min "
                f"≥ SIZE({size_u}) threshold {long_test_threshold_sec / 60:.0f} min."
            )
        if timeout_2x_boost_applied:
            cpu_tip_parts.append(
                f"Raised to ≥2× p95 (cpu:{_round_cpu_tier(2.0 * p95_c)}) "
                f"because {timeout_tests_count} test timeout(s)."
            )
        if timeout_max_policy_applied:
            cpu_tip_parts.append(f"Policy cap for long tests on SIZE({size_u}): cpu:{timeout_max_value}.")
        if small_cap_applied:
            cpu_tip_parts.append("Capped at cpu:1 by SIZE(SMALL) limit.")
        elif medium_cap_applied:
            cpu_tip_parts.append("Capped at cpu:4 by SIZE(MEDIUM) limit.")
        if ya_cpu is not None:
            cpu_tip_parts.append(f"Recommend REQUIREMENTS(cpu:{recommended_req}); ya.make has cpu:{ya_cpu}.")
        else:
            cpu_tip_parts.append(f"Recommend REQUIREMENTS(cpu:{recommended_req}); ya.make has no cpu requirement.")
        recommended_cpu_tooltip = " ".join(cpu_tip_parts)

        cpu_action_tooltips = {
            "raise": (
                f"Increase REQUIREMENTS(cpu) in ya.make from {ya_cpu} to {recommended_req}: "
                f"tests need more CPU than configured."
            ),
            "lower": (
                f"Decrease REQUIREMENTS(cpu) in ya.make from {ya_cpu} to {recommended_req}: "
                f"tests use less CPU; lowering frees runner capacity."
            ),
            "set": (
                f"Add REQUIREMENTS(cpu:{recommended_req}) to ya.make "
                f"(no cpu requirement set today)."
            ),
            "ok": (
                f"Current REQUIREMENTS(cpu:{ya_cpu}) matches recommendation "
                f"(or recommendation is within limits)."
                if ya_cpu is not None
                else f"Recommended cpu:{recommended_req}; no change needed."
            ),
        }
        cpu_action_tooltip = cpu_action_tooltips.get(cpu_action, "")

        ya_ram_gb_tooltip = (
            f"REQUIREMENTS(ram:{ya_ram}) from ya.make."
            if ya_ram is not None
            else "No REQUIREMENTS(ram) in ya.make."
        )
        ya_cpu_cores_tooltip = (
            f"REQUIREMENTS(cpu:{ya_cpu}) from ya.make."
            if ya_cpu is not None
            else "No REQUIREMENTS(cpu) in ya.make."
        )
        size_timeout_min = timeout_sec / 60.0
        ya_size_tooltip = (
            f"SIZE({ya_size}) in ya.make: per-test timeout ≈{size_timeout_min:.0f} min, "
            f"chunk serial-time budget ≈{timeout_budget_sec / 60:.0f} min."
            if ya_size
            else "No SIZE in ya.make (defaults used)."
        )
        if small_cap_applied or medium_cap_applied:
            ya_size_tooltip += " Long tests hit SIZE CPU cap — consider larger SIZE."

        median_cores_tooltip = (
            f"Median estimated cores per chunk run: {median_c:.2f}. "
            f"Computed as cpu_sec_report / duration per run."
        )
        p95_cores_tooltip = (
            f"95th percentile of estimated cores per chunk run: {p95_c:.2f}. "
            f"Used as base for recommended_cpu."
        )
        total_cpu_sec_tooltip = (
            f"Sum of CPU-seconds across deduplicated chunk runs: {by_suite_cpu[suite]:.0f} s."
        )
        total_ram_gb_tooltip = (
            f"Sum of peak RAM (GB) across deduplicated chunk runs: "
            f"{by_suite_ram_kb[suite] / (1024 ** 2):.1f} GB."
        )
        total_dur_sec_tooltip = (
            f"Sum of max(report, evlog) wall time per chunk: {by_suite_dur[suite]:.0f} s "
            f"({by_suite_dur[suite] / 3600:.2f} h)."
        )
        total_dur_report_sec_tooltip = (
            f"Sum of report wall time per chunk: {by_suite_dur_report[suite]:.0f} s."
        )
        total_dur_evlog_sec_tooltip = (
            f"Sum of evlog wall time per chunk: {by_suite_dur_evlog[suite]:.0f} s."
        )

        ps = parallel_stats.get(suite, {})
        max_par = int(ps.get("max_parallel_self", 0) or 0)
        peak_others = int(ps.get("peak_others_during_suite", 0) or 0)
        peak_cpu = float(ps.get("peak_self_cpu_cores_during_suite", 0) or 0)
        peak_ram = float(ps.get("peak_self_ram_gb_during_suite", 0) or 0)
        suite_start_sec = ps.get("suite_start_sec")
        suite_end_sec = ps.get("suite_end_sec")
        suite_wall_h = (
            (float(suite_end_sec) - float(suite_start_sec)) / 3600.0
            if suite_start_sec is not None and suite_end_sec is not None
            else None
        )
        suite_path_tooltip = (
            f"Test suite path. Wall time ≈{suite_wall_h:.2f} h."
            if suite_wall_h is not None
            else "Test suite path."
        )
        max_parallel_self_tooltip = (
            f"Peak parallel chunk processes of this suite: {max_par} "
            f"(of {chunks_real} total chunks). Low value vs chunks → runner contention or sequential phases."
        )
        peak_others_tooltip = (
            f"Peak parallel chunks of OTHER suites while this suite was active: {peak_others}. "
            f"High value → this suite competes for runner threads."
        )
        peak_cpu_tooltip = (
            f"Peak sum of estimated CPU cores for this suite at one moment: {peak_cpu:.1f} cores."
        )
        peak_ram_tooltip = (
            f"Peak sum of RAM for this suite at one moment: {peak_ram:.1f} GB."
        )
        test_status_tooltip = (
            f"Test results: {test_status.get('total', 0)} total, "
            f"{test_status.get('passed', 0)} passed, "
            f"{test_status.get('timeouts', 0)} timeouts, "
            f"{test_status.get('errors', 0)} errors, "
            f"{test_status.get('muted', 0)} muted, "
            f"{test_status.get('skipped', 0)} skipped."
        )
        chunk_status_tooltip = (
            f"Chunk results: {chunk_status.get('total', 0)} total, "
            f"{chunk_status.get('timeouts', 0)} timeouts, "
            f"{chunk_status.get('errors', 0)} errors, "
            f"{chunk_status.get('muted', 0)} muted."
        )

        out.append({
            "suite_path": suite,
            "chunks_count": chunks_real,
            "chunks_count_report": chunks_report,
            "chunks_count_mismatch": chunks_mismatch,
            "recommended_split": recommended_split,
            "recommended_split_action": recommended_split_action,
            "recommended_split_explain": recommended_split_explain,
            "recommended_split_tooltip": recommended_split_tooltip,
            "chunks_count_tooltip": chunks_count_tooltip,
            "split_action_tooltip": split_action_tooltip,
            "ya_split_factor_raw": ya_split_factor_raw,
            "heavy_tests_count": heavy_tests_count,
            "heavy_test_threshold_sec": round(heavy_test_threshold_sec, 3),
            "heavy_tests_examples": heavy_examples[:3],
            "heavy_tests_all": heavy_tests_all,
            "ya_split_factor": ya_split_factor,
            "ya_split_factor_tooltip": ya_split_factor_tooltip,
            "median_cores": round(median_c, 3),
            "p95_cores": round(p95_c, 3),
            "recommended_cpu": recommended_req,
            "recommended_cpu_base": base_recommended,
            "recommended_cpu_timeout_boost": long_test_boost_applied,
            "recommended_cpu_timeout_2x_boost": timeout_2x_boost_applied,
            "recommended_cpu_timeout_max_policy_applied": timeout_max_policy_applied,
            "recommended_cpu_timeout_max_policy_value": timeout_max_value,
            "recommended_cpu_small_cap_applied": small_cap_applied,
            "recommended_cpu_medium_cap_applied": medium_cap_applied,
            "long_test_threshold_sec": long_test_threshold_sec,
            "max_test_duration_sec": round(max_test_duration_sec, 3),
            "recommended_cpu_duration_boost": long_test_boost_applied,
            "timeout_budget_sec": round(timeout_budget_sec, 3),
            "test_p96_duration_sec": round(p96_test_duration_sec, 3),
            "test_total_duration_sec": round(total_test_duration_sec, 3),
            "recommended_cpu_explain": recommended_cpu_explain,
            "recommended_cpu_tooltip": recommended_cpu_tooltip,
            "cpu_action_tooltip": cpu_action_tooltip,
            "ya_ram_gb_tooltip": ya_ram_gb_tooltip,
            "ya_cpu_cores_tooltip": ya_cpu_cores_tooltip,
            "ya_size_tooltip": ya_size_tooltip,
            "median_cores_tooltip": median_cores_tooltip,
            "p95_cores_tooltip": p95_cores_tooltip,
            "total_cpu_sec_tooltip": total_cpu_sec_tooltip,
            "total_ram_gb_tooltip": total_ram_gb_tooltip,
            "total_dur_sec_tooltip": total_dur_sec_tooltip,
            "total_dur_report_sec_tooltip": total_dur_report_sec_tooltip,
            "total_dur_evlog_sec_tooltip": total_dur_evlog_sec_tooltip,
            "suite_path_tooltip": suite_path_tooltip,
            "max_parallel_self_tooltip": max_parallel_self_tooltip,
            "peak_others_tooltip": peak_others_tooltip,
            "peak_self_cpu_tooltip": peak_cpu_tooltip,
            "peak_self_ram_tooltip": peak_ram_tooltip,
            "test_status_tooltip": test_status_tooltip,
            "chunk_status_tooltip": chunk_status_tooltip,
            "total_cpu_sec": round(by_suite_cpu[suite], 2),
            "total_ram_gb": round(by_suite_ram_kb[suite] / (1024.0 * 1024.0), 3),
            "total_dur_sec": round(by_suite_dur[suite], 2),
            "total_dur_report_sec": round(by_suite_dur_report[suite], 2),
            "total_dur_evlog_sec": round(by_suite_dur_evlog[suite], 2),
            "has_synthetic": by_suite_synthetic.get(suite, False),
            "ya_cpu_cores": ya_cpu,
            "ya_ram_gb": ya_ram,
            "ya_size": ya_size,
            "chunk_total": chunk_status.get("total", 0),
            "chunk_passed": chunk_status.get("passed", 0),
            "chunk_errors": chunk_status["errors"],
            "chunk_timeouts": chunk_status["timeouts"],
            "chunk_muted": chunk_status["muted"],
            "chunk_muted_timeouts": chunk_status["muted_timeouts"],
            "chunk_fails_total": chunk_status["fails_total"],
            "test_total": test_status.get("total", 0),
            "test_passed": test_status.get("passed", 0),
            "test_errors": test_status["errors"],
            "test_timeouts": test_status["timeouts"],
            "test_muted": test_status["muted"],
            "test_muted_timeouts": test_status["muted_timeouts"],
            "test_skipped": test_status["skipped"],
            "test_fails_total": test_status["fails_total"],
            "errors": chunk_status["errors"],
            "timeouts": chunk_status["timeouts"],
            "muted": chunk_status["muted"],
            "muted_timeouts": chunk_status["muted_timeouts"],
            "fails_total": chunk_status["fails_total"],
            "cpu_action": cpu_action,
            "suite_start_sec": round(suite_start_us_all[suite] / 1e6, 1) if suite in suite_start_us_all else parallel_stats.get(suite, {}).get("suite_start_sec"),
            "suite_end_sec": round(suite_end_us_all[suite] / 1e6, 1) if suite in suite_end_us_all else parallel_stats.get(suite, {}).get("suite_end_sec"),
            **{k: parallel_stats.get(suite, {}).get(k, v) for k, v in {
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

