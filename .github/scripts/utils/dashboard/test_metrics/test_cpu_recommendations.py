"""
Tests for build_cpu_recommendations: CPU / split / RAM recommendation logic.

These cover the decoupling of the three recommendation axes:
- CPU comes only from observed p95 cores (duration/timeouts no longer inflate it).
- Split is recommended proactively from per-chunk serial wall time (even without timeouts).
- A lower-cpu suggestion is suppressed while a suite is under split/time pressure.
- Memory pressure raises the CPU slot (REQUIREMENTS(ram) is ignored locally), never ram.

Run: python3 test_cpu_recommendations.py
     or: pytest test_cpu_recommendations.py -v
"""

from __future__ import annotations

import sys

from dashboard_cpu_recommendations import _marker_suite_start_us, build_cpu_recommendations

try:
    import pytest
except ImportError:
    pytest = None


GB_KB = 1024 * 1024  # 1 GB expressed in KB


def _run(suite, chunk, start_sec, dur_sec, cores, ram_gb=0.0, status="OK"):
    """Build one chunk run dict with cpu_sec derived from target cores."""
    return {
        "suite_path": suite,
        "chunk": chunk,
        "chunk_group": None,
        "raw_name": f"chunk{chunk}",
        "start_us": int(start_sec * 1_000_000),
        "end_us": int((start_sec + dur_sec) * 1_000_000),
        "dur_us": int(dur_sec * 1_000_000),
        "duration_report_sec": dur_sec,
        "cpu_sec_report": cores * dur_sec,
        "ram_kb_report": ram_gb * GB_KB,
        "status": status,
    }


def _status(total=1, passed=1, timeouts=0, errors=0):
    return {
        "total": total,
        "passed": passed,
        "errors": errors,
        "timeouts": timeouts,
        "muted": 0,
        "muted_timeouts": 0,
        "fails_total": errors + timeouts,
        "skipped": 0,
    }


def _by_suite(rec_list):
    return {r["suite_path"]: r for r in rec_list}


def test_cpu_is_p95_only_no_duration_boost():
    """A long test (>= LARGE threshold) must NOT bump the CPU tier above p95."""
    suite = "s/cpu"
    runs = [
        _run(suite, 0, 0, 100, cores=2.0),
        _run(suite, 1, 0, 100, cores=2.0),
        _run(suite, 2, 0, 100, cores=2.0),
    ]
    recs = _by_suite(build_cpu_recommendations(
        runs,
        requirements_cache={suite: {"cpu_cores": 2, "size": "LARGE", "split_factor": 3}},
        report_status_by_suite={suite: {"tests": _status(), "chunks": _status()}},
        max_test_duration_sec_by_suite={suite: 4000.0},  # >= LARGE 3600 -> long test present
        test_duration_stats_by_suite={suite: {"chunk_loads": []}},
    ))
    r = recs[suite]
    assert r["recommended_cpu"] == 2, r["recommended_cpu_explain"]
    assert r["recommended_cpu_base"] == 2
    # flag is kept as a "long test present" signal but must not change the tier
    assert r["recommended_cpu_timeout_boost"] is True
    assert r["cpu_action"] == "ok"


def test_split_raised_proactively_without_timeout():
    """A chunk over the SIZE budget must raise split even with zero timeouts (at_risk)."""
    suite = "s/split"
    runs = [
        _run(suite, 0, 0, 1000, cores=2.0),
        _run(suite, 1, 0, 1000, cores=2.0),
    ]
    # Overpacked chunk: many small tests summing to 7200s (>budget 3528), heaviest single
    # test only 500s -> split CAN help.
    chunk_loads = [
        {"sum_duration_sec": 7200.0, "max_test_duration_sec": 500.0, "chunk_idx": 0, "chunk_group": None},
        {"sum_duration_sec": 200.0, "max_test_duration_sec": 200.0, "chunk_idx": 1, "chunk_group": None},
    ]
    recs = _by_suite(build_cpu_recommendations(
        runs,
        requirements_cache={suite: {"cpu_cores": 2, "size": "LARGE", "split_factor": 2}},
        report_status_by_suite={suite: {"tests": _status(timeouts=0), "chunks": _status()}},
        max_test_duration_sec_by_suite={suite: 500.0},
        test_duration_stats_by_suite={suite: {"chunk_loads": chunk_loads}},
    ))
    r = recs[suite]
    assert r["split_severity"] == "at_risk", r["recommended_split_explain"]
    assert r["recommended_split_action"] == "raise"
    assert r["recommended_split"] > r["chunks_count"]
    # heaviest chunk 7200s vs target 1800s -> at least 3 extra chunks
    assert r["recommended_split"] >= r["chunks_count"] + 3
    assert r["single_test_blocks_split"] is False


def test_single_test_blocks_split():
    """A single test at/above the budget must flag single_test_blocks_split (split useless)."""
    suite = "s/singletest"
    runs = [
        _run(suite, 0, 0, 1000, cores=2.0),
        _run(suite, 1, 0, 1000, cores=2.0),
    ]
    # One chunk is a single 3601s test (> budget 3528) -> cannot be split further.
    chunk_loads = [
        {"sum_duration_sec": 3601.0, "max_test_duration_sec": 3601.0, "chunk_idx": 0, "chunk_group": None},
        {"sum_duration_sec": 300.0, "max_test_duration_sec": 300.0, "chunk_idx": 1, "chunk_group": None},
    ]
    recs = _by_suite(build_cpu_recommendations(
        runs,
        requirements_cache={suite: {"cpu_cores": 2, "size": "LARGE", "split_factor": 2}},
        report_status_by_suite={suite: {"tests": _status(timeouts=2), "chunks": _status()}},
        max_test_duration_sec_by_suite={suite: 3601.0},
        test_duration_stats_by_suite={suite: {"chunk_loads": chunk_loads}},
    ))
    r = recs[suite]
    assert r["single_test_blocks_split"] is True, r["recommended_split_explain"]
    assert "SPLIT_FACTOR cannot help" in r["recommended_split_tooltip"]


def test_single_test_contention_suspected_on_saturated_runner():
    """A near-budget single test on a CPU-saturated runner suggests raising cpu (decontention)."""
    suite = "s/contention"
    runs = [
        _run(suite, 0, 0, 1000, cores=2.0),
        _run(suite, 1, 0, 1000, cores=2.0),
    ]
    chunk_loads = [
        {"sum_duration_sec": 3601.0, "max_test_duration_sec": 3601.0, "chunk_idx": 0, "chunk_group": None},
        {"sum_duration_sec": 300.0, "max_test_duration_sec": 300.0, "chunk_idx": 1, "chunk_group": None},
    ]
    # Runner pinned near 95% during the suite window ([0,1000] evlog sec).
    cpu_series = [(100.0, 95.0), (400.0, 96.0), (700.0, 94.0), (900.0, 97.0)]
    recs = _by_suite(build_cpu_recommendations(
        runs,
        requirements_cache={suite: {"cpu_cores": 2, "size": "LARGE", "split_factor": 2}},
        report_status_by_suite={suite: {"tests": _status(timeouts=2), "chunks": _status()}},
        max_test_duration_sec_by_suite={suite: 3601.0},
        test_duration_stats_by_suite={suite: {"chunk_loads": chunk_loads}},
        runner_cpu_series=cpu_series,
    ))
    r = recs[suite]
    assert r["single_test_blocks_split"] is True
    assert r["single_test_contention_suspected"] is True, r["recommended_split_explain"]
    assert r["cpu_for_contention"] == 4  # 2 -> 4 (halve parallelism)
    assert r["runner_cpu_pct_median_during_suite"] >= 90
    assert "TRY raising REQUIREMENTS(cpu)" in r["recommended_split_tooltip"]


def test_single_test_not_contention_when_way_over_budget():
    """A single test far over budget is genuinely oversized: no contention suggestion."""
    suite = "s/oversized"
    runs = [
        _run(suite, 0, 0, 1000, cores=2.0),
        _run(suite, 1, 0, 1000, cores=2.0),
    ]
    chunk_loads = [
        {"sum_duration_sec": 7200.0, "max_test_duration_sec": 7200.0, "chunk_idx": 0, "chunk_group": None},
        {"sum_duration_sec": 300.0, "max_test_duration_sec": 300.0, "chunk_idx": 1, "chunk_group": None},
    ]
    cpu_series = [(100.0, 95.0), (400.0, 96.0), (700.0, 94.0), (900.0, 97.0)]
    recs = _by_suite(build_cpu_recommendations(
        runs,
        requirements_cache={suite: {"cpu_cores": 2, "size": "LARGE", "split_factor": 2}},
        report_status_by_suite={suite: {"tests": _status(timeouts=2), "chunks": _status()}},
        max_test_duration_sec_by_suite={suite: 7200.0},
        test_duration_stats_by_suite={suite: {"chunk_loads": chunk_loads}},
        runner_cpu_series=cpu_series,
    ))
    r = recs[suite]
    assert r["single_test_blocks_split"] is True
    assert r["single_test_contention_suspected"] is False, r["recommended_split_explain"]
    assert "Fix the test" in r["recommended_split_tooltip"]


def test_cpu_lower_suppressed_under_split_pressure():
    """When p95 says lower but the suite is overloaded, keep cpu action 'ok'."""
    suite = "s/pressure"
    runs = [
        _run(suite, 0, 0, 1000, cores=2.0),
        _run(suite, 1, 0, 1000, cores=2.0),
    ]
    chunk_loads = [
        {"sum_duration_sec": 7200.0, "max_test_duration_sec": 500.0, "chunk_idx": 0, "chunk_group": None},
    ]
    recs = _by_suite(build_cpu_recommendations(
        runs,
        # ya.make reserves cpu:8 but observed p95 is ~2 -> would normally be "lower"
        requirements_cache={suite: {"cpu_cores": 8, "size": "LARGE", "split_factor": 2}},
        report_status_by_suite={suite: {"tests": _status(timeouts=0), "chunks": _status()}},
        max_test_duration_sec_by_suite={suite: 500.0},
        test_duration_stats_by_suite={suite: {"chunk_loads": chunk_loads}},
    ))
    r = recs[suite]
    assert r["recommended_cpu"] == 2
    assert r["cpu_action"] == "ok"
    assert r["cpu_lower_suppressed"] is True


def test_memory_drives_cpu_bump_not_ram():
    """A memory-heavy chunk must raise the CPU slot (to throttle parallelism), because
    REQUIREMENTS(ram) is ignored by the local scheduler. It must NOT recommend ram."""
    suite = "s/mem_heavy"
    # Heaviest chunk ~58 GB. Budget 200 GB -> at most floor(200/58)=3 parallel chunks
    # -> cpu ceil(96/3)=32. p95 cores are tiny, so memory drives the recommendation.
    runs = [
        _run(suite, 0, 0, 100, cores=2.0, ram_gb=58.0),
        _run(suite, 1, 0, 100, cores=2.0, ram_gb=40.0),
    ]
    recs = _by_suite(build_cpu_recommendations(
        runs,
        requirements_cache={suite: {"cpu_cores": 16, "ram_gb": 32, "size": "LARGE", "split_factor": 2}},
        report_status_by_suite={suite: {"tests": _status(), "chunks": _status()}},
        max_test_duration_sec_by_suite={suite: 50.0},
        test_duration_stats_by_suite={suite: {"chunk_loads": []}},
    ))
    r = recs[suite]
    assert round(r["max_chunk_ram_gb"]) == 58, r["mem_explain"]
    assert r["cpu_for_memory"] == 32, r["mem_explain"]
    assert r["mem_driven_cpu"] is True
    assert r["recommended_cpu"] == 32, r["recommended_cpu_explain"]
    assert r["cpu_action"] == "raise"
    # No ram recommendation fields should exist anymore.
    assert "recommended_ram_gb" not in r
    assert "ram_action" not in r


def test_light_suite_no_memory_cpu_bump():
    """A suite whose heaviest chunk is small (< threshold) gets no memory-driven bump."""
    suite = "s/mem_light"
    runs = [
        _run(suite, 0, 0, 100, cores=2.0, ram_gb=12.0),
        _run(suite, 1, 0, 100, cores=2.0, ram_gb=11.0),
    ]
    recs = _by_suite(build_cpu_recommendations(
        runs,
        requirements_cache={suite: {"cpu_cores": 4, "size": "LARGE", "split_factor": 2}},
        report_status_by_suite={suite: {"tests": _status(), "chunks": _status()}},
        max_test_duration_sec_by_suite={suite: 50.0},
        test_duration_stats_by_suite={suite: {"chunk_loads": []}},
    ))
    r = recs[suite]
    assert r["cpu_for_memory"] is None, r["mem_explain"]
    assert r["mem_driven_cpu"] is False
    # CPU stays p95-based (2), memory does not inflate it.
    assert r["recommended_cpu"] == 2


def test_marker_suite_start_ignores_straggler_prefix():
    """Early isolated chunks must not shift the suite start marker before the main wave."""
    t0 = 1_000_000_000_000_000  # us
    gap_us = int(66 * 60 * 1_000_000)
    starts = [
        t0,
        t0 + 4_000_000,
        t0 + 8_000_000,
        t0 + gap_us,
        t0 + gap_us + 60_000_000,
    ]
    assert _marker_suite_start_us(starts) == t0 + gap_us

    # Continuous run: keep earliest start.
    continuous = [t0 + i * 60_000_000 for i in range(20)]
    assert _marker_suite_start_us(continuous) == continuous[0]


def test_build_cpu_recommendations_straggler_suite_start():
    suite = "ydb/core/blobstorage/ut_blobstorage"
    t0 = 1000.0
    gap = 66 * 60
    runs = [
        _run(suite, 46, t0, 4.0, 1.0),
        _run(suite, 69, t0, 8.0, 1.0),
        _run(suite, 266, t0 + 4, 1.0, 1.0),
        _run(suite, 3, t0 + gap, 6.0, 1.0),
        _run(suite, 4, t0 + gap + 60, 5.0, 1.0),
    ]
    recs = build_cpu_recommendations(
        runs,
        requirements_cache={suite: {"cpu_cores": 4, "ram_gb": 32, "size": "MEDIUM", "split_factor": 300}},
        report_status_by_suite={suite: {"tests": _status(), "chunks": _status()}},
    )
    by_suite = {r["suite_path"]: r for r in recs}
    assert by_suite[suite]["suite_start_sec"] == round(t0 + gap, 1)


def _all_tests():
    return [
        test_marker_suite_start_ignores_straggler_prefix,
        test_build_cpu_recommendations_straggler_suite_start,
        test_cpu_is_p95_only_no_duration_boost,
        test_split_raised_proactively_without_timeout,
        test_single_test_blocks_split,
        test_single_test_contention_suspected_on_saturated_runner,
        test_single_test_not_contention_when_way_over_budget,
        test_cpu_lower_suppressed_under_split_pressure,
        test_memory_drives_cpu_bump_not_ram,
        test_light_suite_no_memory_cpu_bump,
    ]


if __name__ == "__main__":
    failures = 0
    for t in _all_tests():
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError as e:
            failures += 1
            print(f"FAIL {t.__name__}: {e}")
    sys.exit(1 if failures else 0)
