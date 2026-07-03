"""
Tests for build_cpu_recommendations: CPU / split / RAM recommendation logic.

These cover the decoupling of the three recommendation axes:
- CPU comes only from observed p95 cores (duration/timeouts no longer inflate it).
- Split is recommended proactively from per-chunk serial wall time (even without timeouts).
- A lower-cpu suggestion is suppressed while a suite is under split/time pressure.
- RAM is recommended from observed per-chunk peak memory.

Run: python3 test_cpu_recommendations.py
     or: pytest test_cpu_recommendations.py -v
"""

from __future__ import annotations

import sys

from dashboard_cpu_recommendations import build_cpu_recommendations

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
        max_test_duration_sec_by_suite={suite: 2000.0},  # >= LARGE 1800 -> long test present
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
    chunk_loads = [
        {"sum_duration_sec": 3600.0, "chunk_idx": 0, "chunk_group": None},
        {"sum_duration_sec": 200.0, "chunk_idx": 1, "chunk_group": None},
    ]
    recs = _by_suite(build_cpu_recommendations(
        runs,
        requirements_cache={suite: {"cpu_cores": 2, "size": "LARGE", "split_factor": 2}},
        report_status_by_suite={suite: {"tests": _status(timeouts=0), "chunks": _status()}},
        max_test_duration_sec_by_suite={suite: 3600.0},
        test_duration_stats_by_suite={suite: {"chunk_loads": chunk_loads}},
    ))
    r = recs[suite]
    assert r["split_severity"] == "at_risk", r["recommended_split_explain"]
    assert r["recommended_split_action"] == "raise"
    assert r["recommended_split"] > r["chunks_count"]
    # heaviest chunk 3600s vs target 900s -> at least 3 extra chunks
    assert r["recommended_split"] >= r["chunks_count"] + 3


def test_cpu_lower_suppressed_under_split_pressure():
    """When p95 says lower but the suite is overloaded, keep cpu action 'ok'."""
    suite = "s/pressure"
    runs = [
        _run(suite, 0, 0, 1000, cores=2.0),
        _run(suite, 1, 0, 1000, cores=2.0),
    ]
    chunk_loads = [
        {"sum_duration_sec": 3600.0, "chunk_idx": 0, "chunk_group": None},
    ]
    recs = _by_suite(build_cpu_recommendations(
        runs,
        # ya.make reserves cpu:8 but observed p95 is ~2 -> would normally be "lower"
        requirements_cache={suite: {"cpu_cores": 8, "size": "LARGE", "split_factor": 2}},
        report_status_by_suite={suite: {"tests": _status(timeouts=0), "chunks": _status()}},
        max_test_duration_sec_by_suite={suite: 3600.0},
        test_duration_stats_by_suite={suite: {"chunk_loads": chunk_loads}},
    ))
    r = recs[suite]
    assert r["recommended_cpu"] == 2
    assert r["cpu_action"] == "ok"
    assert r["cpu_lower_suppressed"] is True


def test_ram_recommendation_set_for_high_memory():
    """A high per-chunk peak RAM with no ya.make ram should recommend 'set'."""
    suite = "s/ram"
    runs = [
        _run(suite, 0, 0, 100, cores=2.0, ram_gb=30.0),
    ]
    recs = _by_suite(build_cpu_recommendations(
        runs,
        requirements_cache={suite: {"cpu_cores": 2, "size": "LARGE", "split_factor": 1}},
        report_status_by_suite={suite: {"tests": _status(), "chunks": _status()}},
        max_test_duration_sec_by_suite={suite: 50.0},
        test_duration_stats_by_suite={suite: {"chunk_loads": []}},
    ))
    r = recs[suite]
    assert r["recommended_ram_gb"] == 32, r["recommended_ram_explain"]
    assert r["ram_action"] == "set"


def test_ram_ok_when_reservation_fits():
    """Per-chunk peak below the reservation and above half of it -> 'ok'."""
    suite = "s/ram_ok"
    runs = [
        _run(suite, 0, 0, 100, cores=2.0, ram_gb=25.0),
    ]
    recs = _by_suite(build_cpu_recommendations(
        runs,
        requirements_cache={suite: {"cpu_cores": 2, "ram_gb": 32, "size": "LARGE", "split_factor": 1}},
        report_status_by_suite={suite: {"tests": _status(), "chunks": _status()}},
        max_test_duration_sec_by_suite={suite: 50.0},
        test_duration_stats_by_suite={suite: {"chunk_loads": []}},
    ))
    r = recs[suite]
    assert r["recommended_ram_gb"] == 32
    assert r["ram_action"] == "ok"


def _all_tests():
    return [
        test_cpu_is_p95_only_no_duration_boost,
        test_split_raised_proactively_without_timeout,
        test_cpu_lower_suppressed_under_split_pressure,
        test_ram_recommendation_set_for_high_memory,
        test_ram_ok_when_reservation_fits,
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
