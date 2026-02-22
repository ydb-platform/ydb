"""Tests for evaluate_pr_check_rules patterns."""
import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from regression_jobs import REGRESSION_JOB_NAMES, regression_job_names_sql
from pr_check_patterns import (
    pattern_floating_across_days,
    pattern_retry_recovered,
    pattern_muted_test_different_error,
    pattern_duration_increased,
    _parse_date,
)


def test_regression_job_names_constant():
    assert len(REGRESSION_JOB_NAMES) == 8
    assert "Regression-run" in REGRESSION_JOB_NAMES
    assert "Nightly-run" in REGRESSION_JOB_NAMES


def test_regression_job_names_sql():
    sql = regression_job_names_sql()
    assert "'Regression-run'" in sql
    assert "'Nightly-run'" in sql


def test_parse_date():
    d = datetime.date(2025, 2, 20)
    assert _parse_date(d) == d
    assert _parse_date("2025-02-20") == d
    assert _parse_date(datetime.datetime(2025, 2, 20, 12, 0)) == d


def test_floating_across_days_detects():
    runs = [
        {"run_timestamp": datetime.date(2025, 2, 20), "suite_folder": "s", "full_name": "s/t1", "status": "failure", "error_type": "TIMEOUT"},
        {"run_timestamp": datetime.date(2025, 2, 20), "suite_folder": "s", "full_name": "s/t2", "status": "failure", "error_type": "TIMEOUT"},
        {"run_timestamp": datetime.date(2025, 2, 21), "suite_folder": "s", "full_name": "s/t3", "status": "failure", "error_type": "TIMEOUT"},
    ]
    matches = pattern_floating_across_days(runs, {"min_total_fails": 3, "max_per_test": 2})
    assert len(matches) >= 1
    m = matches[0]
    assert m["pattern"] == "floating_across_days"
    assert m["total_timeouts"] == 3
    assert m["suite_folder"] == "s"


def test_floating_across_days_max_per_test_exceeded():
    runs = [
        {"run_timestamp": datetime.date(2025, 2, 20), "suite_folder": "s", "full_name": "s/t1", "status": "failure", "error_type": "TIMEOUT"},
        {"run_timestamp": datetime.date(2025, 2, 20), "suite_folder": "s", "full_name": "s/t1", "status": "failure", "error_type": "TIMEOUT"},
        {"run_timestamp": datetime.date(2025, 2, 20), "suite_folder": "s", "full_name": "s/t1", "status": "failure", "error_type": "TIMEOUT"},
    ]
    matches = pattern_floating_across_days(runs, {"min_total_fails": 3, "max_per_test": 2})
    assert len(matches) == 0  # t1 has 3, exceeds max_per_test 2


def test_floating_across_days_skips_non_timeout():
    runs = [
        {"run_timestamp": datetime.date(2025, 2, 20), "suite_folder": "s", "full_name": "s/t1", "status": "failure", "error_type": "REGULAR"},
        {"run_timestamp": datetime.date(2025, 2, 20), "suite_folder": "s", "full_name": "s/t2", "status": "failure", "error_type": "REGULAR"},
    ]
    matches = pattern_floating_across_days(runs, {"min_total_fails": 3, "max_per_test": 2})
    assert len(matches) == 0


def test_retry_recovered_detects():
    runs = [
        {"job_id": 1, "full_name": "s/t", "run_timestamp": datetime.datetime(2025, 2, 20, 10, 0), "status": "failure"},
        {"job_id": 1, "full_name": "s/t", "run_timestamp": datetime.datetime(2025, 2, 20, 10, 5), "status": "passed"},
    ]
    matches = pattern_retry_recovered(runs, {})
    assert len(matches) == 1
    assert matches[0]["pattern"] == "retry_recovered"
    assert matches[0]["full_name"] == "s/t"


def test_retry_recovered_no_recovery():
    runs = [
        {"job_id": 1, "full_name": "s/t", "run_timestamp": datetime.datetime(2025, 2, 20, 10, 0), "status": "failure"},
        {"job_id": 1, "full_name": "s/t", "run_timestamp": datetime.datetime(2025, 2, 20, 10, 5), "status": "failure"},
    ]
    matches = pattern_retry_recovered(runs, {})
    assert len(matches) == 0


def test_muted_test_different_error():
    muted = {("suite1", "test1")}
    runs = [
        {"full_name": "suite1/test1", "suite_folder": "suite1", "test_name": "test1", "status": "failure", "error_type": "TIMEOUT"},
    ]
    matches = pattern_muted_test_different_error(runs, muted, {})
    assert len(matches) == 1
    assert matches[0]["pattern"] == "muted_test_different_error"
    assert matches[0]["error_type"] == "TIMEOUT"


def test_muted_test_different_error_skips_unmuted():
    muted = set()
    runs = [
        {"full_name": "s/t", "suite_folder": "s", "test_name": "t", "status": "failure", "error_type": "TIMEOUT"},
    ]
    matches = pattern_muted_test_different_error(runs, muted, {})
    assert len(matches) == 0


def test_duration_increased_detects():
    """Duration grew 2x: baseline median 10s, recent median 20s."""
    today = datetime.date(2025, 2, 22)
    runs = [
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=5), "duration": 10.0},
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=4), "duration": 10.0},
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=3), "duration": 10.0},
        {"full_name": "s/t1", "run_timestamp": today, "duration": 20.0},
        {"full_name": "s/t1", "run_timestamp": today, "duration": 20.0},
    ]
    params = {"window_days": 7, "baseline_days": 6, "recent_days": 1, "min_baseline_runs": 3, "min_recent_runs": 2, "growth_factor": 1.5}
    matches = pattern_duration_increased(runs, params)
    assert len(matches) == 1
    assert matches[0]["pattern"] == "duration_increased"
    assert matches[0]["full_name"] == "s/t1"
    assert matches[0]["growth_ratio"] >= 1.5


def test_duration_increased_no_growth():
    """Duration stable: no alert."""
    today = datetime.date(2025, 2, 22)
    runs = [
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=5), "duration": 10.0},
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=4), "duration": 10.0},
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=3), "duration": 10.0},
        {"full_name": "s/t1", "run_timestamp": today, "duration": 10.0},
        {"full_name": "s/t1", "run_timestamp": today, "duration": 11.0},
    ]
    params = {"window_days": 7, "baseline_days": 6, "recent_days": 1, "min_baseline_runs": 3, "min_recent_runs": 2, "growth_factor": 1.5}
    matches = pattern_duration_increased(runs, params)
    assert len(matches) == 0


def test_duration_increased_insufficient_baseline():
    """Not enough baseline runs: no alert."""
    today = datetime.date(2025, 2, 22)
    runs = [
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=5), "duration": 10.0},
        {"full_name": "s/t1", "run_timestamp": today, "duration": 100.0},
        {"full_name": "s/t1", "run_timestamp": today, "duration": 100.0},
    ]
    params = {"window_days": 7, "baseline_days": 6, "recent_days": 1, "min_baseline_runs": 3, "min_recent_runs": 2, "growth_factor": 1.5}
    matches = pattern_duration_increased(runs, params)
    assert len(matches) == 0


def test_duration_increased_skips_zero_duration():
    """Zero/None durations are excluded from median."""
    today = datetime.date(2025, 2, 22)
    runs = [
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=5), "duration": 10.0},
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=4), "duration": 10.0},
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=3), "duration": 10.0},
        {"full_name": "s/t1", "run_timestamp": today, "duration": 0},
        {"full_name": "s/t1", "run_timestamp": today, "duration": 20.0},
    ]
    params = {"window_days": 7, "baseline_days": 6, "recent_days": 1, "min_baseline_runs": 3, "min_recent_runs": 2, "growth_factor": 1.5}
    matches = pattern_duration_increased(runs, params)
    assert len(matches) == 0  # only 1 recent run with duration > 0
