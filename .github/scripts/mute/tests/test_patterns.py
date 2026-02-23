"""Tests for mute.patterns and mute.regression."""
import datetime

from mute.patterns import (
    _parse_date,
    pattern_duration_increased,
    pattern_floating_across_days,
    pattern_muted_test_different_error,
    pattern_retry_recovered,
)
from mute.regression import EXCLUDE_MANUAL_RUNS_SQL, REGRESSION_JOB_NAMES, regression_job_names_sql


def test_regression_job_names():
    assert len(REGRESSION_JOB_NAMES) == 8
    assert "Regression-run" in REGRESSION_JOB_NAMES


def test_exclude_manual_runs_sql():
    assert "_manual" in EXCLUDE_MANUAL_RUNS_SQL


def test_parse_date():
    d = datetime.date(2025, 2, 20)
    assert _parse_date(d) == d
    assert _parse_date("2025-02-20") == d


def test_floating_across_days_detects():
    runs = [
        {"suite_folder": "s", "full_name": "s/t1", "status": "failure", "error_type": "TIMEOUT"},
        {"suite_folder": "s", "full_name": "s/t2", "status": "failure", "error_type": "TIMEOUT"},
        {"suite_folder": "s", "full_name": "s/t3", "status": "failure", "error_type": "TIMEOUT"},
    ]
    matches = pattern_floating_across_days(runs, {"min_total_fails": 3, "max_per_test": 2})
    assert len(matches) >= 1
    assert matches[0]["pattern"] == "floating_across_days"


def test_retry_recovered_detects():
    runs = [
        {"job_id": 1, "full_name": "s/t", "run_timestamp": datetime.datetime(2025, 2, 20, 10, 0), "status": "failure"},
        {"job_id": 1, "full_name": "s/t", "run_timestamp": datetime.datetime(2025, 2, 20, 10, 5), "status": "passed"},
    ]
    matches = pattern_retry_recovered(runs, {})
    assert len(matches) == 1
    assert matches[0]["pattern"] == "retry_recovered"


def test_muted_test_different_error():
    muted = {("suite1", "test1")}
    runs = [
        {"full_name": "suite1/test1", "suite_folder": "suite1", "test_name": "test1", "status": "failure", "error_type": "TIMEOUT"},
    ]
    matches = pattern_muted_test_different_error(runs, muted, {})
    assert len(matches) == 1


