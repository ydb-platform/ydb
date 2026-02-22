"""Tests for behavior_start module."""
import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from behavior_start import find_behavior_start


def test_duration_increased_finds_first():
    today = datetime.date(2025, 2, 22)
    runs = [
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=5), "duration": 10.0, "commit": "abc", "pull": "123"},
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=3), "duration": 20.0, "commit": "def", "pull": "124"},
        {"full_name": "s/t1", "run_timestamp": today, "duration": 25.0, "commit": "ghi", "pull": "125"},
    ]
    match = {"full_name": "s/t1", "baseline_median": 10.0, "growth_ratio": 2.0}
    params = {"growth_factor": 1.5}
    d, c, p = find_behavior_start(match, "duration_increased", runs, params)
    assert d == today - datetime.timedelta(days=3)
    assert c == "def"
    assert p == "124"


def test_muted_test_different_error_finds_first():
    today = datetime.date(2025, 2, 22)
    runs = [
        {"full_name": "s/t1", "run_timestamp": today - datetime.timedelta(days=2), "status": "failure", "error_type": "TIMEOUT", "commit": "a", "pull": "1"},
        {"full_name": "s/t1", "run_timestamp": today, "status": "failure", "error_type": "TIMEOUT", "commit": "b", "pull": "2"},
    ]
    match = {"full_name": "s/t1", "error_type": "TIMEOUT"}
    params = {}
    d, c, p = find_behavior_start(match, "muted_test_different_error", runs, params)
    assert d == today - datetime.timedelta(days=2)
    assert c == "a"


def test_floating_across_days_finds_first():
    today = datetime.date(2025, 2, 22)
    runs = [
        {"suite_folder": "ydb/suite", "run_timestamp": today - datetime.timedelta(days=3), "status": "failure", "error_type": "TIMEOUT", "commit": "x", "pull": "10"},
        {"suite_folder": "ydb/suite", "run_timestamp": today, "status": "failure", "error_type": "TIMEOUT", "commit": "y", "pull": "11"},
    ]
    match = {"suite_folder": "ydb/suite"}
    params = {}
    d, c, p = find_behavior_start(match, "floating_across_days", runs, params)
    assert d == today - datetime.timedelta(days=3)
    assert c == "x"


def test_empty_runs_returns_none():
    match = {"full_name": "s/t1"}
    d, c, p = find_behavior_start(match, "duration_increased", [], {})
    assert d is None
    assert c is None
    assert p is None
