"""Tests for mute.behavior."""
import datetime

from mute.behavior import find_behavior_start


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


def test_empty_runs_returns_none():
    match = {"full_name": "s/t1"}
    d, c, p = find_behavior_start(match, "duration_increased", [], {})
    assert d is None
    assert c is None
    assert p is None
