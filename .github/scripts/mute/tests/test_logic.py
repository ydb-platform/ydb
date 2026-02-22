"""Tests for mute.logic."""
import datetime

import pytest

from mute.logic import (
    _matches_error_filter,
    aggregate_test_data,
    create_debug_string,
    create_test_string,
    get_quarantine_graduation,
    is_chunk_test,
    is_delete_candidate,
    is_mute_candidate,
    is_unmute_candidate,
)


def test_matches_error_filter_empty():
    assert _matches_error_filter(None, None) is True
    assert _matches_error_filter('TIMEOUT', None) is True


def test_matches_error_filter_include_timeout():
    assert _matches_error_filter('TIMEOUT', ['timeout']) is True
    assert _matches_error_filter('REGULAR', ['timeout']) is False


def test_matches_error_filter_exclude():
    assert _matches_error_filter('TIMEOUT', ['!timeout']) is False


def test_is_mute_candidate_high_failures():
    agg = [{"full_name": "s/t", "pass_count": 5, "fail_count": 4, "is_muted": False, "period_days": 4}]
    test = {"full_name": "s/t"}
    assert is_mute_candidate(test, agg, {"min_failures_high": 3, "min_runs_threshold": 10}) is True


def test_is_mute_candidate_already_muted():
    agg = [{"full_name": "s/t", "pass_count": 5, "fail_count": 4, "is_muted": True}]
    test = {"full_name": "s/t"}
    assert is_mute_candidate(test, agg) is False


def test_is_unmute_candidate_stable():
    agg = [{"full_name": "s/t", "pass_count": 5, "fail_count": 0, "mute_count": 0, "is_muted": True}]
    test = {"full_name": "s/t"}
    assert is_unmute_candidate(test, agg, {"min_runs": 4, "max_fails": 0}) is True


def test_is_delete_candidate_no_runs():
    agg = [{"full_name": "s/t", "pass_count": 0, "fail_count": 0, "mute_count": 0, "skip_count": 0, "is_muted": True}]
    test = {"full_name": "s/t"}
    assert is_delete_candidate(test, agg) is True


def test_quarantine_graduation_passes():
    quarantine = {"suite1 test1", "suite2 test2"}
    agg = [{"full_name": "suite1/test1", "pass_count": 3, "fail_count": 1, "mute_count": 0}]
    result = get_quarantine_graduation(quarantine, agg, {"min_runs": 4, "min_passes": 1})
    assert "suite1 test1" in result


def test_is_chunk_test():
    assert is_chunk_test({"is_test_chunk": 1}) is True
    assert is_chunk_test({"test_name": "chunk[1/5]"}) is True


def test_create_test_string():
    assert create_test_string({"suite_folder": "s", "test_name": "t"}) == "s t"
    assert create_test_string({"suite_folder": "s", "test_name": "chunk[1/5]"}, use_wildcards=True) == "s chunk[*/*]"


def test_aggregate_test_data():
    today = datetime.date.today()
    start = today - datetime.timedelta(days=2)
    all_data = [
        {"full_name": "s/t", "test_name": "t", "suite_folder": "s", "build_type": "bt", "branch": "main",
         "pass_count": 1, "fail_count": 0, "mute_count": 0, "skip_count": 0, "date_window": start,
         "state": "Passed", "owner": "o", "is_muted": 0, "is_test_chunk": 0, "days_in_state": 1},
        {"full_name": "s/t", "test_name": "t", "suite_folder": "s", "build_type": "bt", "branch": "main",
         "pass_count": 0, "fail_count": 1, "mute_count": 0, "skip_count": 0,
         "date_window": today, "state": "Flaky", "owner": "o", "is_muted": 0, "is_test_chunk": 0, "days_in_state": 1},
    ]
    agg = aggregate_test_data(all_data, 3)
    assert len(agg) == 1
    assert agg[0]["full_name"] == "s/t"
    assert agg[0]["pass_count"] == 1
    assert agg[0]["fail_count"] == 1
