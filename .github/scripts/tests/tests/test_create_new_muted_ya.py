"""Tests for create_new_muted_ya logic: is_mute_candidate, is_unmute_candidate, is_delete_candidate, get_quarantine_graduation, _matches_error_filter."""
import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from mute_logic import (
    is_mute_candidate,
    is_unmute_candidate,
    is_delete_candidate,
    get_quarantine_graduation,
    _matches_error_filter,
    is_chunk_test,
    create_test_string,
    create_debug_string,
    aggregate_test_data,
)


# ============ _matches_error_filter ============

def test_matches_error_filter_empty():
    assert _matches_error_filter(None, None) is True
    assert _matches_error_filter('TIMEOUT', None) is True
    assert _matches_error_filter('', []) is True


def test_matches_error_filter_include_timeout():
    assert _matches_error_filter('TIMEOUT', ['timeout']) is True
    assert _matches_error_filter('TIMEOUT', ['TIMEOUT']) is True
    assert _matches_error_filter('SOME_TIMEOUT_ERROR', ['timeout']) is True
    assert _matches_error_filter('REGULAR', ['timeout']) is False


def test_matches_error_filter_exclude():
    assert _matches_error_filter('TIMEOUT', ['!timeout']) is False
    assert _matches_error_filter('REGULAR', ['!timeout']) is True


def test_matches_error_filter_mixed():
    assert _matches_error_filter('TIMEOUT', ['timeout', '!other']) is True


def test_matches_error_filter_multiple_includes():
    """Any include matches -> pass."""
    assert _matches_error_filter('REGULAR', ['timeout', 'REGULAR']) is True
    assert _matches_error_filter('TIMEOUT', ['timeout', 'REGULAR']) is True
    assert _matches_error_filter('UNKNOWN', ['timeout', 'REGULAR']) is False


def test_matches_error_filter_empty_err():
    """Empty error_type with include filter -> no match."""
    assert _matches_error_filter('', ['timeout']) is False
    assert _matches_error_filter(None, ['timeout']) is False


# ============ is_mute_candidate ============

def test_is_mute_candidate_high_failures():
    agg = [{"full_name": "s/t", "pass_count": 5, "fail_count": 4, "is_muted": False, "period_days": 4}]
    test = {"full_name": "s/t"}
    assert is_mute_candidate(test, agg, {"min_failures_high": 3, "min_runs_threshold": 10}) is True


def test_is_mute_candidate_low_failures_few_runs():
    agg = [{"full_name": "s/t", "pass_count": 2, "fail_count": 2, "is_muted": False, "period_days": 4}]
    test = {"full_name": "s/t"}
    assert is_mute_candidate(test, agg, {"min_failures_low": 2, "min_runs_threshold": 10}) is True


def test_is_mute_candidate_already_muted():
    agg = [{"full_name": "s/t", "pass_count": 5, "fail_count": 4, "is_muted": True}]
    test = {"full_name": "s/t"}
    assert is_mute_candidate(test, agg) is False


def test_is_mute_candidate_not_enough_failures():
    agg = [{"full_name": "s/t", "pass_count": 5, "fail_count": 1, "is_muted": False}]
    test = {"full_name": "s/t"}
    assert is_mute_candidate(test, agg) is False


def test_is_mute_candidate_error_filter():
    agg = [{"full_name": "s/t", "pass_count": 5, "fail_count": 4, "is_muted": False, "error_type": "REGULAR"}]
    test = {"full_name": "s/t"}
    # Only timeout - should filter out REGULAR
    assert is_mute_candidate(test, agg, {"min_failures_high": 3, "min_runs_threshold": 10}) is True  # no filter
    assert is_mute_candidate(test, agg, {"min_failures_high": 3, "error_filter": ["timeout"]}) is False  # REGULAR != timeout


def test_is_mute_candidate_boundary_total_runs_eq_threshold():
    """total_runs == min_runs_threshold: high path needs > threshold, so no mute."""
    agg = [{"full_name": "s/t", "pass_count": 6, "fail_count": 4, "is_muted": False}]
    test = {"full_name": "s/t"}
    # total=10, fails=4. high path: fails>=3 and total>10 -> False. low path: fails>=2 and total<=10 -> True
    assert is_mute_candidate(test, agg, {"min_failures_high": 3, "min_failures_low": 2, "min_runs_threshold": 10}) is True


def test_is_mute_candidate_boundary_total_runs_gt_threshold_no_mute():
    """total_runs=11, fails=3: high path applies, should mute."""
    agg = [{"full_name": "s/t", "pass_count": 8, "fail_count": 3, "is_muted": False}]
    test = {"full_name": "s/t"}
    assert is_mute_candidate(test, agg, {"min_failures_high": 3, "min_runs_threshold": 10}) is True


def test_is_mute_candidate_test_not_in_aggregated():
    """Test not in aggregated_data -> False."""
    agg = [{"full_name": "other/t", "pass_count": 5, "fail_count": 4, "is_muted": False}]
    test = {"full_name": "s/t"}
    assert is_mute_candidate(test, agg) is False


# ============ is_unmute_candidate ============

def test_is_unmute_candidate_stable():
    agg = [{"full_name": "s/t", "pass_count": 5, "fail_count": 0, "mute_count": 0, "is_muted": True}]
    test = {"full_name": "s/t"}
    assert is_unmute_candidate(test, agg, {"min_runs": 4, "max_fails": 0}) is True


def test_is_unmute_candidate_has_failures():
    agg = [{"full_name": "s/t", "pass_count": 3, "fail_count": 1, "mute_count": 0, "is_muted": True}]
    test = {"full_name": "s/t"}
    assert is_unmute_candidate(test, agg, {"min_runs": 4, "max_fails": 0}) is False


def test_is_unmute_candidate_few_runs():
    agg = [{"full_name": "s/t", "pass_count": 2, "fail_count": 0, "mute_count": 0, "is_muted": True}]
    test = {"full_name": "s/t"}
    assert is_unmute_candidate(test, agg, {"min_runs": 4, "max_fails": 0}) is False


def test_is_unmute_candidate_not_muted():
    agg = [{"full_name": "s/t", "pass_count": 5, "fail_count": 0, "mute_count": 0, "is_muted": False}]
    test = {"full_name": "s/t"}
    assert is_unmute_candidate(test, agg, {"min_runs": 4, "max_fails": 0}) is True  # still qualifies


def test_is_unmute_candidate_mute_count_counts_as_fail():
    """mute_count is part of total_fails: 4 runs with 1 mute -> should NOT unmute (max_fails=0)."""
    agg = [{"full_name": "s/t", "pass_count": 4, "fail_count": 0, "mute_count": 1, "is_muted": True}]
    test = {"full_name": "s/t"}
    assert is_unmute_candidate(test, agg, {"min_runs": 4, "max_fails": 0}) is False


def test_is_unmute_candidate_boundary_exactly_4_runs():
    """Exactly 4 runs, 0 fails -> should unmute."""
    agg = [{"full_name": "s/t", "pass_count": 4, "fail_count": 0, "mute_count": 0, "is_muted": True}]
    test = {"full_name": "s/t"}
    assert is_unmute_candidate(test, agg, {"min_runs": 4, "max_fails": 0}) is True


def test_is_unmute_candidate_test_not_in_aggregated():
    agg = [{"full_name": "other/t", "pass_count": 5, "fail_count": 0, "mute_count": 0}]
    test = {"full_name": "s/t"}
    assert is_unmute_candidate(test, agg) is False


# ============ is_delete_candidate ============

def test_is_delete_candidate_no_runs():
    agg = [{"full_name": "s/t", "pass_count": 0, "fail_count": 0, "mute_count": 0, "skip_count": 0, "is_muted": True}]
    test = {"full_name": "s/t"}
    assert is_delete_candidate(test, agg) is True


def test_is_delete_candidate_has_runs():
    agg = [{"full_name": "s/t", "pass_count": 1, "fail_count": 0, "mute_count": 0, "skip_count": 0, "is_muted": True}]
    test = {"full_name": "s/t"}
    assert is_delete_candidate(test, agg) is False


def test_is_delete_candidate_skip_only():
    """Only skip_count (no pass/fail/mute) -> total_runs=1, not 0, should NOT delete."""
    agg = [{"full_name": "s/t", "pass_count": 0, "fail_count": 0, "mute_count": 0, "skip_count": 1, "is_muted": True}]
    test = {"full_name": "s/t"}
    assert is_delete_candidate(test, agg) is False


def test_is_delete_candidate_test_not_in_aggregated():
    agg = [{"full_name": "other/t", "pass_count": 0, "fail_count": 0, "mute_count": 0, "skip_count": 0}]
    test = {"full_name": "s/t"}
    assert is_delete_candidate(test, agg) is False


# ============ get_quarantine_graduation ============

def test_quarantine_graduation_passes():
    quarantine = {"suite1 test1", "suite2 test2"}
    agg = [
        {"full_name": "suite1/test1", "pass_count": 2, "fail_count": 0, "mute_count": 0},
    ]
    result = get_quarantine_graduation(quarantine, agg, {"min_runs": 4, "min_passes": 1})
    # suite1/test1: 2 runs, 2 pass -> total 2, not 4. So no graduation
    assert result == set()

    agg2 = [
        {"full_name": "suite1/test1", "pass_count": 3, "fail_count": 1, "mute_count": 0},
    ]
    result2 = get_quarantine_graduation(quarantine, agg2, {"min_runs": 4, "min_passes": 1})
    assert "suite1 test1" in result2


def test_quarantine_graduation_no_pass():
    quarantine = {"suite1 test1"}
    agg = [{"full_name": "suite1/test1", "pass_count": 0, "fail_count": 4, "mute_count": 0}]
    result = get_quarantine_graduation(quarantine, agg, {"min_runs": 4, "min_passes": 1})
    assert result == set()


def test_quarantine_graduation_not_in_agg():
    quarantine = {"suite1 test1"}
    agg = []
    result = get_quarantine_graduation(quarantine, agg, {"min_runs": 4, "min_passes": 1})
    assert result == set()


def test_quarantine_graduation_malformed_line():
    """Malformed quarantine line (no space) -> skipped."""
    quarantine = {"suite1", "suite2 test2"}  # "suite1" has no test_name
    agg = [{"full_name": "suite2/test2", "pass_count": 5, "fail_count": 0, "mute_count": 0}]
    result = get_quarantine_graduation(quarantine, agg, {"min_runs": 4, "min_passes": 1})
    assert "suite1" not in result
    assert "suite2 test2" in result


def test_quarantine_graduation_boundary_exactly_4_runs_1_pass():
    """Exactly 4 runs, 1 pass -> should graduate."""
    quarantine = {"suite1 test1"}
    agg = [{"full_name": "suite1/test1", "pass_count": 1, "fail_count": 3, "mute_count": 0}]
    result = get_quarantine_graduation(quarantine, agg, {"min_runs": 4, "min_passes": 1})
    assert "suite1 test1" in result


# ============ is_chunk_test ============

def test_is_chunk_test():
    assert is_chunk_test({"is_test_chunk": 1}) is True
    assert is_chunk_test({"is_test_chunk": 0}) is False
    assert is_chunk_test({"test_name": "chunk[1/5]"}) is True
    assert is_chunk_test({"test_name": "normal"}) is False


# ============ create_test_string ============

def test_create_test_string():
    assert create_test_string({"suite_folder": "s", "test_name": "t"}) == "s t"
    assert create_test_string({"suite_folder": "s", "test_name": "chunk[1/5]"}, use_wildcards=True) == "s chunk[*/*]"


# ============ aggregate_test_data ============

def test_aggregate_test_data():
    base = datetime.date(1970, 1, 1)
    today = datetime.date.today()
    start = today - datetime.timedelta(days=2)
    days_start = (start - base).days
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
    assert agg[0]["success_rate"] == 50.0
