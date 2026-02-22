"""Rule mix tests: quarantine excludes from mute."""
from mute.apply import create_file_set
from mute.logic import is_mute_candidate


def test_quarantine_excludes_from_mute_candidates():
    """Test in quarantine + would be mute candidate -> exclude_check excludes it from to_mute."""
    aggregated = [
        {"full_name": "s/t1", "suite_folder": "s", "test_name": "t1", "pass_count": 5, "fail_count": 4, "is_muted": False},
        {"full_name": "s/t2", "suite_folder": "s", "test_name": "t2", "pass_count": 5, "fail_count": 4, "is_muted": False},
    ]
    params = {"min_failures_high": 3, "min_runs_threshold": 10}

    def is_mute(test):
        return is_mute_candidate(test, aggregated, params)

    to_mute_all, _ = create_file_set(
        aggregated, is_mute, mute_check=lambda s, t: True,
        use_wildcards=False, exclude_check=None
    )
    assert "s t1" in to_mute_all
    assert "s t2" in to_mute_all

    def quarantine(suite, test):
        return (suite, test) == ("s", "t1")

    to_mute_filtered, _ = create_file_set(
        aggregated, is_mute, mute_check=lambda s, t: True,
        use_wildcards=False, exclude_check=quarantine
    )
    assert "s t1" not in to_mute_filtered
    assert "s t2" in to_mute_filtered
