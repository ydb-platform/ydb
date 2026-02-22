"""Rule mix tests: quarantine excludes from mute, graduation vs unmute."""
import sys
import os
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
# Mock all heavy deps BEFORE importing create_new_muted_ya
with patch.dict('sys.modules', {
    'ydb': MagicMock(),
    'update_mute_issues': MagicMock(),
}):
    import create_new_muted_ya  # noqa: E402
    create_new_muted_ya.YDBWrapper = MagicMock()
    create_new_muted_ya.write_mute_decisions = MagicMock()
    create_file_set = create_new_muted_ya.create_file_set
    from mute_logic import is_mute_candidate


def test_quarantine_excludes_from_mute_candidates():
    """Test in quarantine + would be mute candidate -> exclude_check excludes it from to_mute."""
    aggregated = [
        {"full_name": "s/t1", "suite_folder": "s", "test_name": "t1", "pass_count": 5, "fail_count": 4, "is_muted": False},
        {"full_name": "s/t2", "suite_folder": "s", "test_name": "t2", "pass_count": 5, "fail_count": 4, "is_muted": False},
    ]
    params = {"min_failures_high": 3, "min_runs_threshold": 10}

    def is_mute(test):
        return is_mute_candidate(test, aggregated, params)

    # No quarantine: both t1 and t2 should be in to_mute
    to_mute_all, _ = create_file_set(
        aggregated, is_mute, mute_check=lambda s, t: True,
        use_wildcards=False, exclude_check=None
    )
    assert "s t1" in to_mute_all
    assert "s t2" in to_mute_all

    # Quarantine t1: only t2 in to_mute
    def quarantine(suite, test):
        return (suite, test) == ("s", "t1")

    to_mute_filtered, _ = create_file_set(
        aggregated, is_mute, mute_check=lambda s, t: True,
        use_wildcards=False, exclude_check=quarantine
    )
    assert "s t1" not in to_mute_filtered
    assert "s t2" in to_mute_filtered
