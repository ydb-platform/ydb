"""Tests for mute.decisions module."""
from unittest.mock import MagicMock, patch

import pytest

# Mock ydb before importing mute.decisions (it imports ydb via ydb_wrapper)
with patch.dict('sys.modules', {'ydb': MagicMock()}):
    from mute.decisions import _test_line_to_full_name, write_mute_decisions, write_pattern_matches


def test_test_line_to_full_name():
    assert _test_line_to_full_name("suite1 test1") == "suite1/test1"
    assert _test_line_to_full_name("suite test.with.dots") == "suite/test.with.dots"
    assert _test_line_to_full_name("a b c") == "a/b c"


def test_write_mute_decisions_empty():
    mock_wrapper = MagicMock()
    mock_wrapper.get_table_path.return_value = "test_results/analytics/mute_decisions"
    mock_wrapper.check_credentials.return_value = True
    n = write_mute_decisions(
        mock_wrapper,
        branch="main",
        build_type="relwithdebinfo",
        to_mute=[],
        to_unmute=[],
        to_delete=[],
        to_graduated=set(),
    )
    assert n == 0


def test_write_mute_decisions_with_data():
    mock_wrapper = MagicMock()
    mock_wrapper.get_table_path.return_value = "test_results/analytics/mute_decisions"
    mock_wrapper.check_credentials.return_value = True
    with patch.object(mock_wrapper, 'create_table'):
        with patch.object(mock_wrapper, 'bulk_upsert') as bulk:
            n = write_mute_decisions(
                mock_wrapper,
                branch="main",
                build_type="relwithdebinfo",
                to_mute=["suite1 test1"],
                to_unmute=[],
                to_delete=[],
                to_graduated=set(),
                to_mute_debug=["suite1 test1 # reason"],
            )
            assert n == 1
            assert bulk.called
            rows = bulk.call_args[0][1]
            assert len(rows) == 1
            assert rows[0]["action"] == "mute"
            assert rows[0]["full_name"] == "suite1/test1"


def test_write_mute_decisions_v4_writes_to_mute_v4():
    mock_wrapper = MagicMock()
    mock_wrapper.get_table_path.return_value = "test_results/mute/v4_decisions"
    with patch.object(mock_wrapper, 'create_table'):
        with patch.object(mock_wrapper, 'bulk_upsert') as bulk:
            n = write_mute_decisions(
                mock_wrapper,
                branch="main",
                build_type="relwithdebinfo",
                to_mute=["suite1 test1"],
                to_unmute=[],
                to_delete=[],
                to_graduated=set(),
                system_version="v4_direct",
            )
            assert n == 1
            mock_wrapper.get_table_path.assert_called_with("mute_decisions_v4")
            rows = bulk.call_args[0][1]
            assert rows[0]["action"] == "mute"


def test_write_pattern_matches_empty():
    mock_wrapper = MagicMock()
    mock_wrapper.get_table_path.return_value = "test_results/analytics/mute_decisions"
    n = write_pattern_matches(mock_wrapper, "main", "relwithdebinfo", [])
    assert n == 0


def test_write_pattern_matches_with_data():
    mock_wrapper = MagicMock()
    mock_wrapper.get_table_path.return_value = "test_results/analytics/mute_decisions"
    with patch.object(mock_wrapper, 'create_table'):
        with patch.object(mock_wrapper, 'bulk_upsert') as bulk:
            matches = [
                {"full_name": "s/t", "rule_id": "test_duration_increased", "reaction": "alert", "pattern": "duration_increased", "growth_ratio": 2.0},
            ]
            n = write_pattern_matches(mock_wrapper, "main", "relwithdebinfo", matches)
            assert n == 1
            assert bulk.called
            rows = bulk.call_args[0][1]
            assert len(rows) == 1
            assert "alert:test_duration_increased" in rows[0]["action"]
            assert rows[0]["full_name"] == "s/t"
            assert rows[0]["match_details"] is not None
