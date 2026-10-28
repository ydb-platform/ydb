import pytest

from nots import _parse_ts_checks, _ts_check_size


def test_parse_ts_checks_preserves_command_with_spaces_and_multiple_checks():
    raw = (
        "$_TS_CHECK_LIST~~~nots:lint lint no "
        "~~~nots:custom lint yes eslint src --max-warnings 0 "
        "~~~nots:test test no"
    )

    assert _parse_ts_checks(raw, "~~~") == [
        ["nots:lint", "lint", "no", ""],
        ["nots:custom", "lint", "yes", "eslint src --max-warnings 0"],
        ["nots:test", "test", "no", ""],
    ]


@pytest.mark.parametrize(
    ("check_type", "timeout_medium", "expected_size"),
    [
        ("lint", "no", "SMALL"),
        ("lint", "yes", "MEDIUM"),
        ("test", "no", None),
        ("test", "yes", "MEDIUM"),
    ],
)
def test_ts_check_size(check_type, timeout_medium, expected_size):
    assert _ts_check_size(check_type, timeout_medium) == expected_size
