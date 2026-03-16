"""Test Cron."""

# external
import pytest

# local
from validators import ValidationError, cron


@pytest.mark.parametrize(
    "value",
    [
        "* * * * *",
        "*/5 * * * *",
        "0 0 * * *",
        "30 3 * * 1-5",
        "15 5 * * 1,3,5",
        "0 12 1 */2 *",
        "0 */3 * * *",
        "0 0 1 1 *",
        "0 12 * 1-6 1-5",
        "0 3-6 * * *",
        "*/15 0,6,12,18 * * *",
        "0 12 * * 0",
        "*/61 * * * *",
        # "5-10/2 * * * *", # this is valid, but not supported yet
    ],
)
def test_returns_true_on_valid_cron(value: str):
    """Test returns true on valid cron string."""
    assert cron(value)


@pytest.mark.parametrize(
    "value",
    [
        "* * * * * *",
        "* * * *",
        "*/5 25 * * *",
        "*/5 * *-1 * *",
        "32-30 * * * *",
        "0 12 32 * *",
        "0 12 * * 8",
        "0 */0 * * *",
        "30-20 * * * *",
        "10-* * * * *",
        "*/15 0,6,12,24 * * *",
        "& * * & * *",
        "* - * * - *",
    ],
)
def test_returns_failed_validation_on_invalid_cron(value: str):
    """Test returns failed validation on invalid cron string."""
    assert isinstance(cron(value), ValidationError)
