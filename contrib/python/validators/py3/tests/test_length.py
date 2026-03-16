"""Test Length."""

# external
import pytest

# local
from validators import ValidationError, length


@pytest.mark.parametrize(
    ("value", "min_val", "max_val"),
    [("password", 2, None), ("password", None, None), ("password", 0, 10), ("password", 8, 8)],
)
def test_returns_true_on_valid_length(value: str, min_val: int, max_val: int):
    """Test returns true on valid length."""
    assert length(value, min_val=min_val, max_val=max_val)


@pytest.mark.parametrize(
    ("value", "min_val", "max_val"),
    [("something", 14, 12), ("something", -10, -20), ("something", 0, -2), ("something", 13, 14)],
)
def test_returns_failed_validation_on_invalid_range(value: str, min_val: int, max_val: int):
    """Test returns failed validation on invalid range."""
    assert isinstance(length(value, min_val=min_val, max_val=max_val), ValidationError)
