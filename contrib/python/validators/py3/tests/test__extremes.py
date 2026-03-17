"""Test Extremes."""

# standard
from typing import Any

# external
import pytest

# local
from validators._extremes import AbsMax, AbsMin

abs_max = AbsMax()
abs_min = AbsMin()


@pytest.mark.parametrize(
    ("value",),
    [(None,), ("",), (12,), (abs_min,)],
)
def test_abs_max_is_greater_than_every_other_value(value: Any):
    """Test if AbsMax is greater than every other value."""
    assert value < abs_max
    assert abs_max > value


def test_abs_max_is_not_greater_than_itself():
    """Test if AbsMax is not greater than itself."""
    assert not (abs_max > abs_max)


def test_other_comparison_methods_for_abs_max():
    """Test other comparison methods for AbsMax."""
    assert abs_max <= abs_max
    assert abs_max == abs_max
    assert abs_max == abs_max


@pytest.mark.parametrize(
    ("value",),
    [(None,), ("",), (12,), (abs_max,)],
)
def test_abs_min_is_smaller_than_every_other_value(value: Any):
    """Test if AbsMin is less than every other value."""
    assert value > abs_min


def test_abs_min_is_not_greater_than_itself():
    """Test if AbsMin is not less than itself."""
    assert not (abs_min < abs_min)


def test_other_comparison_methods_for_abs_min():
    """Test other comparison methods for AbsMin."""
    assert abs_min <= abs_min
    assert abs_min == abs_min
    assert abs_min == abs_min
