"""Test French validators."""

# standard
from typing import Union

# external
import pytest

# local
from validators import ValidationError
from validators.i18n.fr import fr_department, fr_ssn


@pytest.mark.parametrize(
    ("value",),
    [
        ("1 84 12 76 451 089 46",),
        ("1 84 12 76 451 089",),  # control key is optional
        ("2 99 05 75 202 818 97",),
        ("2 99 05 75 202 817 01",),
        ("2 99 05 2A 202 817 58",),
        ("2 99 05 2B 202 817 85",),
        ("2 99 05 971 12 817 70",),
    ],
)
def test_returns_true_on_valid_ssn(value: str):
    """Test returns true on valid ssn."""
    assert fr_ssn(value)


@pytest.mark.parametrize(
    ("value",),
    [
        (None,),
        ("",),
        ("3 84 12 76 451 089 46",),  # wrong gender number
        ("1 84 12 76 451 089 47",),  # wrong control key
        ("1 84 00 76 451 089",),  # invalid month
        ("1 84 13 76 451 089",),  # invalid month
        ("1 84 12 00 451 089",),  # invalid department
        ("1 84 12 2C 451 089",),
        ("1 84 12 98 451 089",),  # invalid department
        # ("1 84 12 971 451 089",), # ?
    ],
)
def test_returns_failed_validation_on_invalid_ssn(value: str):
    """Test returns failed validation on invalid_ssn."""
    assert isinstance(fr_ssn(value), ValidationError)


@pytest.mark.parametrize(
    ("value",),
    [
        ("01",),
        ("2A",),  # Corsica
        ("2B",),
        (14,),
        ("95",),
        ("971",),
        (971,),
    ],
)
def test_returns_true_on_valid_department(value: Union[str, int]):
    """Test returns true on valid department."""
    assert fr_department(value)


@pytest.mark.parametrize(
    ("value",),
    [
        (None,),
        ("",),
        ("00",),
        (0,),
        ("2C",),
        ("97",),
        ("978",),
        ("98",),
        ("96",),
        ("20",),
        (20,),
    ],
)
def test_returns_failed_validation_on_invalid_department(value: Union[str, int]):
    """Test returns failed validation on invalid department."""
    assert isinstance(fr_department(value), ValidationError)
