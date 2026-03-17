"""Test Finance."""

# external
import pytest

# local
from validators import ValidationError, cusip, isin, sedol

# ==> CUSIP <== #


@pytest.mark.parametrize("value", ["912796X38", "912796X20", "912796x20"])
def test_returns_true_on_valid_cusip(value: str):
    """Test returns true on valid cusip."""
    assert cusip(value)


@pytest.mark.parametrize("value", ["912796T67", "912796T68", "XCVF", "00^^^1234"])
def test_returns_failed_validation_on_invalid_cusip(value: str):
    """Test returns failed validation on invalid cusip."""
    assert isinstance(cusip(value), ValidationError)


# ==> ISIN <== #


@pytest.mark.parametrize("value", ["US0004026250", "JP000K0VF054", "US0378331005"])
def test_returns_true_on_valid_isin(value: str):
    """Test returns true on valid isin."""
    assert isin(value)


@pytest.mark.parametrize("value", ["010378331005", "XCVF", "00^^^1234", "A000009"])
def test_returns_failed_validation_on_invalid_isin(value: str):
    """Test returns failed validation on invalid isin."""
    assert isinstance(isin(value), ValidationError)


# ==> SEDOL <== #


@pytest.mark.parametrize("value", ["0263494", "0540528", "B000009"])
def test_returns_true_on_valid_sedol(value: str):
    """Test returns true on valid sedol."""
    assert sedol(value)


@pytest.mark.parametrize("value", ["0540526", "XCVF", "00^^^1234", "A000009"])
def test_returns_failed_validation_on_invalid_sedol(value: str):
    """Test returns failed validation on invalid sedol."""
    assert isinstance(sedol(value), ValidationError)
