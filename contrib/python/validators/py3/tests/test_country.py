"""Test Country."""

# external
import pytest

# local
from validators import ValidationError, calling_code, country_code, currency


@pytest.mark.parametrize(("value"), ["+1", "+371"])
def test_returns_true_on_valid_calling_code(value: str):
    """Test returns true on valid calling code."""
    assert calling_code(value)


@pytest.mark.parametrize(("value"), ["+19", "+37", "-9"])
def test_returns_failed_validation_invalid_calling_code(value: str):
    """Test returns failed validation invalid calling code."""
    assert isinstance(calling_code(value), ValidationError)


@pytest.mark.parametrize(
    ("value", "iso_format"),
    [
        ("ISR", "auto"),
        ("US", "alpha2"),
        ("USA", "alpha3"),
        ("840", "numeric"),
    ],
)
def test_returns_true_on_valid_country_code(value: str, iso_format: str):
    """Test returns true on valid country code."""
    assert country_code(value, iso_format=iso_format)


@pytest.mark.parametrize(
    ("value", "iso_format"),
    [
        (None, "auto"),
        ("", "auto"),
        ("123456", "auto"),
        ("XY", "alpha2"),
        ("PPP", "alpha3"),
        ("123", "numeric"),
        ("us", "auto"),
        ("uSa", "auto"),
        ("US ", "auto"),
        ("U.S", "auto"),
        ("1ND", "unknown"),
        ("ISR", None),
    ],
)
def test_returns_failed_validation_on_invalid_country_code(value: str, iso_format: str):
    """Test returns failed validation on invalid country code."""
    assert isinstance(country_code(value, iso_format=iso_format), ValidationError)


@pytest.mark.parametrize(
    ("value", "skip_symbols", "ignore_case"), [("$", False, False), ("uSd", True, True)]
)
def test_returns_true_on_valid_currency(value: str, skip_symbols: bool, ignore_case: bool):
    """Test returns true on valid currency."""
    assert currency(value, skip_symbols=skip_symbols, ignore_case=ignore_case)


@pytest.mark.parametrize(
    ("value", "skip_symbols", "ignore_case"),
    [("$", True, False), ("uSd", True, False), ("Bucks", True, True)],
)
def test_returns_failed_validation_invalid_currency(
    value: str, skip_symbols: bool, ignore_case: bool
):
    """Test returns failed validation invalid currency."""
    assert isinstance(
        currency(value, skip_symbols=skip_symbols, ignore_case=ignore_case), ValidationError
    )
