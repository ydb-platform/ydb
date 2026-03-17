"""Test Indian validators."""

# external
import pytest

# local
from validators import ValidationError
from validators.i18n import ind_aadhar, ind_pan


@pytest.mark.parametrize("value", ["3675 9834 6012", "5046 3182 4299"])
def test_returns_true_on_valid_ind_aadhar(value: str):
    """Test returns true on valid ind aadhar."""
    assert ind_aadhar(value)


@pytest.mark.parametrize("value", ["3675 9834 6012 8", "417598346012", "3675 98AF 60#2"])
def test_returns_failed_validation_on_invalid_ind_aadhar(value: str):
    """Test returns failed validation on invalid ind aadhar."""
    assert isinstance(ind_aadhar(value), ValidationError)


@pytest.mark.parametrize("value", ["ABCDE9999K", "AAAPL1234C"])
def test_returns_true_on_valid_ind_pan(value: str):
    """Test returns true on valid ind pan."""
    assert ind_pan(value)


@pytest.mark.parametrize("value", ["ABC5d7896B", "417598346012", "AaaPL1234C"])
def test_returns_failed_validation_on_invalid_ind_pan(value: str):
    """Test returns failed validation on invalid ind pan."""
    assert isinstance(ind_pan(value), ValidationError)
