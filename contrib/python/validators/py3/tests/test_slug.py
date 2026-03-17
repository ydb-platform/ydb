"""Test Slug."""

# external
import pytest

# local
from validators import ValidationError, slug


@pytest.mark.parametrize(
    "value",
    [
        "123-asd-7sda",
        "123-k-123",
        "dac-12sa-459",
        "dac-12sa7-ad31as",
    ],
)
def test_returns_true_on_valid_slug(value: str):
    """Test returns true on valid slug."""
    assert slug(value)


@pytest.mark.parametrize(
    "value",
    [
        "some.slug&",
        "1231321%",
        "   21312",
        "-47q-p--123",
    ],
)
def test_returns_failed_validation_on_invalid_slug(value: str):
    """Test returns failed validation on invalid slug."""
    assert isinstance(slug(value), ValidationError)
