"""Test i18n/inn."""

# external
import pytest

# local
from validators import ValidationError
from validators.i18n.ru import ru_inn


@pytest.mark.parametrize(
    ("value",),
    [
        ("2222058686",),
        ("7709439560",),
        ("5003052454",),
        ("7730257499",),
        ("3664016814",),
        ("026504247480",),
        ("780103209220",),
        ("7707012148",),
        ("140700989885",),
        ("774334078053",),
    ],
)
def test_returns_true_on_valid_ru_inn(value: str):
    """Test returns true on valid russian individual tax number."""
    assert ru_inn(value)


@pytest.mark.parametrize(
    ("value",),
    [
        ("2222058687",),
        ("7709439561",),
        ("5003052453",),
        ("7730257490",),
        ("3664016815",),
        ("026504247481",),
        ("780103209222",),
        ("7707012149",),
        ("140700989886",),
        ("774334078054",),
    ],
)
def test_returns_false_on_valid_ru_inn(value: str):
    """Test returns true on valid russian individual tax number."""
    assert isinstance(ru_inn(value), ValidationError)
