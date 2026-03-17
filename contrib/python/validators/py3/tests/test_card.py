"""Test Card."""

# external
import pytest

# local
from validators import (
    ValidationError,
    amex,
    card_number,
    diners,
    discover,
    jcb,
    mastercard,
    mir,
    unionpay,
    visa,
)

visa_cards = ["4242424242424242", "4000002760003184"]
mastercard_cards = ["5555555555554444", "2223003122003222"]
amex_cards = ["378282246310005", "371449635398431"]
unionpay_cards = ["6200000000000005"]
diners_cards = ["3056930009020004", "36227206271667"]
jcb_cards = ["3566002020360505"]
discover_cards = ["6011111111111117", "6011000990139424"]
mir_cards = ["2200123456789019", "2204987654321098"]


@pytest.mark.parametrize(
    "value",
    visa_cards
    + mastercard_cards
    + amex_cards
    + unionpay_cards
    + diners_cards
    + jcb_cards
    + discover_cards
    + mir_cards,
)
def test_returns_true_on_valid_card_number(value: str):
    """Test returns true on valid card number."""
    assert card_number(value)


@pytest.mark.parametrize(
    "value",
    [
        "4242424242424240",
        "4000002760003180",
        "400000276000318X",
        "220012345678901X",
    ],
)
def test_returns_failed_on_valid_card_number(value: str):
    """Test returns failed on valid card number."""
    assert isinstance(card_number(value), ValidationError)


@pytest.mark.parametrize("value", visa_cards)
def test_returns_true_on_valid_visa(value: str):
    """Test returns true on valid visa."""
    assert visa(value)


@pytest.mark.parametrize(
    "value",
    mastercard_cards + amex_cards + unionpay_cards + diners_cards + jcb_cards + discover_cards,
)
def test_returns_failed_on_valid_visa(value: str):
    """Test returns failed on valid visa."""
    assert isinstance(visa(value), ValidationError)


@pytest.mark.parametrize("value", mastercard_cards)
def test_returns_true_on_valid_mastercard(value: str):
    """Test returns true on valid mastercard."""
    assert mastercard(value)


@pytest.mark.parametrize(
    "value",
    visa_cards + amex_cards + unionpay_cards + diners_cards + jcb_cards + discover_cards,
)
def test_returns_failed_on_valid_mastercard(value: str):
    """Test returns failed on valid mastercard."""
    assert isinstance(mastercard(value), ValidationError)


@pytest.mark.parametrize("value", amex_cards)
def test_returns_true_on_valid_amex(value: str):
    """Test returns true on valid amex."""
    assert amex(value)


@pytest.mark.parametrize(
    "value",
    visa_cards
    + mastercard_cards
    + unionpay_cards
    + diners_cards
    + jcb_cards
    + discover_cards
    + mir_cards,
)
def test_returns_failed_on_valid_amex(value: str):
    """Test returns failed on valid amex."""
    assert isinstance(amex(value), ValidationError)


@pytest.mark.parametrize("value", unionpay_cards)
def test_returns_true_on_valid_unionpay(value: str):
    """Test returns true on valid unionpay."""
    assert unionpay(value)


@pytest.mark.parametrize(
    "value",
    visa_cards
    + mastercard_cards
    + amex_cards
    + diners_cards
    + jcb_cards
    + discover_cards
    + mir_cards,
)
def test_returns_failed_on_valid_unionpay(value: str):
    """Test returns failed on valid unionpay."""
    assert isinstance(unionpay(value), ValidationError)


@pytest.mark.parametrize("value", diners_cards)
def test_returns_true_on_valid_diners(value: str):
    """Test returns true on valid diners."""
    assert diners(value)


@pytest.mark.parametrize(
    "value",
    visa_cards
    + mastercard_cards
    + amex_cards
    + unionpay_cards
    + jcb_cards
    + discover_cards
    + mir_cards,
)
def test_returns_failed_on_valid_diners(value: str):
    """Test returns failed on valid diners."""
    assert isinstance(diners(value), ValidationError)


@pytest.mark.parametrize("value", jcb_cards)
def test_returns_true_on_valid_jcb(value: str):
    """Test returns true on valid jcb."""
    assert jcb(value)


@pytest.mark.parametrize(
    "value",
    visa_cards
    + mastercard_cards
    + amex_cards
    + unionpay_cards
    + diners_cards
    + discover_cards
    + mir_cards,
)
def test_returns_failed_on_valid_jcb(value: str):
    """Test returns failed on valid jcb."""
    assert isinstance(jcb(value), ValidationError)


@pytest.mark.parametrize("value", discover_cards)
def test_returns_true_on_valid_discover(value: str):
    """Test returns true on valid discover."""
    assert discover(value)


@pytest.mark.parametrize(
    "value",
    visa_cards
    + mastercard_cards
    + amex_cards
    + unionpay_cards
    + diners_cards
    + jcb_cards
    + mir_cards,
)
def test_returns_failed_on_valid_discover(value: str):
    """Test returns failed on valid discover."""
    assert isinstance(discover(value), ValidationError)


@pytest.mark.parametrize("value", mir_cards)
def test_returns_true_on_valid_mir(value: str):
    """Test returns true on valid Mir card."""
    assert mir(value)


@pytest.mark.parametrize(
    "value",
    visa_cards
    + mastercard_cards
    + amex_cards
    + unionpay_cards
    + diners_cards
    + jcb_cards
    + discover_cards,
)
def test_returns_failed_on_valid_mir(value: str):
    """Test returns failed on invalid Mir card (other payment systems)."""
    assert isinstance(mir(value), ValidationError)
