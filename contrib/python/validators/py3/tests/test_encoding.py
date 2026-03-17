"""Test Encodings."""

# external
import pytest

# local
from validators import ValidationError, base16, base32, base58, base64

# ==> base16 <== #


@pytest.mark.parametrize(
    "value",
    [
        "a3f4b2",
        "01ef",
        "abcdef0123456789",
        "1234567890abcdef",
        "1a2b3c",
        "abcdef",
        "000102030405060708090A0B0C0D0E0F",
    ],
)
def test_returns_true_on_valid_base16(value: str):
    """Test returns true on valid base16."""
    assert base16(value)


@pytest.mark.parametrize(
    "value",
    ["12345g", "hello world", "1234567890abcdeg", "GHIJKL", "12345G", "!@#$%^", "1a2h3c", "a3f4Z1"],
)
def test_returns_failed_validation_on_invalid_base16(value: str):
    """Test returns failed validation on invalid base16."""
    assert isinstance(base16(value), ValidationError)


# ==> base32 <== #


@pytest.mark.parametrize(
    "value",
    [
        "JBSWY3DPEHPK3PXP",
        "MFRGGZDFMZTWQ2LK",
        "MZXW6YTBOI======",
        "MFZWIZLTOQ======",
        "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ",
        "MFRGGZDFMZTWQ2LKNNWG23Q=",
    ],
)
def test_returns_true_on_valid_base32(value: str):
    """Test returns true on valid base32."""
    assert base32(value)


@pytest.mark.parametrize(
    "value",
    [
        "ThisIsNotBase32!",
        "12345!",
        "Any==invalid=base32=",
        "MzXW6yTBOI======",
        "JBSWY8DPEHPK9PXP",
        "MfZW3zLT9Q======",
    ],
)
def test_returns_failed_validation_on_invalid_base32(value: str):
    """Test returns failed validation on invalid base32."""
    assert isinstance(base32(value), ValidationError)


# ==> base58 <== #


@pytest.mark.parametrize(
    "value",
    [
        "cUSECaVvAiV3srWbFRvVPzm5YzcXJwPSwZfE7veYPHoXmR9h6YMQ",
        "18KToMF5ckjXBYt2HAj77qsG3GPeej3PZn",
        "n4FFXRNNEW1aA2WPscSuzHTCjzjs4TVE2Z",
        "38XzQ9dPGb1uqbZsjPtUajp7omy8aefjqj",
    ],
)
def test_returns_true_on_valid_base58(value: str):
    """Test returns true on valid base58."""
    assert base58(value)


@pytest.mark.parametrize(
    "value",
    ["ThisIsAReallyLongStringThatIsDefinitelyNotBase58Encoded", "abcABC!@#", "InvalidBase58!"],
)
def test_returns_failed_validation_on_invalid_base58(value: str):
    """Test returns failed validation on invalid base58."""
    assert isinstance(base58(value), ValidationError)


# ==> base64 <== #


@pytest.mark.parametrize(
    "value",
    ["SGVsbG8gV29ybGQ=", "U29tZSBkYXRhIHN0cmluZw==", "YW55IGNhcm5hbCBwbGVhcw=="],
)
def test_returns_true_on_valid_base64(value: str):
    """Test returns true on valid base64."""
    assert base64(value)


@pytest.mark.parametrize(
    "value",
    ["SGVsbG8gV29ybGQ", "U29tZSBkYXRhIHN0cmluZw", "YW55IGNhcm5hbCBwbGVhc"],
)
def test_returns_failed_validation_on_invalid_base64(value: str):
    """Test returns failed validation on invalid base64."""
    assert isinstance(base64(value), ValidationError)
