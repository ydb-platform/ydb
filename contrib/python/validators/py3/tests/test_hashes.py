"""Test Hashes."""

# external
import pytest

# local
from validators import ValidationError, base58, base64, md5, sha1, sha224, sha256, sha384, sha512

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


# ==> md5 <== #


@pytest.mark.parametrize(
    "value", ["d41d8cd98f00b204e9800998ecf8427e", "D41D8CD98F00B204E9800998ECF8427E"]
)
def test_returns_true_on_valid_md5(value: str):
    """Test returns true on valid md5."""
    assert md5(value)


@pytest.mark.parametrize(
    "value",
    [
        "z41d8cd98f00b204e9800998ecf8427e",
        "z8cd98f00b204e9800998ecf8427e",
        "z4aaaa1d8cd98f00b204e9800998ecf8427e",
    ],
)
def test_returns_failed_validation_on_invalid_md5(value: str):
    """Test returns failed validation on invalid md5."""
    assert isinstance(md5(value), ValidationError)


# ==> sha1 <== #


@pytest.mark.parametrize(
    "value",
    ["da39a3ee5e6b4b0d3255bfef95601890afd80709", "DA39A3EE5E6B4B0D3255BFEF95601890AFD80709"],
)
def test_returns_true_on_valid_sha1(value: str):
    """Test returns true on valid sha1."""
    assert sha1(value)


@pytest.mark.parametrize(
    "value",
    [
        "za39a3ee5e6b4b0d3255bfef95601890afd80709",
        "da39e5e6b4b0d3255bfef95601890afd80709",
        "daaaa39a3ee5e6b4b0d3255bfef95601890afd80709",
    ],
)
def test_returns_failed_validation_on_invalid_sha1(value: str):
    """Test returns failed validation on invalid sha1."""
    assert isinstance(sha1(value), ValidationError)


# ==> sha224 <== #


@pytest.mark.parametrize(
    "value",
    [
        "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
        "D14A028C2A3A2BC9476102BB288234C415A2B01F828EA62AC5B3E42F",
    ],
)
def test_returns_true_on_valid_sha224(value: str):
    """Test returns true on valid sha224."""
    assert sha224(value)


@pytest.mark.parametrize(
    "value",
    [
        "z14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
        "d028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
        "daaa14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
    ],
)
def test_returns_failed_validation_on_invalid_sha224(value: str):
    """Test returns failed validation on invalid sha224."""
    assert isinstance(sha224(value), ValidationError)


# ==> sha256 <== #


@pytest.mark.parametrize(
    "value",
    [
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
    ],
)
def test_returns_true_on_valid_sha256(value: str):
    """Test returns true on valid sha256."""
    assert sha256(value)


@pytest.mark.parametrize(
    "value",
    [
        "z3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "ec44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "eaaaa3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    ],
)
def test_returns_failed_validation_on_invalid_sha256(value: str):
    """Test returns failed validation on invalid sha256."""
    assert isinstance(sha256(value), ValidationError)


# ==> sha384 <== #


@pytest.mark.parametrize(
    "value",
    [
        "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7",
        "CB00753F45A35E8BB5A03D699AC65007272C32AB0EDED1631A8B605A43FF5BED8086072BA1E7CC2358BAECA134C825A7",
        "bfd76c0ebbd006fee583410547c1887b0292be76d582d96c242d2a792723e3fd6fd061f9d5cfd13b8f961358e6adba4a",
        "F21EF1F8DBF806106813C8504AF864D8D9BFDFA8D67FA9B7DFF1C5B61C2584394A05897C4F157CEEE0E8FBC29205BB8B",
    ],
)
def test_returns_true_on_valid_sha384(value: str):
    """Test returns true on valid sha384."""
    assert sha384(value)


@pytest.mark.parametrize(
    "value",
    [
        "zb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7",
        "c753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7",
        "cb00aaaa753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7",
    ],
)
def test_returns_failed_validation_on_invalid_sha384(value: str):
    """Test returns failed validation on invalid sha384."""
    assert isinstance(sha384(value), ValidationError)


# ==> sha512 <== #


@pytest.mark.parametrize(
    "value",
    [
        (
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d"
            "13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        ),
        (
            "CF83E1357EEFB8BDF1542850D66D8007D620E4050B5715DC83F4A921D36CE9CE47D0D"
            "13C5D85F2B0FF8318D2877EEC2F63B931BD47417A81A538327AF927DA3E"
        ),
    ],
)
def test_returns_true_on_valid_sha512(value: str):
    """Test returns true on valid sha512."""
    assert sha512(value)


@pytest.mark.parametrize(
    "value",
    [
        (
            "zf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d"
            "13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        ),
        (
            "cf8357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c"
            "5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        ),
        (
            "cf8aaaa3e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce4"
            "7d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        ),
    ],
)
def test_returns_failed_validation_on_invalid_sha512(value: str):
    """Test returns failed validation on invalid sha512."""
    assert isinstance(sha512(value), ValidationError)
