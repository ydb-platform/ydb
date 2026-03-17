"""Test IP Address."""

# external
import pytest

# local
from validators import ValidationError, ipv4, ipv6


@pytest.mark.parametrize(
    ("address",),
    [
        ("127.0.0.1",),
        ("123.5.77.88",),
        ("12.12.12.12",),
    ],
)
def test_returns_true_on_valid_ipv4_address(address: str):
    """Test returns true on valid ipv4 address."""
    assert ipv4(address)
    assert not ipv6(address)


@pytest.mark.parametrize(
    ("address", "cidr", "strict", "host_bit"),
    [
        ("127.0.0.1/0", True, True, True),
        ("123.5.77.88", True, False, True),
        ("12.12.12.0/24", True, True, False),
    ],
)
def test_returns_true_on_valid_ipv4_cidr_address(
    address: str, cidr: bool, strict: bool, host_bit: bool
):
    """Test returns true on valid ipv4 CIDR address."""
    assert ipv4(address, cidr=cidr, strict=strict, host_bit=host_bit)
    assert not ipv6(address, cidr=cidr, strict=strict, host_bit=host_bit)


@pytest.mark.parametrize(
    ("address",),
    [
        # leading zeroes error-out from Python 3.9.5
        # ("100.100.033.033",),
        ("900.200.100.75",),
        ("0127.0.0.1",),
        ("abc.0.0.1",),
    ],
)
def test_returns_failed_validation_on_invalid_ipv4_address(address: str):
    """Test returns failed validation on invalid ipv4 address."""
    assert isinstance(ipv4(address), ValidationError)


@pytest.mark.parametrize(
    ("address", "cidr", "strict", "host_bit"),
    [
        ("1.1.1.1/1", False, True, True),
        ("1.1.1.1/33", True, False, True),
        ("1.1.1.1/24", True, True, False),
        ("1.1.1.1/-1", True, True, True),
    ],
)
def test_returns_failed_validation_on_invalid_ipv4_cidr_address(
    address: str, cidr: bool, strict: bool, host_bit: bool
):
    """Test returns failed validation on invalid ipv4 CIDR address."""
    assert isinstance(ipv4(address, cidr=cidr, strict=strict, host_bit=host_bit), ValidationError)


@pytest.mark.parametrize(
    ("address",),
    [
        ("::",),
        ("::1",),
        ("1::",),
        ("dead:beef:0:0:0:0000:42:1",),
        ("abcd:ef::42:1",),
        ("0:0:0:0:0:ffff:1.2.3.4",),
        ("::192.168.30.2",),
        ("0000:0000:0000:0000:0000::",),
        ("0:a:b:c:d:e:f::",),
    ],
)
def test_returns_true_on_valid_ipv6_address(address: str):
    """Test returns true on valid ipv6 address."""
    assert ipv6(address)
    assert not ipv4(address)


@pytest.mark.parametrize(
    ("address", "cidr", "strict", "host_bit"),
    [
        ("::1/128", True, True, True),
        ("::1/0", True, True, True),
        ("dead:beef:0:0:0:0:42:1/8", True, True, True),
        ("abcd:ef::42:1/32", True, True, True),
        ("0:0:0:0:0:ffff:1.2.3.4/16", True, True, True),
        ("2001:0db8:85a3:0000:0000:8a2e:0370:7334/64", True, True, True),
        ("::192.168.30.2/128", True, True, True),
    ],
)
def test_returns_true_on_valid_ipv6_cidr_address(
    address: str, cidr: bool, strict: bool, host_bit: bool
):
    """Test returns true on valid ipv6 CIDR address."""
    assert ipv6(address, cidr=cidr, strict=strict, host_bit=host_bit)
    assert not ipv4(address, cidr=cidr, strict=strict, host_bit=host_bit)


@pytest.mark.parametrize(
    ("address",),
    [
        ("abc.0.0.1",),
        ("abcd:1234::123::1",),
        ("1:2:3:4:5:6:7:8:9",),
        ("1:2:3:4:5:6:7:8::",),
        ("1:2:3:4:5:6:7::8:9",),
        ("abcd::1ffff",),
        ("1111:",),
        (":8888",),
        (":1.2.3.4",),
        ("18:05",),
        (":",),
        (":1:2:",),
        (":1:2::",),
        ("::1:2::",),
        ("8::1:2::9",),
        ("02001:0000:1234:0000:0000:C1C0:ABCD:0876",),
    ],
)
def test_returns_failed_validation_on_invalid_ipv6_address(address: str):
    """Test returns failed validation on invalid ipv6 address."""
    assert isinstance(ipv6(address), ValidationError)


@pytest.mark.parametrize(
    ("address", "cidr", "strict", "host_bit"),
    [
        ("::1/128", False, True, True),
        ("::1/129", True, False, True),
        ("dead:beef:0:0:0:0:42:1/8", True, True, False),
        ("::1/-130", True, True, True),
    ],
)
def test_returns_failed_validation_on_invalid_ipv6_cidr_address(
    address: str, cidr: bool, strict: bool, host_bit: bool
):
    """Test returns failed validation on invalid ipv6 CIDR address."""
    assert isinstance(ipv6(address, cidr=cidr, strict=strict, host_bit=host_bit), ValidationError)


@pytest.mark.parametrize(
    ("address", "private"),
    [
        ("10.1.1.1", True),
        ("192.168.1.1", True),
        ("169.254.1.1", True),
        ("127.0.0.1", True),
        ("0.0.0.0", True),
    ],
)
def test_returns_true_on_valid_private_ipv4_address(address: str, private: bool):
    """Test returns true on private ipv4 address."""
    assert ipv4(address, private=private)


@pytest.mark.parametrize(
    ("address", "private"),
    [
        ("1.1.1.1", True),
        ("192.169.1.1", True),
        ("7.53.12.1", True),
    ],
)
def test_returns_failed_validation_on_invalid_private_ipv4_address(address: str, private: bool):
    """Test returns failed validation on invalid private ipv4 address."""
    assert isinstance(ipv4(address, private=private), ValidationError)


@pytest.mark.parametrize(
    ("address", "private"),
    [
        ("1.1.1.1", False),
        ("192.169.1.1", False),
        ("7.53.12.1", False),
    ],
)
def test_returns_true_on_valid_public_ipv4_address(address: str, private: bool):
    """Test returns true on valid public ipv4 address."""
    assert ipv4(address, private=private)


@pytest.mark.parametrize(
    ("address", "private"),
    [
        ("10.1.1.1", False),
        ("192.168.1.1", False),
        ("169.254.1.1", False),
        ("127.0.0.1", False),
        ("0.0.0.0", False),
    ],
)
def test_returns_failed_validation_on_invalid_public_ipv4_address(address: str, private: bool):
    """Test returns failed validation on private ipv4 address."""
    assert isinstance(ipv4(address, private=private), ValidationError)
