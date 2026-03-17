"""MAC Address."""

# external
import pytest

# local
from validators import ValidationError, mac_address


@pytest.mark.parametrize(
    ("address",),
    [
        ("01:23:45:67:ab:CD",),
        ("01-23-45-67-ab-CD",),
        ("01:2F:45:37:ab:CD",),
        ("A1-2F-4E-68-ab-CD",),
    ],
)
def test_returns_true_on_valid_mac_address(address: str):
    """Test returns true on valid mac address."""
    assert mac_address(address)


@pytest.mark.parametrize(
    ("address",),
    [
        ("00-00:-00-00-00",),
        ("01:23:45:67:89:",),
        ("01:23-45:67-89:gh",),
        ("123:23:45:67:89:00",),
    ],
)
def test_returns_failed_validation_on_invalid_mac_address(address: str):
    """Test returns failed validation on invalid mac address."""
    assert isinstance(mac_address(address), ValidationError)
