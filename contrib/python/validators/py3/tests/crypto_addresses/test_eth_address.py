"""Test ETH address."""

# external
import pytest

# local
from validators import ValidationError, eth_address

pytest.importorskip("eth_hash")


@pytest.mark.parametrize(
    "value",
    [
        "0x8ba1f109551bd432803012645ac136ddd64dba72",
        "0x9cc14ba4f9f68ca159ea4ebf2c292a808aaeb598",
        "0x5AEDA56215b167893e80B4fE645BA6d5Bab767DE",
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "0x742d35Cc6634C0532925a3b844Bc454e4438f44e",
        "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
        "0x1234567890123456789012345678901234567890",
        "0x57Ab1ec28D129707052df4dF418D58a2D46d5f51",
    ],
)
def test_returns_true_on_valid_eth_address(value: str):
    """Test returns true on valid eth address."""
    assert eth_address(value)


@pytest.mark.parametrize(
    "value",
    [
        "0x742d35Cc6634C0532925a3b844Bc454e4438f44g",
        "0x742d35Cc6634C0532925a3b844Bc454e4438f44",
        "0xAbcdefg1234567890Abcdefg1234567890Abcdefg",
        "0x7c8EE9977c6f96b6b9774b3e8e4Cc9B93B12b2c72",
        "0x80fBD7F8B3f81D0e1d6EACAb69AF104A6508AFB1",
        "0x7c8EE9977c6f96b6b9774b3e8e4Cc9B93B12b2c7g",
        "0x7c8EE9977c6f96b6b9774b3e8e4Cc9B93B12b2c",
        "0x7Fb21a171205f3B8d8E4d88A2d2f8A56E45DdB5c",
        "validators.eth",
    ],
)
def test_returns_failed_validation_on_invalid_eth_address(value: str):
    """Test returns failed validation on invalid eth address."""
    assert isinstance(eth_address(value), ValidationError)
