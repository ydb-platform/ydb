"""Test TRX address."""

# external
import pytest

# local
from validators import ValidationError, trx_address


@pytest.mark.parametrize(
    "value",
    [
        "TLjfbTbpZYDQ4EoA4N5CLNgGjfbF8ZWz38",
        "TDQ6C92wuNqvMWE967sMptCFaXq77uj1PF",
        "TFuGbxCQGSL4oLnJzVsen844LDwFbrUY4e",
        "TFAPKADDRhkSe3v27CsR8TZSjN8eJ8ycDK",
        "TSJHywLNva2MNjCD5iYfn5QAKD9Rk5Ncit",
        "TEi1qhi5LuTicg1u9oAstyXCSf5uibSyqo",
        "TAGvx5An6VBeHTu91cQwdABNcAYMRPcP4n",
        "TXbE5tXTejqT3Q47sYKCDb9NJDm3xrFpab",
        "TMTxQWNuWHXvHcYXc5D1wQhFmZFJijAxcG",
        "TPHgw9E8QYM3esNWih5KVnUVpUHwLTPfpA",
        "TFFLtBTi9jdaGwV3hznjCmPYaJme5AeqwU",
        "TC74QG8tbtixG5Raa4fEifywgjrFs45fNz",
    ],
)
def test_returns_true_on_valid_trx_address(value: str):
    """Test returns true on valid trx address."""
    assert trx_address(value)


@pytest.mark.parametrize(
    "value",
    [
        "T12345678901234567890123456789012345",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ12345678",
        "TR2G7Rm4vFqF8EpY4U5xdLdQ7XgJ2U8Vd",
        "TP6ah2v5mdsj8Z3hGz1yDMvDq7BzEbK8o",
        "TQmmhp6uz2Xre8yL3FsPYZyo4mhtw4vg4XX",
        "TQNy2C6VHJPk4P32bsEX3QSGx2Qqm4J2k9",
        "TP6ah2v5mdsj8Z3hGz1yDMvDq7BzEbK8oN",
        "TSTVdfU1x4L7K3Bc3v5C28Gp2J1rPyeL3f",
        "THPByuCzvU5QER9j2NC2mUQ2JPyRCam4e7",
        "TW5eZqUZgdW4rxFKAKsc2ryJbfFA94WXvD",
        "TR2G7Rm4vFqF8EpY4U5xdLdQ7XgJ2U8Vdd",
        "tQmmhp6uz2Xre8yL3FsPYZyo4mhtw4vg4X",
        "TR2G7Rm4vFqF8EpY4U5xdLdQ7Xg",
        "TQmmhp6uz2Xre8yL3FsPYZyo4mhtw4vg4x",
        "my-trox-address.trx",
    ],
)
def test_returns_failed_validation_on_invalid_trx_address(value: str):
    """Test returns failed validation on invalid trx address."""
    assert isinstance(trx_address(value), ValidationError)
