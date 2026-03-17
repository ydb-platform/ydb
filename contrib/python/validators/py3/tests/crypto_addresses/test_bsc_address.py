"""Test BSC address."""

# external
import pytest

# local
from validators import ValidationError, bsc_address


@pytest.mark.parametrize(
    "value",
    [
        "0x4e5acf9684652BEa56F2f01b7101a225Ee33d23f",
        "0x22B0f92af10FdC25659e4C3A590c2F0D0c809c27",
        "0xb61724F993E7942ef2d8e4A94fF7c9e1cc26995F",
        "0x9c3dF8a511Fec8076D4B8EFb4d5E733B9F953dD7",
        "0x4536337B91c0623a4FD098023E6065e4773117c5",
        "0xAC484e1CE274eD1d40A7C2AeAb0bEA863634286F",
        "0x1FDE521fBe3483Cbb5957E6275028225a74387e4",
        "0x1693c3D1bA787Ba2bf81ac8897614AAaee5cb800",
        "0xf4C3Fd476A40658aEd9e595DA49c37d8965D2fFE",
        "0xc053E3D4932640787D6Cf67FcA36021E7BE62653",
        "0xaFd563A5aED0bC363e802842aD93Af46c1168b8a",
    ],
)
def test_returns_true_on_valid_bsc_address(value: str):
    """Test returns true on valid bsc address."""
    assert bsc_address(value)


@pytest.mark.parametrize(
    "value",
    [
        "1x32Be343B94f860124dC4fEe278FDCBD38C102D88",
        "0x32Be343B94f860124dC4fEe278FDCBD38C102D",
        "0x32Be343B94f860124dC4fEe278FDCBD38C102D88aabbcc",
        "0x4g5acf9684652BEa56F2f01b7101a225Eh33d23z",
        "0x",
        "Wrong@Address.com",
        "0x32Be343B94f860124dC4fEe278FDCBD38C102D__",
        "0x32Be343B94f860124dC4fEe278FDCBD38C102D88G",
        "0X32Be343B94f860124dC4fEe278FDCBD38C102D88",
        "0X32BE343B94F860124DCFEE278FDCBD38C102D88",
        "0x32Be 343B94f860124dC4fEe278FDCBD38C102D88",
        "0x32Be343B94f860124dC4fEe278FDCBD38C102D88!",
        "ox32Be343B94f860124dC4fEe278FDCBD38C102D88",
        "0x32Be343B94f860124dC4fEe278FDCBD38C102D88XYZ",
    ],
)
def test_returns_failed_validation_on_invalid_bsc_address(value: str):
    """Test returns failed validation on invalid bsc address."""
    assert isinstance(bsc_address(value), ValidationError)
