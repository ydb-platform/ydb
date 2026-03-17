"""barcode.errors"""
from __future__ import annotations

__docformat__ = "restructuredtext en"


class BarcodeError(Exception):
    def __init__(self, msg) -> None:
        self.msg = msg

    def __str__(self) -> str:
        return self.msg


class IllegalCharacterError(BarcodeError):
    """Raised when a barcode-string contains illegal characters."""


class BarcodeNotFoundError(BarcodeError):
    """Raised when an unknown barcode is requested."""


class NumberOfDigitsError(BarcodeError):
    """Raised when the number of digits do not match."""


class WrongCountryCodeError(BarcodeError):
    """Raised when a JAN (Japan Article Number) don't starts with 450-459
    or 490-499.
    """
