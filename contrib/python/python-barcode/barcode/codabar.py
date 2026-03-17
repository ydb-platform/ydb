"""Module: barcode.codabar

:Provided barcodes: Codabar (NW-7)
"""
from __future__ import annotations

__docformat__ = "restructuredtext en"

from barcode.base import Barcode
from barcode.charsets import codabar
from barcode.errors import BarcodeError
from barcode.errors import IllegalCharacterError


class CODABAR(Barcode):
    """Initializes a new CODABAR instance.

    :parameters:
        code : String
            Codabar (NW-7) string that matches [ABCD][0-9$:/.+-]+[ABCD]
        writer : barcode.writer Instance
            The writer to render the barcode (default: SVGWriter).
        narrow: Integer
            Width of the narrow elements (default: 2)
        wide: Integer
            Width of the wide elements (default: 5)
            wide/narrow must be in the range 2..3
    """

    name = "Codabar (NW-7)"

    def __init__(self, code, writer=None, narrow=2, wide=5) -> None:
        self.code = code
        self.writer = writer or self.default_writer()
        self.narrow = narrow
        self.wide = wide

    def __str__(self) -> str:
        return self.code

    def get_fullcode(self):
        return self.code

    def build(self) -> list[str]:
        try:
            data = (
                codabar.STARTSTOP[self.code[0]] + "n"
            )  # Start with [A-D], followed by a narrow space

        except KeyError:
            raise BarcodeError("Codabar should start with either A,B,C or D") from None

        try:
            data += "n".join(
                [codabar.CODES[c] for c in self.code[1:-1]]
            )  # separated by a narrow space
        except KeyError:
            raise IllegalCharacterError(
                "Codabar can only contain numerics or $:/.+-"
            ) from None

        try:
            data += "n" + codabar.STARTSTOP[self.code[-1]]  # End with [A-D]
        except KeyError:
            raise BarcodeError("Codabar should end with either A,B,C or D") from None

        raw = ""
        for e in data:
            if e == "W":
                raw += "1" * self.wide
            if e == "w":
                raw += "0" * self.wide
            if e == "N":
                raw += "1" * self.narrow
            if e == "n":
                raw += "0" * self.narrow
        return [raw]
