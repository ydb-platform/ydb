"""This package provides a simple way to create standard barcodes.
It needs no external packages to be installed, the barcodes are
created as SVG objects. If Pillow is installed, the barcodes can also be
rendered as images (all formats supported by Pillow).
"""
from __future__ import annotations

import os
from typing import TYPE_CHECKING
from typing import BinaryIO
from typing import overload

from barcode.codabar import CODABAR
from barcode.codex import PZN
from barcode.codex import Code39
from barcode.codex import Code128
from barcode.codex import Gs1_128
from barcode.ean import EAN8
from barcode.ean import EAN8_GUARD
from barcode.ean import EAN13
from barcode.ean import EAN13_GUARD
from barcode.ean import EAN14
from barcode.ean import JAN
from barcode.errors import BarcodeNotFoundError
from barcode.isxn import ISBN10
from barcode.isxn import ISBN13
from barcode.isxn import ISSN
from barcode.itf import ITF
from barcode.upc import UPCA
from barcode.version import version  # noqa: F401

if TYPE_CHECKING:
    from barcode.base import Barcode
    from barcode.writer import BaseWriter

__BARCODE_MAP: dict[str, type[Barcode]] = {
    "codabar": CODABAR,
    "code128": Code128,
    "code39": Code39,
    "ean": EAN13,
    "ean13": EAN13,
    "ean13-guard": EAN13_GUARD,
    "ean14": EAN14,
    "ean8": EAN8,
    "ean8-guard": EAN8_GUARD,
    "gs1": ISBN13,
    "gs1_128": Gs1_128,
    "gtin": EAN14,
    "isbn": ISBN13,
    "isbn10": ISBN10,
    "isbn13": ISBN13,
    "issn": ISSN,
    "itf": ITF,
    "jan": JAN,
    "nw-7": CODABAR,
    "pzn": PZN,
    "upc": UPCA,
    "upca": UPCA,
}

PROVIDED_BARCODES = list(__BARCODE_MAP)
PROVIDED_BARCODES.sort()


@overload
def get(
    name: str, code: str, writer: BaseWriter | None = None, options: dict | None = None
) -> Barcode:
    ...


@overload
def get(
    name: str,
    code: None = None,
    writer: BaseWriter | None = None,
    options: dict | None = None,
) -> type[Barcode]:
    ...


def get(
    name: str,
    code: str | None = None,
    writer: BaseWriter | None = None,
    options: dict | None = None,
) -> Barcode | type[Barcode]:
    """Helper method for getting a generator or even a generated code.

    :param name: The name of the type of barcode desired.
    :param code: The actual information to encode. If this parameter is
        provided, a generated barcode is returned. Otherwise, the barcode class
        is returned.
    :param Writer writer: An alternative writer to use when generating the
        barcode.
    :param options: Additional options to be passed on to the barcode when
        generating.
    """
    options = options or {}
    barcode: type[Barcode]
    try:
        barcode = __BARCODE_MAP[name.lower()]
    except KeyError as e:
        raise BarcodeNotFoundError(f"The barcode {name!r} is not known.") from e
    if code is not None:
        return barcode(code, writer, **options)

    return barcode


def get_class(name: str) -> type[Barcode]:
    return get_barcode(name)


def generate(
    name: str,
    code: str,
    writer: BaseWriter | None = None,
    output: str | os.PathLike | BinaryIO | None = None,
    writer_options: dict | None = None,
    text: str | None = None,
) -> str | None:
    """Shortcut to generate a barcode in one line.

    :param name: Name of the type of barcode to use.
    :param code: Data to encode into the barcode.
    :param writer: A writer to use (e.g.: ImageWriter or SVGWriter).
    :param output: Destination file-like or path-like where to save the generated
     barcode.
    :param writer_options: Options to pass on to the writer instance.
    :param text: Text to render under the barcode.
    """
    from barcode.base import Barcode

    if output is None:
        raise TypeError("'output' cannot be None")

    writer = writer or Barcode.default_writer()
    writer.set_options(writer_options or {})

    barcode = get(name, code, writer)

    if isinstance(output, str):
        return barcode.save(output, writer_options, text)
    if isinstance(output, os.PathLike):
        with open(output, "wb") as fp:
            barcode.write(fp, writer_options, text)
        return None
    barcode.write(output, writer_options, text)
    return None


get_barcode = get
get_barcode_class = get_class
