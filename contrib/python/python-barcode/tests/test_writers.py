from __future__ import annotations

import os
from io import BytesIO

from barcode import EAN13
from barcode.writer import ImageWriter
from barcode.writer import SVGWriter

import yatest.common
TESTPATH = yatest.common.output_path()

if ImageWriter is not None:

    def test_saving_image_to_byteio() -> None:
        assert ImageWriter is not None  # workaround for mypy

        rv = BytesIO()
        EAN13(str(100000902922), writer=ImageWriter()).write(rv)

        with open(f"{TESTPATH}/somefile.jpeg", "wb") as f:
            EAN13("100000011111", writer=ImageWriter()).write(f)

    def test_saving_rgba_image() -> None:
        assert ImageWriter is not None  # workaround for mypy

        rv = BytesIO()
        EAN13(str(100000902922), writer=ImageWriter()).write(rv)

        with open(f"{TESTPATH}/ean13-with-transparent-bg.png", "wb") as f:
            writer = ImageWriter(mode="RGBA")

            EAN13("100000011111", writer=writer).write(
                f, options={"background": "rgba(255,0,0,0)"}
            )


def test_saving_svg_to_byteio() -> None:
    rv = BytesIO()
    EAN13(str(100000902922), writer=SVGWriter()).write(rv)

    with open(f"{TESTPATH}/somefile.svg", "wb") as f:
        EAN13("100000011111", writer=SVGWriter()).write(f)


def test_saving_svg_to_byteio_with_guardbar() -> None:
    rv = BytesIO()
    EAN13(str(100000902922), writer=SVGWriter(), guardbar=True).write(rv)

    with open(f"{TESTPATH}/somefile_guardbar.svg", "wb") as f:
        EAN13("100000011111", writer=SVGWriter(), guardbar=True).write(f)
