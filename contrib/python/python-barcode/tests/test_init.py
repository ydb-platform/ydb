from __future__ import annotations

import os
from io import BytesIO

import pytest

import barcode
from barcode.writer import SVGWriter

import yatest.common
TESTPATH = yatest.common.output_path()


def test_generate_without_output() -> None:
    with pytest.raises(TypeError, match="'output' cannot be None"):
        barcode.generate("ean13", "123455559121112")


def test_generate_with_file() -> None:
    with open(os.path.join(TESTPATH, "generate_with_file.jpeg"), "wb") as f:
        barcode.generate("ean13", "123455559121112", output=f)


def test_generate_with_filepath() -> None:
    # FIXME: extension is added to the filepath even if you include it.
    rv = barcode.generate(
        "ean13",
        "123455559121112",
        output=os.path.join(TESTPATH, "generate_with_filepath"),
    )
    assert rv == os.path.abspath(os.path.join(TESTPATH, "generate_with_filepath.svg"))


def test_generate_with_file_and_writer() -> None:
    with open(os.path.join(TESTPATH, "generate_with_file_and_writer.jpeg"), "wb") as f:
        barcode.generate("ean13", "123455559121112", output=f, writer=SVGWriter())


def test_generate_with_bytesio() -> None:
    bio = BytesIO()
    barcode.generate("ean13", "123455559121112", output=bio)
    # XXX: File is not 100% deterministic; needs to be addressed at some point.
    # assert len(bio.getvalue()) == 6127  # noqa: ERA001
