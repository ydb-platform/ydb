"""Generates barcodes for visually inspecting the results."""
from __future__ import annotations

import codecs
import os
from typing import Iterator

import pytest

from barcode import get_barcode
from barcode import get_barcode_class
from barcode import version
from barcode.writer import ImageWriter

PATH = os.path.dirname(os.path.abspath(__file__))
TESTPATH = os.path.join(PATH, "test_outputs")
HTMLFILE = os.path.join(TESTPATH, "index.html")

HTML = """<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <title>python-barcode {version} Test</title>
    </head>
    <body>
        <h1>python-barcode {version} Tests</h1>
        {body}
    </body>
</html>
"""

OBJECTS = '<p><h2>{name}</h2><br>\n<img src="{filename}" alt="SVG {name}">\n'

IMAGES = '<h3>As PNG-Image</h3><br>\n<img src="{filename}" alt="PNG {name}"></p>\n'

NO_PIL = "<h3>Pillow was not found. No PNG-Image created.</h3></p>\n"

TESTCODES = (
    ("ean8", "40267708"),
    ("ean8-guard", "40267708"),
    ("ean13", "5901234123457"),
    ("ean13-guard", "5901234123457"),
    ("ean14", "12345678911230"),
    ("upca", "36000291453"),
    ("jan", "4901234567894"),
    ("isbn10", "3-12-517154-7"),
    ("isbn13", "978-3-16-148410-0"),
    ("issn", "1144875X"),
    ("code39", "Example Code 39"),
    ("pzn", "487780"),
    ("code128", "Example Code 128 998866"),
    ("itf", "12341234"),
)


@pytest.mark.parametrize(("codename", "code"), TESTCODES)
def test_generating_barcodes(
    codename: str, code: str, gather_image_elements_into_html: list[str]
) -> None:
    os.makedirs(TESTPATH, exist_ok=True)

    image_elements = gather_image_elements_into_html

    def append(x, y) -> None:
        image_elements.append(OBJECTS.format(filename=x, name=y))

    def append_img(x, y) -> None:
        image_elements.append(IMAGES.format(filename=x, name=y))

    options = {}
    bcode = get_barcode(codename, code)
    if codename.startswith("i"):
        options["center_text"] = False
    else:
        options["center_text"] = True
    filename = bcode.save(os.path.join(TESTPATH, codename), options=options)
    print(f"Code: {bcode.name}, Input: {code}, Output: {bcode.get_fullcode()}")
    append(os.path.basename(filename), bcode.name)
    if ImageWriter is not None:
        bcodec = get_barcode_class(codename)
        bcode = bcodec(code, writer=ImageWriter())
        opts = {}
        if codename.startswith("i"):
            opts["center_text"] = False
        else:
            opts["center_text"] = True
        filename = bcode.save(os.path.join(TESTPATH, codename), options=opts)
        append_img(os.path.basename(filename), bcode.name)
    else:
        image_elements.append(NO_PIL)


@pytest.fixture(scope="module")
def gather_image_elements_into_html() -> Iterator[list[str]]:
    image_elements: list[str] = []
    yield image_elements

    # Save htmlfile with all objects
    with codecs.open(HTMLFILE, "w", encoding="utf-8") as f:
        obj = "\n".join(image_elements)
        f.write(HTML.format(version=version, body=obj))

    print(f"\nNow open {HTMLFILE} in your browser.")
