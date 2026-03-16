from __future__ import annotations

from barcode import get_barcode


def test_ean8_builds() -> None:
    ref = "1010100011000110100100110101111010101000100100010011100101001000101"
    ean = get_barcode("ean8", "40267708")
    bc = ean.build()
    assert ref == bc[0]


def test_ean8_builds_with_longer_bars() -> None:
    ref = "G0G01000110001101001001101011110G0G01000100100010011100101001000G0G"
    ean = get_barcode("ean8", "40267708", options={"guardbar": True})
    bc = ean.build()
    assert ref == bc[0]
