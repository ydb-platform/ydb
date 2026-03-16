from __future__ import annotations

import sys

import pytest

from barcode.ean import EAN13


def test_ean_checksum_generated() -> None:
    ean = EAN13("842167143322")  # input has 12 digits
    assert ean.calculate_checksum() == 5
    assert ean.ean == "8421671433225"


def test_ean_checksum_zeroed() -> None:
    ean = EAN13("842167143322", no_checksum=True)  # input has 12 digits
    assert ean.calculate_checksum() == 5
    assert ean.ean == "8421671433220"


def test_ean_checksum_supplied_and_generated() -> None:
    ean = EAN13("8421671433225")  # input has 13 digits
    assert ean.calculate_checksum() == 5
    assert ean.ean == "8421671433225"


def test_ean_checksum_supplied_and_matching() -> None:
    ean = EAN13("8421671433225", no_checksum=True)  # input has 13 digits
    assert ean.calculate_checksum() == 5
    assert ean.ean == "8421671433225"


def test_ean_checksum_supplied_and_different() -> None:
    ean = EAN13("8421671433229", no_checksum=True)  # input has 13 digits
    assert ean.calculate_checksum() == 5
    assert ean.ean == "8421671433229"


def test_ean_checksum_generated_placeholder() -> None:
    ean = EAN13("977114487500X")  # input has 13 digits
    assert ean.calculate_checksum() == 7
    assert ean.ean == "9771144875007"


@pytest.mark.skipif(sys.platform == "win32", reason="no /dev/null")
def test_ean_checksum_supplied_placeholder() -> None:
    ean = EAN13("977114487500X", no_checksum=True)  # input has 13 digits
    assert ean.calculate_checksum() == 7
    assert ean.ean == "9771144875000"

    with open("/dev/null", "wb") as f:
        ean.write(f)
