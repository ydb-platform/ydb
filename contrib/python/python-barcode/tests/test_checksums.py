from __future__ import annotations

from barcode import get_barcode


def test_code39_checksum() -> None:
    code39 = get_barcode("code39", "Code39")
    assert code39.get_fullcode() == "CODE39W"


def test_pzn_checksum() -> None:
    pzn = get_barcode("pzn", "103940")
    assert pzn.get_fullcode() == "PZN-1039406"


def test_ean13_checksum() -> None:
    ean = get_barcode("ean13", "400614457735")
    assert ean.get_fullcode() == "4006144577350"


def test_ean8_checksum() -> None:
    ean = get_barcode("ean8", "6032299")
    assert ean.get_fullcode() == "60322999"


def test_jan_checksum() -> None:
    jan = get_barcode("jan", "491400614457")
    assert jan.get_fullcode() == "4914006144575"


def test_ean14_checksum() -> None:
    ean = get_barcode("ean14", "1234567891258")
    assert ean.get_fullcode() == "12345678912589"


def test_isbn10_checksum() -> None:
    isbn = get_barcode("isbn10", "376926085")
    assert isbn.isbn10 == "3769260856"  # type: ignore[attr-defined]


def test_isbn13_checksum() -> None:
    isbn = get_barcode("isbn13", "978376926085")
    assert isbn.get_fullcode() == "9783769260854"


def test_gs1_128_checksum() -> None:
    gs1_128 = get_barcode("gs1_128", "00376401856400470087")
    assert gs1_128.get_fullcode() == "00376401856400470087"
