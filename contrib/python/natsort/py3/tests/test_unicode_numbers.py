# -*- coding: utf-8 -*-
"""\
Test the Unicode numbers module.
"""

import unicodedata
import warnings

from natsort.unicode_numbers import (
    decimal_chars,
    decimals,
    digit_chars,
    digits,
    digits_no_decimals,
    numeric,
    numeric_chars,
    numeric_no_decimals,
)
from natsort.unicode_numeric_hex import numeric_hex


def test_numeric_chars_contains_only_valid_unicode_numeric_characters() -> None:
    for a in numeric_chars:
        assert unicodedata.numeric(a, None) is not None


def test_digit_chars_contains_only_valid_unicode_digit_characters() -> None:
    for a in digit_chars:
        assert unicodedata.digit(a, None) is not None


def test_decimal_chars_contains_only_valid_unicode_decimal_characters() -> None:
    for a in decimal_chars:
        assert unicodedata.decimal(a, None) is not None


def test_numeric_chars_contains_all_valid_unicode_numeric_and_digit_characters() -> (
    None
):
    set_numeric_chars = set(numeric_chars)
    set_digit_chars = set(digit_chars)
    set_decimal_chars = set(decimal_chars)

    assert set_decimal_chars.isdisjoint(digits_no_decimals)
    assert set_digit_chars.issuperset(digits_no_decimals)

    assert set_decimal_chars.isdisjoint(numeric_no_decimals)
    assert set_numeric_chars.issuperset(numeric_no_decimals)


def test_missing_unicode_number_in_collection() -> None:
    ok = True
    set_numeric_hex = set(numeric_hex)
    for i in range(0x110000):
        try:
            a = chr(i)
        except ValueError:
            break
        if a in "0123456789":
            continue
        if unicodedata.numeric(a, None) is not None:
            if i not in set_numeric_hex:
                ok = False
    if not ok:
        warnings.warn(
            """\
Not all numeric unicode characters are represented in natsort/unicode_numeric_hex.py
This can be addressed by running dev/generate_new_unicode_numbers.py with the current \
version of Python.
It would be much appreciated if you would submit a Pull Request to the natsort
repository (https://github.com/SethMMorton/natsort) with the resulting change.
""",
            stacklevel=2,
        )


def test_combined_string_contains_all_characters_in_list() -> None:
    assert numeric == "".join(numeric_chars)
    assert digits == "".join(digit_chars)
    assert decimals == "".join(decimal_chars)
