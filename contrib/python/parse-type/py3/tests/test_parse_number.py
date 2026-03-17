# -*- coding: UTF-8 -*-
"""
Additional unit tests for the :mod`parse` module.
Related to auto-detection of number base (base=10, 2, 8, 16).
"""

from __future__ import absolute_import, print_function
import pytest
import parse

parse_version = parse.__version__
print("USING: parse-%s" % parse_version)
if parse_version in ("1.17.0", "1.16.0"):
    # -- REQUIRES: parse >= 1.18.0 -- WORKAROUND HERE
    print("USING: parse_type.parse (INSTEAD)")
    from parse_type import parse

def assert_parse_number_with_format_d(text, expected):
    parser = parse.Parser("{value:d}")
    result = parser.parse(text)
    assert result.named == dict(value=expected)

@pytest.mark.parametrize("text, expected", [
    ("123", 123)
])
def test_parse_number_with_base10(text, expected):
    assert_parse_number_with_format_d(text, expected)

@pytest.mark.parametrize("text, expected", [
    ("0b0", 0),
    ("0b1011", 11),
])
def test_parse_number_with_base2(text, expected):
    assert_parse_number_with_format_d(text, expected)

@pytest.mark.parametrize("text, expected", [
    ("0o0", 0),
    ("0o10", 8),
    ("0o12", 10),
])
def test_parse_number_with_base8(text, expected):
    assert_parse_number_with_format_d(text, expected)

@pytest.mark.parametrize("text, expected", [
    ("0x0", 0),
    ("0x01", 1),
    ("0x12", 18),
])
def test_parse_number_with_base16(text, expected):
    assert_parse_number_with_format_d(text, expected)


@pytest.mark.parametrize("text1, expected1, text2, expected2", [
    ("0x12", 18, "12", 12)
])
def test_parse_number_twice(text1, expected1, text2, expected2):
    """ENSURE: Issue #121 int_convert memory effect is fixed."""
    parser = parse.Parser("{:d}")
    result1 = parser.parse(text1)
    result2 = parser.parse(text2)
    assert result1.fixed[0] == expected1
    assert result2.fixed[0] == expected2
