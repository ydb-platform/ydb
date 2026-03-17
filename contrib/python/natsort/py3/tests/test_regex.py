# -*- coding: utf-8 -*-
"""These test the splitting regular expressions."""

from typing import List, Pattern

import pytest
from natsort import ns, numeric_regex_chooser
from natsort.ns_enum import NSType
from natsort.utils import NumericalRegularExpressions as NumRegex


regex_names = {
    NumRegex.int_nosign(): "int_nosign",
    NumRegex.int_sign(): "int_sign",
    NumRegex.float_nosign_noexp(): "float_nosign_noexp",
    NumRegex.float_sign_noexp(): "float_sign_noexp",
    NumRegex.float_nosign_exp(): "float_nosign_exp",
    NumRegex.float_sign_exp(): "float_sign_exp",
}

# Regex Aliases (so lines stay a reasonable length.
i_u = NumRegex.int_nosign()
i_s = NumRegex.int_sign()
f_u = NumRegex.float_nosign_noexp()
f_s = NumRegex.float_sign_noexp()
f_ue = NumRegex.float_nosign_exp()
f_se = NumRegex.float_sign_exp()

# Assemble a test suite of regular strings and their regular expression
# splitting result. Organize by the input string.
regex_tests = {
    "-123.45e+67": {
        i_u: ["-", "123", ".", "45", "e+", "67", ""],
        i_s: ["", "-123", ".", "45", "e", "+67", ""],
        f_u: ["-", "123.45", "e+", "67", ""],
        f_s: ["", "-123.45", "e", "+67", ""],
        f_ue: ["-", "123.45e+67", ""],
        f_se: ["", "-123.45e+67", ""],
    },
    "a-123.45e+67b": {
        i_u: ["a-", "123", ".", "45", "e+", "67", "b"],
        i_s: ["a", "-123", ".", "45", "e", "+67", "b"],
        f_u: ["a-", "123.45", "e+", "67", "b"],
        f_s: ["a", "-123.45", "e", "+67", "b"],
        f_ue: ["a-", "123.45e+67", "b"],
        f_se: ["a", "-123.45e+67", "b"],
    },
    "hello": {
        i_u: ["hello"],
        i_s: ["hello"],
        f_u: ["hello"],
        f_s: ["hello"],
        f_ue: ["hello"],
        f_se: ["hello"],
    },
    "abc12.34.56-7def": {
        i_u: ["abc", "12", ".", "34", ".", "56", "-", "7", "def"],
        i_s: ["abc", "12", ".", "34", ".", "56", "", "-7", "def"],
        f_u: ["abc", "12.34", "", ".56", "-", "7", "def"],
        f_s: ["abc", "12.34", "", ".56", "", "-7", "def"],
        f_ue: ["abc", "12.34", "", ".56", "-", "7", "def"],
        f_se: ["abc", "12.34", "", ".56", "", "-7", "def"],
    },
    "a1b2c3d4e5e6": {
        i_u: ["a", "1", "b", "2", "c", "3", "d", "4", "e", "5", "e", "6", ""],
        i_s: ["a", "1", "b", "2", "c", "3", "d", "4", "e", "5", "e", "6", ""],
        f_u: ["a", "1", "b", "2", "c", "3", "d", "4", "e", "5", "e", "6", ""],
        f_s: ["a", "1", "b", "2", "c", "3", "d", "4", "e", "5", "e", "6", ""],
        f_ue: ["a", "1", "b", "2", "c", "3", "d", "4e5", "e", "6", ""],
        f_se: ["a", "1", "b", "2", "c", "3", "d", "4e5", "e", "6", ""],
    },
    "eleven۱۱eleven11eleven১১": {  # All of these are the decimal 11
        i_u: ["eleven", "۱۱", "eleven", "11", "eleven", "১১", ""],
        i_s: ["eleven", "۱۱", "eleven", "11", "eleven", "১১", ""],
        f_u: ["eleven", "۱۱", "eleven", "11", "eleven", "১১", ""],
        f_s: ["eleven", "۱۱", "eleven", "11", "eleven", "১১", ""],
        f_ue: ["eleven", "۱۱", "eleven", "11", "eleven", "১১", ""],
        f_se: ["eleven", "۱۱", "eleven", "11", "eleven", "১১", ""],
    },
    "12①②ⅠⅡ⅓": {  # Two decimals, Two digits, Two numerals, fraction
        i_u: ["", "12", "", "①", "", "②", "ⅠⅡ⅓"],
        i_s: ["", "12", "", "①", "", "②", "ⅠⅡ⅓"],
        f_u: ["", "12", "", "①", "", "②", "", "Ⅰ", "", "Ⅱ", "", "⅓", ""],
        f_s: ["", "12", "", "①", "", "②", "", "Ⅰ", "", "Ⅱ", "", "⅓", ""],
        f_ue: ["", "12", "", "①", "", "②", "", "Ⅰ", "", "Ⅱ", "", "⅓", ""],
        f_se: ["", "12", "", "①", "", "②", "", "Ⅰ", "", "Ⅱ", "", "⅓", ""],
    },
}


# From the above collections, create the parametrized tests and labels.
regex_params = [
    (given, expected, regex)
    for given, values in regex_tests.items()
    for regex, expected in values.items()
]
labels = ["{}-{}".format(given, regex_names[regex]) for given, _, regex in regex_params]


@pytest.mark.parametrize("x, expected, regex", regex_params, ids=labels)
def test_regex_splits_correctly(
    x: str, expected: List[str], regex: Pattern[str]
) -> None:
    # noinspection PyUnresolvedReferences
    assert regex.split(x) == expected


@pytest.mark.parametrize(
    "given, expected",
    [
        (ns.INT, NumRegex.int_nosign()),
        (ns.INT | ns.UNSIGNED, NumRegex.int_nosign()),
        (ns.INT | ns.SIGNED, NumRegex.int_sign()),
        (ns.INT | ns.NOEXP, NumRegex.int_nosign()),
        (ns.FLOAT, NumRegex.float_nosign_exp()),
        (ns.FLOAT | ns.UNSIGNED, NumRegex.float_nosign_exp()),
        (ns.FLOAT | ns.SIGNED, NumRegex.float_sign_exp()),
        (ns.FLOAT | ns.NOEXP, NumRegex.float_nosign_noexp()),
        (ns.FLOAT | ns.SIGNED | ns.NOEXP, NumRegex.float_sign_noexp()),
        (ns.FLOAT | ns.UNSIGNED | ns.NOEXP, NumRegex.float_nosign_noexp()),
    ],
)
def test_regex_chooser(given: NSType, expected: Pattern[str]) -> None:
    assert numeric_regex_chooser(given) == expected.pattern[1:-1]  # remove parens
