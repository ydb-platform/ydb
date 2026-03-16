# -*- coding: utf-8 -*-
"""These test the utils.py functions."""
from typing import Callable

import pytest
from hypothesis import example, given
from hypothesis.strategies import integers, text
from natsort.ns_enum import NSType, NS_DUMB, ns
from natsort.utils import input_string_transform_factory


def thousands_separated_int(n: str) -> str:
    """Insert thousands separators in an int."""
    new_int = ""
    for i, y in enumerate(reversed(n), 1):
        new_int = y + new_int
        # For every third digit, insert a thousands separator.
        if i % 3 == 0 and i != len(n):
            new_int = "," + new_int
    return new_int


@given(text())
def test_input_string_transform_factory_is_no_op_for_no_alg_options(x: str) -> None:
    input_string_transform_func = input_string_transform_factory(ns.DEFAULT)
    assert input_string_transform_func(x) is x


@pytest.mark.parametrize(
    "alg, example_func",
    [
        (ns.IGNORECASE, lambda x: x.casefold()),
        (NS_DUMB, lambda x: x.swapcase()),
        (ns.LOWERCASEFIRST, lambda x: x.swapcase()),
        (NS_DUMB | ns.LOWERCASEFIRST, lambda x: x),  # No-op
        (ns.IGNORECASE | ns.LOWERCASEFIRST, lambda x: x.swapcase().casefold()),
    ],
)
@given(x=text())
def test_input_string_transform_factory(
    x: str, alg: NSType, example_func: Callable[[str], str]
) -> None:
    input_string_transform_func = input_string_transform_factory(alg)
    assert input_string_transform_func(x) == example_func(x)


@example(12543642642534980)  # 12,543,642,642,534,980 => 12543642642534980
@given(x=integers(min_value=1000))
@pytest.mark.usefixtures("with_locale_en_us")
def test_input_string_transform_factory_cleans_thousands(x: int) -> None:
    int_str = str(x).rstrip("lL")
    thousands_int_str = thousands_separated_int(int_str)
    assert thousands_int_str.replace(",", "") != thousands_int_str

    input_string_transform_func = input_string_transform_factory(ns.LOCALE)
    assert input_string_transform_func(thousands_int_str) == int_str

    # Using LOCALEALPHA does not affect numbers.
    input_string_transform_func_no_op = input_string_transform_factory(ns.LOCALEALPHA)
    assert input_string_transform_func_no_op(thousands_int_str) == thousands_int_str


# These might be too much to test with hypothesis.


@pytest.mark.parametrize(
    "x, expected",
    [
        ("12,543,642642.5345,34980", "12543,642642.5345,34980"),
        ("12,59443,642,642.53,4534980", "12,59443,642642.53,4534980"),  # No change
        ("12543,642,642.5,34534980", "12543,642642.5,34534980"),
    ],
)
@pytest.mark.usefixtures("with_locale_en_us")
def test_input_string_transform_factory_handles_us_locale(
    x: str, expected: str
) -> None:
    input_string_transform_func = input_string_transform_factory(ns.LOCALE)
    assert input_string_transform_func(x) == expected


@pytest.mark.parametrize(
    "x, expected",
    [
        ("12.543.642642,5345.34980", "12543.642642,5345.34980"),
        ("12.59443.642.642,53.4534980", "12.59443.642642,53.4534980"),  # No change
        ("12543.642.642,5.34534980", "12543.642642,5.34534980"),
    ],
)
@pytest.mark.usefixtures("with_locale_de_de")
def test_input_string_transform_factory_handles_de_locale(
    x: str, expected: str
) -> None:
    input_string_transform_func = input_string_transform_factory(ns.LOCALE)
    assert input_string_transform_func(x) == expected


@pytest.mark.parametrize(
    "alg, expected",
    [
        (ns.LOCALE, "1543,753"),  # Does nothing without FLOAT
        (ns.LOCALE | ns.FLOAT, "1543.753"),
        (ns.LOCALEALPHA, "1543,753"),  # LOCALEALPHA won't do anything, need LOCALENUM
    ],
)
@pytest.mark.usefixtures("with_locale_de_de")
def test_input_string_transform_factory_handles_german_locale(
    alg: NSType, expected: str
) -> None:
    input_string_transform_func = input_string_transform_factory(alg)
    assert input_string_transform_func("1543,753") == expected


@pytest.mark.usefixtures("with_locale_de_de")
def test_input_string_transform_factory_does_nothing_with_non_num_input() -> None:
    input_string_transform_func = input_string_transform_factory(ns.LOCALE | ns.FLOAT)
    expected = "154s,t53"
    assert input_string_transform_func("154s,t53") == expected
