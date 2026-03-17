# -*- coding: utf-8 -*-
"""These test the utils.py functions."""

from typing import Optional, Tuple, Union

import pytest
from hypothesis import given
from hypothesis.strategies import floats, integers
from natsort.ns_enum import NSType, ns
from natsort.utils import NumTransformer, parse_number_or_none_factory


@pytest.mark.usefixtures("with_locale_en_us")
@pytest.mark.parametrize(
    "alg, example_func",
    [
        (ns.DEFAULT, lambda x: ("", x)),
        (ns.PATH, lambda x: (("", x),)),
        (ns.UNGROUPLETTERS | ns.LOCALE, lambda x: (("xx",), ("", x))),
        (ns.PATH | ns.UNGROUPLETTERS | ns.LOCALE, lambda x: ((("xx",), ("", x)),)),
    ],
)
@given(x=floats(allow_nan=False, allow_infinity=False) | integers())
def test_parse_number_factory_makes_function_that_returns_tuple(
    x: Union[float, int], alg: NSType, example_func: NumTransformer
) -> None:
    parse_number_func = parse_number_or_none_factory(alg, "", "xx")
    assert parse_number_func(x) == example_func(x)


@pytest.mark.parametrize(
    "alg, x, result",
    [
        (ns.DEFAULT, 57, ("", 57)),
        (
            ns.DEFAULT,
            float("nan"),
            ("", float("-inf"), "1"),
        ),  # NaN transformed to -infinity
        (
            ns.NANLAST,
            float("nan"),
            ("", float("+inf"), "3"),
        ),  # NANLAST makes it +infinity
        (ns.DEFAULT, None, ("", float("-inf"), "2")),  # None transformed to -infinity
        (ns.NANLAST, None, ("", float("+inf"), "2")),  # NANLAST makes it +infinity
        (ns.DEFAULT, float("-inf"), ("", float("-inf"), "3")),
        (ns.NANLAST, float("+inf"), ("", float("+inf"), "1")),
    ],
)
def test_parse_number_factory_treats_nan_and_none_special(
    alg: NSType, x: Optional[Union[float, int]], result: Tuple[str, Union[float, int]]
) -> None:
    parse_number_func = parse_number_or_none_factory(alg, "", "xx")
    assert parse_number_func(x) == result
