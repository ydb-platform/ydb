# -*- coding: utf-8 -*-
"""These test the utils.py functions."""

from functools import partial
from typing import Any, Callable, FrozenSet, Union

import pytest
from hypothesis import assume, example, given
from hypothesis.strategies import floats, integers, text
from natsort.compat.fastnumbers import try_float, try_int
from natsort.compat.locale import get_strxfrm
from natsort.ns_enum import NSType, NS_DUMB, ns
from natsort.utils import groupletters, string_component_transform_factory

# There are some unicode values that are known failures with the builtin locale
# library on OSX and some other BSD-based systems that has nothing to do with
# natsort (a ValueError is raised by strxfrm). Let's filter them out.
try:
    bad_uni_chars = frozenset(chr(x) for x in range(0x10FEFD, 0x10FFFF + 1))
except ValueError:
    # Narrow unicode build... no worries.
    bad_uni_chars = frozenset()


def no_bad_uni_chars(x: str, _bad_chars: FrozenSet[str] = bad_uni_chars) -> bool:
    """Ensure text does not contain bad unicode characters"""
    return not any(y in _bad_chars for y in x)


def no_null(x: str) -> bool:
    """Ensure text does not contain a null character."""
    return "\0" not in x


def input_is_ok_with_locale(x: str) -> bool:
    """Ensure this input won't cause locale.strxfrm to barf"""
    # Bad input can cause an OSError if the OS doesn't support the value
    try:
        get_strxfrm()(x)
    except OSError:
        return False
    else:
        return True


@pytest.mark.parametrize(
    "alg, example_func",
    [
        (ns.INT, partial(try_int, map=True)),
        (ns.DEFAULT, partial(try_int, map=True)),
        (ns.FLOAT, partial(try_float, map=True, nan=float("-inf"))),
        (ns.FLOAT | ns.NANLAST, partial(try_float, map=True, nan=float("+inf"))),
        (ns.GROUPLETTERS, partial(try_int, map=True, on_fail=groupletters)),
        (ns.LOCALE, partial(try_int, map=True, on_fail=lambda x: get_strxfrm()(x))),
        (
            ns.GROUPLETTERS | ns.LOCALE,
            partial(
                try_int, map=True, on_fail=lambda x: get_strxfrm()(groupletters(x))
            ),
        ),
        (
            NS_DUMB | ns.LOCALE,
            partial(
                try_int, map=True, on_fail=lambda x: get_strxfrm()(groupletters(x))
            ),
        ),
        (
            ns.GROUPLETTERS | ns.LOCALE | ns.FLOAT | ns.NANLAST,
            partial(
                try_float,
                map=True,
                on_fail=lambda x: get_strxfrm()(groupletters(x)),
                nan=float("+inf"),
            ),
        ),
    ],
)
@example(x=float("nan"))
@example(x="Å")
@given(
    x=integers()
    | floats()
    | text().filter(bool).filter(no_bad_uni_chars).filter(no_null)
)
@pytest.mark.usefixtures("with_locale_en_us")
def test_string_component_transform_factory(
    x: Union[str, float, int], alg: NSType, example_func: Callable[[str], Any]
) -> None:
    string_component_transform_func = string_component_transform_factory(alg)
    x = str(x)
    assume(input_is_ok_with_locale(x))
    try:
        assert list(string_component_transform_func(x)) == list(example_func(x))
    except ValueError as e:  # handle broken locale lib on OSX.
        if "is not in range" not in str(e):
            raise
