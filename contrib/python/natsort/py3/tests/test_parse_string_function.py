# -*- coding: utf-8 -*-
"""These test the utils.py functions."""

import unicodedata
from typing import Any, Callable, Iterable, List, Tuple, Union

import pytest
from hypothesis import given
from hypothesis.strategies import floats, integers, lists, text
from natsort.compat.fastnumbers import try_float
from natsort.ns_enum import NSType, NS_DUMB, ns
from natsort.utils import (
    FinalTransform,
    NumericalRegularExpressions as NumRegex,
    StrParser,
)
from natsort.utils import parse_string_factory


class CustomTuple(Tuple[Any, ...]):
    """Used to ensure what is given during testing is what is returned."""

    original: Any = None


def input_transform(x: Any) -> Any:
    """Make uppercase."""
    try:
        return x.upper()
    except AttributeError:
        return x


def final_transform(x: Iterable[Any], original: str) -> FinalTransform:
    """Make the input a CustomTuple."""
    t = CustomTuple(x)
    t.original = original
    return t


def parse_string_func_factory(alg: NSType) -> StrParser:
    """A parse_string_factory result with sample arguments."""
    sep = ""
    return parse_string_factory(
        alg,
        sep,
        NumRegex.int_nosign().split,
        input_transform,
        lambda x: try_float(x, map=True),
        final_transform,
    )


@given(x=floats() | integers())
def test_parse_string_factory_raises_type_error_if_given_number(
    x: Union[int, float]
) -> None:
    parse_string_func = parse_string_func_factory(ns.DEFAULT)
    with pytest.raises(TypeError):
        assert parse_string_func(x)  # type: ignore


# noinspection PyCallingNonCallable
@pytest.mark.parametrize(
    "alg, orig_func",
    [
        (ns.DEFAULT, lambda x: x.upper()),
        (ns.LOCALE, lambda x: x.upper()),
        (ns.LOCALE | NS_DUMB, lambda x: x),  # This changes the "original" handling.
    ],
)
@given(
    x=lists(
        elements=floats(allow_nan=False) | text() | integers(), min_size=1, max_size=10
    )
)
@pytest.mark.usefixtures("with_locale_en_us")
def test_parse_string_factory_invariance(
    x: List[Union[float, str, int]], alg: NSType, orig_func: Callable[[str], str]
) -> None:
    parse_string_func = parse_string_func_factory(alg)
    # parse_string_factory is the high-level combination of several dedicated
    # functions involved in splitting and manipulating a string. The details of
    # what those functions do is not relevant to testing parse_string_factory.
    # What is relevant is that the form of the output matches the invariant
    # that even elements are string and odd are numerical. That each component
    # function is doing what it should is tested elsewhere.
    value = "".join(map(str, x))  # Convert the input to a single string.
    result = parse_string_func(value)
    result_types = list(map(type, result))
    expected_types = [str if i % 2 == 0 else float for i in range(len(result))]
    assert result_types == expected_types

    # The result is in our CustomTuple.
    assert isinstance(result, CustomTuple)

    # Original should have gone through the "input_transform"
    # which is uppercase in these tests.
    assert result.original == orig_func(unicodedata.normalize("NFD", value))
