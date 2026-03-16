# -*- coding: utf-8 -*-
"""\
Here are a collection of examples of how this module can be used.
See the README or the natsort homepage for more details.
"""

from operator import itemgetter
from typing import List

import pytest
from natsort import (
    as_ascii,
    as_utf8,
    decoder,
    humansorted,
    index_humansorted,
    index_natsorted,
    index_realsorted,
    natsorted,
    ns,
    order_by_index,
    realsorted,
)


@pytest.fixture
def version_list() -> List[str]:
    return ["1.9.9a", "1.11", "1.9.9b", "1.11.4", "1.10.1"]


@pytest.fixture
def float_list() -> List[str]:
    return ["a50", "a51.", "a50.31", "a-50", "a50.4", "a5.034e1", "a50.300"]


@pytest.fixture
def fruit_list() -> List[str]:
    return ["Apple", "corn", "Corn", "Banana", "apple", "banana"]


def test_decoder_returns_function_that_decodes_bytes_but_returns_other_as_is() -> None:
    func = decoder("latin1")
    str_obj = "bytes"
    int_obj = 14
    assert func(b"bytes") == str_obj
    assert func(int_obj) is int_obj  # returns as-is, same object ID
    assert func(str_obj) is str_obj  # same object returned b/c only bytes has decode


def test_as_ascii_converts_bytes_to_ascii() -> None:
    assert decoder("ascii")(b"bytes") == as_ascii(b"bytes")


def test_as_utf8_converts_bytes_to_utf8() -> None:
    assert decoder("utf8")(b"bytes") == as_utf8(b"bytes")


def test_realsorted_is_identical_to_natsorted_with_real_alg(
    float_list: List[str],
) -> None:
    assert realsorted(float_list) == natsorted(float_list, alg=ns.REAL)


@pytest.mark.usefixtures("with_locale_en_us")
def test_humansorted_is_identical_to_natsorted_with_locale_alg(
    fruit_list: List[str],
) -> None:
    assert humansorted(fruit_list) == natsorted(fruit_list, alg=ns.LOCALE)


def test_index_natsorted_returns_integer_list_of_sort_order_for_input_list() -> None:
    given = ["num3", "num5", "num2"]
    other = ["foo", "bar", "baz"]
    index = index_natsorted(given)
    assert index == [2, 0, 1]
    assert [given[i] for i in index] == ["num2", "num3", "num5"]
    assert [other[i] for i in index] == ["baz", "foo", "bar"]


def test_index_natsorted_reverse() -> None:
    given = ["num3", "num5", "num2"]
    assert index_natsorted(given, reverse=True) == index_natsorted(given)[::-1]


def test_index_natsorted_applies_key_function_before_sorting() -> None:
    given = [("a", "num3"), ("b", "num5"), ("c", "num2")]
    expected = [2, 0, 1]
    assert index_natsorted(given, key=itemgetter(1)) == expected


def test_index_natsorted_can_presort() -> None:
    expected = [2, 0, 3, 1]
    given = ["a1", "a1.4500", "a01", "a1.45"]
    result = index_natsorted(given, alg=ns.FLOAT | ns.PRESORT)
    assert result == expected


def test_index_realsorted_is_identical_to_index_natsorted_with_real_alg(
    float_list: List[str],
) -> None:
    assert index_realsorted(float_list) == index_natsorted(float_list, alg=ns.REAL)


@pytest.mark.usefixtures("with_locale_en_us")
def test_index_humansorted_is_identical_to_index_natsorted_with_locale_alg(
    fruit_list: List[str],
) -> None:
    assert index_humansorted(fruit_list) == index_natsorted(fruit_list, alg=ns.LOCALE)


def test_order_by_index_sorts_list_according_to_order_of_integer_list() -> None:
    given = ["num3", "num5", "num2"]
    index = [2, 0, 1]
    expected = [given[i] for i in index]
    assert expected == ["num2", "num3", "num5"]
    assert order_by_index(given, index) == expected


def test_order_by_index_returns_generator_with_iter_true() -> None:
    given = ["num3", "num5", "num2"]
    index = [2, 0, 1]
    assert order_by_index(given, index, True) != [given[i] for i in index]
    assert list(order_by_index(given, index, True)) == [given[i] for i in index]
