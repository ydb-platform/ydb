# -*- coding: utf-8 -*-
"""These test the utils.py functions."""
from typing import Any, List, NoReturn, Tuple, Union, cast

from hypothesis import given
from hypothesis.strategies import binary, floats, integers, lists, text
from natsort.utils import natsort_key


def str_func(x: Any) -> Tuple[str]:
    if isinstance(x, str):
        return (x,)
    else:
        raise TypeError("Not a str!")


def fail(_: Any) -> NoReturn:
    raise AssertionError("This should never be reached!")


@given(floats(allow_nan=False) | integers())
def test_natsort_key_with_numeric_input_takes_number_path(x: Union[float, int]) -> None:
    assert natsort_key(x, None, str_func, fail, lambda y: ("", y))[1] is x


@given(binary().filter(bool))
def test_natsort_key_with_bytes_input_takes_bytes_path(x: bytes) -> None:
    assert natsort_key(x, None, str_func, lambda y: (y,), fail)[0] is x


@given(text())
def test_natsort_key_with_text_input_takes_string_path(x: str) -> None:
    assert natsort_key(x, None, str_func, fail, fail)[0] is x


@given(lists(elements=text(), min_size=1, max_size=10))
def test_natsort_key_with_nested_input_takes_nested_path(x: List[str]) -> None:
    assert natsort_key(x, None, str_func, fail, fail) == tuple((y,) for y in x)


@given(text())
def test_natsort_key_with_key_argument_applies_key_before_processing(x: str) -> None:
    assert natsort_key(x, len, str_func, fail, lambda y: ("", cast(int, y)))[1] == len(
        x
    )
