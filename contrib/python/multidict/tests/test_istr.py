import gc
import sys
from typing import Callable, Type

import pytest

IMPLEMENTATION = getattr(sys, "implementation")  # to suppress mypy error


def test_ctor(case_insensitive_str_class: Type[str]) -> None:
    s = case_insensitive_str_class()
    assert "" == s


def test_ctor_str(case_insensitive_str_class: Type[str]) -> None:
    s = case_insensitive_str_class("aBcD")
    assert "aBcD" == s


def test_ctor_istr(case_insensitive_str_class: Type[str]) -> None:
    s = case_insensitive_str_class("A")
    s2 = case_insensitive_str_class(s)
    assert "A" == s
    assert s == s2


def test_ctor_buffer(case_insensitive_str_class: Type[str]) -> None:
    s = case_insensitive_str_class(b"aBc")
    assert "b'aBc'" == s


def test_ctor_repr(case_insensitive_str_class: Type[str]) -> None:
    s = case_insensitive_str_class(None)
    assert "None" == s


def test_str(case_insensitive_str_class: Type[str]) -> None:
    s = case_insensitive_str_class("aBcD")
    s1 = str(s)
    assert s1 == "aBcD"
    assert type(s1) is str


def test_eq(case_insensitive_str_class: Type[str]) -> None:
    s1 = "Abc"
    s2 = case_insensitive_str_class(s1)
    assert s1 == s2


@pytest.fixture
def create_istrs(case_insensitive_str_class: Type[str]) -> Callable[[], None]:
    """Make a callable populating memory with a few ``istr`` objects."""

    def _create_strs() -> None:
        case_insensitive_str_class("foobarbaz")
        istr2 = case_insensitive_str_class()
        case_insensitive_str_class(istr2)

    return _create_strs


@pytest.mark.skipif(
    IMPLEMENTATION.name != "cpython",
    reason="PyPy has different GC implementation",
)
def test_leak(create_istrs: Callable[[], None]) -> None:
    gc.collect()
    cnt = len(gc.get_objects())
    for _ in range(10000):
        create_istrs()

    gc.collect()
    cnt2 = len(gc.get_objects())
    assert abs(cnt - cnt2) < 10  # on PyPy these numbers are not equal
