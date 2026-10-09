from __future__ import annotations

import contextlib
import gc
import operator
import platform
import sys
import threading
import time
import weakref
from collections import deque
from collections.abc import Callable, Iterable, Iterator, KeysView, Mapping
from concurrent.futures import ThreadPoolExecutor
from types import ModuleType
from typing import Any, TypeVar, cast

import pytest

import multidict
from multidict import (
    CIMultiDict,
    MultiDict,
    MultiDictProxy,
    MultiMapping,
    MutableMultiMapping,
    istr,
)

_T = TypeVar("_T")
IS_PYPY = platform.python_implementation() == "PyPy"


def chained_callable(
    module: ModuleType,
    callables: Iterable[str],
) -> Callable[..., MultiMapping[int | str] | MutableMultiMapping[int | str]]:
    """
    Return callable that will get and call all given objects in module in
    exact order.
    """

    def chained_call(
        *args: object,
        **kwargs: object,
    ) -> MultiMapping[int | str] | MutableMultiMapping[int | str]:
        callable_chain = (getattr(module, name) for name in callables)
        first_callable = next(callable_chain)

        value = first_callable(*args, **kwargs)
        for element in callable_chain:
            value = element(value)

        return cast(
            MultiMapping[int | str] | MutableMultiMapping[int | str],
            value,
        )

    return chained_call


@pytest.fixture
def cls(
    request: pytest.FixtureRequest,
    multidict_module: ModuleType,
) -> Callable[..., MultiMapping[int | str] | MutableMultiMapping[int | str]]:
    """Make a callable from multidict module, requested by name."""
    return chained_callable(multidict_module, request.param)


def test_exposed_names(any_multidict_class_name: str) -> None:
    assert any_multidict_class_name in multidict.__all__


@pytest.mark.parametrize(
    ("cls", "key_cls"),
    (
        (("MultiDict",), str),
        (
            ("MultiDict", "MultiDictProxy"),
            str,
        ),
    ),
    indirect=["cls"],
)
def test__iter__types(
    cls: type[MultiDict[str | int]],
    key_cls: type[str],
) -> None:
    d = cls([("key", "one"), ("key2", "two"), ("key", 3)])
    for i in d:
        assert type(i) is key_cls, (type(i), key_cls)


def test_proxy_copy(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
) -> None:
    d1 = any_multidict_class(key="value", a="b")
    p1 = any_multidict_proxy_class(d1)

    d2 = p1.copy()
    assert d1 == d2
    assert d1 is not d2


def test_multidict_subclassing(
    any_multidict_class: type[MultiDict[str]],
) -> None:
    class DummyMultidict(any_multidict_class):  # type: ignore[valid-type,misc]
        pass


def test_multidict_proxy_subclassing(
    any_multidict_proxy_class: type[MultiDictProxy[str]],
) -> None:
    class DummyMultidictProxy(
        any_multidict_proxy_class,  # type: ignore[valid-type,misc]
    ):
        pass


def test_multidict_subclass_new_and_init_are_called(
    any_multidict_class: type[MultiDict[str]],
) -> None:
    calls = []

    class DummyMultidict(any_multidict_class):  # type: ignore[valid-type,misc]
        def __new__(cls, *args: object, **kwargs: object) -> DummyMultidict:
            calls.append("new")
            return cast(DummyMultidict, super().__new__(cls))

        def __init__(self, *args: object, **kwargs: object) -> None:
            calls.append("init")
            super().__init__(*args, **kwargs)

    d = DummyMultidict([("key", "value")], extra="1")

    assert calls == ["new", "init"]
    assert d == {"key": "value", "extra": "1"}


def test_multidict_proxy_subclass_init_is_called(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
) -> None:
    calls = []

    class DummyMultidictProxy(
        any_multidict_proxy_class,  # type: ignore[valid-type,misc]
    ):
        def __init__(self, arg: object) -> None:
            calls.append("init")
            super().__init__(arg)

    md = any_multidict_class(key="value")
    p = DummyMultidictProxy(md)

    assert calls == ["init"]
    assert p == md


class BaseMultiDictTest:
    def test_instantiate__empty(self, cls: type[MutableMultiMapping[str]]) -> None:
        d = cls()
        empty: Mapping[str, str] = {}
        assert d == empty
        assert len(d) == 0
        assert list(d.keys()) == []
        assert list(d.values()) == []
        assert list(d.items()) == []

        assert cls() != list()  # type: ignore[comparison-overlap]
        with pytest.raises(TypeError, match=r"3 were given"):
            cls(("key1", "value1"), ("key2", "value2"))  # type: ignore[call-arg]  # noqa: E501

    @pytest.mark.parametrize("arg0", ([("key", "value1")], {"key": "value1"}))
    def test_instantiate__from_arg0(
        self,
        cls: type[MultiDict[str]],
        arg0: list[tuple[str, str]] | dict[str, str],
    ) -> None:
        d = cls(arg0)

        assert d == {"key": "value1"}
        assert len(d) == 1
        assert list(d.keys()) == ["key"]
        assert list(d.values()) == ["value1"]
        assert list(d.items()) == [("key", "value1")]

    def test_instantiate__with_kwargs(
        self,
        cls: type[MultiDict[str]],
    ) -> None:
        d = cls([("key", "value1")], key2="value2")

        assert d == {"key": "value1", "key2": "value2"}
        assert len(d) == 2
        assert sorted(d.keys()) == ["key", "key2"]
        assert sorted(d.values()) == ["value1", "value2"]
        assert sorted(d.items()) == [("key", "value1"), ("key2", "value2")]

    def test_instantiate__from_generator(
        self, cls: type[MultiDict[int]] | type[CIMultiDict[int]]
    ) -> None:
        d = cls((str(i), i) for i in range(2))

        assert d == {"0": 0, "1": 1}
        assert len(d) == 2
        assert sorted(d.keys()) == ["0", "1"]
        assert sorted(d.values()) == [0, 1]
        assert sorted(d.items()) == [("0", 0), ("1", 1)]

    def test_instantiate__from_list_of_lists(
        self,
        cls: type[MutableMultiMapping[str]],
    ) -> None:
        # Should work at runtime, but won't type check.
        d = cls([["key", "value1"]])  # type: ignore[call-arg]
        assert d == {"key": "value1"}

    def test_instantiate__from_list_of_custom_pairs(
        self,
        cls: type[MultiDict[str]],
    ) -> None:
        class Pair:
            def __len__(self) -> int:
                return 2

            def __getitem__(self, pos: int) -> str:
                return ("key", "value1")[pos]

        # Works at runtime, but won't type check.
        d = cls([Pair()])  # type: ignore[list-item]
        assert d == {"key": "value1"}

    def test_getone(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")], key="value2")

        assert d.getone("key") == "value1"
        assert d.get("key") == "value1"
        assert d["key"] == "value1"

        with pytest.raises(KeyError, match="key2"):
            d["key2"]
        with pytest.raises(KeyError, match="key2"):
            d.getone("key2")

        assert d.getone("key2", "default") == "default"

    def test_call_with_kwargs(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("present", "value")])
        assert d.getall(default="missing", key="notfound") == "missing"

    def test__iter__(
        self,
        cls: type[MultiDict[str | int]] | type[CIMultiDict[str | int]],
    ) -> None:
        d = cls([("key", "one"), ("key2", "two"), ("key", 3)])
        assert list(d) == ["key", "key2", "key"]

    def test__contains(
        self,
        cls: type[MultiDict[str | int]] | type[CIMultiDict[str | int]],
    ) -> None:
        d = cls([("key", "one"), ("key2", "two"), ("key", 3)])

        assert list(d) == ["key", "key2", "key"]

        assert "key" in d
        assert "key2" in d

        assert "foo" not in d
        assert 42 not in d  # type: ignore[comparison-overlap]

    def test_keys__contains(
        self,
        cls: type[MultiDict[str | int]] | type[CIMultiDict[str | int]],
    ) -> None:
        d = cls([("key", "one"), ("key2", "two"), ("key", 3)])

        assert list(d.keys()) == ["key", "key2", "key"]

        assert "key" in d.keys()
        assert "key2" in d.keys()

        assert "foo" not in d.keys()
        assert 42 not in d.keys()  # type: ignore[comparison-overlap]

    def test_values__contains(
        self,
        cls: type[MultiDict[str | int]] | type[CIMultiDict[str | int]],
    ) -> None:
        d = cls([("key", "one"), ("key", "two"), ("key", 3)])

        assert list(d.values()) == ["one", "two", 3]

        assert "one" in d.values()
        assert "two" in d.values()
        assert 3 in d.values()

        assert "foo" not in d.values()

    def test_items__contains(
        self,
        cls: type[MultiDict[str | int]] | type[CIMultiDict[str | int]],
    ) -> None:
        d = cls([("key", "one"), ("key", "two"), ("key", 3)])

        assert list(d.items()) == [("key", "one"), ("key", "two"), ("key", 3)]

        assert ("key", "one") in d.items()
        assert ("key", "two") in d.items()
        assert ("key", 3) in d.items()

        assert ("foo", "bar") not in d.items()
        assert (42, 3) not in d.items()  # type: ignore[comparison-overlap]
        assert 42 not in d.items()  # type: ignore[operator]

    def test_cannot_create_from_unaccepted(
        self,
        cls: type[MutableMultiMapping[str]],
    ) -> None:
        with pytest.raises(
            ValueError,
            match=r"^multidict update sequence element #0 has length 3; 2 is required$",
        ):
            cls([(1, 2, 3)])  # type: ignore[call-arg]

    def test_cannot_create_from_item_with_failing_getitem(
        self,
        cls: type[MutableMultiMapping[str]],
    ) -> None:
        class BadItem:
            def __len__(self) -> int:
                return 2

            def __getitem__(self, i: int) -> object:
                raise RuntimeError("intentional getitem failure")

        with pytest.raises(
            ValueError,
            match=r"^multidict update sequence element #0's key could not be fetched$",
        ):
            cls([BadItem()])  # type: ignore[call-arg]

    def test_cannot_create_from_item_with_failing_getitem_value(
        self,
        cls: type[MutableMultiMapping[str]],
    ) -> None:
        class BadValueItem:
            def __len__(self) -> int:
                return 2

            def __getitem__(self, i: int) -> object:
                if i == 0:
                    return "key"
                raise RuntimeError("intentional getitem failure")

        with pytest.raises(
            ValueError,
            match=r"^multidict update sequence element #0's value could not be fetched$",
        ):
            cls([BadValueItem()])  # type: ignore[call-arg]

    def test_keys_is_set_less(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert d.keys() < {"key", "key2"}

    @pytest.mark.parametrize(
        ("contents", "expected"),
        (
            ([("key", "value1")], True),
            ([("key", "value1"), ("key2", "value2")], True),
            ([("key", "value1"), ("key2", "value2"), ("key3", "value3")], False),
            ([("key", "value1"), ("key3", "value3")], False),
        ),
    )
    def test_keys_is_set_less_equal(
        self,
        cls: type[MultiDict[str]],
        contents: list[tuple[str, str]],
        expected: bool,
    ) -> None:
        d = cls(contents)

        result = d.keys() <= {"key", "key2"}
        assert result is expected

    def test_keys_is_set_equal(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert d.keys() == {"key"}

    def test_items_is_set_equal(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert d.items() == {("key", "value1")}

    def test_keys_is_set_greater(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert d.keys() > {"key"}

    @pytest.mark.parametrize(
        ("set_", "expected"),
        (
            ({"key"}, True),
            ({"key", "key2"}, True),
            ({"key", "key2", "key3"}, False),
            ({"key3"}, False),
        ),
    )
    def test_keys_is_set_greater_equal(
        self, cls: type[MultiDict[str]], set_: set[str], expected: bool
    ) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        result = d.keys() >= set_
        assert result is expected

    def test_keys_less_than_not_implemented(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        sentinel_operation_result = object()

        class RightOperand:
            def __gt__(self, other: KeysView[str]) -> object:
                assert isinstance(other, KeysView)
                return sentinel_operation_result

        assert (d.keys() < RightOperand()) is sentinel_operation_result

    def test_keys_less_than_or_equal_not_implemented(
        self, cls: type[MultiDict[str]]
    ) -> None:
        d = cls([("key", "value1")])

        sentinel_operation_result = object()

        class RightOperand:
            def __ge__(self, other: KeysView[str]) -> object:
                assert isinstance(other, KeysView)
                return sentinel_operation_result

        assert (d.keys() <= RightOperand()) is sentinel_operation_result

    def test_keys_greater_than_not_implemented(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        sentinel_operation_result = object()

        class RightOperand:
            def __lt__(self, other: KeysView[str]) -> object:
                assert isinstance(other, KeysView)
                return sentinel_operation_result

        assert (d.keys() > RightOperand()) is sentinel_operation_result

    def test_keys_greater_than_or_equal_not_implemented(
        self, cls: type[MultiDict[str]]
    ) -> None:
        d = cls([("key", "value1")])

        sentinel_operation_result = object()

        class RightOperand:
            def __le__(self, other: KeysView[str]) -> object:
                assert isinstance(other, KeysView)
                return sentinel_operation_result

        assert (d.keys() >= RightOperand()) is sentinel_operation_result

    def test_keys_is_set_not_equal(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert d.keys() != {"key2"}

    def test_keys_not_equal_unrelated_type(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert d.keys() != "other"  # type: ignore[comparison-overlap]

    def test_eq(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key": "value1"} == d

    def test_eq2(self, cls: type[MultiDict[str]]) -> None:
        d1 = cls([("key", "value1")])
        d2 = cls([("key2", "value1")])

        assert d1 != d2

    def test_eq3(self, cls: type[MultiDict[str]]) -> None:
        d1 = cls([("key", "value1")])
        d2 = cls()

        assert d1 != d2

    def test_eq_other_mapping_contains_more_keys(
        self,
        cls: type[MultiDict[str]],
    ) -> None:
        d1 = cls(foo="bar")
        d2 = dict(foo="bar", bar="baz")

        assert d1 != d2

    def test_eq_bad_mapping_len(
        self, cls: type[MultiDict[int]] | type[CIMultiDict[int]]
    ) -> None:
        class BadMapping(Mapping[str, int]):
            def __getitem__(self, key: str) -> int:
                return 1  # pragma: no cover  # `len()` fails earlier

            def __iter__(self) -> Iterator[str]:
                yield "a"  # pragma: no cover  # `len()` fails earlier

            def __len__(self) -> int:
                return 1 // 0

        d1 = cls(a=1)
        d2 = BadMapping()
        with pytest.raises(ZeroDivisionError):
            d1 == d2

    def test_eq_bad_mapping_getitem(
        self,
        cls: type[MultiDict[int]] | type[CIMultiDict[int]],
    ) -> None:
        class BadMapping(Mapping[str, int]):
            def __getitem__(self, key: str) -> int:
                return 1 // 0

            def __iter__(self) -> Iterator[str]:
                yield "a"  # pragma: no cover  # foreign objects no iterated

            def __len__(self) -> int:
                return 1

        d1 = cls(a=1)
        d2 = BadMapping()
        with pytest.raises(ZeroDivisionError):
            d1 == d2

    def test_ne(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert d != {"key": "another_value"}

    def test_and(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key"} == d.keys() & {"key", "key2"}

    def test_and2(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key"} == {"key", "key2"} & d.keys()

    def test_bitwise_and_not_implemented(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        sentinel_operation_result = object()

        class RightOperand:
            def __rand__(self, other: KeysView[str]) -> object:
                assert isinstance(other, KeysView)
                return sentinel_operation_result

        assert d.keys() & RightOperand() is sentinel_operation_result

    def test_bitwise_and_iterable_not_set(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key"} == d.keys() & ["key", "key2"]

    def test_or(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key", "key2"} == d.keys() | {"key2"}

    def test_or2(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key", "key2"} == {"key2"} | d.keys()

    def test_bitwise_or_not_implemented(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        sentinel_operation_result = object()

        class RightOperand:
            def __ror__(self, other: KeysView[str]) -> object:
                assert isinstance(other, KeysView)
                return sentinel_operation_result

        assert d.keys() | RightOperand() is sentinel_operation_result

    def test_bitwise_or_iterable_not_set(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key", "key2"} == d.keys() | ["key2"]

    def test_sub(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert {"key"} == d.keys() - {"key2"}

    def test_sub2(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert {"key3"} == {"key", "key2", "key3"} - d.keys()

    def test_sub_not_implemented(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        sentinel_operation_result = object()

        class RightOperand:
            def __rsub__(self, other: KeysView[str]) -> object:
                assert isinstance(other, KeysView)
                return sentinel_operation_result

        assert d.keys() - RightOperand() is sentinel_operation_result

    def test_sub_iterable_not_set(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert {"key"} == d.keys() - ["key2"]

    def test_xor(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert {"key", "key3"} == d.keys() ^ {"key2", "key3"}

    def test_xor2(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert {"key", "key3"} == {"key2", "key3"} ^ d.keys()

    def test_xor_not_implemented(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        sentinel_operation_result = object()

        class RightOperand:
            def __rxor__(self, other: KeysView[str]) -> object:
                assert isinstance(other, KeysView)
                return sentinel_operation_result

        assert d.keys() ^ RightOperand() is sentinel_operation_result

    def test_xor_iterable_not_set(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert {"key", "key3"} == d.keys() ^ ["key2", "key3"]

    @pytest.mark.parametrize(
        ("key", "value", "expected"),
        (("key2", "v", True), ("key", "value1", False)),
    )
    def test_isdisjoint(
        self, cls: type[MultiDict[str]], key: str, value: str, expected: bool
    ) -> None:
        d = cls([("key", "value1")])
        assert d.items().isdisjoint({(key, value)}) is expected
        assert d.keys().isdisjoint({key}) is expected

    def test_repr_aiohttp_issue_410(self, cls: type[MutableMultiMapping[str]]) -> None:
        d = cls()

        try:
            raise Exception
            pytest.fail("Should never happen")  # pragma: no cover
        except Exception as e:
            repr(d)

            assert sys.exc_info()[1] == e  # noqa: PT017

    def test__repr__quotes_keys(self, cls: type[MultiDict[str]]) -> None:
        # Keys containing quotes must be repr'd as parseable Python literals,
        # not naively wrapped in single quotes.
        d = cls([("a'b", "v")])
        _cls = type(d)

        # repr("a'b") == '"a\'b"' (Python uses double quotes when the string
        # contains a single quote and no double quote).
        assert str(d) == f"<{_cls.__name__}(\"a'b\": 'v')>"

    def test_items__repr__quotes_keys(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("a'b", "v")])
        assert repr(d.items()) == "<_ItemsView(\"a'b\": 'v')>"

    def test_keys__repr__quotes_keys(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("a'b", "v")])
        assert repr(d.keys()) == '<_KeysView("a\'b")>'

    @pytest.mark.parametrize(
        "op",
        (operator.or_, operator.and_, operator.sub, operator.xor),
    )
    @pytest.mark.parametrize("other", ({"other"},))
    def test_op_issue_aiohttp_issue_410(
        self,
        cls: type[MultiDict[str]],
        op: Callable[[object, object], object],
        other: set[str],
    ) -> None:
        d = cls([("key", "value")])

        try:
            raise Exception
            pytest.fail("Should never happen")  # pragma: no cover
        except Exception as e:
            op(d.keys(), other)

            assert sys.exc_info()[1] == e  # noqa: PT017

    def test_weakref(self, cls: type[MutableMultiMapping[str]]) -> None:
        called = False

        def cb(wr: object) -> None:
            nonlocal called
            called = True

        d = cls()
        wr = weakref.ref(d, cb)
        del d
        gc.collect()
        assert called
        del wr

    def test_iter_length_hint_keys(
        self,
        cls: type[MultiDict[int]] | type[CIMultiDict[int]],
    ) -> None:
        md = cls(a=1, b=2)
        it = iter(md.keys())
        assert it.__length_hint__() == 2  # type: ignore[attr-defined]

    def test_iter_length_hint_items(
        self,
        cls: type[MultiDict[int]] | type[CIMultiDict[int]],
    ) -> None:
        md = cls(a=1, b=2)
        it = iter(md.items())
        assert it.__length_hint__() == 2  # type: ignore[attr-defined]

    def test_iter_length_hint_values(
        self,
        cls: type[MultiDict[int]] | type[CIMultiDict[int]],
    ) -> None:
        md = cls(a=1, b=2)
        it = iter(md.values())
        assert it.__length_hint__() == 2

    def test_reversed_keys(
        self,
        cls: type[MultiDict[int | str]] | type[CIMultiDict[int | str]],
    ) -> None:
        d = cls([("key", "one"), ("key2", "two"), ("key", 3)])
        assert list(reversed(d.keys())) == ["key", "key2", "key"]  # type: ignore[call-overload]

    def test_reversed_values(
        self,
        cls: type[MultiDict[int | str]] | type[CIMultiDict[int | str]],
    ) -> None:
        d = cls([("key", "one"), ("key2", "two"), ("key", 3)])
        assert list(reversed(d.values())) == [3, "two", "one"]

    def test_reversed_items(
        self,
        cls: type[MultiDict[int | str]] | type[CIMultiDict[int | str]],
    ) -> None:
        d = cls([("key", "one"), ("key2", "two"), ("key", 3)])
        assert list(reversed(d.items())) == [  # type: ignore[call-overload]
            ("key", 3),
            ("key2", "two"),
            ("key", "one"),
        ]

    def test_reversed_empty(
        self,
        cls: type[MultiDict[int]] | type[CIMultiDict[int]],
    ) -> None:
        d = cls()
        assert list(reversed(d.keys())) == []  # type: ignore[call-overload]
        assert list(reversed(d.values())) == []
        assert list(reversed(d.items())) == []  # type: ignore[call-overload]

    def test_reversed_length_hint(
        self,
        cls: type[MultiDict[int]] | type[CIMultiDict[int]],
    ) -> None:
        md = cls(a=1, b=2)
        keys_it = reversed(md.keys())  # type: ignore[call-overload]
        items_it = reversed(md.items())  # type: ignore[call-overload]
        values_it = reversed(md.values())
        assert keys_it.__length_hint__() == 2
        assert items_it.__length_hint__() == 2
        assert values_it.__length_hint__() == 2  # type: ignore[attr-defined]

    def test_ctor_list_arg_and_kwds(
        self,
        cls: type[MultiDict[int]] | type[CIMultiDict[int]],
    ) -> None:
        arg = [("a", 1)]
        obj = cls(arg, b=2)
        assert list(obj.items()) == [("a", 1), ("b", 2)]
        assert arg == [("a", 1)]

    def test_ctor_tuple_arg_and_kwds(
        self,
        cls: type[MultiDict[int]] | type[CIMultiDict[int]],
    ) -> None:
        arg = (("a", 1),)
        obj = cls(arg, b=2)
        assert list(obj.items()) == [("a", 1), ("b", 2)]
        assert arg == (("a", 1),)

    def test_ctor_deque_arg_and_kwds(
        self,
        cls: type[MultiDict[int]] | type[CIMultiDict[int]],
    ) -> None:
        arg = deque([("a", 1)])
        obj = cls(arg, b=2)
        assert list(obj.items()) == [("a", 1), ("b", 2)]
        assert arg == deque([("a", 1)])

    def test_ucs2_ucs4_comparison(self, cls: type[MultiDict[str]]) -> None:
        expected = [
            ("k\u00e9y", "1"),  # UCS-1
            ("k\u4f60y", "2"),  # UCS-2
            ("k\U0001f600y", "3"),  # UCS-4
            ("k\U0001f600y1", "4"),  # UCS-4
            ("k\U0001f600y2", "5"),  # UCS-4
        ]
        obj = cls(expected)
        for k, v in expected:
            assert obj[k] == v


class TestMultiDict(BaseMultiDictTest):
    @pytest.fixture(
        params=[
            ("MultiDict",),
            ("MultiDict", "MultiDictProxy"),
        ],
    )
    def cls(
        self,
        request: pytest.FixtureRequest,
        multidict_module: ModuleType,
    ) -> Callable[..., MultiMapping[int | str] | MutableMultiMapping[int | str]]:
        """Make a case-sensitive multidict class/proxy constructor."""
        return chained_callable(multidict_module, request.param)

    def test__repr__(self, cls: type[MultiDict[str]]) -> None:
        d = cls()
        _cls = type(d)

        assert str(d) == f"<{_cls.__name__}()>"

        d = cls([("key", "one"), ("key", "two")])

        assert str(d) == f"<{_cls.__name__}('key': 'one', 'key': 'two')>"

    def test__repr___recursive(
        self, any_multidict_class: type[MultiDict[object]]
    ) -> None:
        d = any_multidict_class()
        _cls = type(d)

        d = any_multidict_class()
        d["key"] = d

        assert str(d) == f"<{_cls.__name__}('key': ...)>"

    def test_proxy__repr___recursive(
        self,
        any_multidict_class: type[MultiDict[object]],
        any_multidict_proxy_class: type[MultiDictProxy[object]],
    ) -> None:
        d = any_multidict_class()
        d["key"] = any_multidict_proxy_class(d)
        proxy = any_multidict_proxy_class(d)
        _cls = type(proxy)

        expected = f"<{_cls.__name__}('key': <{_cls.__name__}('key': ...)>)>"
        assert str(proxy) == expected

    def test_getall(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")], key="value2")

        assert d != {"key": "value1"}
        assert len(d) == 2

        assert d.getall("key") == ["value1", "value2"]

        with pytest.raises(KeyError, match="some_key"):
            d.getall("some_key")

        default = object()
        assert d.getall("some_key", default) is default

    def test_preserve_stable_ordering(
        self,
        cls: type[MultiDict[str | int]],
    ) -> None:
        d = cls([("a", 1), ("b", "2"), ("a", 3)])
        s = "&".join(f"{k}={v}" for k, v in d.items())

        assert s == "a=1&b=2&a=3"

    def test_get(self, cls: type[MultiDict[int]]) -> None:
        d = cls([("a", 1), ("a", 2)])
        assert d["a"] == 1

    def test_items__repr__(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")], key="value2")
        expected = "<_ItemsView('key': 'value1', 'key': 'value2')>"
        assert repr(d.items()) == expected

    def test_items__repr__recursive(
        self, any_multidict_class: type[MultiDict[object]]
    ) -> None:
        d = any_multidict_class()
        d["key"] = d.items()
        expected = "<_ItemsView('key': <_ItemsView('key': ...)>)>"
        assert repr(d.items()) == expected

    def test_keys__repr__(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")], key="value2")
        assert repr(d.keys()) == "<_KeysView('key', 'key')>"

    def test_keys__repr__recursive(
        self, case_sensitive_multidict_class: type[MultiDict[object]]
    ) -> None:
        d = case_sensitive_multidict_class()
        kv = d.keys()

        class Key(str):
            def __repr__(self) -> str:
                return repr(kv)

        # a quote forces md_repr() to call repr() on the key instead of
        # writing its characters directly, so the custom __repr__() above
        # is exercised and can recurse back into the keys view.
        d[Key("a'b")] = "value"

        assert repr(kv) == "<_KeysView(...)>"

    def test_values__repr__(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")], key="value2")
        assert repr(d.values()) == "<_ValuesView('value1', 'value2')>"

    def test_values__repr__recursive(
        self, any_multidict_class: type[MultiDict[object]]
    ) -> None:
        d = any_multidict_class()
        d["key"] = d.values()
        assert repr(d.values()) == "<_ValuesView(<_ValuesView(...)>)>"

    def test_istr_key_is_not_case_folded(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
        case_insensitive_str_class: type[istr],
    ) -> None:
        key = case_insensitive_str_class("Key")
        d = case_sensitive_multidict_class([(key, "value")])

        assert "Key" in d
        assert d["Key"] == "value"
        assert "key" not in d
        assert d.getall("key", None) is None

    def test_istr_lookup_is_not_case_folded(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
        case_insensitive_str_class: type[istr],
    ) -> None:
        d = case_sensitive_multidict_class([("key", "value")])

        assert case_insensitive_str_class("Key") not in d
        assert d.get(case_insensitive_str_class("Key")) is None
        assert case_insensitive_str_class("key") in d


class TestCIMultiDict(BaseMultiDictTest):
    @pytest.fixture(
        params=[
            ("CIMultiDict",),
            ("CIMultiDict", "CIMultiDictProxy"),
        ],
    )
    def cls(
        self,
        request: pytest.FixtureRequest,
        multidict_module: ModuleType,
    ) -> Callable[..., MultiMapping[int | str] | MutableMultiMapping[int | str]]:
        """Make a case-insensitive multidict class/proxy constructor."""
        return chained_callable(multidict_module, request.param)

    def test_basics(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], KEY="value2")

        assert d.getone("key") == "value1"
        assert d.get("key") == "value1"
        assert d.get("key2", "val") == "val"
        assert d["key"] == "value1"
        assert "key" in d

        with pytest.raises(KeyError, match="key2"):
            d["key2"]
        with pytest.raises(KeyError, match="key2"):
            d.getone("key2")

    def test_from_md_and_kwds(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")])
        d2 = cls(d, KEY="value2")

        assert list(d2.items()) == [("KEY", "value1"), ("KEY", "value2")]

    def test_getall(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], KEY="value2")

        assert not d == {"KEY": "value1"}
        assert len(d) == 2

        assert d.getall("key") == ["value1", "value2"]

        with pytest.raises(KeyError, match="some_key"):
            d.getall("some_key")

    def test_get(self, cls: type[CIMultiDict[int]]) -> None:
        d = cls([("A", 1), ("a", 2)])
        assert 1 == d["a"]

    def test__repr__(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], key="value2")
        _cls = type(d)

        expected = f"<{_cls.__name__}('KEY': 'value1', 'key': 'value2')>"
        assert str(d) == expected

    def test_items__repr__(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], key="value2")
        expected = "<_ItemsView('KEY': 'value1', 'key': 'value2')>"
        assert repr(d.items()) == expected

    def test_keys__repr__(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], key="value2")
        assert repr(d.keys()) == "<_KeysView('KEY', 'key')>"

    def test_values__repr__(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], key="value2")
        assert repr(d.values()) == "<_ValuesView('value1', 'value2')>"

    def test_items_iter_of_iter(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], key="value2")
        it = iter(d.items())
        assert iter(it) is it

    def test_keys_iter_of_iter(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], key="value2")
        it = iter(d.keys())
        assert iter(it) is it

    def test_values_iter_of_iter(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], key="value2")
        it = iter(d.values())
        assert iter(it) is it

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param({"key"}, {"KEY"}, id="ok"),
            pytest.param({"key", 123}, {"KEY"}, id="non-str"),
        ),
    )
    def test_keys_case_insensitive_and(
        self, cls: type[CIMultiDict[str]], arg: set[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one")])
        assert d.keys() & arg == expected

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param(["key"], {"key"}, id="ok"),
            pytest.param(["key", 123], {"key"}, id="non-str"),
        ),
    )
    def test_keys_case_insensitive_rand(
        self, cls: type[CIMultiDict[str]], arg: list[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one")])
        assert type(arg) is list
        assert arg & d.keys() == expected

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param({"key", "other"}, {"KEY", "other"}, id="ok"),
            pytest.param({"key", "other", 123}, {"KEY", "other", 123}, id="non-str"),
        ),
    )
    def test_keys_case_insensitive_or(
        self, cls: type[CIMultiDict[str]], arg: set[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one")])

        assert d.keys() | arg == expected

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param(["key", "other"], {"key", "other"}, id="ok"),
            pytest.param(["key", "other", 123], {"key", "other", 123}, id="non-str"),
        ),
    )
    def test_keys_case_insensitive_ror(
        self, cls: type[CIMultiDict[str]], arg: list[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one")])
        assert type(arg) is list

        assert arg | d.keys() == expected

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param({"key", "other"}, {"KEY2"}, id="ok"),
            pytest.param({"key", "other", 123}, {"KEY2"}, id="non-str"),
        ),
    )
    def test_keys_case_insensitive_sub(
        self, cls: type[CIMultiDict[str]], arg: set[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one"), ("KEY2", "two")])

        assert d.keys() - arg == expected

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param(["key", "other"], {"other"}, id="ok"),
            pytest.param(["key", "other", 123], {"other", 123}, id="non-str"),
        ),
    )
    def test_keys_case_insensitive_rsub(
        self, cls: type[CIMultiDict[str]], arg: list[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one"), ("KEY2", "two")])
        assert type(arg) is list

        assert arg - d.keys() == expected

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param(["key", "other"], {"KEY2", "other"}, id="ok"),
            pytest.param(["key", "other", 123], {"KEY2", "other", 123}, id="non-str"),
        ),
    )
    def test_keys_case_insensitive_xor(
        self, cls: type[CIMultiDict[str]], arg: list[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one"), ("KEY2", "two")])

        assert d.keys() ^ arg == expected

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param(["key", "other"], {"KEY2", "other"}, id="ok"),
            pytest.param(["key", "other", 123], {"KEY2", "other", 123}, id="non-str"),
        ),
    )
    def test_keys_case_insensitive_rxor(
        self, cls: type[CIMultiDict[str]], arg: list[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one"), ("KEY2", "two")])

        assert arg ^ d.keys() == expected

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param({"key"}, False, id="ok"),
            pytest.param({123}, True, id="non-str"),
        ),
    )
    def test_keys_case_insensitive_isdisjoint(
        self, cls: type[CIMultiDict[str]], arg: set[_T], expected: bool
    ) -> None:
        d = cls([("KEY", "one")])
        assert d.keys().isdisjoint(arg) == expected

    def test_keys_case_insensitive_not_iterable(
        self, cls: type[CIMultiDict[str]]
    ) -> None:
        d = cls([("KEY", "one"), ("KEY2", "two")])

        with pytest.raises(TypeError):
            123 & d.keys()  # type: ignore[operator]

        with pytest.raises(TypeError):
            d.keys() & 123  # type: ignore[operator]

        with pytest.raises(TypeError):
            123 | d.keys()  # type: ignore[operator]

        with pytest.raises(TypeError):
            d.keys() | 123  # type: ignore[operator]

        with pytest.raises(TypeError):
            123 ^ d.keys()  # type: ignore[operator]

        with pytest.raises(TypeError):
            d.keys() ^ 123  # type: ignore[operator]

        with pytest.raises(TypeError):
            d.keys() - 123  # type: ignore[operator]

        with pytest.raises(TypeError):
            123 - d.keys()  # type: ignore[operator]

    @pytest.mark.parametrize(
        "param",
        (
            pytest.param("non-tuple", id="not-tuple"),
            pytest.param(("key2", "two", "three"), id="not-2-elems"),
            pytest.param((123, "two"), id="not-str"),
        ),
    )
    def test_items_case_insensitive_parse_item(
        self, cls: type[CIMultiDict[str]], param: _T
    ) -> None:
        d = cls([("KEY", "one")])
        assert d.items() | {param} == {("KEY", "one"), param}

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param({("key", "one")}, {("KEY", "one")}, id="ok"),
            pytest.param(
                {("key", "one"), (123, "two")},
                {("KEY", "one")},
                id="non-str",
            ),
            pytest.param(
                {("key", "one"), ("key", "two")},
                {("KEY", "one")},
                id="nonequal-value",
            ),
        ),
    )
    def test_items_case_insensitive_and(
        self, cls: type[CIMultiDict[str]], arg: set[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one")])
        assert d.items() & arg == expected

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param([("key", "one")], {("key", "one")}, id="ok"),
            pytest.param(
                [("key", "one"), (123, "two")],
                {("key", "one")},
                id="non-str",
            ),
            pytest.param(
                [("key", "one"), ("key", "two")],
                {("key", "one")},
                id="nonequal-value",
            ),
        ),
    )
    def test_items_case_insensitive_rand(
        self, cls: type[CIMultiDict[str]], arg: list[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one")])
        assert type(arg) is list
        assert arg & d.items() == expected

    def test_items_case_insensitive_or(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("K", "v"), ("KEY", "one")])

        assert d.items() | {("key", "one"), ("other", "two")} == {
            ("K", "v"),
            ("KEY", "one"),
            ("other", "two"),
        }

    def test_items_case_insensitive_ror(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("K", "v"), ("KEY", "one"), ("KEY2", "three")])

        assert [("key", "one"), ("other", "two")] | d.items() == {
            ("K", "v"),
            ("key", "one"),
            ("other", "two"),
            ("KEY2", "three"),
        }

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param(
                {("key", "one"), ("other", "three")}, {("KEY2", "two")}, id="ok"
            ),
            pytest.param(
                {("key", "one"), (123, "three")}, {("KEY2", "two")}, id="non-str"
            ),
        ),
    )
    def test_items_case_insensitive_sub(
        self, cls: type[CIMultiDict[str]], arg: set[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one"), ("KEY2", "two")])

        assert d.items() - arg == expected

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param(
                [("key", "one"), ("other", "three")], {("other", "three")}, id="ok"
            ),
            pytest.param(
                [("key", "one"), (123, "three")], {(123, "three")}, id="non-str"
            ),
        ),
    )
    def test_items_case_insensitive_rsub(
        self, cls: type[CIMultiDict[str]], arg: set[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one"), ("KEY2", "two")])

        assert arg - d.items() == expected

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param(
                {("key", "one"), ("other", "three")},
                {("KEY2", "two"), ("other", "three")},
                id="ok",
            ),
            pytest.param(
                {("key", "one"), (123, "three")},
                {("KEY2", "two"), (123, "three")},
                id="non-str",
            ),
        ),
    )
    def test_items_case_insensitive_xor(
        self, cls: type[CIMultiDict[str]], arg: set[_T], expected: set[_T]
    ) -> None:
        d = cls([("KEY", "one"), ("KEY2", "two")])

        assert d.items() ^ arg == expected

    def test_items_case_insensitive_rxor(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "one"), ("KEY2", "two")])

        assert [("key", "one"), ("other", "three")] ^ d.items() == {
            ("KEY2", "two"),
            ("other", "three"),
        }

    def test_items_case_insensitive_non_iterable(
        self, cls: type[CIMultiDict[str]]
    ) -> None:
        d = cls([("KEY", "one")])

        with pytest.raises(TypeError):
            d.items() & None  # type: ignore[operator]

        with pytest.raises(TypeError):
            None & d.items()  # type: ignore[operator]

        with pytest.raises(TypeError):
            d.items() | None  # type: ignore[operator]

        with pytest.raises(TypeError):
            None | d.items()  # type: ignore[operator]

        with pytest.raises(TypeError):
            d.items() ^ None  # type: ignore[operator]

        with pytest.raises(TypeError):
            None ^ d.items()  # type: ignore[operator]

        with pytest.raises(TypeError):
            d.items() - None  # type: ignore[operator]

        with pytest.raises(TypeError):
            None - d.items()  # type: ignore[operator]

    @pytest.mark.parametrize(
        ("arg", "expected"),
        (
            pytest.param({("key", "one")}, False, id="ok"),
            pytest.param({(123, "one")}, True, id="non-str"),
        ),
    )
    def test_items_case_insensitive_isdisjoint(
        self, cls: type[CIMultiDict[str]], arg: set[_T], expected: bool
    ) -> None:
        d = cls([("KEY", "one")])
        assert d.items().isdisjoint(arg) == expected


class _ReentrantEq:
    """A value whose __eq__() calls back into `md` mid-comparison.

    items() set algebra walks the hash chain for a key while comparing
    stored values against a caller-supplied one; if that comparison can
    run arbitrary code (a custom __eq__) before the walk finishes, the
    reentrant call must still see a fully consistent multidict, not
    entries the in-progress walk has temporarily hidden.
    """

    def __init__(self, md: MultiDict[str], key: str, matches: str) -> None:
        self.md = md
        self.key = key
        self.matches = matches
        self.observed: list[str] | None = None

    def __eq__(self, other: object) -> bool:
        self.observed = self.md.getall(self.key)
        return other == self.matches

    def __hash__(self) -> int:
        return hash(self.matches)


def test_items_and_reentrant_equality(
    any_multidict_class: type[MultiDict[str]],
) -> None:
    md = any_multidict_class([("key", "first"), ("key", "second")])
    needle = _ReentrantEq(md, "key", "second")

    assert md.items() & {("key", needle)} == {("key", "second")}
    assert needle.observed == ["first", "second"]


def test_items_rand_reentrant_equality(
    any_multidict_class: type[MultiDict[str]],
) -> None:
    md = any_multidict_class([("key", "first"), ("key", "second")])
    needle = _ReentrantEq(md, "key", "second")
    other: list[tuple[str, object]] = [("key", needle)]

    assert other & md.items() == {("key", "second")}
    assert needle.observed == ["first", "second"]


def test_items_or_reentrant_equality(any_multidict_class: type[MultiDict[str]]) -> None:
    md = any_multidict_class([("key", "first"), ("key", "second")])
    needle = _ReentrantEq(md, "key", "second")

    assert md.items() | {("key", needle)} == {("key", "first"), ("key", "second")}
    assert needle.observed == ["first", "second"]


def test_items_rsub_reentrant_equality(
    any_multidict_class: type[MultiDict[str]],
) -> None:
    md = any_multidict_class([("key", "first"), ("key", "second")])
    needle = _ReentrantEq(md, "key", "second")

    assert [("key", needle)] - md.items() == set()
    assert needle.observed == ["first", "second"]


def test_items_contains_reentrant_equality(
    any_multidict_class: type[MultiDict[str]],
) -> None:
    md = any_multidict_class([("key", "first"), ("key", "second")])
    needle = _ReentrantEq(md, "key", "second")
    pair: tuple[str, object] = ("key", needle)

    assert pair in md.items()
    assert needle.observed == ["first", "second"]


def test_items_isdisjoint_reentrant_equality(
    any_multidict_class: type[MultiDict[str]],
) -> None:
    md = any_multidict_class([("key", "first"), ("key", "second")])
    needle = _ReentrantEq(md, "key", "second")

    assert md.items().isdisjoint([("key", needle)]) is False
    assert needle.observed == ["first", "second"]


def test_create_multidict_from_existing_multidict_new_pairs() -> None:
    """Test creating a MultiDict from an existing one does not mutate the original."""
    original = MultiDict([("h1", "header1"), ("h2", "header2"), ("h3", "header3")])
    new = MultiDict(original, h4="header4")
    assert "h4" in new
    assert "h4" not in original


def test_convert_multidict_to_cimultidict_and_back(
    case_sensitive_multidict_class: type[MultiDict[str]],
    case_insensitive_multidict_class: type[CIMultiDict[str]],
    case_insensitive_str_class: type[istr],
) -> None:
    """Test conversion from MultiDict to CIMultiDict."""
    start_as_md = case_sensitive_multidict_class(
        [("KEY", "value1"), ("key2", "value2")]
    )
    assert start_as_md.get("KEY") == "value1"
    assert start_as_md["KEY"] == "value1"
    assert start_as_md.get("key2") == "value2"
    assert start_as_md["key2"] == "value2"
    start_as_cimd = case_insensitive_multidict_class(
        [("KEY", "value1"), ("key2", "value2")]
    )
    assert start_as_cimd.get("key") == "value1"
    assert start_as_cimd["key"] == "value1"
    assert start_as_cimd.get("key2") == "value2"
    assert start_as_cimd["key2"] == "value2"
    converted_to_ci = case_insensitive_multidict_class(start_as_md)
    assert converted_to_ci.get("key") == "value1"
    assert converted_to_ci["key"] == "value1"
    assert converted_to_ci.get("key2") == "value2"
    assert converted_to_ci["key2"] == "value2"
    converted_to_md = case_sensitive_multidict_class(converted_to_ci)
    assert all(type(k) is case_insensitive_str_class for k in converted_to_ci.keys())
    assert converted_to_md.get("KEY") == "value1"
    assert converted_to_md["KEY"] == "value1"
    assert converted_to_md.get("key2") == "value2"
    assert converted_to_md["key2"] == "value2"


def test_convert_multidict_to_cimultidict_eq(
    case_sensitive_multidict_class: type[MultiDict[str]],
    case_insensitive_multidict_class: type[CIMultiDict[str]],
) -> None:
    """Test compare after conversion from MultiDict to CIMultiDict."""
    original = case_sensitive_multidict_class(
        [("h1", "header1"), ("h2", "header2"), ("h3", "header3")]
    )
    assert case_insensitive_multidict_class(
        original
    ) == case_insensitive_multidict_class(
        [("H1", "header1"), ("H2", "header2"), ("H3", "header3")]
    )


def test_reinitialize_releases_previous_values(
    any_multidict_class: type[MultiDict[object]],
) -> None:
    class Value:
        pass

    value = Value()
    value_ref = weakref.ref(value)
    d = any_multidict_class([("old", value)])
    del value

    d.__init__([("new", "value")])  # type: ignore[misc]

    gc.collect()
    assert value_ref() is None
    assert list(d.items()) == [("new", "value")]

    source = any_multidict_class([("source", "value")])
    d.__init__(source)  # type: ignore[misc]

    assert list(d.items()) == [("source", "value")]

    d.__init__(d)  # type: ignore[misc]

    assert list(d.items()) == [("source", "value")]


@pytest.mark.skipif(IS_PYPY, reason="getrefcount is not supported on PyPy")
def test_extend_does_not_alter_refcount(
    case_sensitive_multidict_class: type[MultiDict[str]],
) -> None:
    """Test that extending a MultiDict with a MultiDict does not alter the refcount of the original."""
    original = case_sensitive_multidict_class([("h1", "header1")])
    new = case_sensitive_multidict_class([("h2", "header2")])
    original_refcount = sys.getrefcount(original)
    new.extend(original)
    assert sys.getrefcount(original) == original_refcount


@pytest.mark.parametrize("use_proxy", (False, True), ids=("self", "proxy"))
@pytest.mark.parametrize("deleted", (False, True), ids=("full", "deleted"))
def test_extend_with_itself(
    any_multidict_class: type[MultiDict[int]],
    any_multidict_proxy_class: type[MultiDictProxy[int]],
    use_proxy: bool,
    deleted: bool,
) -> None:
    md = any_multidict_class((str(index), index) for index in range(6))
    if deleted:
        del md["0"]
    source = any_multidict_proxy_class(md) if use_proxy else md

    md.extend(source)

    expected = [(str(index), index) for index in range(1 if deleted else 0, 6)]
    assert list(md.items()) == expected * 2


@pytest.mark.parametrize("use_proxy", (False, True), ids=("self", "proxy"))
@pytest.mark.parametrize("method", ("update", "merge"))
def test_update_and_merge_with_itself(
    any_multidict_class: type[MultiDict[int]],
    any_multidict_proxy_class: type[MultiDictProxy[int]],
    use_proxy: bool,
    method: str,
) -> None:
    expected = [("key", 1), ("key", 2)]
    md = any_multidict_class(expected)
    source = any_multidict_proxy_class(md) if use_proxy else md

    getattr(md, method)(source)

    assert list(md.items()) == expected


@pytest.mark.skipif(IS_PYPY, reason="getrefcount is not supported on PyPy")
def test_update_does_not_alter_refcount(
    case_sensitive_multidict_class: type[MultiDict[str]],
) -> None:
    """Test that updating a MultiDict with a MultiDict does not alter the refcount of the original."""
    original = case_sensitive_multidict_class([("h1", "header1")])
    new = case_sensitive_multidict_class([("h2", "header2")])
    original_refcount = sys.getrefcount(original)
    new.update(original)
    assert sys.getrefcount(original) == original_refcount


@pytest.mark.skipif(IS_PYPY, reason="getrefcount is not supported on PyPy")
def test_init_does_not_alter_refcount(
    case_sensitive_multidict_class: type[MultiDict[str]],
) -> None:
    """Test that initializing a MultiDict with a MultiDict does not alter the refcount of the original."""
    original = case_sensitive_multidict_class([("h1", "header1")])
    original_refcount = sys.getrefcount(original)
    case_sensitive_multidict_class(original)
    assert sys.getrefcount(original) == original_refcount


@pytest.mark.c_extension
@pytest.mark.skipif(
    IS_PYPY or "free-threading" in sys.version, reason="getrefcount is not supported"
)
def test_items_contains_does_not_leak_key_on_error() -> None:
    """`x in md.items()` must not leak the first element when reading the
    second one raises.  The C items-view `__contains__` fetched element 0,
    then returned on an element-1 failure without releasing element 0.  This
    is a C-extension-only concern (the pure-Python version relies on the GC)."""
    md = multidict.MultiDict([("a", "1")])

    key = object()

    class BadSeq:
        def __len__(self) -> int:
            return 2

        def __getitem__(self, index: int) -> object:
            if index == 0:
                return key
            raise ValueError("boom")

    baseline = sys.getrefcount(key)
    items = md.items()
    for _ in range(1000):
        with pytest.raises(ValueError):
            items.__contains__(BadSeq())  # type:ignore[operator]
    assert sys.getrefcount(key) == baseline


@pytest.mark.c_extension
def test_repr_raises_when_mutated_during_iteration() -> None:
    """`repr()` of a MultiDict whose value mutates it mid-iteration raises
    RuntimeError (and, in the C extension, must not leak the writer)."""
    md: MultiDict[object] = MultiDict()

    class Evil:
        def __repr__(self) -> str:
            md.add("x", 1)  # bump the version mid-repr
            return "e"

    md.add("k", Evil())
    md.add("k2", Evil())
    with pytest.raises(RuntimeError, match="changed during iteration"):
        repr(md)


def test_update_extend_merge_thread_safety() -> None:
    """Concurrent update()/extend()/merge() must not crash or corrupt state.

    Regression test for a segfault on the free-threaded build: the C
    extension used to release self's lock between processing the
    positional argument and running the soft-delete cleanup in
    update()/merge(), so a concurrent reader of the same multidict (used
    as the argument to another thread's extend()/update()/merge() call)
    could observe entries mid-cleanup (identity set, key/value NULL).

    The pure-Python implementation has its own, unrelated version of this
    race: plain Python bytecode is not atomic even under the GIL, so two
    threads calling update()/extend()/merge() on the same multidict (or
    reading one as the argument to another thread's call) can interleave
    mid hash-table insert and corrupt the shared index/entries arrays,
    hanging in an infinite probe loop or raising AttributeError. This is
    reproducible on a normal, GIL-enabled interpreter given enough
    contention (the coverage tracing this suite runs under is enough to
    widen the window reliably), no free-threaded build required. Both
    implementations now hold a lock for the duration of the read-and-write."""
    d1 = MultiDict((str(i), i) for i in range(100))
    d2 = MultiDict((str(i), i) for i in range(100, 200))

    def worker(n: int) -> None:
        for _ in range(200):
            if n % 3 == 0:
                d1.update(d2)
            elif n % 3 == 1:
                d2.merge(d1)
            else:
                tmp: MultiDict[int] = MultiDict()
                tmp.extend(d1)
                tmp.extend(d2)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(worker, range(8)))

    assert len(d1) == 200
    assert len(d2) == 200


def test_clear_thread_safety() -> None:
    """Concurrent clear() alongside extend() must not crash or corrupt state.

    Regression test for the same class of free-threaded-build segfault as
    test_update_extend_merge_thread_safety(): clear() used to walk and free
    self's entries without holding self's lock, so a concurrent extend() on
    the same multidict could run in the middle of the walk.

    The pure-Python implementation shares the same exposure for the same
    reason given there: its bytecode isn't atomic under the GIL either, so
    an unlocked clear() interleaved with an unlocked extend() could observe
    or leave a half-built hash table. Both implementations now hold a lock
    around clear() and extend().

    The exact final size isn't asserted: clear() and extend() from
    different threads interleave with no ordering guarantee between them,
    so how many (possibly duplicate) entries are left behind depends on
    scheduling, not just on correctness. What must hold regardless of
    scheduling is that the multidict stays internally consistent."""
    d: MultiDict[int] = MultiDict((str(i), i) for i in range(200))

    def clearer(_n: int) -> None:
        for _ in range(200):
            d.clear()
            d.extend((str(i), i) for i in range(200))

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(clearer, range(8)))

    assert len(d) == len(list(d.items()))


def test_popitem_thread_safety() -> None:
    """Concurrent popitem() alongside update() must not crash or corrupt
    state.

    The pure-Python implementation used to pop an entry off the end of
    the entries list before clearing the matching indices slot, leaving a
    window where an unlocked reader could see an indices slot pointing
    past the end of the (now shorter) entries list. Combined with the
    same lack of locking as update()/extend()/merge()/clear(), running
    popitem() concurrently with update() on the same multidict (drained
    faster than it refills) could corrupt the hash table. Both
    implementations now hold a lock around popitem()."""
    d: MultiDict[int] = MultiDict((str(i), i) for i in range(200))

    def worker(n: int) -> None:
        for i in range(200):
            if n % 2 == 0:
                with contextlib.suppress(KeyError):
                    d.popitem()
            else:
                d.update({f"u{n}-{i}": i})

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(worker, range(8)))

    assert len(d) == len(list(d.items()))


@pytest.mark.c_extension
def test_clear_finalizer_thread_safety() -> None:
    """Concurrent clear() alongside update() must not crash or corrupt state.

    Regression test for a free-threaded-build finding flagged in review:
    clear() used to release each entry's key/value/identity references
    while the old, partially-cleared hash table was still published.
    Releasing a value can run arbitrary Python code (a __del__), which
    can suspend the held critical section; a concurrent, properly-locked
    caller could then observe the multidict mid-clear (some entries
    already released, others not), a state nothing else in the codebase
    expects. clear() now swaps in the empty table before releasing any
    entry's references, so a suspended thread only ever exposes the
    fully populated table or the fully empty one. This is a
    C-extension-only concern: the pure-Python implementation has no
    locking of its own to regress."""

    class Evil:
        def __del__(self) -> None:
            time.sleep(0)

    def trial(_n: int) -> None:
        d: MultiDict[Evil] = MultiDict()
        for i in range(50):
            d.add(str(i), Evil())

        def clearer() -> None:
            d.clear()

        def updater() -> None:
            d.update({})

        with ThreadPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(clearer)
            f2 = executor.submit(updater)
            f1.result()
            f2.result()

        assert len(d) == 0

    with ThreadPoolExecutor(max_workers=16) as executor:
        list(executor.map(trial, range(500)))


@pytest.mark.c_extension
def test_reinit_thread_safety() -> None:
    """Concurrent __init__() alongside other methods must not crash.

    Regression test for a free-threaded-build crash flagged in review:
    __init__() used to reset self's storage (md_init(), which frees the
    old hash table and replaces it) before acquiring self's lock. That's
    harmless for the usual case where self is still private to the
    constructor call, but __init__() can also be invoked explicitly on
    an already-published, potentially shared multidict
    (``d.__init__(other)``), at which point a concurrent caller of
    another locked method could observe self mid-reset. This is a
    C-extension-only concern: the pure-Python implementation has no
    locking of its own to regress."""
    d: MultiDict[int] = MultiDict((str(i), i) for i in range(200))
    other: MultiDict[int] = MultiDict((f"o{i}", i) for i in range(200))

    def worker(n: int) -> None:
        for _ in range(200):
            if n % 2 == 0:
                d.__init__(other)  # type: ignore[misc]
            else:
                len(d)
                d.update({"extra": 1})

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(worker, range(8)))

    assert len(d) == len(list(d.items()))


@pytest.mark.c_extension
def test_update_from_dict_arg_thread_safety() -> None:
    """Concurrent update() from a plain dict alongside mutation of that
    same dict must not crash.

    Regression test for a free-threaded-build crash flagged in review:
    the dict-argument path used to iterate a plain dict `arg` with
    PyDict_Next() while holding only self's lock, not arg's. PyDict_Next()
    is not thread-safe against concurrent mutation of the dict it is
    iterating, so a shared dict being read by update()/extend()/merge()
    on one thread while another thread mutates it (even through dict's
    own, individually-locked methods) was unsafe. This is a
    C-extension-only concern: the pure-Python implementation has no
    locking of its own to regress."""
    shared = {str(i): i for i in range(300)}

    def mutator(n: int) -> None:
        for _ in range(200):
            shared[f"x{n}"] = n
            shared.pop(f"x{n}", None)

    def updater(_n: int) -> None:
        for _ in range(200):
            d: MultiDict[int] = MultiDict()
            d.update(shared)
            len(d)

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(mutator, i) for i in range(8)]
        futures += [executor.submit(updater, i) for i in range(8)]
        for f in futures:
            f.result()


@pytest.mark.c_extension
def test_single_item_ops_thread_safety() -> None:
    """Concurrent add()/__setitem__/__delitem__/pop()/popitem()/setdefault()
    alongside __getitem__/__contains__/len()/iteration must not crash.

    Regression test for the free-threaded build: unlike update()/extend()/
    merge()/clear()/repr() (protected earlier), the single-item operations
    -- add(), __setitem__/__delitem__, get()/getone()/__getitem__,
    __contains__, setdefault(), pop()/popone()/popall()/popitem(), and
    iteration -- used to run without holding self's lock at all. A resize
    triggered by one thread's mutation could free the hash table a
    concurrent reader on another thread was still walking (a
    use-after-free), or a concurrent mutator could observe/interleave with
    a half-applied insert or deletion. This is a C-extension-only concern:
    the pure-Python implementation has no locking of its own to regress.
    __eq__ is exercised against both another MultiDict (the two-object
    CRITICAL_SECTION2 path) and a plain dict (the single-object,
    generic-mapping path)."""
    d: MultiDict[int] = MultiDict((str(i), i) for i in range(200))
    d2: MultiDict[int] = MultiDict((str(i), i) for i in range(200))
    other_mapping = {str(i): i for i in range(200)}

    def worker(n: int) -> None:
        for i in range(300):
            key = str(i % 200)
            if n % 2 == 0:
                target = d if n % 4 == 0 else d2
                op = i % 9
                if op == 0:
                    target.add(key, i)
                elif op == 1:
                    target[key] = i
                elif op == 2:
                    target.setdefault(f"sd{n}-{i}", i)
                elif op == 3:
                    with contextlib.suppress(KeyError):
                        del target[key]
                elif op == 4:
                    target.pop(key, None)
                elif op == 5:
                    target.getall(key, [])
                elif op == 6:
                    target.popone(key, None)
                elif op == 7:
                    target.popall(key, None)
                else:
                    with contextlib.suppress(KeyError):
                        target.popitem()
            else:
                key in d
                d.get(key)
                d.getone(key, None)
                with contextlib.suppress(KeyError):
                    d[key]
                len(d)
                d == d2
                d == other_mapping
                # A concurrent mutation from another worker can legitimately
                # be detected mid-iteration (same as dict's own "changed
                # size during iteration" check); that is not a bug here.
                with contextlib.suppress(RuntimeError):
                    list(d.items())
                    list(d.keys())
                    list(d.values())

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(worker, range(8)))

    assert len(d) == len(list(d.items()))
    assert len(d2) == len(list(d2.items()))


@pytest.mark.c_extension
def test_view_set_ops_thread_safety() -> None:
    """Concurrent items()/keys() set-algebra (&, |, -, ^, in, isdisjoint())
    alongside mutation must not crash.

    Regression test for the free-threaded build: itemsview's and keysview's
    &/|/-/^/in/isdisjoint() implementations used to walk self's hash table
    directly (md_calc_identity()/md_init_finder()/md_contains()/md_next())
    without holding self's lock, so a concurrent resize triggered by
    mutation on another thread could free the table mid-walk. This is a
    C-extension-only concern: the pure-Python implementation has no
    locking of its own to regress."""
    d: MultiDict[int] = MultiDict((str(i), i) for i in range(200))
    other = {str(i): i for i in range(100, 300)}

    def worker(n: int) -> None:
        for i in range(150):
            if n % 2 == 0:
                key = str(i % 200)
                if i % 2 == 0:
                    d.add(key, i)
                else:
                    d.pop(key, None)
            else:
                # A concurrent mutation from another worker can
                # legitimately be detected mid-walk (same as dict's own
                # "changed size during iteration" check); that is not a
                # bug here.
                with contextlib.suppress(RuntimeError):
                    d.items() & other.items()
                    d.items() | other.items()
                    d.items() - other.items()
                    d.items() ^ other.items()
                    d.items().isdisjoint(other.items())
                    d.keys() & other.keys()
                    d.keys() | other.keys()
                    d.keys() - other.keys()
                    d.keys() ^ other.keys()
                    d.keys().isdisjoint(other.keys())
                    "100" in d.keys()
                    ("100", 100) in d.items()

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(worker, range(8)))

    assert len(d) == len(list(d.items()))


@pytest.mark.c_extension
def test_contains_lock_free_thread_safety() -> None:
    """Concurrent __contains__ alongside heavy add()/pop() churn must not
    crash.

    Regression test for the free-threaded build: __contains__ (via
    md_contains() with pret == NULL) is now genuinely lock-free -- it
    does not take self's critical section at all, unlike every other
    single-item operation, which still does. Its safety instead comes
    from _md_reader_enter()/_md_reader_exit() (a coarse active_readers
    gate) plus per-table retirement: a resize/shrink/clear no longer
    frees the old hash table immediately, only once no lock-free reader
    could still be walking it. This drives many resizes concurrently
    with many __contains__ calls specifically to exercise that
    retire/drain path, not just the (always safe) case where
    active_readers happens to be 0 at retirement time. This is a
    C-extension-only concern: the pure-Python implementation has no
    locking of its own to regress."""
    d: MultiDict[int] = MultiDict((str(i), i) for i in range(500))

    def mutator(n: int) -> None:
        for i in range(3000):
            key = f"m{n}-{i}"
            d.add(key, i)
            d.pop(key, None)

    def reader(_n: int) -> None:
        for i in range(3000):
            str(i % 500) in d

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(mutator, i) for i in range(8)]
        futures += [executor.submit(reader, i) for i in range(8)]
        for f in futures:
            f.result()

    assert len(d) == 500


@pytest.mark.c_extension
def test_get_lock_free_thread_safety() -> None:
    """Concurrent get()/getone()/__getitem__ alongside heavy add()/pop()
    churn must not crash.

    Regression test for the free-threaded build: on CPython 3.14+,
    get()/getone()/__getitem__ (md_get_one() with pret == NULL) are now
    genuinely lock-free, falling back to a critical section only when a
    candidate entry's identity or value can't be safely referenced
    (PyUnstable_TryIncRef() fails, or the field changed mid-read). On
    3.13 -- which has no public API for a third-party extension to
    safely try-incref an object that might concurrently be reaching
    refcount zero (PyUnstable_TryIncRef()/PyUnstable_EnableTryIncRef()
    were only added in 3.14) -- it always takes the critical section,
    same as before this file added any lock-free reading of entry
    contents. Either way this must not crash: it drives many entry
    inserts/deletes concurrently with many get() calls to exercise
    both the lock-free fast path (3.14+) and the locked fallback
    (3.13, or a 3.14+ TryIncRef failure). Deliberately uses only
    add()/pop() on the mutating side, not __setitem__: __setitem__'s
    replace path has a separate, pre-existing, unrelated race that
    this test is not about and should not trip. This is a
    C-extension-only concern: the pure-Python implementation has no
    locking of its own to regress."""
    d: MultiDict[int] = MultiDict((str(i), i) for i in range(500))

    def mutator(n: int) -> None:
        for i in range(3000):
            key = f"m{n}-{i}"
            d.add(key, i)
            d.pop(key, None)

    def reader(_n: int) -> None:
        for i in range(3000):
            key = str(i % 500)
            d.get(key)
            d.getone(key, None)
            with contextlib.suppress(KeyError):
                d[key]

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(mutator, i) for i in range(8)]
        futures += [executor.submit(reader, i) for i in range(8)]
        for f in futures:
            f.result()

    assert len(d) == 500


@pytest.mark.c_extension
def test_popall_lock_free_get_thread_safety() -> None:
    """Concurrent popall() alongside lock-free get() must not crash.

    Regression test for the free-threaded build: popall() (like
    popone()/__delitem__) rewrites the removed entry's hash table index
    slot to DKIX_DUMMY via htkeys_set_index(), while a lock-free
    get()/getone()/__getitem__ walks that same index array via
    htkeysiter_next()/htkeys_get_index() and holds no lock at all.
    ThreadSanitizer flagged a genuine data race here between
    multidict_popall() and multidict_get(): both htkeys_get_index() and
    htkeys_set_index() used to be plain, non-atomic array accesses; they
    now go through relaxed atomics under Py_GIL_DISABLED. Deliberately
    uses popall() rather than pop()/popone() to target that call site
    specifically. This is a C-extension-only concern: the pure-Python
    implementation has no locking of its own to regress."""
    d: MultiDict[int] = MultiDict((str(i), i) for i in range(500))

    def mutator(n: int) -> None:
        for i in range(3000):
            key = f"m{n}-{i}"
            d.add(key, i)
            d.popall(key, None)

    def reader(_n: int) -> None:
        for i in range(3000):
            key = str(i % 500)
            d.get(key)
            d.getone(key, None)
            with contextlib.suppress(KeyError):
                d[key]

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(mutator, i) for i in range(8)]
        futures += [executor.submit(reader, i) for i in range(8)]
        for f in futures:
            f.result()

    assert len(d) == 500


@pytest.mark.c_extension
def test_getall_update_vs_lock_free_reads_thread_safety() -> None:
    """Concurrent getall()/update() alongside lock-free contains()/get()
    on the same, never-deleted keys must not crash and must never observe
    a present key as absent.

    Regression test for the free-threaded build: getall() and popall()
    (via md_find_next()/md_finder_cleanup()) and update()/extend()/merge()
    (via md_post_update()) temporarily mark/unmark the matching entry's
    hash while holding self's critical section. That critical section
    excludes other mutators but not a lock-free reader: __contains__ and
    get()/getone()/__getitem__ (with no default requested) load the same
    field via an atomic op with no lock at all. Before the fix, the
    mark/unmark writes were plain, non-atomic stores racing that atomic
    load; this drives getall() and update() against contains()/get() on
    keys that are never removed, so a lock-free reader observing one of
    them as missing would be a real regression, not a benign race."""
    keys = [str(i) for i in range(500)]
    d: MultiDict[int] = MultiDict((key, i) for i, key in enumerate(keys))

    def getall_worker(_n: int) -> None:
        for i in range(3000):
            assert d.getall(keys[i % 500]) != []

    def update_worker(n: int) -> None:
        for i in range(3000):
            d.update({keys[i % 500]: n * 10000 + i})

    def reader_worker(_n: int) -> None:
        for i in range(3000):
            key = keys[i % 500]
            assert key in d
            assert d.get(key) is not None

    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(getall_worker, i) for i in range(4)]
        futures += [executor.submit(update_worker, i) for i in range(4)]
        futures += [executor.submit(reader_worker, i) for i in range(4)]
        for f in futures:
            f.result()

    assert len(d) == 500


@pytest.mark.c_extension
def test_to_dict_vs_lock_free_reads_thread_safety() -> None:
    """Concurrent to_dict() alongside lock-free contains()/get() on the
    same, never-deleted keys must not crash and must never observe a
    present key as absent.

    Regression test for the free-threaded build: to_dict() (via
    md_to_dict()) marks every entry's hash while it walks the table, then
    clears every mark in one pass via _md_restore_all_hashes(), all while
    holding self's critical section. That critical section excludes other
    mutators but not a lock-free reader: __contains__ and
    get()/getone()/__getitem__ (with no default requested) load the same
    field via an atomic op with no lock at all. Before the fix,
    _md_restore_all_hashes() unmarked entries with a plain, non-atomic
    store racing that atomic load; this drives to_dict() against
    contains()/get() on keys that are never removed, so a lock-free reader
    observing one of them as missing would be a real regression, not a
    benign race."""
    keys = [str(i) for i in range(500)]
    d: MultiDict[int] = MultiDict((key, i) for i, key in enumerate(keys))

    def to_dict_worker(_n: int) -> None:
        for _ in range(1000):
            assert len(d.to_dict()) == 500

    def reader_worker(_n: int) -> None:
        for i in range(3000):
            key = keys[i % 500]
            assert key in d
            assert d.get(key) is not None

    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(to_dict_worker, i) for i in range(4)]
        futures += [executor.submit(reader_worker, i) for i in range(8)]
        for f in futures:
            f.result()

    assert len(d) == 500


@pytest.mark.c_extension
def test_version_thread_safety() -> None:
    """Concurrently mutating independent multidicts must never hand out
    the same version number twice.

    Regression test for a version-counter race: every mutation derives
    its instance's version from ``state->global_version``, a counter
    shared by every ``MultiDict``/``CIMultiDict`` instance in the process
    (it lives on the module state, not the object), so unrelated
    multidicts can be compared and always disagree. Bumping that shared
    counter used to be a plain ``++`` with no synchronization of its own,
    relying entirely on each instance's own critical section; under a
    free-threaded build, two threads mutating two *different* instances
    could bump it at the same time and step on each other's update,
    handing out one version number to two objects, or a smaller one to a
    later mutation than an earlier one already got. This is a
    C-extension-only concern: the pure-Python implementation has the
    same shared-counter shape but no locking of its own to regress.
    """
    n_threads = 16
    n_iters = 3000
    all_versions: list[list[int]] = []
    lock = threading.Lock()

    def worker(_n: int) -> None:
        m: MultiDict[object] = MultiDict()
        versions = []
        for i in range(n_iters):
            m["key"] = i
            versions.append(multidict.getversion(m))
        with lock:
            all_versions.append(versions)

    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        list(executor.map(worker, range(n_threads)))

    flat_versions = [v for versions in all_versions for v in versions]
    assert len(set(flat_versions)) == len(flat_versions)


@pytest.mark.c_extension
def test_reader_exit_drains_retired_thread_safety() -> None:
    """A retired hash table must eventually be freed by reader traffic
    alone, with no further mutation. Regression test for
    aio-libs/multidict#1443.

    md->retired only used to be drained inside _md_retire(), i.e. as a
    side effect of some *later* resize/clear checking whether the
    active-readers gate had reached 0. _md_reader_exit() never triggered
    a drain itself, so under read traffic frequent enough that the gate
    rarely lands on exactly 0 at the moment some other resize/clear
    happens to check it, a table already on md->retired could sit there
    for the rest of the object's life. This drives many reader threads
    continuously across a single clear() (so the active-readers gate is
    essentially always nonzero at the instant clear() checks it, forcing
    the table onto md->retired instead of freeing it immediately) and
    then relies solely on later reader exits, with no further mutation,
    to reclaim it. Before the fix this leaves the weakrefs alive forever;
    after it, some reader's own exit drains the table once the gate
    happens to fall to 0. Each reader signals readiness only after a
    warm-up batch of reads, then keeps going straight into its main
    loop with no further blocking; clear() waits for every signal
    before running, so it cannot land before a reader has actually
    started (a plain submit() only queues the call, it says nothing
    about whether the thread has run yet). This is a C-extension-only
    concern: the pure-Python implementation has no retirement scheme to
    regress."""

    class Marker:
        pass

    d: MultiDict[Marker] = MultiDict()
    markers = [Marker() for _ in range(200)]
    refs = [weakref.ref(m) for m in markers]
    for i, m in enumerate(markers):
        d.add(str(i), m)
    # A for loop's variables outlive the loop in their enclosing scope, so
    # `i`/`m` would otherwise keep the last marker alive right through the
    # `assert` below regardless of what multidict does.
    del markers, i, m

    ready_events = [threading.Event() for _ in range(16)]

    def reader(_n: int, ready: threading.Event) -> None:
        for i in range(200):
            str(i % 200) in d
        ready.set()
        for i in range(20_000):
            str(i % 200) in d

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(reader, i, ready_events[i]) for i in range(16)]
        for ready in ready_events:
            ready.wait()
        d.clear()
        for f in futures:
            f.result()

    gc.collect()
    assert all(r() is None for r in refs)


@pytest.mark.c_extension
def test_setitem_update_thread_safety() -> None:
    """Concurrent __setitem__()/update() on colliding keys, alongside
    concurrent add()/pop() churn that drives frequent resizes, must not
    corrupt state or crash.

    Regression test for several related, pre-existing free-threaded-build
    races in the replace path (__setitem__()/update()/extend()/merge()),
    found while stress-testing with an artificially widened suspension
    window (see the PR description for how). The replace path finds a
    key's entry, temporarily marks its hash (the sign bit) to track
    progress across a scan that can span more than one match, mutates the
    entry, then restores the mark once done -- and every step of that can
    transiently suspend the critical section, exactly like the read-path
    and _md_resize() races fixed earlier. A concurrent resize triggered by
    an entirely different thread's add()/pop() could then observe or
    mishandle that temporarily-marked state, corrupting the table (wrong
    bucket placement, a different key's mark overwritten, a stale
    entries-array pointer used after the table it pointed into was freed).
    This is a C-extension-only concern: the pure-Python implementation has
    no locking of its own to regress.

    Also the regression test (via reader_worker()'s get()/__getitem__()/
    __contains__() calls, all lock-free reads) for a used-after-free in
    _md_drain_retired(): its coarse "no reader in flight" gate reading
    zero did not reliably mean every such reader had also finished
    walking its own table, so a table whose own reader count was still
    nonzero could be freed while a lock-free reader on another thread was
    still walking it. Only reproduces intermittently and needs heavy
    thread oversubscription (a handful of CPUs, far more threads); it is
    what the ThreadSanitizer CI job caught."""
    nkeys = 30
    d: MultiDict[object] = MultiDict((str(i), i) for i in range(nkeys))

    def setitem_worker(n: int) -> None:
        for i in range(1500):
            d[str(i % nkeys)] = (n, i)

    def dup_worker(n: int) -> None:
        # Exercises the duplicate-collapsing path in __setitem__: add a
        # genuine duplicate, then replace it, which must delete the dup.
        for i in range(800):
            key = str(i % nkeys)
            d.add(key, ("dup", n, i))
            d[key] = ("collapsed", n, i)

    def update_worker(n: int) -> None:
        for i in range(800):
            d.update({str(i % nkeys): (n, i), str((i + 1) % nkeys): (n, i)})

    def churn_worker(n: int) -> None:
        for i in range(1200):
            key = f"churn-{n}-{i}"
            d.add(key, i)
            d.pop(key, None)

    def reader_worker(_n: int) -> None:
        for i in range(1200):
            key = str(i % nkeys)
            d.get(key)
            with contextlib.suppress(KeyError):
                d[key]
            key in d

    with ThreadPoolExecutor(max_workers=28) as executor:
        futures = [executor.submit(setitem_worker, i) for i in range(6)]
        futures += [executor.submit(dup_worker, i) for i in range(4)]
        futures += [executor.submit(update_worker, i) for i in range(4)]
        futures += [executor.submit(churn_worker, i) for i in range(8)]
        futures += [executor.submit(reader_worker, i) for i in range(6)]
        for f in futures:
            f.result()

    assert len(d) == nkeys
    assert len(d) == len(list(d.items()))


@pytest.mark.c_extension
def test_drain_retired_defers_busy_table_thread_safety() -> None:
    """Concurrent get()/__getitem__()/__contains__() against a table
    resized on nearly every insert must not crash.

    Regression test for the free-threaded build: _md_drain_retired()'s
    coarse "no lock-free reader in flight" gate can read zero for the
    whole object while one specific retired table's own reader count is
    still nonzero (a reader caught between incrementing that count and
    decrementing it, not between A and B where the coarse gate actually
    protects). Isolates that scenario from test_setitem_update_thread_
    safety() above: only churn (to force frequent resizes, hence frequent
    retirements) and lock-free reads, at heavy thread oversubscription to
    make a reader getting caught mid-walk likely. This is a
    C-extension-only concern: the pure-Python implementation has no
    locking of its own to regress."""
    nkeys = 30
    d: MultiDict[int] = MultiDict((str(i), i) for i in range(nkeys))

    def churn_worker(n: int) -> None:
        for i in range(2000):
            key = f"churn-{n}-{i}"
            d.add(key, i)
            d.pop(key, None)

    def reader_worker(_n: int) -> None:
        for i in range(2000):
            key = str(i % nkeys)
            d.get(key)
            with contextlib.suppress(KeyError):
                d[key]
            key in d

    with ThreadPoolExecutor(max_workers=28) as executor:
        futures = [executor.submit(churn_worker, i) for i in range(14)]
        futures += [executor.submit(reader_worker, i) for i in range(14)]
        for f in futures:
            f.result()

    assert len(d) == nkeys


# Pure-Python thread-safety tests. Import the pure-Python implementation
# directly (`multidict._multidict_py` is always importable, regardless of
# whether the C extension is built), rather than going through `multidict`'s
# own backend selection, so these run against pure Python on every leg of
# the test matrix, including one where the C extension is the active
# default backend.
import multidict._multidict_py as _pure  # noqa: E402

# Pure Python bytecode is not atomic even under the GIL (the GIL can be
# released between any two bytecodes), so several multidict operations have
# a pre-existing, unlocked race that predates this file's free-threading
# locking: add(), __setitem__()/__delitem__(), setdefault(),
# popone()/popall(), __init__() re-init, and the items()/keys() set-algebra
# operations are still unlocked on a GIL-enabled build (out of scope here),
# so a test driving several real threads through shared pure-Python
# multidict state via those operations is only reliable where the
# free-threading locking this file added actually applies.
# update()/extend()/merge()/clear()/popitem() no longer need this skip:
# their instance of the same race is reachable on a plain GIL build too
# (CI's shared runners' coverage tracing widens the window enough to hit
# it reliably), so they're now locked unconditionally -- see
# `_pure._locked_always`/`_pure._locked_pair_always`.
_gil_build_race_skip = pytest.mark.skipif(
    not _pure._FREE_THREADED,
    reason=(
        "shares a pre-existing, unlocked GIL-build race with other pure-"
        "Python multidict operations (out of scope here: GIL builds "
        "intentionally get no locking from this change); only reliable "
        "where the locking this change adds actually applies"
    ),
)


@_gil_build_race_skip
def test_pure_python_single_item_ops_thread_safety() -> None:
    """Concurrent add()/__setitem__/__delitem__/pop()/popitem()/setdefault()
    alongside __getitem__/__contains__/len()/iteration must not crash."""
    d: _pure.MultiDict[int] = _pure.MultiDict((str(i), i) for i in range(200))
    d2: _pure.MultiDict[int] = _pure.MultiDict((str(i), i) for i in range(200))
    other_mapping = {str(i): i for i in range(200)}

    def worker(n: int) -> None:
        for i in range(300):
            key = str(i % 200)
            if n % 2 == 0:
                target = d if n % 4 == 0 else d2
                op = i % 9
                if op == 0:
                    target.add(key, i)
                elif op == 1:
                    target[key] = i
                elif op == 2:
                    target.setdefault(f"sd{n}-{i}", i)
                elif op == 3:
                    with contextlib.suppress(KeyError):
                        del target[key]
                elif op == 4:
                    target.pop(key, None)
                elif op == 5:
                    target.getall(key, [])
                elif op == 6:
                    target.popone(key, None)
                elif op == 7:
                    target.popall(key, None)
                else:
                    with contextlib.suppress(KeyError):
                        target.popitem()
            else:
                key in d
                d.get(key)
                d.getone(key, None)
                with contextlib.suppress(KeyError):
                    d[key]
                len(d)
                d == d2
                d == other_mapping
                with contextlib.suppress(RuntimeError):
                    list(d.items())
                    list(d.keys())
                    list(d.values())

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(worker, range(8)))

    assert len(d) == len(list(d.items()))
    assert len(d2) == len(list(d2.items()))


def test_pure_python_update_extend_merge_thread_safety() -> None:
    """Concurrent update()/extend()/merge() must not crash or corrupt
    state, on a free-threaded build and on a plain GIL-enabled one alike:
    see `_pure._locked_pair_always`."""
    d1: _pure.MultiDict[int] = _pure.MultiDict((str(i), i) for i in range(30))
    d2: _pure.MultiDict[int] = _pure.MultiDict((str(i), i) for i in range(30, 60))

    def worker(n: int) -> None:
        for _ in range(20):
            if n % 3 == 0:
                d1.update(d2)
            elif n % 3 == 1:
                d2.merge(d1)
            else:
                tmp: _pure.MultiDict[int] = _pure.MultiDict()
                tmp.extend(d1)
                tmp.extend(d2)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(worker, range(8)))

    assert len(d1) == 60
    assert len(d2) == 60


def test_pure_python_clear_thread_safety() -> None:
    """Concurrent clear() alongside extend() must not crash or corrupt
    state, on a free-threaded build and on a plain GIL-enabled one alike:
    see `_pure._locked_always`."""
    d: _pure.MultiDict[int] = _pure.MultiDict((str(i), i) for i in range(50))

    def clearer(_n: int) -> None:
        for _ in range(30):
            d.clear()
            d.extend((str(i), i) for i in range(50))

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(clearer, range(8)))

    assert len(d) == len(list(d.items()))


def test_pure_python_popitem_thread_safety() -> None:
    """Concurrent popitem() alongside update() must not crash or corrupt
    state, on a free-threaded build and on a plain GIL-enabled one alike:
    see `_pure._locked_always`."""
    d: _pure.MultiDict[int] = _pure.MultiDict((str(i), i) for i in range(100))

    def worker(n: int) -> None:
        for i in range(60):
            if n % 2 == 0:
                with contextlib.suppress(KeyError):
                    d.popitem()
            else:
                d.update({f"u{n}-{i}": i})

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(worker, range(8)))

    assert len(d) == len(list(d.items()))


@_gil_build_race_skip
def test_pure_python_reinit_thread_safety() -> None:
    """Concurrent __init__() alongside other methods must not crash, and
    must keep using the same lock across a re-init on a published, shared
    instance (see `_pure.MultiDict.__new__`)."""
    d: _pure.MultiDict[int] = _pure.MultiDict((str(i), i) for i in range(200))
    other: _pure.MultiDict[int] = _pure.MultiDict((f"o{i}", i) for i in range(200))

    def worker(n: int) -> None:
        for _ in range(200):
            if n % 2 == 0:
                d.__init__(other)  # type: ignore[misc]
            else:
                len(d)
                d.update({"extra": 1})

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(worker, range(8)))

    assert len(d) == len(list(d.items()))


@_gil_build_race_skip
def test_pure_python_view_set_ops_thread_safety() -> None:
    """Concurrent items()/keys() set-algebra (&, |, -, ^, in, isdisjoint())
    alongside mutation must not crash."""
    d: _pure.MultiDict[int] = _pure.MultiDict((str(i), i) for i in range(200))
    other = {str(i): i for i in range(100, 300)}

    def worker(n: int) -> None:
        for i in range(150):
            if n % 2 == 0:
                key = str(i % 200)
                if i % 2 == 0:
                    d.add(key, i)
                else:
                    d.pop(key, None)
            else:
                with contextlib.suppress(RuntimeError):
                    d.items() & other.items()
                    d.items() | other.items()
                    d.items() - other.items()
                    d.items() ^ other.items()
                    d.items().isdisjoint(other.items())
                    d.keys() & other.keys()
                    d.keys() | other.keys()
                    d.keys() - other.keys()
                    d.keys() ^ other.keys()
                    d.keys().isdisjoint(other.keys())
                    "100" in d.keys()
                    ("100", 100) in d.items()

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(worker, range(8)))

    assert len(d) == len(list(d.items()))


def test_pure_python_reciprocal_view_ops_no_deadlock() -> None:
    """`a.items() & b.items()` racing `b.items() & a.items()` (and the
    same for the other set-algebra ops) must not deadlock.

    Regression test: a view's set-algebra methods used to hold only
    their own multidict's lock while iterating an `other` argument that
    can itself be a view over a *different* multidict -- each step of
    that iteration takes the other multidict's own lock too (see
    `_Iter.__next__`). Two threads doing the reciprocal operation could
    each hold one lock while blocked waiting for the other: a classic
    AB-BA deadlock. Fixed by pairing both locks up front, in a fixed
    order (`_locked_md_pair`), the same way cross-multidict operations
    like `update()` already do.
    """
    if not _pure._FREE_THREADED:
        pytest.skip("the two-lock pairing this test exercises is a no-op without it")

    a: _pure.MultiDict[int] = _pure.MultiDict((str(i), i) for i in range(50))
    b: _pure.MultiDict[int] = _pure.MultiDict((str(i), i) for i in range(50, 100))

    def worker1() -> None:
        for _ in range(500):
            a.items() & b.items()
            a.keys() | b.keys()
            a.keys() - b.items()
            a.keys().isdisjoint(b.keys())

    def worker2() -> None:
        for _ in range(500):
            b.items() & a.items()
            b.keys() | a.keys()
            b.keys() - a.items()
            b.keys().isdisjoint(a.keys())

    t1 = threading.Thread(target=worker1, daemon=True)
    t2 = threading.Thread(target=worker2, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=20)
    t2.join(timeout=20)
    assert not t1.is_alive(), "worker1 still running: deadlock"
    assert not t2.is_alive(), "worker2 still running: deadlock"


def test_pure_python_reciprocal_raw_iterator_ops_no_deadlock() -> None:
    """Same as `test_pure_python_reciprocal_view_ops_no_deadlock`, but with
    the `other` argument passed as a bare `iter(view)` rather than the view
    itself.

    Regression test: `_other_lock()` recognized `_ItemsView`/`_KeysView`/
    `_ValuesView` but not the `_Iter` `iter()` returns, so `a.items() &
    iter(b.items())` racing `b.items() & iter(a.items())` could still
    deadlock the same way. Also covers `update()`/`extend()`/`merge()`
    called with a bare iterator over another multidict's view.
    """
    if not _pure._FREE_THREADED:
        pytest.skip("the two-lock pairing this test exercises is a no-op without it")

    a: _pure.MultiDict[int] = _pure.MultiDict((str(i), i) for i in range(50))
    b: _pure.MultiDict[int] = _pure.MultiDict((str(i), i) for i in range(50, 100))

    def worker1() -> None:
        for _ in range(500):
            a.items() & iter(b.items())
            _pure.MultiDict[int]().update(iter(b.items()))

    def worker2() -> None:
        for _ in range(500):
            b.items() & iter(a.items())
            _pure.MultiDict[int]().update(iter(a.items()))

    t1 = threading.Thread(target=worker1, daemon=True)
    t2 = threading.Thread(target=worker2, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=20)
    t2.join(timeout=20)
    assert not t1.is_alive(), "worker1 still running: deadlock"
    assert not t2.is_alive(), "worker2 still running: deadlock"


@_gil_build_race_skip
def test_pure_python_version_thread_safety() -> None:
    """Concurrently mutating independent multidicts must never hand out the
    same version number twice: `_pure._version` is a single counter shared
    by every instance in the process (see `_pure.MultiDict._incr_version`)."""
    n_threads = 16
    n_iters = 2000
    all_versions: list[list[int]] = []
    lock = threading.Lock()

    def worker(_n: int) -> None:
        mm: _pure.MultiDict[object] = _pure.MultiDict()
        versions = []
        for i in range(n_iters):
            mm["key"] = i
            versions.append(_pure.getversion(mm))
        with lock:
            all_versions.append(versions)

    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        list(executor.map(worker, range(n_threads)))

    flat_versions = [v for versions in all_versions for v in versions]
    assert len(set(flat_versions)) == len(flat_versions)


def test_pure_python_repr_reentrant_thread_safety() -> None:
    """`repr()` calling back into the same, already-locked multidict must
    not deadlock: the per-instance lock is an RLock precisely so a
    callback like this (a value's own `__repr__` mutating the multidict
    it's part of) can still acquire it from the same thread."""
    md: _pure.MultiDict[object] = _pure.MultiDict()

    class Evil:
        def __repr__(self) -> str:
            md.add("x", 1)
            return "e"

    md.add("k", Evil())
    md.add("k2", Evil())
    assert isinstance(repr(md), str)


def test_pure_python_extend_reentrant_no_deadlock() -> None:
    """extend()/update()/merge() reading an argument that calls back into
    the same, already-locked multidict must not deadlock.

    A `SupportsKeys` argument's `keys()` (or a plain sequence's iteration)
    runs arbitrary Python code while `_locked_pair_always` already holds
    `self._lock`; since that lock is an RLock, the same thread can still
    acquire it again from such a callback, on every build."""
    d: _pure.MultiDict[int] = _pure.MultiDict({"a": 1})

    class ReentrantMapping(dict[str, int]):
        def keys(self) -> Any:
            d.add("reentrant", 1)
            return super().keys()

    d.extend(ReentrantMapping(b=2))
    assert d["a"] == 1
    assert d["b"] == 2
    assert d["reentrant"] == 1


def test_pure_python_locking_is_free_threaded_only() -> None:
    """Methods whose only race is free-threading-specific must be the
    exact same function objects as their unlocked implementations on a
    GIL-enabled interpreter -- no wrapper, no lock, no overhead beyond
    what the module had before it gained any locking. On a free-threaded
    interpreter, they must be wrapped (the lock actually applies).

    getall()/getone()/__contains__() are read-only and still fall in this
    category. add()/__setitem__() are not: they share the GIL-reachable
    race update()/extend()/merge()/clear()/popitem() have (see
    `_locked_always`), so they're wrapped on every build."""
    if _pure._FREE_THREADED:
        assert hasattr(_pure.MultiDict.getall, "__wrapped__")
        assert hasattr(_pure.MultiDict.getone, "__wrapped__")
        assert hasattr(_pure.MultiDict.__contains__, "__wrapped__")
    else:
        assert not hasattr(_pure.MultiDict.getall, "__wrapped__")
        assert not hasattr(_pure.MultiDict.getone, "__wrapped__")
        assert not hasattr(_pure.MultiDict.__contains__, "__wrapped__")

    assert hasattr(_pure.MultiDict.add, "__wrapped__")
    assert hasattr(_pure.MultiDict.__setitem__, "__wrapped__")
    assert hasattr(_pure.MultiDict.update, "__wrapped__")


def test_subclassed_multidict(
    any_multidict_class: type[MultiDict[str]],
) -> None:
    """Test that subclassed MultiDicts work as expected."""

    class SubclassedMultiDict(any_multidict_class):  # type: ignore[valid-type, misc]
        """Subclassed MultiDict."""

    d1 = SubclassedMultiDict([("key", "value1")])
    d2 = SubclassedMultiDict([("key", "value2")])
    d3 = SubclassedMultiDict([("key", "value1")])
    assert d1 != d2
    assert d1 == d3
    assert d1 == SubclassedMultiDict([("key", "value1")])
    assert d1 != SubclassedMultiDict([("key", "value2")])


@pytest.mark.c_extension
def test_view_direct_instantiation_segfault() -> None:
    """Test that view objects cannot be instantiated directly (issue: segfault).

    This test only applies to the C extension implementation.
    """
    # Test that _ItemsView cannot be instantiated directly
    with pytest.raises(
        TypeError, match="cannot create '.*_ItemsView' instances directly"
    ):
        multidict._ItemsView()  # type: ignore[attr-defined]

    # Test that _KeysView cannot be instantiated directly
    with pytest.raises(
        TypeError, match="cannot create '.*_KeysView' instances directly"
    ):
        multidict._KeysView()  # type: ignore[attr-defined]

    # Test that _ValuesView cannot be instantiated directly
    with pytest.raises(
        TypeError, match="cannot create '.*_ValuesView' instances directly"
    ):
        multidict._ValuesView()  # type: ignore[attr-defined]


@pytest.mark.c_extension
def test_extend_update_merge_self_reference() -> None:
    """Updating a multidict from itself must not crash.  The C extension
    cached a raw pointer into the source table and then inserted into the
    destination; when they are the same object a resize freed the table being
    iterated (use-after-free).  ``extend(self)`` doubles the contents;
    ``update(self)``/``merge(self)`` leave it unchanged."""
    d = multidict.MultiDict([(f"k{i}", i) for i in range(64)])
    d.extend(d)
    assert len(d) == 128
    assert d.getall("k0") == [0, 0]

    d2 = multidict.MultiDict([("a", 1), ("a", 2), ("b", 3)])
    d2.update(d2)
    assert sorted(d2.items()) == [("a", 1), ("a", 2), ("b", 3)]

    d3 = multidict.CIMultiDict([("A", 1), ("b", 2)])
    d3.merge(d3)
    assert sorted(d3.items()) == [("A", 1), ("b", 2)]
    d3.extend(d3)
    assert len(d3) == 4


@pytest.mark.c_extension
def test_update_from_list_mutated_by_key_lookup() -> None:
    """A case-insensitive key whose ``.lower()`` shrinks the source list must
    not read past the end.  The C list fast-path cached the size once and then
    indexed with a stale value after the callback mutated the list."""
    seq: list[list[object]] = []

    class EvilKey(str):
        def lower(self) -> str:
            del seq[1:]  # shrink the list while it is being consumed
            return "x"

    for i in range(32):
        seq.append([EvilKey(f"K{i}"), i])
    # Must not segfault; the exact result is unspecified, only memory safety.
    multidict.CIMultiDict(seq)  # type: ignore[arg-type]


@pytest.mark.c_extension
@pytest.mark.parametrize("cls_name", ("MultiDict", "CIMultiDict"))
def test_new_without_init_is_valid_empty(cls_name: str) -> None:
    """A container built with ``__new__`` but no ``__init__`` (or a subclass
    that skips ``super().__init__()``) must be a usable empty mapping, not a
    segfault.  ``tp_new`` initialises the internal state to empty."""
    cls = getattr(multidict, cls_name)

    d = cls.__new__(cls)
    assert len(d) == 0
    assert d.get("k") is None
    with pytest.raises(KeyError):
        d["k"]
    d["a"] = "1"
    assert d["a"] == "1"

    # case-insensitivity is preserved for CIMultiDict built this way
    if cls_name == "CIMultiDict":
        e = cls.__new__(cls)
        e["A"] = "1"
        assert e["a"] == "1"

    # a subclass that forgets to call super().__init__() is also safe
    class Sub(cls):  # type: ignore[valid-type, misc]
        def __init__(self) -> None:
            pass

    s = Sub()
    assert len(s) == 0
    assert s.get("missing") is None


@pytest.mark.c_extension
def test_iter_direct_instantiation_segfault() -> None:
    """Iterator objects cannot be instantiated directly (issue: segfault).

    Companion to ``test_view_direct_instantiation_segfault``: the iterator
    types share the same hole -- ``type(iter(md.keys())).__new__(t)`` used to
    build an uninitialised iterator whose ``next()`` dereferenced a NULL
    ``md`` pointer and segfaulted.  This test only applies to the C extension.
    """
    md = multidict.MultiDict([("a", "1")])
    for view_name, iter_name in (
        ("keys", "_keysiter"),
        ("items", "_itemsiter"),
        ("values", "_valuesiter"),
    ):
        iter_type = type(iter(getattr(md, view_name)()))
        with pytest.raises(
            TypeError, match=f"cannot create '.*{iter_name}' instances directly"
        ):
            iter_type.__new__(iter_type)  # type: ignore[call-overload]


@pytest.mark.c_extension
def test_non_typeerror_exceptions_are_not_swallowed() -> None:
    """Feature-detection fallbacks (probing ``__len__``/``keys``/``items``)
    must clear only the expected TypeError/AttributeError, not swallow every
    exception -- a ``MemoryError`` or ``KeyboardInterrupt`` raised by the
    probed object has to propagate."""
    md = multidict.MultiDict([("a", "1")])

    class BadLen:
        def __len__(self) -> int:
            raise MemoryError("boom")

    # view richcompare probes len(other)
    with pytest.raises(MemoryError):
        md.keys() <= BadLen()  # type: ignore[operator]  # noqa: B015

    # items-view __contains__ probes len(candidate)
    with pytest.raises(MemoryError):
        md.items().__contains__(BadLen())  # type: ignore[operator]

    # extend()/constructor probes arg.items()
    class BadItems:
        def keys(self) -> list[str]:
            return ["x"]  # pragma: no cover

        def items(self) -> object:
            raise MemoryError("boom")

        def __getitem__(self, key: str) -> int:
            return 1  # pragma: no cover

    with pytest.raises(MemoryError):
        multidict.MultiDict(BadItems())

    # __eq__ against a non-mapping still works (AttributeError is cleared)
    assert md != [("a", "1")]
