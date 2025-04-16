from __future__ import annotations

import gc
import operator
import sys
import weakref
from collections import deque
from collections.abc import Callable, Iterable, Iterator, KeysView, Mapping
from types import ModuleType
from typing import TypeVar, Union, cast

import pytest

import multidict
from multidict import (
    CIMultiDict,
    MultiDict,
    MultiDictProxy,
    MultiMapping,
    MutableMultiMapping,
)

_T = TypeVar("_T")


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
        nonlocal callables

        callable_chain = (getattr(module, name) for name in callables)
        first_callable = next(callable_chain)

        value = first_callable(*args, **kwargs)
        for element in callable_chain:
            value = element(value)

        return cast(
            Union[
                MultiMapping[Union[int, str]],
                MutableMultiMapping[Union[int, str]],
            ],
            value,
        )

    return chained_call


@pytest.fixture
def cls(  # type: ignore[misc]
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
    cls: type[MultiDict[Union[str, int]]],
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
        arg0: Union[list[tuple[str, str]], dict[str, str]],
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
        self, cls: Union[type[MultiDict[int]], type[CIMultiDict[int]]]
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
        cls: Union[
            type[MultiDict[Union[str, int]]],
            type[CIMultiDict[Union[str, int]]],
        ],
    ) -> None:
        d = cls([("key", "one"), ("key2", "two"), ("key", 3)])
        assert list(d) == ["key", "key2", "key"]

    def test__contains(
        self,
        cls: Union[
            type[MultiDict[Union[str, int]]],
            type[CIMultiDict[Union[str, int]]],
        ],
    ) -> None:
        d = cls([("key", "one"), ("key2", "two"), ("key", 3)])

        assert list(d) == ["key", "key2", "key"]

        assert "key" in d
        assert "key2" in d

        assert "foo" not in d
        assert 42 not in d  # type: ignore[comparison-overlap]

    def test_keys__contains(
        self,
        cls: Union[
            type[MultiDict[Union[str, int]]],
            type[CIMultiDict[Union[str, int]]],
        ],
    ) -> None:
        d = cls([("key", "one"), ("key2", "two"), ("key", 3)])

        assert list(d.keys()) == ["key", "key2", "key"]

        assert "key" in d.keys()
        assert "key2" in d.keys()

        assert "foo" not in d.keys()
        assert 42 not in d.keys()  # type: ignore[comparison-overlap]

    def test_values__contains(
        self,
        cls: Union[
            type[MultiDict[Union[str, int]]],
            type[CIMultiDict[Union[str, int]]],
        ],
    ) -> None:
        d = cls([("key", "one"), ("key", "two"), ("key", 3)])

        assert list(d.values()) == ["one", "two", 3]

        assert "one" in d.values()
        assert "two" in d.values()
        assert 3 in d.values()

        assert "foo" not in d.values()

    def test_items__contains(
        self,
        cls: Union[
            type[MultiDict[Union[str, int]]],
            type[CIMultiDict[Union[str, int]]],
        ],
    ) -> None:
        d = cls([("key", "one"), ("key", "two"), ("key", 3)])

        assert list(d.items()) == [("key", "one"), ("key", "two"), ("key", 3)]

        assert ("key", "one") in d.items()
        assert ("key", "two") in d.items()
        assert ("key", 3) in d.items()

        assert ("foo", "bar") not in d.items()
        assert (42, 3) not in d.items()  # type: ignore[comparison-overlap]
        assert 42 not in d.items()  # type: ignore[comparison-overlap]

    def test_cannot_create_from_unaccepted(
        self,
        cls: type[MutableMultiMapping[str]],
    ) -> None:
        with pytest.raises(ValueError, match="multidict update sequence element"):
            cls([(1, 2, 3)])  # type: ignore[call-arg]

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
        self, cls: Union[type[MultiDict[int]], type[CIMultiDict[int]]]
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
        cls: Union[type[MultiDict[int]], type[CIMultiDict[int]]],
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
        cls: Union[type[MultiDict[int]], type[CIMultiDict[int]]],
    ) -> None:
        md = cls(a=1, b=2)
        it = iter(md.keys())
        assert it.__length_hint__() == 2  # type: ignore[attr-defined]

    def test_iter_length_hint_items(
        self,
        cls: Union[type[MultiDict[int]], type[CIMultiDict[int]]],
    ) -> None:
        md = cls(a=1, b=2)
        it = iter(md.items())
        assert it.__length_hint__() == 2  # type: ignore[attr-defined]

    def test_iter_length_hint_values(
        self,
        cls: Union[type[MultiDict[int]], type[CIMultiDict[int]]],
    ) -> None:
        md = cls(a=1, b=2)
        it = iter(md.values())
        assert it.__length_hint__() == 2

    def test_ctor_list_arg_and_kwds(
        self,
        cls: Union[type[MultiDict[int]], type[CIMultiDict[int]]],
    ) -> None:
        arg = [("a", 1)]
        obj = cls(arg, b=2)
        assert list(obj.items()) == [("a", 1), ("b", 2)]
        assert arg == [("a", 1)]

    def test_ctor_tuple_arg_and_kwds(
        self,
        cls: Union[type[MultiDict[int]], type[CIMultiDict[int]]],
    ) -> None:
        arg = (("a", 1),)
        obj = cls(arg, b=2)
        assert list(obj.items()) == [("a", 1), ("b", 2)]
        assert arg == (("a", 1),)

    def test_ctor_deque_arg_and_kwds(
        self,
        cls: Union[type[MultiDict[int]], type[CIMultiDict[int]]],
    ) -> None:
        arg = deque([("a", 1)])
        obj = cls(arg, b=2)
        assert list(obj.items()) == [("a", 1), ("b", 2)]
        assert arg == deque([("a", 1)])


class TestMultiDict(BaseMultiDictTest):
    @pytest.fixture(
        params=[
            ("MultiDict",),
            ("MultiDict", "MultiDictProxy"),
        ],
    )
    def cls(  # type: ignore[misc]
        self,
        request: pytest.FixtureRequest,
        multidict_module: ModuleType,
    ) -> Callable[..., MultiMapping[int | str] | MutableMultiMapping[int | str]]:
        """Make a case-sensitive multidict class/proxy constructor."""
        return chained_callable(multidict_module, request.param)

    def test__repr__(self, cls: type[MultiDict[str]]) -> None:
        d = cls()
        _cls = type(d)

        assert str(d) == "<%s()>" % _cls.__name__

        d = cls([("key", "one"), ("key", "two")])

        assert str(d) == "<%s('key': 'one', 'key': 'two')>" % _cls.__name__

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
        cls: type[MultiDict[Union[str, int]]],
    ) -> None:
        d = cls([("a", 1), ("b", "2"), ("a", 3)])
        s = "&".join("{}={}".format(k, v) for k, v in d.items())

        assert s == "a=1&b=2&a=3"

    def test_get(self, cls: type[MultiDict[int]]) -> None:
        d = cls([("a", 1), ("a", 2)])
        assert d["a"] == 1

    def test_items__repr__(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")], key="value2")
        expected = "<_ItemsView('key': 'value1', 'key': 'value2')>"
        assert repr(d.items()) == expected

    def test_keys__repr__(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")], key="value2")
        assert repr(d.keys()) == "<_KeysView('key', 'key')>"

    def test_values__repr__(self, cls: type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")], key="value2")
        assert repr(d.values()) == "<_ValuesView('value1', 'value2')>"


class TestCIMultiDict(BaseMultiDictTest):
    @pytest.fixture(
        params=[
            ("CIMultiDict",),
            ("CIMultiDict", "CIMultiDictProxy"),
        ],
    )
    def cls(  # type: ignore[misc]
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

        expected = "<%s('KEY': 'value1', 'key': 'value2')>" % _cls.__name__
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
        d = cls([("KEY", "one")])

        assert d.items() | {("key", "one"), ("other", "two")} == {
            ("KEY", "one"),
            ("other", "two"),
        }

    def test_items_case_insensitive_ror(self, cls: type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "one"), ("KEY2", "three")])

        assert [("key", "one"), ("other", "two")] | d.items() == {
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
