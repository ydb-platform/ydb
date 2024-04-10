from __future__ import annotations

import gc
import operator
import sys
import weakref
from collections import deque
from collections.abc import Mapping
from types import ModuleType
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    KeysView,
    List,
    Mapping,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

import pytest

import multidict
from multidict import CIMultiDict, MultiDict, MultiMapping, MutableMultiMapping


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
    assert any_multidict_class_name in multidict.__all__  # type: ignore[attr-defined]


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
    cls: Type[MultiDict[Union[str, int]]],
    key_cls: Type[object],
) -> None:
    d = cls([("key", "one"), ("key2", "two"), ("key", 3)])
    for i in d:
        assert type(i) is key_cls, (type(i), key_cls)


def test_proxy_copy(
    any_multidict_class: Type[MutableMultiMapping[str]],
    any_multidict_proxy_class: Type[MultiMapping[str]],
) -> None:
    d1 = any_multidict_class(key="value", a="b")
    p1 = any_multidict_proxy_class(d1)

    d2 = p1.copy()  # type: ignore[attr-defined]
    assert d1 == d2
    assert d1 is not d2


def test_multidict_subclassing(
    any_multidict_class: Type[MutableMultiMapping[str]],
) -> None:
    class DummyMultidict(any_multidict_class):  # type: ignore[valid-type,misc]
        pass


def test_multidict_proxy_subclassing(
    any_multidict_proxy_class: Type[MultiMapping[str]],
) -> None:
    class DummyMultidictProxy(
        any_multidict_proxy_class,  # type: ignore[valid-type,misc]
    ):
        pass


class BaseMultiDictTest:
    def test_instantiate__empty(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls()
        empty: Mapping[str, str] = {}
        assert d == empty
        assert len(d) == 0
        assert list(d.keys()) == []
        assert list(d.values()) == []
        assert list(d.items()) == []

        assert cls() != list()  # type: ignore[comparison-overlap]
        with pytest.raises(TypeError, match=r"(2 given)"):
            cls(("key1", "value1"), ("key2", "value2"))  # type: ignore[call-arg]  # noqa: E501

    @pytest.mark.parametrize("arg0", ([("key", "value1")], {"key": "value1"}))
    def test_instantiate__from_arg0(
        self,
        cls: Type[MutableMultiMapping[str]],
        arg0: Union[List[Tuple[str, str]], Dict[str, str]],
    ) -> None:
        d = cls(arg0)

        assert d == {"key": "value1"}
        assert len(d) == 1
        assert list(d.keys()) == ["key"]
        assert list(d.values()) == ["value1"]
        assert list(d.items()) == [("key", "value1")]

    def test_instantiate__with_kwargs(
        self,
        cls: Type[MutableMultiMapping[str]],
    ) -> None:
        d = cls([("key", "value1")], key2="value2")

        assert d == {"key": "value1", "key2": "value2"}
        assert len(d) == 2
        assert sorted(d.keys()) == ["key", "key2"]
        assert sorted(d.values()) == ["value1", "value2"]
        assert sorted(d.items()) == [("key", "value1"), ("key2", "value2")]

    def test_instantiate__from_generator(
        self, cls: Union[Type[MultiDict[int]], Type[CIMultiDict[int]]]
    ) -> None:
        d = cls((str(i), i) for i in range(2))

        assert d == {"0": 0, "1": 1}
        assert len(d) == 2
        assert sorted(d.keys()) == ["0", "1"]
        assert sorted(d.values()) == [0, 1]
        assert sorted(d.items()) == [("0", 0), ("1", 1)]

    def test_instantiate__from_list_of_lists(
        self,
        cls: Type[MutableMultiMapping[str]],
    ) -> None:
        # Should work at runtime, but won't type check.
        d = cls([["key", "value1"]])  # type: ignore[call-arg]
        assert d == {"key": "value1"}

    def test_instantiate__from_list_of_custom_pairs(
        self,
        cls: Type[MutableMultiMapping[str]],
    ) -> None:
        class Pair:
            def __len__(self) -> int:
                return 2

            def __getitem__(self, pos: int) -> str:
                return ("key", "value1")[pos]

        # Works at runtime, but won't type check.
        d = cls([Pair()])
        assert d == {"key": "value1"}

    def test_getone(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1")], key="value2")

        assert d.getone("key") == "value1"
        assert d.get("key") == "value1"
        assert d["key"] == "value1"

        with pytest.raises(KeyError, match="key2"):
            d["key2"]
        with pytest.raises(KeyError, match="key2"):
            d.getone("key2")

        assert d.getone("key2", "default") == "default"

    def test_call_with_kwargs(self, cls: Type[MultiDict[str]]) -> None:
        d = cls([("present", "value")])
        assert d.getall(default="missing", key="notfound") == "missing"

    def test__iter__(
        self,
        cls: Union[
            Type[MultiDict[Union[str, int]]],
            Type[CIMultiDict[Union[str, int]]],
        ],
    ) -> None:
        d = cls([("key", "one"), ("key2", "two"), ("key", 3)])
        assert list(d) == ["key", "key2", "key"]

    def test_keys__contains(
        self,
        cls: Union[
            Type[MultiDict[Union[str, int]]],
            Type[CIMultiDict[Union[str, int]]],
        ],
    ) -> None:
        d = cls([("key", "one"), ("key2", "two"), ("key", 3)])

        assert list(d.keys()) == ["key", "key2", "key"]

        assert "key" in d.keys()
        assert "key2" in d.keys()

        assert "foo" not in d.keys()

    def test_values__contains(
        self,
        cls: Union[
            Type[MultiDict[Union[str, int]]],
            Type[CIMultiDict[Union[str, int]]],
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
            Type[MultiDict[Union[str, int]]],
            Type[CIMultiDict[Union[str, int]]],
        ],
    ) -> None:
        d = cls([("key", "one"), ("key", "two"), ("key", 3)])

        assert list(d.items()) == [("key", "one"), ("key", "two"), ("key", 3)]

        assert ("key", "one") in d.items()
        assert ("key", "two") in d.items()
        assert ("key", 3) in d.items()

        assert ("foo", "bar") not in d.items()

    def test_cannot_create_from_unaccepted(
        self,
        cls: Type[MutableMultiMapping[str]],
    ) -> None:
        with pytest.raises(TypeError):
            cls([(1, 2, 3)])  # type: ignore[call-arg]

    def test_keys_is_set_less(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1")])

        assert d.keys() < {"key", "key2"}

    def test_keys_is_set_less_equal(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1")])

        assert d.keys() <= {"key"}

    def test_keys_is_set_equal(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1")])

        assert d.keys() == {"key"}

    def test_keys_is_set_greater(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key", "key2"} > d.keys()

    def test_keys_is_set_greater_equal(
        self,
        cls: Type[MutableMultiMapping[str]],
    ) -> None:
        d = cls([("key", "value1")])

        assert {"key"} >= d.keys()

    def test_keys_is_set_not_equal(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1")])

        assert d.keys() != {"key2"}

    def test_eq(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key": "value1"} == d

    def test_eq2(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d1 = cls([("key", "value1")])
        d2 = cls([("key2", "value1")])

        assert d1 != d2

    def test_eq3(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d1 = cls([("key", "value1")])
        d2 = cls()

        assert d1 != d2

    def test_eq_other_mapping_contains_more_keys(
        self,
        cls: Type[MutableMultiMapping[str]],
    ) -> None:
        d1 = cls(foo="bar")
        d2 = dict(foo="bar", bar="baz")

        assert d1 != d2

    def test_eq_bad_mapping_len(
        self, cls: Union[Type[MultiDict[int]], Type[CIMultiDict[int]]]
    ) -> None:
        class BadMapping(Mapping[str, int]):
            def __getitem__(self, key: str) -> int:
                return 1  # pragma: no cover  # `len()` fails earlier

            def __iter__(self) -> Iterator[str]:
                yield "a"  # pragma: no cover  # `len()` fails earlier

            def __len__(self) -> int:  # type: ignore[return]
                1 / 0

        d1 = cls(a=1)
        d2 = BadMapping()
        with pytest.raises(ZeroDivisionError):
            d1 == d2

    def test_eq_bad_mapping_getitem(
        self,
        cls: Union[Type[MultiDict[int]], Type[CIMultiDict[int]]],
    ) -> None:
        class BadMapping(Mapping[str, int]):
            def __getitem__(self, key: str) -> int:  # type: ignore[return]
                1 / 0

            def __iter__(self) -> Iterator[str]:
                yield "a"  # pragma: no cover  # foreign objects no iterated

            def __len__(self) -> int:
                return 1

        d1 = cls(a=1)
        d2 = BadMapping()
        with pytest.raises(ZeroDivisionError):
            d1 == d2

    def test_ne(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1")])

        assert d != {"key": "another_value"}

    def test_and(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key"} == d.keys() & {"key", "key2"}

    def test_and2(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key"} == {"key", "key2"} & d.keys()

    def test_bitwise_and_not_implemented(
        self, cls: Type[MutableMultiMapping[str]]
    ) -> None:
        d = cls([("key", "value1")])

        sentinel_operation_result = object()

        class RightOperand:
            def __rand__(self, other: KeysView[str]) -> object:
                assert isinstance(other, KeysView)
                return sentinel_operation_result

        assert d.keys() & RightOperand() is sentinel_operation_result

    def test_bitwise_and_iterable_not_set(
        self, cls: Type[MutableMultiMapping[str]]
    ) -> None:
        d = cls([("key", "value1")])

        assert {"key"} == d.keys() & ["key", "key2"]

    def test_or(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key", "key2"} == d.keys() | {"key2"}

    def test_or2(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1")])

        assert {"key", "key2"} == {"key2"} | d.keys()

    def test_bitwise_or_not_implemented(
        self, cls: Type[MutableMultiMapping[str]]
    ) -> None:
        d = cls([("key", "value1")])

        sentinel_operation_result = object()

        class RightOperand:
            def __ror__(self, other: KeysView[str]) -> object:
                assert isinstance(other, KeysView)
                return sentinel_operation_result

        assert d.keys() | RightOperand() is sentinel_operation_result

    def test_bitwise_or_iterable_not_set(
        self, cls: Type[MutableMultiMapping[str]]
    ) -> None:
        d = cls([("key", "value1")])

        assert {"key", "key2"} == d.keys() | ["key2"]

    def test_sub(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert {"key"} == d.keys() - {"key2"}

    def test_sub2(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert {"key3"} == {"key", "key2", "key3"} - d.keys()

    def test_sub_not_implemented(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        sentinel_operation_result = object()

        class RightOperand:
            def __rsub__(self, other: KeysView[str]) -> object:
                assert isinstance(other, KeysView)
                return sentinel_operation_result

        assert d.keys() - RightOperand() is sentinel_operation_result

    def test_sub_iterable_not_set(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert {"key"} == d.keys() - ["key2"]

    def test_xor(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert {"key", "key3"} == d.keys() ^ {"key2", "key3"}

    def test_xor2(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert {"key", "key3"} == {"key2", "key3"} ^ d.keys()

    def test_xor_not_implemented(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        sentinel_operation_result = object()

        class RightOperand:
            def __rxor__(self, other: KeysView[str]) -> object:
                assert isinstance(other, KeysView)
                return sentinel_operation_result

        assert d.keys() ^ RightOperand() is sentinel_operation_result

    def test_xor_iterable_not_set(self, cls: Type[MutableMultiMapping[str]]) -> None:
        d = cls([("key", "value1"), ("key2", "value2")])

        assert {"key", "key3"} == d.keys() ^ ["key2", "key3"]

    @pytest.mark.parametrize(
        ("key", "value", "expected"),
        (("key2", "v", True), ("key", "value1", False)),
    )
    def test_isdisjoint(
        self, cls: Type[MutableMultiMapping[str]], key: str, value: str, expected: bool
    ) -> None:
        d = cls([("key", "value1")])
        assert d.items().isdisjoint({(key, value)}) is expected
        assert d.keys().isdisjoint({key}) is expected

    def test_repr_aiohttp_issue_410(self, cls: Type[MutableMultiMapping[str]]) -> None:
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
        cls: Type[MutableMultiMapping[str]],
        op: Callable[[object, object], object],
        other: Set[str],
    ) -> None:
        d = cls([("key", "value")])

        try:
            raise Exception
            pytest.fail("Should never happen")  # pragma: no cover
        except Exception as e:
            op(d.keys(), other)

            assert sys.exc_info()[1] == e  # noqa: PT017

    def test_weakref(self, cls: Type[MutableMultiMapping[str]]) -> None:
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
        cls: Union[Type[MultiDict[int]], Type[CIMultiDict[int]]],
    ) -> None:
        md = cls(a=1, b=2)
        it = iter(md.keys())
        assert it.__length_hint__() == 2  # type: ignore[attr-defined]

    def test_iter_length_hint_items(
        self,
        cls: Union[Type[MultiDict[int]], Type[CIMultiDict[int]]],
    ) -> None:
        md = cls(a=1, b=2)
        it = iter(md.items())
        assert it.__length_hint__() == 2  # type: ignore[attr-defined]

    def test_iter_length_hint_values(
        self,
        cls: Union[Type[MultiDict[int]], Type[CIMultiDict[int]]],
    ) -> None:
        md = cls(a=1, b=2)
        it = iter(md.values())
        assert it.__length_hint__() == 2  # type: ignore[attr-defined]

    def test_ctor_list_arg_and_kwds(
        self,
        cls: Union[Type[MultiDict[int]], Type[CIMultiDict[int]]],
    ) -> None:
        arg = [("a", 1)]
        obj = cls(arg, b=2)
        assert list(obj.items()) == [("a", 1), ("b", 2)]
        assert arg == [("a", 1)]

    def test_ctor_tuple_arg_and_kwds(
        self,
        cls: Union[Type[MultiDict[int]], Type[CIMultiDict[int]]],
    ) -> None:
        arg = (("a", 1),)
        obj = cls(arg, b=2)
        assert list(obj.items()) == [("a", 1), ("b", 2)]
        assert arg == (("a", 1),)

    def test_ctor_deque_arg_and_kwds(
        self,
        cls: Union[Type[MultiDict[int]], Type[CIMultiDict[int]]],
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

    def test__repr__(self, cls: Type[MultiDict[str]]) -> None:
        d = cls()
        _cls = type(d)

        assert str(d) == "<%s()>" % _cls.__name__

        d = cls([("key", "one"), ("key", "two")])

        assert str(d) == "<%s('key': 'one', 'key': 'two')>" % _cls.__name__

    def test_getall(self, cls: Type[MultiDict[str]]) -> None:
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
        cls: Type[MultiDict[Union[str, int]]],
    ) -> None:
        d = cls([("a", 1), ("b", "2"), ("a", 3)])
        s = "&".join("{}={}".format(k, v) for k, v in d.items())

        assert s == "a=1&b=2&a=3"

    def test_get(self, cls: Type[MultiDict[int]]) -> None:
        d = cls([("a", 1), ("a", 2)])
        assert d["a"] == 1

    def test_items__repr__(self, cls: Type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")], key="value2")
        expected = "_ItemsView('key': 'value1', 'key': 'value2')"
        assert repr(d.items()) == expected

    def test_keys__repr__(self, cls: Type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")], key="value2")
        assert repr(d.keys()) == "_KeysView('key', 'key')"

    def test_values__repr__(self, cls: Type[MultiDict[str]]) -> None:
        d = cls([("key", "value1")], key="value2")
        assert repr(d.values()) == "_ValuesView('value1', 'value2')"


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

    def test_basics(self, cls: Type[CIMultiDict[str]]) -> None:
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

    def test_getall(self, cls: Type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], KEY="value2")

        assert not d == {"KEY": "value1"}
        assert len(d) == 2

        assert d.getall("key") == ["value1", "value2"]

        with pytest.raises(KeyError, match="some_key"):
            d.getall("some_key")

    def test_get(self, cls: Type[CIMultiDict[int]]) -> None:
        d = cls([("A", 1), ("a", 2)])
        assert 1 == d["a"]

    def test__repr__(self, cls: Type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], key="value2")
        _cls = type(d)

        expected = "<%s('KEY': 'value1', 'key': 'value2')>" % _cls.__name__
        assert str(d) == expected

    def test_items__repr__(self, cls: Type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], key="value2")
        expected = "_ItemsView('KEY': 'value1', 'key': 'value2')"
        assert repr(d.items()) == expected

    def test_keys__repr__(self, cls: Type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], key="value2")
        assert repr(d.keys()) == "_KeysView('KEY', 'key')"

    def test_values__repr__(self, cls: Type[CIMultiDict[str]]) -> None:
        d = cls([("KEY", "value1")], key="value2")
        assert repr(d.values()) == "_ValuesView('value1', 'value2')"
