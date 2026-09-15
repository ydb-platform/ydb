import string
import sys
from typing import Union

import pytest
from multidict import (
    CIMultiDict,
    CIMultiDictProxy,
    MultiDict,
    MultiDictProxy,
    istr,
)


class TestMutableMultiDict:
    def test_copy(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d1 = case_sensitive_multidict_class(key="value", a="b")

        d2 = d1.copy()
        assert d1 == d2
        assert d1 is not d2

    def test__repr__(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class()
        assert str(d) == "<%s()>" % case_sensitive_multidict_class.__name__

        d = case_sensitive_multidict_class([("key", "one"), ("key", "two")])

        expected = (
            f"<{case_sensitive_multidict_class.__name__}('key': 'one', 'key': 'two')>"
        )
        assert str(d) == expected

    def test_getall(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class([("key", "value1")], key="value2")
        assert len(d) == 2

        assert d.getall("key") == ["value1", "value2"]

        with pytest.raises(KeyError, match="some_key"):
            d.getall("some_key")

        default = object()
        assert d.getall("some_key", default) is default

    def test_add(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class()

        assert d == {}
        d["key"] = "one"
        assert d == {"key": "one"}
        assert d.getall("key") == ["one"]

        d["key"] = "two"
        assert d == {"key": "two"}
        assert d.getall("key") == ["two"]

        d.add("key", "one")
        assert 2 == len(d)
        assert d.getall("key") == ["two", "one"]

        d.add("foo", "bar")
        assert 3 == len(d)
        assert d.getall("foo") == ["bar"]

    def test_extend(
        self,
        case_sensitive_multidict_class: type[MultiDict[Union[str, int]]],
    ) -> None:
        d = case_sensitive_multidict_class()
        assert d == {}

        d.extend([("key", "one"), ("key", "two")], key=3, foo="bar")
        assert d != {"key": "one", "foo": "bar"}
        assert 4 == len(d)
        itms = d.items()
        # we can't guarantee order of kwargs
        assert ("key", "one") in itms
        assert ("key", "two") in itms
        assert ("key", 3) in itms
        assert ("foo", "bar") in itms

        other = case_sensitive_multidict_class(bar="baz")
        assert other == {"bar": "baz"}

        d.extend(other)
        assert ("bar", "baz") in d.items()

        d.extend({"foo": "moo"})
        assert ("foo", "moo") in d.items()

        d.extend()
        assert 6 == len(d)

        with pytest.raises(TypeError):
            d.extend("foo", "bar")  # type: ignore[arg-type, call-arg]

    def test_extend_from_proxy(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
        case_sensitive_multidict_proxy_class: type[MultiDictProxy[str]],
    ) -> None:
        d = case_sensitive_multidict_class([("a", "a"), ("b", "b")])
        proxy = case_sensitive_multidict_proxy_class(d)

        d2 = case_sensitive_multidict_class()
        d2.extend(proxy)

        assert [("a", "a"), ("b", "b")] == list(d2.items())

    def test_clear(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class([("key", "one")], key="two", foo="bar")

        d.clear()
        assert d == {}
        assert list(d.items()) == []

    def test_del(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class([("key", "one"), ("key", "two")], foo="bar")
        assert list(d.keys()) == ["key", "key", "foo"]

        del d["key"]
        assert d == {"foo": "bar"}
        assert list(d.items()) == [("foo", "bar")]

        with pytest.raises(KeyError, match="key"):
            del d["key"]

    def test_set_default(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class([("key", "one"), ("key", "two")], foo="bar")
        assert "one" == d.setdefault("key", "three")
        assert "three" == d.setdefault("otherkey", "three")
        assert "otherkey" in d
        assert "three" == d["otherkey"]

    def test_set_default_single_arg(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class([("key", "one"), ("key", "two")], foo="bar")
        assert d.setdefault("key") == "one"  # type: ignore[call-arg]
        assert d.setdefault("noexist") is None  # type: ignore[call-arg]
        assert d["noexist"] is None

    def test_popitem(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class()
        d.add("key", "val1")
        d.add("key", "val2")

        assert ("key", "val2") == d.popitem()
        assert len(d) == 1
        assert [("key", "val1")] == list(d.items())

    def test_popitem2(
        self,
        case_sensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class()
        d.add("key", "val1")
        d.add("key", "val2")
        d.add("key2", "val3")

        del d["key2"]  # make dummy at the end

        assert ("key", "val2") == d.popitem()
        assert [("key", "val1")] == list(d.items())

    def test_popitem_empty_multidict(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class()

        with pytest.raises(KeyError):
            d.popitem()

    def test_pop(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class()
        d.add("key", "val1")
        d.add("key", "val2")

        assert "val1" == d.pop("key")
        assert {"key": "val2"} == d

    def test_pop2(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class()
        d.add("key", "val1")
        d.add("key2", "val2")
        d.add("key", "val3")

        assert "val1" == d.pop("key")
        assert [("key2", "val2"), ("key", "val3")] == list(d.items())

    def test_pop_default(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class(other="val")

        assert "default" == d.pop("key", "default")
        assert "other" in d

    def test_pop_raises(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class(other="val")

        with pytest.raises(KeyError, match="key"):
            d.pop("key")

        assert "other" in d

    def test_replacement_order(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class()
        d.add("key1", "val1")
        d.add("key2", "val2")
        d.add("key1", "val3")
        d.add("key2", "val4")

        d["key1"] = "val"

        expected = [("key1", "val"), ("key2", "val2"), ("key2", "val4")]

        assert expected == list(d.items())

    def test_nonstr_key(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class()
        with pytest.raises(TypeError):
            d[1] = "val"  # type: ignore[index]

    def test_istr_key(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
        case_insensitive_str_class: type[str],
    ) -> None:
        d = case_sensitive_multidict_class()
        d[case_insensitive_str_class("1")] = "val"
        assert type(list(d.keys())[0]) is case_insensitive_str_class

    def test_str_derived_key(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        class A(str):
            pass

        d = case_sensitive_multidict_class()
        d[A("1")] = "val"
        assert type(list(d.keys())[0]) is A

    def test_istr_key_add(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
        case_insensitive_str_class: type[str],
    ) -> None:
        d = case_sensitive_multidict_class()
        d.add(case_insensitive_str_class("1"), "val")
        assert type(list(d.keys())[0]) is case_insensitive_str_class

    def test_str_derived_key_add(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        class A(str):
            pass

        d = case_sensitive_multidict_class()
        d.add(A("1"), "val")
        assert type(list(d.keys())[0]) is A

    def test_popall(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class()
        d.add("key1", "val1")
        d.add("key2", "val2")
        d.add("key1", "val3")
        ret = d.popall("key1")
        assert ["val1", "val3"] == ret
        assert {"key2": "val2"} == d

    def test_popall_default(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class()
        assert "val" == d.popall("key", "val")

    def test_popall_key_error(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
    ) -> None:
        d = case_sensitive_multidict_class()
        with pytest.raises(KeyError, match="key"):
            d.popall("key")

    def test_large_multidict_resizing(
        self,
        case_sensitive_multidict_class: type[MultiDict[int]],
    ) -> None:
        SIZE = 1024
        d = case_sensitive_multidict_class()
        for i in range(SIZE):
            d["key" + str(i)] = i

        for i in range(SIZE - 1):
            del d["key" + str(i)]

        assert {"key" + str(SIZE - 1): SIZE - 1} == d

    def test_update(
        self,
        case_sensitive_multidict_class: type[MultiDict[Union[str, int]]],
    ) -> None:
        d = case_sensitive_multidict_class()
        assert d == {}

        d.update([("key", "one"), ("key", "two")], key=3, foo="bar")
        assert d != {"key": "one", "foo": "bar"}
        assert 4 == len(d)
        itms = d.items()
        # we can't guarantee order of kwargs
        assert ("key", "one") in itms
        assert ("key", "two") in itms
        assert ("key", 3) in itms
        assert ("foo", "bar") in itms

        other = case_sensitive_multidict_class(bar="baz")
        assert other == {"bar": "baz"}

        d.update(other)
        assert ("bar", "baz") in d.items()

        d.update({"foo": "moo"})
        assert ("foo", "moo") in d.items()

        d.update()
        assert 5 == len(d)

        with pytest.raises(TypeError):
            d.update("foo", "bar")  # type: ignore[arg-type, call-arg]

    def test_repr_with_dummy(
        self, case_sensitive_multidict_class: type[MultiDict[int]]
    ) -> None:
        d = case_sensitive_multidict_class({"a": 1, "b": 2, "c": 3})
        cls = d.__class__.__name__
        del d["b"]  # make a dummy entry
        assert repr(d) == f"<{cls}('a': 1, 'c': 3)>"

    def test_items_repr_with_dummy(
        self, case_sensitive_multidict_class: type[MultiDict[int]]
    ) -> None:
        d = case_sensitive_multidict_class({"a": 1, "b": 2, "c": 3})
        del d["b"]  # make a dummy entry
        cls = d.items().__class__.__name__
        assert repr(d.items()) == f"<{cls}('a': 1, 'c': 3)>"

    def test_keys_repr_with_dummy(
        self, case_sensitive_multidict_class: type[MultiDict[int]]
    ) -> None:
        d = case_sensitive_multidict_class({"a": 1, "b": 2, "c": 3})
        del d["b"]  # make a dummy entry
        cls = d.keys().__class__.__name__
        assert repr(d.keys()) == f"<{cls}('a', 'c')>"

    def test_values_repr_with_dummy(
        self, case_sensitive_multidict_class: type[MultiDict[int]]
    ) -> None:
        d = case_sensitive_multidict_class({"a": 1, "b": 2, "c": 3})
        del d["b"]  # make a dummy entry
        cls = d.values().__class__.__name__
        assert repr(d.values()) == f"<{cls}(1, 3)>"

    def test_huge_md(
        self,
        case_sensitive_multidict_class: type[MultiDict[int]],
    ) -> None:
        size = 1 << 16
        d = case_sensitive_multidict_class((str(i), i) for i in range(size))
        assert d[str(size // 2)] == size // 2

    def test_create_from_proxy(
        self,
        case_sensitive_multidict_class: type[MultiDict[int]],
        case_sensitive_multidict_proxy_class: type[MultiDictProxy[int]],
    ) -> None:
        d = case_sensitive_multidict_class({"a": 1, "b": 2, "c": 3})
        p = case_sensitive_multidict_proxy_class(d)
        d2 = case_sensitive_multidict_class(p)
        assert d2 == d

    def test_merge(
        self,
        case_sensitive_multidict_class: type[MultiDict[Union[str, int]]],
    ) -> None:
        d = case_sensitive_multidict_class({"key": "one"})
        assert d == {"key": "one"}

        d.merge([("key", "other"), ("key2", "two")], key2=3, foo="bar")
        assert 4 == len(d)
        itms = d.items()
        # we can't guarantee order of kwargs
        assert ("key", "one") in itms
        assert ("key2", "two") in itms
        assert ("key2", 3) in itms
        assert ("foo", "bar") in itms

        other = case_sensitive_multidict_class({"key": "other"}, bar="baz")

        d.merge(other)
        assert ("bar", "baz") in d.items()

        d.merge({"key": "other", "boo": "moo"})
        assert ("boo", "moo") in d.items()

        d.merge()
        assert 6 == len(d)

        assert ("key", "other") not in d.items()

        with pytest.raises(TypeError):
            d.merge("foo", "bar")  # type: ignore[arg-type, call-arg]

    def test_merge_from_proxy(
        self,
        case_sensitive_multidict_class: type[MultiDict[str]],
        case_sensitive_multidict_proxy_class: type[MultiDictProxy[str]],
    ) -> None:
        d = case_sensitive_multidict_class([("a", "a"), ("b", "b")])
        proxy = case_sensitive_multidict_proxy_class(d)

        d2 = case_sensitive_multidict_class()
        d2.merge(proxy)

        assert [("a", "a"), ("b", "b")] == list(d2.items())


class TestCIMutableMultiDict:
    def test_getall(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class([("KEY", "value1")], KEY="value2")

        assert d != {"KEY": "value1"}
        assert len(d) == 2

        assert d.getall("key") == ["value1", "value2"]

        with pytest.raises(KeyError, match="some_key"):
            d.getall("some_key")

    def test_ctor(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class(k1="v1")
        assert "v1" == d["K1"]
        assert ("k1", "v1") in d.items()

    def test_setitem(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class()
        d["k1"] = "v1"
        assert "v1" == d["K1"]
        assert ("k1", "v1") in d.items()

    def test_delitem(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class()
        d["k1"] = "v1"
        assert "K1" in d
        del d["k1"]
        assert "K1" not in d

    def test_copy(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d1 = case_insensitive_multidict_class(key="KEY", a="b")

        d2 = d1.copy()
        assert d1 == d2
        assert d1.items() == d2.items()
        assert d1 is not d2

    def test__repr__(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class()
        assert str(d) == "<%s()>" % case_insensitive_multidict_class.__name__

        d = case_insensitive_multidict_class([("KEY", "one"), ("KEY", "two")])

        expected = (
            f"<{case_insensitive_multidict_class.__name__}('KEY': 'one', 'KEY': 'two')>"
        )
        assert str(d) == expected

    def test_add(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class()

        assert d == {}
        d["KEY"] = "one"
        assert ("KEY", "one") in d.items()
        assert d == case_insensitive_multidict_class({"Key": "one"})
        assert d.getall("key") == ["one"]

        d["KEY"] = "two"
        assert ("KEY", "two") in d.items()
        assert d == case_insensitive_multidict_class({"Key": "two"})
        assert d.getall("key") == ["two"]

        d.add("KEY", "one")
        assert ("KEY", "one") in d.items()
        assert 2 == len(d)
        assert d.getall("key") == ["two", "one"]

        d.add("FOO", "bar")
        assert ("FOO", "bar") in d.items()
        assert 3 == len(d)
        assert d.getall("foo") == ["bar"]

        d.add(key="test", value="test")
        assert ("test", "test") in d.items()
        assert 4 == len(d)
        assert d.getall("test") == ["test"]

    def test_extend(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[Union[str, int]]],
    ) -> None:
        d = case_insensitive_multidict_class()
        assert d == {}

        d.extend([("KEY", "one"), ("key", "two")], key=3, foo="bar")
        assert 4 == len(d)
        itms = d.items()
        # we can't guarantee order of kwargs
        assert ("KEY", "one") in itms
        assert ("key", "two") in itms
        assert ("key", 3) in itms
        assert ("foo", "bar") in itms

        other = case_insensitive_multidict_class(Bar="baz")
        assert other == {"Bar": "baz"}

        d.extend(other)
        assert ("Bar", "baz") in d.items()
        assert "bar" in d

        d.extend({"Foo": "moo"})
        assert ("Foo", "moo") in d.items()
        assert "foo" in d

        d.extend()
        assert 6 == len(d)

        with pytest.raises(TypeError):
            d.extend("foo", "bar")  # type: ignore[arg-type, call-arg]

    def test_extend_from_proxy(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
        case_insensitive_multidict_proxy_class: type[CIMultiDictProxy[str]],
    ) -> None:
        d = case_insensitive_multidict_class([("a", "a"), ("b", "b")])
        proxy = case_insensitive_multidict_proxy_class(d)

        d2 = case_insensitive_multidict_class()
        d2.extend(proxy)

        assert [("a", "a"), ("b", "b")] == list(d2.items())

    def test_clear(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class([("KEY", "one")], key="two", foo="bar")

        d.clear()
        assert d == {}
        assert list(d.items()) == []

    def test_del(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class(
            [("KEY", "one"), ("key", "two")],
            foo="bar",
        )

        del d["key"]
        assert d == {"foo": "bar"}
        assert list(d.items()) == [("foo", "bar")]

        with pytest.raises(KeyError, match="key"):
            del d["key"]

    def test_set_default(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class(
            [("KEY", "one"), ("key", "two")],
            foo="bar",
        )
        assert "one" == d.setdefault("key", "three")
        assert "three" == d.setdefault("otherkey", "three")
        assert "otherkey" in d
        assert ("otherkey", "three") in d.items()
        assert "three" == d["OTHERKEY"]

    def test_popitem(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class()
        d.add("KEY", "val1")
        d.add("key", "val2")

        pair = d.popitem()
        assert ("key", "val2") == pair
        assert isinstance(pair[0], str)
        assert [("KEY", "val1")] == list(d.items())

    def test_popitem_empty_multidict(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class()

        with pytest.raises(KeyError):
            d.popitem()

    def test_pop(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class()
        d.add("KEY", "val1")
        d.add("key", "val2")

        assert "val1" == d.pop("KEY")
        assert {"key": "val2"} == d

    def test_pop_lowercase(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class()
        d.add("KEY", "val1")
        d.add("key", "val2")

        assert "val1" == d.pop("key")
        assert {"key": "val2"} == d

    def test_pop_default(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class(OTHER="val")

        assert "default" == d.pop("key", "default")
        assert "other" in d

    def test_pop_raises(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d = case_insensitive_multidict_class(OTHER="val")

        with pytest.raises(KeyError, match="KEY"):
            d.pop("KEY")

        assert "other" in d

    def test_extend_with_istr(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
        case_insensitive_str_class: type[istr],
    ) -> None:
        us = case_insensitive_str_class("aBc")
        d = case_insensitive_multidict_class()

        d.extend([(us, "val")])
        assert [("aBc", "val")] == list(d.items())

    def test_copy_istr(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
        case_insensitive_str_class: type[istr],
    ) -> None:
        d = case_insensitive_multidict_class({case_insensitive_str_class("Foo"): "bar"})
        d2 = d.copy()
        assert d == d2

    def test_eq(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        d1 = case_insensitive_multidict_class(Key="val")
        d2 = case_insensitive_multidict_class(KEY="val")

        assert d1 == d2

    @pytest.mark.skipif(
        sys.implementation.name == "pypy",
        reason="getsizeof() is not implemented on PyPy",
    )
    def test_sizeof(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        md = case_insensitive_multidict_class()
        s1 = sys.getsizeof(md)
        for i in string.ascii_lowercase:
            for j in string.ascii_uppercase:
                md[i + j] = i + j
        # multidict should be resized
        s2 = sys.getsizeof(md)
        assert s2 > s1

    @pytest.mark.skipif(
        sys.implementation.name == "pypy",
        reason="getsizeof() is not implemented on PyPy",
    )
    def test_min_sizeof(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        md = case_insensitive_multidict_class()
        assert sys.getsizeof(md) < 1024

    def test_issue_620_items(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        # https://github.com/aio-libs/multidict/issues/620
        d = case_insensitive_multidict_class({"a": "123, 456", "b": "789"})
        before_mutation_items = d.items()
        d["c"] = "000"
        # This causes an error on pypy.
        list(before_mutation_items)

    def test_issue_620_keys(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        # https://github.com/aio-libs/multidict/issues/620
        d = case_insensitive_multidict_class({"a": "123, 456", "b": "789"})
        before_mutation_keys = d.keys()
        d["c"] = "000"
        # This causes an error on pypy.
        list(before_mutation_keys)

    def test_issue_620_values(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
    ) -> None:
        # https://github.com/aio-libs/multidict/issues/620
        d = case_insensitive_multidict_class({"a": "123, 456", "b": "789"})
        before_mutation_values = d.values()
        d["c"] = "000"
        # This causes an error on pypy.
        list(before_mutation_values)

    def test_keys_type(
        self,
        case_insensitive_multidict_class: type[CIMultiDict[str]],
        case_insensitive_str_class: type[istr],
    ) -> None:
        d = case_insensitive_multidict_class(
            [
                ("KEY", "one"),
            ]
        )
        d["k2"] = "2"
        d.extend(k3="3")

        for k in d:
            assert type(k) is case_insensitive_str_class

        for k in d.keys():
            assert type(k) is case_insensitive_str_class

        for k, v in d.items():
            assert type(k) is case_insensitive_str_class

        k, v = d.popitem()
        assert type(k) is case_insensitive_str_class

    def test_issue_1195(
        self, case_insensitive_multidict_class: type[CIMultiDict[bytes]]
    ) -> None:
        md = case_insensitive_multidict_class(
            {
                "User-Agent": b"Bacon/1.0",
                "Cookie": b"valued-visitor=yes;foo=bar",
                "X-Bar": b"Foo",
                "X-Foo": b"Bar",
                "Referer": b"https://httpie.org/",
            }
        )

        md2 = md.copy()

        md.popone("User-Agent")
        assert md.keys() == md2.keys() - {"User-Agent"}
        md.update([("User-Agent", b"Bacon/1.0")])
        assert md.keys() == md2.keys()

    def test_update_with_crash_in_the_middle(
        self, case_insensitive_multidict_class: type[CIMultiDict[str]]
    ) -> None:
        class Hack(str):
            def lower(self) -> str:
                raise RuntimeError

        d = case_insensitive_multidict_class([("a", "a"), ("b", "b")])
        with pytest.raises(RuntimeError):
            lst = [("c", "c"), ("a", "a2"), (Hack("b"), "b2")]
            d.update(lst)

        assert [("a", "a2"), ("b", "b"), ("c", "c")] == list(d.items())


def test_multidict_shrink_regression() -> None:
    """
    Regression test for _md_shrink pointer increment bug in 6.6.0.

    The bug was introduced in PR #1200 which added _md_shrink to optimize
    memory usage. The bug occurs when new_ep == old_ep (first non-deleted
    entry), causing new_ep to not be incremented. This results in the first
    entry being overwritten and memory corruption.

    See: https://github.com/aio-libs/multidict/issues/1221
    """
    # Test case that reproduces the corruption
    md: MultiDict[str] = MultiDict()

    # Create pattern: [kept, deleted, kept, kept, ...]
    # This triggers new_ep == old_ep on first iteration of _md_shrink
    for i in range(10):
        md[f"k{i}"] = f"v{i}"

    # Delete some entries but keep the first one
    # This creates the exact condition for the bug
    for i in range(1, 10, 2):
        del md[f"k{i}"]

    # Trigger shrink by adding many entries
    # When the internal array needs to resize, it will call _md_shrink
    # because md->used < md->keys->nentries
    for i in range(50):
        md[f"new{i}"] = f"val{i}"

    # The bug would cause k0 to be lost due to memory corruption!
    assert "k0" in md, "First entry k0 was lost due to memory corruption!"
    assert md["k0"] == "v0", "First entry value was corrupted!"

    # Verify all other kept entries survived
    for i in range(0, 10, 2):
        assert f"k{i}" in md, f"Entry k{i} missing!"
        assert md[f"k{i}"] == f"v{i}", f"Entry k{i} has wrong value!"

    # Verify new entries
    for i in range(50):
        assert md[f"new{i}"] == f"val{i}"
