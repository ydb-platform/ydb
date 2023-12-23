import string
import sys

import pytest


class TestMutableMultiDict:
    @pytest.fixture
    def cls(self, _multidict):
        return _multidict.MultiDict

    @pytest.fixture
    def proxy_cls(self, _multidict):
        return _multidict.MultiDictProxy

    @pytest.fixture
    def istr(self, _multidict):
        return _multidict.istr

    def test_copy(self, cls):
        d1 = cls(key="value", a="b")

        d2 = d1.copy()
        assert d1 == d2
        assert d1 is not d2

    def test__repr__(self, cls):
        d = cls()
        assert str(d) == "<%s()>" % cls.__name__

        d = cls([("key", "one"), ("key", "two")])

        expected = "<%s('key': 'one', 'key': 'two')>" % cls.__name__
        assert str(d) == expected

    def test_getall(self, cls):
        d = cls([("key", "value1")], key="value2")
        assert len(d) == 2

        assert d.getall("key") == ["value1", "value2"]

        with pytest.raises(KeyError, match="some_key"):
            d.getall("some_key")

        default = object()
        assert d.getall("some_key", default) is default

    def test_add(self, cls):
        d = cls()

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

    def test_extend(self, cls):
        d = cls()
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

        other = cls(bar="baz")
        assert other == {"bar": "baz"}

        d.extend(other)
        assert ("bar", "baz") in d.items()

        d.extend({"foo": "moo"})
        assert ("foo", "moo") in d.items()

        d.extend()
        assert 6 == len(d)

        with pytest.raises(TypeError):
            d.extend("foo", "bar")

    def test_extend_from_proxy(self, cls, proxy_cls):
        d = cls([("a", "a"), ("b", "b")])
        proxy = proxy_cls(d)

        d2 = cls()
        d2.extend(proxy)

        assert [("a", "a"), ("b", "b")] == list(d2.items())

    def test_clear(self, cls):
        d = cls([("key", "one")], key="two", foo="bar")

        d.clear()
        assert d == {}
        assert list(d.items()) == []

    def test_del(self, cls):
        d = cls([("key", "one"), ("key", "two")], foo="bar")
        assert list(d.keys()) == ["key", "key", "foo"]

        del d["key"]
        assert d == {"foo": "bar"}
        assert list(d.items()) == [("foo", "bar")]

        with pytest.raises(KeyError, match="key"):
            del d["key"]

    def test_set_default(self, cls):
        d = cls([("key", "one"), ("key", "two")], foo="bar")
        assert "one" == d.setdefault("key", "three")
        assert "three" == d.setdefault("otherkey", "three")
        assert "otherkey" in d
        assert "three" == d["otherkey"]

    def test_popitem(self, cls):
        d = cls()
        d.add("key", "val1")
        d.add("key", "val2")

        assert ("key", "val1") == d.popitem()
        assert [("key", "val2")] == list(d.items())

    def test_popitem_empty_multidict(self, cls):
        d = cls()

        with pytest.raises(KeyError):
            d.popitem()

    def test_pop(self, cls):
        d = cls()
        d.add("key", "val1")
        d.add("key", "val2")

        assert "val1" == d.pop("key")
        assert {"key": "val2"} == d

    def test_pop2(self, cls):
        d = cls()
        d.add("key", "val1")
        d.add("key2", "val2")
        d.add("key", "val3")

        assert "val1" == d.pop("key")
        assert [("key2", "val2"), ("key", "val3")] == list(d.items())

    def test_pop_default(self, cls):
        d = cls(other="val")

        assert "default" == d.pop("key", "default")
        assert "other" in d

    def test_pop_raises(self, cls):
        d = cls(other="val")

        with pytest.raises(KeyError, match="key"):
            d.pop("key")

        assert "other" in d

    def test_replacement_order(self, cls):
        d = cls()
        d.add("key1", "val1")
        d.add("key2", "val2")
        d.add("key1", "val3")
        d.add("key2", "val4")

        d["key1"] = "val"

        expected = [("key1", "val"), ("key2", "val2"), ("key2", "val4")]

        assert expected == list(d.items())

    def test_nonstr_key(self, cls):
        d = cls()
        with pytest.raises(TypeError):
            d[1] = "val"

    def test_istr_key(self, cls, istr):
        d = cls()
        d[istr("1")] = "val"
        assert type(list(d.keys())[0]) is istr

    def test_str_derived_key(self, cls):
        class A(str):
            pass

        d = cls()
        d[A("1")] = "val"
        assert type(list(d.keys())[0]) is A

    def test_istr_key_add(self, cls, istr):
        d = cls()
        d.add(istr("1"), "val")
        assert type(list(d.keys())[0]) is istr

    def test_str_derived_key_add(self, cls):
        class A(str):
            pass

        d = cls()
        d.add(A("1"), "val")
        assert type(list(d.keys())[0]) is A

    def test_popall(self, cls):
        d = cls()
        d.add("key1", "val1")
        d.add("key2", "val2")
        d.add("key1", "val3")
        ret = d.popall("key1")
        assert ["val1", "val3"] == ret
        assert {"key2": "val2"} == d

    def test_popall_default(self, cls):
        d = cls()
        assert "val" == d.popall("key", "val")

    def test_popall_key_error(self, cls):
        d = cls()
        with pytest.raises(KeyError, match="key"):
            d.popall("key")

    def test_large_multidict_resizing(self, cls):
        SIZE = 1024
        d = cls()
        for i in range(SIZE):
            d["key" + str(i)] = i

        for i in range(SIZE - 1):
            del d["key" + str(i)]

        assert {"key" + str(SIZE - 1): SIZE - 1} == d


class TestCIMutableMultiDict:
    @pytest.fixture
    def cls(self, _multidict):
        return _multidict.CIMultiDict

    @pytest.fixture
    def proxy_cls(self, _multidict):
        return _multidict.CIMultiDictProxy

    @pytest.fixture
    def istr(self, _multidict):
        return _multidict.istr

    def test_getall(self, cls):
        d = cls([("KEY", "value1")], KEY="value2")

        assert d != {"KEY": "value1"}
        assert len(d) == 2

        assert d.getall("key") == ["value1", "value2"]

        with pytest.raises(KeyError, match="some_key"):
            d.getall("some_key")

    def test_ctor(self, cls):
        d = cls(k1="v1")
        assert "v1" == d["K1"]
        assert ("k1", "v1") in d.items()

    def test_setitem(self, cls):
        d = cls()
        d["k1"] = "v1"
        assert "v1" == d["K1"]
        assert ("k1", "v1") in d.items()

    def test_delitem(self, cls):
        d = cls()
        d["k1"] = "v1"
        assert "K1" in d
        del d["k1"]
        assert "K1" not in d

    def test_copy(self, cls):
        d1 = cls(key="KEY", a="b")

        d2 = d1.copy()
        assert d1 == d2
        assert d1.items() == d2.items()
        assert d1 is not d2

    def test__repr__(self, cls):
        d = cls()
        assert str(d) == "<%s()>" % cls.__name__

        d = cls([("KEY", "one"), ("KEY", "two")])

        expected = "<%s('KEY': 'one', 'KEY': 'two')>" % cls.__name__
        assert str(d) == expected

    def test_add(self, cls):
        d = cls()

        assert d == {}
        d["KEY"] = "one"
        assert ("KEY", "one") in d.items()
        assert d == cls({"Key": "one"})
        assert d.getall("key") == ["one"]

        d["KEY"] = "two"
        assert ("KEY", "two") in d.items()
        assert d == cls({"Key": "two"})
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

    def test_extend(self, cls):
        d = cls()
        assert d == {}

        d.extend([("KEY", "one"), ("key", "two")], key=3, foo="bar")
        assert 4 == len(d)
        itms = d.items()
        # we can't guarantee order of kwargs
        assert ("KEY", "one") in itms
        assert ("key", "two") in itms
        assert ("key", 3) in itms
        assert ("foo", "bar") in itms

        other = cls(Bar="baz")
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
            d.extend("foo", "bar")

    def test_extend_from_proxy(self, cls, proxy_cls):
        d = cls([("a", "a"), ("b", "b")])
        proxy = proxy_cls(d)

        d2 = cls()
        d2.extend(proxy)

        assert [("a", "a"), ("b", "b")] == list(d2.items())

    def test_clear(self, cls):
        d = cls([("KEY", "one")], key="two", foo="bar")

        d.clear()
        assert d == {}
        assert list(d.items()) == []

    def test_del(self, cls):
        d = cls([("KEY", "one"), ("key", "two")], foo="bar")

        del d["key"]
        assert d == {"foo": "bar"}
        assert list(d.items()) == [("foo", "bar")]

        with pytest.raises(KeyError, match="key"):
            del d["key"]

    def test_set_default(self, cls):
        d = cls([("KEY", "one"), ("key", "two")], foo="bar")
        assert "one" == d.setdefault("key", "three")
        assert "three" == d.setdefault("otherkey", "three")
        assert "otherkey" in d
        assert ("otherkey", "three") in d.items()
        assert "three" == d["OTHERKEY"]

    def test_popitem(self, cls):
        d = cls()
        d.add("KEY", "val1")
        d.add("key", "val2")

        pair = d.popitem()
        assert ("KEY", "val1") == pair
        assert isinstance(pair[0], str)
        assert [("key", "val2")] == list(d.items())

    def test_popitem_empty_multidict(self, cls):
        d = cls()

        with pytest.raises(KeyError):
            d.popitem()

    def test_pop(self, cls):
        d = cls()
        d.add("KEY", "val1")
        d.add("key", "val2")

        assert "val1" == d.pop("KEY")
        assert {"key": "val2"} == d

    def test_pop_lowercase(self, cls):
        d = cls()
        d.add("KEY", "val1")
        d.add("key", "val2")

        assert "val1" == d.pop("key")
        assert {"key": "val2"} == d

    def test_pop_default(self, cls):
        d = cls(OTHER="val")

        assert "default" == d.pop("key", "default")
        assert "other" in d

    def test_pop_raises(self, cls):
        d = cls(OTHER="val")

        with pytest.raises(KeyError, match="KEY"):
            d.pop("KEY")

        assert "other" in d

    def test_extend_with_istr(self, cls, istr):
        us = istr("aBc")
        d = cls()

        d.extend([(us, "val")])
        assert [("aBc", "val")] == list(d.items())

    def test_copy_istr(self, cls, istr):
        d = cls({istr("Foo"): "bar"})
        d2 = d.copy()
        assert d == d2

    def test_eq(self, cls):
        d1 = cls(Key="val")
        d2 = cls(KEY="val")

        assert d1 == d2

    @pytest.mark.skipif(
        sys.implementation.name == "pypy",
        reason="getsizeof() is not implemented on PyPy",
    )
    def test_sizeof(self, cls):
        md = cls()
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
    def test_min_sizeof(self, cls):
        md = cls()
        assert sys.getsizeof(md) < 1024

    def test_issue_620_items(self, cls):
        # https://github.com/aio-libs/multidict/issues/620
        d = cls({"a": "123, 456", "b": "789"})
        before_mutation_items = d.items()
        d["c"] = "000"
        # This causes an error on pypy.
        list(before_mutation_items)

    def test_issue_620_keys(self, cls):
        # https://github.com/aio-libs/multidict/issues/620
        d = cls({"a": "123, 456", "b": "789"})
        before_mutation_keys = d.keys()
        d["c"] = "000"
        # This causes an error on pypy.
        list(before_mutation_keys)

    def test_issue_620_values(self, cls):
        # https://github.com/aio-libs/multidict/issues/620
        d = cls({"a": "123, 456", "b": "789"})
        before_mutation_values = d.values()
        d["c"] = "000"
        # This causes an error on pypy.
        list(before_mutation_values)
