from collections import deque

import pytest

from multidict._compat import USE_EXTENSIONS
from multidict._multidict_py import CIMultiDict as PyCIMultiDict
from multidict._multidict_py import MultiDict as PyMultiDict  # noqa: E402

if USE_EXTENSIONS:
    from multidict._multidict import CIMultiDict, MultiDict  # type: ignore


@pytest.fixture(
    params=([MultiDict, CIMultiDict] if USE_EXTENSIONS else [])
    + [PyMultiDict, PyCIMultiDict],
    ids=(["MultiDict", "CIMultiDict"] if USE_EXTENSIONS else [])
    + ["PyMultiDict", "PyCIMultiDict"],
)
def cls(request):
    return request.param


@pytest.fixture
def md_cls(_multidict):
    return _multidict.MultiDict


@pytest.fixture
def ci_md_cls(_multidict):
    return _multidict.CIMultiDict


@pytest.fixture
def istr(_multidict):
    return _multidict.istr


def test_update_replace(cls):
    obj1 = cls([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = cls([("a", 4), ("b", 5), ("a", 6)])
    obj1.update(obj2)
    expected = [("a", 4), ("b", 5), ("a", 6), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_append(cls):
    obj1 = cls([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = cls([("a", 4), ("a", 5), ("a", 6)])
    obj1.update(obj2)
    expected = [("a", 4), ("b", 2), ("a", 5), ("c", 10), ("a", 6)]
    assert list(obj1.items()) == expected


def test_update_remove(cls):
    obj1 = cls([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = cls([("a", 4)])
    obj1.update(obj2)
    expected = [("a", 4), ("b", 2), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_replace_seq(cls):
    obj1 = cls([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = [("a", 4), ("b", 5), ("a", 6)]
    obj1.update(obj2)
    expected = [("a", 4), ("b", 5), ("a", 6), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_replace_seq2(cls):
    obj1 = cls([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj1.update([("a", 4)], b=5, a=6)
    expected = [("a", 4), ("b", 5), ("a", 6), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_append_seq(cls):
    obj1 = cls([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = [("a", 4), ("a", 5), ("a", 6)]
    obj1.update(obj2)
    expected = [("a", 4), ("b", 2), ("a", 5), ("c", 10), ("a", 6)]
    assert list(obj1.items()) == expected


def test_update_remove_seq(cls):
    obj1 = cls([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = [("a", 4)]
    obj1.update(obj2)
    expected = [("a", 4), ("b", 2), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_md(md_cls):
    d = md_cls()
    d.add("key", "val1")
    d.add("key", "val2")
    d.add("key2", "val3")

    d.update(key="val")

    assert [("key", "val"), ("key2", "val3")] == list(d.items())


def test_update_istr_ci_md(ci_md_cls, istr):
    d = ci_md_cls()
    d.add(istr("KEY"), "val1")
    d.add("key", "val2")
    d.add("key2", "val3")

    d.update({istr("key"): "val"})

    assert [("key", "val"), ("key2", "val3")] == list(d.items())


def test_update_ci_md(ci_md_cls):
    d = ci_md_cls()
    d.add("KEY", "val1")
    d.add("key", "val2")
    d.add("key2", "val3")

    d.update(Key="val")

    assert [("Key", "val"), ("key2", "val3")] == list(d.items())


def test_update_list_arg_and_kwds(cls):
    obj = cls()
    arg = [("a", 1)]
    obj.update(arg, b=2)
    assert list(obj.items()) == [("a", 1), ("b", 2)]
    assert arg == [("a", 1)]


def test_update_tuple_arg_and_kwds(cls):
    obj = cls()
    arg = (("a", 1),)
    obj.update(arg, b=2)
    assert list(obj.items()) == [("a", 1), ("b", 2)]
    assert arg == (("a", 1),)


def test_update_deque_arg_and_kwds(cls):
    obj = cls()
    arg = deque([("a", 1)])
    obj.update(arg, b=2)
    assert list(obj.items()) == [("a", 1), ("b", 2)]
    assert arg == deque([("a", 1)])
