from collections.abc import Callable
from typing import TypeVar, Union

import pytest

from multidict import CIMultiDict, CIMultiDictProxy, MultiDict, MultiDictProxy

_T = TypeVar("_T")
_MD_Types = Union[
    MultiDict[_T], CIMultiDict[_T], MultiDictProxy[_T], CIMultiDictProxy[_T]
]
GetVersion = Callable[[_MD_Types[_T]], int]


def test_getversion_bad_param(multidict_getversion_callable: GetVersion[str]) -> None:
    with pytest.raises(TypeError):
        multidict_getversion_callable(1)  # type: ignore[arg-type]


def test_ctor(
    any_multidict_class: type[MultiDict[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m1 = any_multidict_class()
    v1 = multidict_getversion_callable(m1)
    m2 = any_multidict_class()
    v2 = multidict_getversion_callable(m2)
    assert v1 != v2


def test_add(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    m.add("key", "val")
    v2 = multidict_getversion_callable(m)
    assert v2 > v
    assert v2 == multidict_getversion_callable(p)


def test_delitem(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    del m["key"]
    v2 = multidict_getversion_callable(m)
    assert v2 > v
    assert v2 == multidict_getversion_callable(p)


def test_delitem_not_found(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    with pytest.raises(KeyError):
        del m["notfound"]
    assert multidict_getversion_callable(m) == v
    assert v == multidict_getversion_callable(p)


def test_setitem(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    m["key"] = "val2"
    v2 = multidict_getversion_callable(m)
    assert v2 > v
    assert v2 == multidict_getversion_callable(p)


def test_setitem_not_found(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    m["notfound"] = "val2"
    v2 = multidict_getversion_callable(m)
    assert v2 > v
    assert v2 == multidict_getversion_callable(p)


def test_clear(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    m.clear()
    v2 = multidict_getversion_callable(m)
    assert v2 > v
    assert v2 == multidict_getversion_callable(p)


def test_setdefault(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    m.setdefault("key2", "val2")
    v2 = multidict_getversion_callable(m)
    assert v2 > v
    assert v2 == multidict_getversion_callable(p)


def test_popone(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    m.popone("key")
    v2 = multidict_getversion_callable(m)
    assert v2 > v
    assert v2 == multidict_getversion_callable(p)


def test_popone_default(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    m.popone("key2", "default")
    v2 = multidict_getversion_callable(m)
    assert v2 == v
    assert v2 == multidict_getversion_callable(p)


def test_popone_key_error(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    with pytest.raises(KeyError):
        m.popone("key2")
    v2 = multidict_getversion_callable(m)
    assert v2 == v
    assert v2 == multidict_getversion_callable(p)


def test_pop(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    m.pop("key")
    v2 = multidict_getversion_callable(m)
    assert v2 > v
    assert v2 == multidict_getversion_callable(p)


def test_pop_default(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    m.pop("key2", "default")
    v2 = multidict_getversion_callable(m)
    assert v2 == v
    assert v2 == multidict_getversion_callable(p)


def test_pop_key_error(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    with pytest.raises(KeyError):
        m.pop("key2")
    v2 = multidict_getversion_callable(m)
    assert v2 == v
    assert v2 == multidict_getversion_callable(p)


def test_popall(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    m.popall("key")
    v2 = multidict_getversion_callable(m)
    assert v2 > v
    assert v2 == multidict_getversion_callable(p)


def test_popall_default(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    m.popall("key2", "default")
    v2 = multidict_getversion_callable(m)
    assert v2 == v
    assert v2 == multidict_getversion_callable(p)


def test_popall_key_error(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    with pytest.raises(KeyError):
        m.popall("key2")
    v2 = multidict_getversion_callable(m)
    assert v2 == v
    assert v2 == multidict_getversion_callable(p)


def test_popitem(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    m.popitem()
    v2 = multidict_getversion_callable(m)
    assert v2 > v
    assert v2 == multidict_getversion_callable(p)


def test_popitem_key_error(
    any_multidict_class: type[MultiDict[str]],
    any_multidict_proxy_class: type[MultiDictProxy[str]],
    multidict_getversion_callable: GetVersion[str],
) -> None:
    m = any_multidict_class()
    p = any_multidict_proxy_class(m)
    v = multidict_getversion_callable(m)
    assert v == multidict_getversion_callable(p)
    with pytest.raises(KeyError):
        m.popitem()
    v2 = multidict_getversion_callable(m)
    assert v2 == v
    assert v2 == multidict_getversion_callable(p)
