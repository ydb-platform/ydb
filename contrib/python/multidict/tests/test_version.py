from typing import Callable, Type

import pytest

from multidict import MultiMapping


def test_getversion_bad_param(multidict_getversion_callable):
    with pytest.raises(TypeError):
        multidict_getversion_callable(1)


def test_ctor(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m1 = any_multidict_class()
    v1 = multidict_getversion_callable(m1)
    m2 = any_multidict_class()
    v2 = multidict_getversion_callable(m2)
    assert v1 != v2


def test_add(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    v = multidict_getversion_callable(m)
    m.add("key", "val")
    assert multidict_getversion_callable(m) > v


def test_delitem(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    del m["key"]
    assert multidict_getversion_callable(m) > v


def test_delitem_not_found(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    with pytest.raises(KeyError):
        del m["notfound"]
    assert multidict_getversion_callable(m) == v


def test_setitem(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    m["key"] = "val2"
    assert multidict_getversion_callable(m) > v


def test_setitem_not_found(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    m["notfound"] = "val2"
    assert multidict_getversion_callable(m) > v


def test_clear(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    m.clear()
    assert multidict_getversion_callable(m) > v


def test_setdefault(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    m.setdefault("key2", "val2")
    assert multidict_getversion_callable(m) > v


def test_popone(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    m.popone("key")
    assert multidict_getversion_callable(m) > v


def test_popone_default(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    m.popone("key2", "default")
    assert multidict_getversion_callable(m) == v


def test_popone_key_error(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    with pytest.raises(KeyError):
        m.popone("key2")
    assert multidict_getversion_callable(m) == v


def test_pop(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    m.pop("key")
    assert multidict_getversion_callable(m) > v


def test_pop_default(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    m.pop("key2", "default")
    assert multidict_getversion_callable(m) == v


def test_pop_key_error(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    with pytest.raises(KeyError):
        m.pop("key2")
    assert multidict_getversion_callable(m) == v


def test_popall(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    m.popall("key")
    assert multidict_getversion_callable(m) > v


def test_popall_default(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    m.popall("key2", "default")
    assert multidict_getversion_callable(m) == v


def test_popall_key_error(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    with pytest.raises(KeyError):
        m.popall("key2")
    assert multidict_getversion_callable(m) == v


def test_popitem(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    m.add("key", "val")
    v = multidict_getversion_callable(m)
    m.popitem()
    assert multidict_getversion_callable(m) > v


def test_popitem_key_error(
    any_multidict_class: Type[MultiMapping[str]],
    multidict_getversion_callable: Callable,
) -> None:
    m = any_multidict_class()
    v = multidict_getversion_callable(m)
    with pytest.raises(KeyError):
        m.popitem()
    assert multidict_getversion_callable(m) == v
