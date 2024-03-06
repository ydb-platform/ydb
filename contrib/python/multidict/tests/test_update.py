from collections import deque
from typing import Type

from multidict import MultiMapping


def test_update_replace(any_multidict_class: Type[MultiMapping[str]]) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = any_multidict_class([("a", 4), ("b", 5), ("a", 6)])
    obj1.update(obj2)
    expected = [("a", 4), ("b", 5), ("a", 6), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_append(any_multidict_class: Type[MultiMapping[str]]) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = any_multidict_class([("a", 4), ("a", 5), ("a", 6)])
    obj1.update(obj2)
    expected = [("a", 4), ("b", 2), ("a", 5), ("c", 10), ("a", 6)]
    assert list(obj1.items()) == expected


def test_update_remove(any_multidict_class: Type[MultiMapping[str]]) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = any_multidict_class([("a", 4)])
    obj1.update(obj2)
    expected = [("a", 4), ("b", 2), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_replace_seq(any_multidict_class: Type[MultiMapping[str]]) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = [("a", 4), ("b", 5), ("a", 6)]
    obj1.update(obj2)
    expected = [("a", 4), ("b", 5), ("a", 6), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_replace_seq2(any_multidict_class: Type[MultiMapping[str]]) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj1.update([("a", 4)], b=5, a=6)
    expected = [("a", 4), ("b", 5), ("a", 6), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_append_seq(any_multidict_class: Type[MultiMapping[str]]) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = [("a", 4), ("a", 5), ("a", 6)]
    obj1.update(obj2)
    expected = [("a", 4), ("b", 2), ("a", 5), ("c", 10), ("a", 6)]
    assert list(obj1.items()) == expected


def test_update_remove_seq(any_multidict_class: Type[MultiMapping[str]]) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = [("a", 4)]
    obj1.update(obj2)
    expected = [("a", 4), ("b", 2), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_md(
    case_sensitive_multidict_class: Type[MultiMapping[str]],
) -> None:
    d = case_sensitive_multidict_class()
    d.add("key", "val1")
    d.add("key", "val2")
    d.add("key2", "val3")

    d.update(key="val")

    assert [("key", "val"), ("key2", "val3")] == list(d.items())


def test_update_istr_ci_md(
    case_insensitive_multidict_class: Type[MultiMapping[str]],
    case_insensitive_str_class: str,
) -> None:
    d = case_insensitive_multidict_class()
    d.add(case_insensitive_str_class("KEY"), "val1")
    d.add("key", "val2")
    d.add("key2", "val3")

    d.update({case_insensitive_str_class("key"): "val"})

    assert [("key", "val"), ("key2", "val3")] == list(d.items())


def test_update_ci_md(
    case_insensitive_multidict_class: Type[MultiMapping[str]],
) -> None:
    d = case_insensitive_multidict_class()
    d.add("KEY", "val1")
    d.add("key", "val2")
    d.add("key2", "val3")

    d.update(Key="val")

    assert [("Key", "val"), ("key2", "val3")] == list(d.items())


def test_update_list_arg_and_kwds(
    any_multidict_class: Type[MultiMapping[str]],
) -> None:
    obj = any_multidict_class()
    arg = [("a", 1)]
    obj.update(arg, b=2)
    assert list(obj.items()) == [("a", 1), ("b", 2)]
    assert arg == [("a", 1)]


def test_update_tuple_arg_and_kwds(
    any_multidict_class: Type[MultiMapping[str]],
) -> None:
    obj = any_multidict_class()
    arg = (("a", 1),)
    obj.update(arg, b=2)
    assert list(obj.items()) == [("a", 1), ("b", 2)]
    assert arg == (("a", 1),)


def test_update_deque_arg_and_kwds(
    any_multidict_class: Type[MultiMapping[str]],
) -> None:
    obj = any_multidict_class()
    arg = deque([("a", 1)])
    obj.update(arg, b=2)
    assert list(obj.items()) == [("a", 1), ("b", 2)]
    assert arg == deque([("a", 1)])
