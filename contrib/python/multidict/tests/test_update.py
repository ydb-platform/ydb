from collections import deque

from multidict import CIMultiDict, MultiDict
from multidict._multidict_py import MultiDict as PyMultiDict

_MD_Classes = type[MultiDict[int]] | type[CIMultiDict[int]]


def test_update_replace(any_multidict_class: _MD_Classes) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = any_multidict_class([("a", 4), ("b", 5), ("a", 6)])
    obj1.update(obj2)
    expected = [("a", 4), ("b", 5), ("a", 6), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_append(any_multidict_class: _MD_Classes) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = any_multidict_class([("a", 4), ("a", 5), ("a", 6)])
    obj1.update(obj2)
    expected = [("a", 4), ("b", 2), ("a", 5), ("c", 10), ("a", 6)]
    assert list(obj1.items()) == expected


def test_update_remove(any_multidict_class: _MD_Classes) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = any_multidict_class([("a", 4)])
    obj1.update(obj2)
    expected = [("a", 4), ("b", 2), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_replace_seq(any_multidict_class: _MD_Classes) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = [("a", 4), ("b", 5), ("a", 6)]
    obj1.update(obj2)
    expected = [("a", 4), ("b", 5), ("a", 6), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_replace_seq2(any_multidict_class: _MD_Classes) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj1.update([("a", 4)], b=5, a=6)
    expected = [("a", 4), ("b", 5), ("a", 6), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_append_seq(any_multidict_class: _MD_Classes) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = [("a", 4), ("a", 5), ("a", 6)]
    obj1.update(obj2)
    expected = [("a", 4), ("b", 2), ("a", 5), ("c", 10), ("a", 6)]
    assert list(obj1.items()) == expected


def test_update_remove_seq(any_multidict_class: _MD_Classes) -> None:
    obj1 = any_multidict_class([("a", 1), ("b", 2), ("a", 3), ("c", 10)])
    obj2 = [("a", 4)]
    obj1.update(obj2)
    expected = [("a", 4), ("b", 2), ("c", 10)]
    assert list(obj1.items()) == expected


def test_update_md(case_sensitive_multidict_class: type[CIMultiDict[str]]) -> None:
    d = case_sensitive_multidict_class()
    d.add("key", "val1")
    d.add("key", "val2")
    d.add("key2", "val3")

    d.update(key="val")

    assert [("key", "val"), ("key2", "val3")] == list(d.items())


def test_update_istr_ci_md(
    case_insensitive_multidict_class: type[CIMultiDict[str]],
    case_insensitive_str_class: type[str],
) -> None:
    d = case_insensitive_multidict_class()
    d.add(case_insensitive_str_class("KEY"), "val1")
    d.add("key", "val2")
    d.add("key2", "val3")

    d.update({case_insensitive_str_class("key"): "val"})

    assert [("key", "val"), ("key2", "val3")] == list(d.items())


def test_update_ci_md(case_insensitive_multidict_class: type[CIMultiDict[str]]) -> None:
    d = case_insensitive_multidict_class()
    d.add("KEY", "val1")
    d.add("key", "val2")
    d.add("key2", "val3")

    d.update(Key="val")

    assert [("Key", "val"), ("key2", "val3")] == list(d.items())


def test_update_list_arg_and_kwds(any_multidict_class: _MD_Classes) -> None:
    obj = any_multidict_class()
    arg = [("a", 1)]
    obj.update(arg, b=2)
    assert list(obj.items()) == [("a", 1), ("b", 2)]
    assert arg == [("a", 1)]


def test_update_tuple_arg_and_kwds(any_multidict_class: _MD_Classes) -> None:
    obj = any_multidict_class()
    arg = (("a", 1),)
    obj.update(arg, b=2)
    assert list(obj.items()) == [("a", 1), ("b", 2)]
    assert arg == (("a", 1),)


def test_update_deque_arg_and_kwds(any_multidict_class: _MD_Classes) -> None:
    obj = any_multidict_class()
    arg = deque([("a", 1)])
    obj.update(arg, b=2)
    assert list(obj.items()) == [("a", 1), ("b", 2)]
    assert arg == deque([("a", 1)])


def test_update_with_second_md(any_multidict_class: _MD_Classes) -> None:
    obj1 = any_multidict_class()
    obj2 = any_multidict_class([("a", 2)])
    obj1.update(obj2)
    assert obj1 == obj2


def test_compact_after_deletion(any_multidict_class: _MD_Classes) -> None:
    # multidict is resized when it is filled up to 2/3 of the index table size
    NUM = 16 * 2 // 3
    obj = any_multidict_class((str(i), i) for i in range(NUM - 1))
    # keys.usable == 0
    # delete items, it adds empty entries but not reduce keys.usable
    for i in range(5):
        del obj[str(i)]
    # adding an entry requres keys resizing to remove empty entries
    dct = {str(i): i for i in range(100, 105)}
    obj.extend(dct)
    assert obj == {str(i): i for i in range(5, NUM - 1)} | dct


def test_update_with_empty_slots(any_multidict_class: _MD_Classes) -> None:
    # multidict is resized when it is filled up to 2/3 of the index table size
    obj = any_multidict_class([("0", 0), ("1", 1), ("1", 2)])
    del obj["0"]
    obj.update({"1": 100})
    assert obj == {"1": 100}


def test_pure_python_parse_args_size_hint_with_seq_and_kwargs() -> None:
    """Regression test for pure-Python ``_parse_args`` size hint.

    When a positional iterable and keyword arguments are both supplied,
    ``_parse_args`` previously yielded ``len(arg) + len(kwargs)`` after
    merging ``kwargs.items()`` into ``arg``. That double-counted the
    kwargs and over-allocated the internal hash table. The hint must
    equal the actual number of yielded entries.
    """
    md: PyMultiDict[int] = PyMultiDict()
    arg = [("a", 1), ("b", 2)]
    kwargs = {"c": 3, "d": 4}

    it = md._parse_args(arg, kwargs)
    size_hint = next(it)
    entries = list(it)

    assert size_hint == len(entries) == len(arg) + len(kwargs)


def test_pure_python_parse_args_size_hint_with_mapping_and_kwargs() -> None:
    """Same regression but exercising the mapping (``keys()``) branch."""
    md: PyMultiDict[int] = PyMultiDict()
    arg = {"a": 1, "b": 2}
    kwargs = {"c": 3, "d": 4}

    it = md._parse_args(arg, kwargs)
    size_hint = next(it)
    entries = list(it)

    assert size_hint == len(entries) == len(arg) + len(kwargs)


def test_pure_python_parse_args_size_hint_with_md_and_kwargs() -> None:
    """``MultiDict`` positional argument already yields the correct hint."""
    md: PyMultiDict[int] = PyMultiDict()
    arg = PyMultiDict([("a", 1), ("a", 2), ("b", 3)])
    kwargs = {"c": 4}

    it = md._parse_args(arg, kwargs)
    size_hint = next(it)
    entries = list(it)

    assert size_hint == len(entries) == len(arg) + len(kwargs)
