from deepmerge.exception import *
from collections import OrderedDict, defaultdict
import pytest


from deepmerge import (
    always_merger,
    conservative_merger,
    merge_or_raise,
)


def test_fill_missing_value():
    base = {"foo": 0, "baz": 2}
    nxt = {"bar": 1}
    always_merger.merge(base, nxt)
    assert base == {"foo": 0, "bar": 1, "baz": 2}


def test_handles_set_values_via_union():
    base = {"a": set("123"), "b": 3}
    nxt = {"a": set("2345"), "c": 1}
    always_merger.merge(base, nxt)
    assert base == {"a": set("12345"), "b": 3, "c": 1}


def test_merge_or_raise_raises_exception():
    base = {"foo": 0, "baz": 2}
    nxt = {"bar": 1, "foo": "a string!"}
    with pytest.raises(InvalidMerge) as exc_info:
        merge_or_raise.merge(base, nxt)
    exc = exc_info.value
    assert exc.strategy_list_name == "type conflict"
    assert exc.config == merge_or_raise
    assert exc.path == ["foo"]
    assert exc.base == 0
    assert exc.nxt == "a string!"


@pytest.mark.parametrize("base, nxt, expected", [("dooby", "fooby", "dooby"), (-10, "goo", -10)])
def test_use_existing(base, nxt, expected):
    assert conservative_merger.merge(base, nxt) == expected


def test_example():
    base = {"foo": "value", "baz": ["a"]}
    next = {"bar": "value2", "baz": ["b"]}

    always_merger.merge(base, next)

    assert base == {"foo": "value", "bar": "value2", "baz": ["a", "b"]}


def test_subtypes():
    base = OrderedDict({"foo": "value", "baz": ["a"]})
    next = defaultdict(str, {"bar": "value2", "baz": ["b"]})

    result = always_merger.merge(base, next)

    assert dict(result) == {"foo": "value", "bar": "value2", "baz": ["a", "b"]}


@pytest.mark.parametrize(
    "initializer,expectation",
    [
        (None, {"foo": "value", "bar": "value3", "baz": ["a", "b", "c"]}),
        ({}, {"foo": "value", "bar": "value3", "baz": ["a", "b", "c"]}),
        (
            {"init": "value"},
            {"foo": "value", "bar": "value3", "baz": ["a", "b", "c"], "init": "value"},
        ),
    ],
    ids=["no initilaizer", "empty initializer", "set initializer"],
)
def test_reduce(initializer, expectation):
    # Given
    from functools import reduce

    base = {"foo": "value", "baz": ["a"]}
    next = {"bar": "value2", "baz": ["b"]}
    more = {"bar": "value3", "baz": ["c"]}

    list_to_merge = [base, next, more]

    # When
    if initializer is None:
        result = reduce(always_merger.merge, list_to_merge)
    else:
        result = reduce(always_merger.merge, list_to_merge, initializer)

    # Then
    assert result == expectation

    if initializer is None:
        assert result is base
    else:
        assert result is not base
