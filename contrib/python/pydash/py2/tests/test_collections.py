# -*- coding: utf-8 -*-

import math
from operator import itemgetter

import pytest

import pydash as _
from pydash._compat import PY2

from . import fixtures
from .fixtures import parametrize


@parametrize(
    "case,expected",
    [
        ((["a", "b", "c", "d", "e"], [0], [2], [4]), ["a", "c", "e"]),
        ((["moe", "larry", "curly"], 0, 2), ["moe", "curly"]),
        (({"a": 1, "b": 2, "c": 3}, "a", "b"), [1, 2]),
    ],
)
def test_at(case, expected):
    assert _.at(*case) == expected


@parametrize(
    "case,expected",
    [
        (([4.3, 6.1, 6.4], lambda num: int(math.floor(num))), {4: 1, 6: 2}),
        (([{"one": 1}, {"one": 1}, {"two": 2}, {"one": 1}], {"one": 1}), {True: 3, False: 1}),
        (([{"one": 1}, {"one": 1}, {"two": 2}, {"one": 1}], "one"), {1: 3, None: 1}),
        (({1: 0, 2: 0, 4: 3},), {0: 2, 3: 1}),
    ],
)
def test_count_by(case, expected):
    assert _.count_by(*case) == expected


@parametrize(
    "case,expected",
    [
        (([True, 1, None, "yes"], bool), False),
        (([True, 1, None, "yes"],), False),
        (([{"name": "moe", "age": 40}, {"name": "larry", "age": 50}], "age"), True),
        (([{"name": "moe", "age": 40}, {"name": "larry", "age": 50}], {"age": 50}), False),
    ],
)
def test_every(case, expected):
    assert _.every(*case) == expected


@parametrize(
    "case,expected",
    [
        (([0, True, False, None, 1, 2, 3],), [True, 1, 2, 3]),
        (([1, 2, 3, 4, 5, 6], lambda num: num % 2 == 0), [2, 4, 6]),
        (
            (
                [
                    {"name": "barney", "age": 36, "blocked": False},
                    {"name": "fred", "age": 40, "blocked": True},
                ],
                "blocked",
            ),
            [{"name": "fred", "age": 40, "blocked": True}],
        ),
        (
            (
                [
                    {"name": "barney", "age": 36, "blocked": False},
                    {"name": "fred", "age": 40, "blocked": True},
                ],
                {"age": 36},
            ),
            [{"name": "barney", "age": 36, "blocked": False}],
        ),
        (
            ([{"name": "moe", "age": 40}, {"name": "larry", "age": 50}], {"age": 40}),
            [{"name": "moe", "age": 40}],
        ),
    ],
)
def test_filter_(case, expected):
    assert _.filter_(*case) == expected


@parametrize(
    "case,expected",
    [
        (
            (
                [
                    {"name": "barney", "age": 36, "blocked": False},
                    {"name": "fred", "age": 40, "blocked": True},
                    {"name": "pebbles", "age": 1, "blocked": False},
                ],
                lambda c: c["age"] < 40,
            ),
            {"name": "barney", "age": 36, "blocked": False},
        ),
        (
            (
                [
                    {"name": "barney", "age": 36, "blocked": False},
                    {"name": "fred", "age": 40, "blocked": True},
                    {"name": "pebbles", "age": 1, "blocked": False},
                ],
                {"age": 1},
            ),
            {"name": "pebbles", "age": 1, "blocked": False},
        ),
        (
            (
                [
                    {"name": "barney", "age": 36, "blocked": False},
                    {"name": "fred", "age": 40, "blocked": True},
                    {"name": "pebbles", "age": 1, "blocked": False},
                ],
                "blocked",
            ),
            {"name": "fred", "age": 40, "blocked": True},
        ),
        (
            (
                [
                    {"name": "barney", "age": 36, "blocked": False},
                    {"name": "fred", "age": 40, "blocked": True},
                    {"name": "pebbles", "age": 1, "blocked": False},
                ],
            ),
            {"name": "barney", "age": 36, "blocked": False},
        ),
        (({"abc": 1, "xyz": 2, "c": 3}.keys(), lambda x: "y" in x), "xyz"),
        (({"abc": 1, "xyz": 2, "c": 3}.values(), lambda x: x < 2), 1),
    ],
)
def test_find(case, expected):
    assert _.find(*case) == expected


@pytest.mark.skipif(PY2, reason="test requires Python 3 inspect.signature")
@parametrize(
    "case,expected",
    [(({"abc": 1, "xyz": 2, "c": 3}.values(), fixtures.Filter(lambda x: x < 2)), 1)],
)
def test_find_using_callable_class(case, expected):
    assert _.find(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4], lambda num: num % 2 == 1), 3),
        (({"abc": 1, "xyz": 2, "c": 3}.keys(), lambda x: "y" in x), "xyz"),
        (({"abc": 1, "xyz": 2, "c": 3}.values(), lambda x: x < 2), 1),
    ],
)
def test_find_last(case, expected):
    assert _.find_last(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], None), [1, 2, 3]),
        (([[1], [2], [3]], None), [1, 2, 3]),
        (([[[1]], [[2]], [[3]]], None), [[1], [2], [3]]),
        (([1, 2, 3], lambda x: [x - 1]), [0, 1, 2]),
        (([1, 2, 3], lambda x: [[x], [x]]), [[1], [1], [2], [2], [3], [3]]),
    ],
)
def test_flat_map(case, expected):
    assert _.flat_map(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], None), [1, 2, 3]),
        (([[1], [2], [3]], None), [1, 2, 3]),
        (([[[1]], [[2]], [[3]]], None), [1, 2, 3]),
        (([1, 2, 3], lambda x: [x - 1]), [0, 1, 2]),
        (([1, 2, 3], lambda x: [[x], [x]]), [1, 1, 2, 2, 3, 3]),
    ],
)
def test_flat_map_deep(case, expected):
    assert _.flat_map_deep(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], None), [1, 2, 3]),
        (([[1], [2], [3]], None), [1, 2, 3]),
        (([[[1]], [[2]], [[3]]], None), [[1], [2], [3]]),
        (([[[1]], [[2]], [[3]]], None, 1), [[1], [2], [3]]),
        (([[[1]], [[2]], [[3]]], None, 2), [1, 2, 3]),
    ],
)
def test_flat_map_depth(case, expected):
    assert _.flat_map_depth(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], fixtures.noop), [1, 2, 3]),
        (([1, 2, 3], lambda value: value < 2), [1, 2, 3]),
        (({"one": 1, "two": 2, "three": 3}, fixtures.noop), {"one": 1, "two": 2, "three": 3}),
    ],
)
def test_for_each(case, expected):
    assert _.for_each(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], fixtures.noop), [1, 2, 3]),
        (([1, 2, 3], lambda value: value < 2), [1, 2, 3]),
        (({"one": 1, "two": 2, "three": 3}, fixtures.noop), {"one": 1, "two": 2, "three": 3}),
    ],
)
def test_for_each_right(case, expected):
    assert _.for_each_right(*case) == expected


@parametrize(
    "case,expected",
    [
        (([4.2, 6.1, 6.4], lambda num: int(math.floor(num))), {4: [4.2], 6: [6.1, 6.4]}),
    ],
)
def test_group_by(case, expected):
    assert _.group_by(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], 1), True),
        (([1, 2, 3], 1, 2), False),
        (({"name": "fred", "age": 40}, "fred"), True),
        (("pebbles", "eb"), True),
    ],
)
def test_includes(case, expected):
    assert _.includes(*case) == expected


@parametrize(
    "case,expected",
    [
        (([{"a": {"b": 2}}, {"a": {"c": 3}}], "a.items"), [{"b": 2}.items(), {"c": 3}.items()]),
        (
            ([{"a": {"b": {"c": 2}}}, {"a": {"b": {"c": 3}}}], "a.b.items"),
            [{"c": 2}.items(), {"c": 3}.items()],
        ),
    ],
)
def test_invoke_map(case, expected):
    assert _.invoke_map(*case) == expected


@parametrize(
    "case,expected",
    [
        (
            ([{"dir": "left", "code": 97}, {"dir": "right", "code": 100}], "dir"),
            {"left": {"dir": "left", "code": 97}, "right": {"dir": "right", "code": 100}},
        ),
    ],
)
def test_key_by(case, expected):
    assert _.key_by(*case) == expected


@parametrize(
    "case,expected,sort_results",
    [
        (([1, 2, 3],), [1, 2, 3], False),
        (([1.1, 2.1, 3.1], int), [1, 2, 3], False),
        (([1, 2, 3], lambda num: num * 3), [3, 6, 9], False),
        (([[1], [2, 3], [4, 5, 6]], len), [1, 2, 3], False),
        (({"one": 1, "two": 2, "three": 3}, lambda num: num * 3), [3, 6, 9], True),
        (
            ([{"name": "moe", "age": 40}, {"name": "larry", "age": 50}], "name"),
            ["moe", "larry"],
            False,
        ),
        (
            (
                [
                    {"level1": {"level2": {"level3": {"value": 1}}}},
                    {"level1": {"level2": {"level3": {"value": 2}}}},
                    {"level1": {"level2": {"level3": {"value": 3}}}},
                    {"level1": {"level2": {"level3": {"value": 4}}}},
                    {"level1": {"level2": {}}},
                    {},
                ],
                "level1.level2.level3.value",
            ),
            [1, 2, 3, 4, None, None],
            False,
        ),
        (([[0, 1], [2, 3], [4, 5]], 1), [1, 3, 5], False),
        (
            (
                [{"a": 1, "b": 2, "c": -1}, {"a": 3, "b": 4, "c": -1}, {"a": 5, "b": 6, "c": -1}],
                itemgetter("a", "b"),
            ),
            [(1, 2), (3, 4), (5, 6)],
            False,
        ),
    ],
)
def test_map_(case, expected, sort_results):
    actual = _.map_(*case)

    if sort_results:
        actual = sorted(actual)

    assert actual == expected


@parametrize(
    "case,expected",
    [
        (
            (
                [
                    {"a": 1, "b": 2, "c": 3},
                    {"a": 1, "b": 2, "c": 4},
                    {"a": 1, "b": 2, "c": 5},
                    {"a": 1, "b": 1, "c": 6},
                    {"a": 1, "b": 1, "c": 7},
                    {"a": 2, "b": 2, "c": 8},
                    {"a": 2, "b": 2, "c": 9},
                    {"a": 2, "b": 2, "c": 10},
                    {"a": 3, "b": 1, "c": 11},
                ],
                "a",
            ),
            {
                1: [
                    {"a": 1, "b": 2, "c": 3},
                    {"a": 1, "b": 2, "c": 4},
                    {"a": 1, "b": 2, "c": 5},
                    {"a": 1, "b": 1, "c": 6},
                    {"a": 1, "b": 1, "c": 7},
                ],
                2: [{"a": 2, "b": 2, "c": 8}, {"a": 2, "b": 2, "c": 9}, {"a": 2, "b": 2, "c": 10}],
                3: [{"a": 3, "b": 1, "c": 11}],
            },
        ),
        (
            (
                [
                    {"a": 1, "b": 2, "c": 3},
                    {"a": 1, "b": 2, "c": 4},
                    {"a": 1, "b": 2, "c": 5},
                    {"a": 1, "b": 1, "c": 6},
                    {"a": 1, "b": 1, "c": 7},
                    {"a": 2, "b": 2, "c": 8},
                    {"a": 2, "b": 2, "c": 9},
                    {"a": 2, "b": 2, "c": 10},
                    {"a": 3, "b": 1, "c": 11},
                ],
                "a",
                "b",
            ),
            {
                1: {
                    2: [
                        {"a": 1, "b": 2, "c": 3},
                        {"a": 1, "b": 2, "c": 4},
                        {"a": 1, "b": 2, "c": 5},
                    ],
                    1: [{"a": 1, "b": 1, "c": 6}, {"a": 1, "b": 1, "c": 7}],
                },
                2: {
                    2: [
                        {"a": 2, "b": 2, "c": 8},
                        {"a": 2, "b": 2, "c": 9},
                        {"a": 2, "b": 2, "c": 10},
                    ]
                },
                3: {1: [{"a": 3, "b": 1, "c": 11}]},
            },
        ),
        (
            (
                [
                    {"a": 1, "b": 2, "c": 3},
                    {"a": 1, "b": 2, "c": 4},
                    {"a": 1, "b": 2, "c": 5},
                    {"a": 1, "b": 1, "c": 6},
                    {"a": 1, "b": 1, "c": 7},
                    {"a": 2, "b": 2, "c": 8},
                    {"a": 2, "b": 2, "c": 9},
                    {"a": 2, "b": 2, "c": 10},
                    {"a": 3, "b": 1, "c": 11},
                ],
            ),
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": 1, "b": 2, "c": 4},
                {"a": 1, "b": 2, "c": 5},
                {"a": 1, "b": 1, "c": 6},
                {"a": 1, "b": 1, "c": 7},
                {"a": 2, "b": 2, "c": 8},
                {"a": 2, "b": 2, "c": 9},
                {"a": 2, "b": 2, "c": 10},
                {"a": 3, "b": 1, "c": 11},
            ],
        ),
    ],
)
def test_nest(case, expected):
    assert _.nest(*case) == expected


@parametrize(
    "case,expected",
    [
        (
            (
                [
                    {"user": "barney", "age": 36},
                    {"user": "fred", "age": 40},
                    {"user": "barney", "age": 26},
                    {"user": "fred", "age": 30},
                ],
                [],
            ),
            [
                {"user": "barney", "age": 36},
                {"user": "fred", "age": 40},
                {"user": "barney", "age": 26},
                {"user": "fred", "age": 30},
            ],
        ),
        (
            (
                [
                    {"user": "barney", "age": 36},
                    {"user": "fred", "age": 40},
                    {"user": "barney", "age": 26},
                    {"user": "fred", "age": 30},
                ],
                ["user", "age"],
            ),
            [
                {"user": "barney", "age": 26},
                {"user": "barney", "age": 36},
                {"user": "fred", "age": 30},
                {"user": "fred", "age": 40},
            ],
        ),
        (
            (
                [
                    {"user": "barney", "age": 36},
                    {"user": "fred", "age": 40},
                    {"user": "barney", "age": 26},
                    {"user": "fred", "age": 30},
                ],
                ["-user", "age"],
            ),
            [
                {"user": "fred", "age": 30},
                {"user": "fred", "age": 40},
                {"user": "barney", "age": 26},
                {"user": "barney", "age": 36},
            ],
        ),
        (
            (
                [
                    {"user": "barney", "age": 36},
                    {"user": "fred", "age": 40},
                    {"user": "barney", "age": 26},
                    {"user": "fred", "age": 30},
                ],
                ["user", "-age"],
            ),
            [
                {"user": "barney", "age": 36},
                {"user": "barney", "age": 26},
                {"user": "fred", "age": 40},
                {"user": "fred", "age": 30},
            ],
        ),
        (
            (
                [
                    {"user": "barney", "age": 36},
                    {"user": "fred", "age": 40},
                    {"user": "barney", "age": 26},
                    {"user": "fred", "age": 30},
                ],
                ["-user", "-age"],
            ),
            [
                {"user": "fred", "age": 40},
                {"user": "fred", "age": 30},
                {"user": "barney", "age": 36},
                {"user": "barney", "age": 26},
            ],
        ),
        (
            (
                {
                    1: {"user": "barney", "age": 36},
                    2: {"user": "fred", "age": 40},
                    3: {"user": "barney", "age": 26},
                    4: {"user": "fred", "age": 30},
                },
                ["user", "age"],
            ),
            [
                {"user": "barney", "age": 26},
                {"user": "barney", "age": 36},
                {"user": "fred", "age": 30},
                {"user": "fred", "age": 40},
            ],
        ),
        (
            (
                [
                    {"user": "barney", "age": 36},
                    {"user": "fred", "age": 40},
                    {"user": "barney", "age": 26},
                    {"user": "fred", "age": 30},
                ],
                [],
                True,
            ),
            [
                {"user": "barney", "age": 36},
                {"user": "fred", "age": 40},
                {"user": "barney", "age": 26},
                {"user": "fred", "age": 30},
            ],
        ),
        (
            (
                [
                    {"user": "barney", "age": 36},
                    {"user": "fred", "age": 40},
                    {"user": "barney", "age": 26},
                    {"user": "fred", "age": 30},
                ],
                ["user", "age"],
                True,
            ),
            list(
                reversed(
                    [
                        {"user": "barney", "age": 26},
                        {"user": "barney", "age": 36},
                        {"user": "fred", "age": 30},
                        {"user": "fred", "age": 40},
                    ]
                )
            ),
        ),
        (
            (
                [
                    {"user": "barney", "age": 36},
                    {"user": "fred", "age": 40},
                    {"user": "barney", "age": 26},
                    {"user": "fred", "age": 30},
                ],
                ["-user", "age"],
                True,
            ),
            list(
                reversed(
                    [
                        {"user": "fred", "age": 30},
                        {"user": "fred", "age": 40},
                        {"user": "barney", "age": 26},
                        {"user": "barney", "age": 36},
                    ]
                )
            ),
        ),
        (
            (
                [
                    {"user": "barney", "age": 36},
                    {"user": "fred", "age": 40},
                    {"user": "barney", "age": 26},
                    {"user": "fred", "age": 30},
                ],
                ["user", "-age"],
                True,
            ),
            list(
                reversed(
                    [
                        {"user": "barney", "age": 36},
                        {"user": "barney", "age": 26},
                        {"user": "fred", "age": 40},
                        {"user": "fred", "age": 30},
                    ]
                )
            ),
        ),
        (
            (
                [
                    {"user": "barney", "age": 36},
                    {"user": "fred", "age": 40},
                    {"user": "barney", "age": 26},
                    {"user": "fred", "age": 30},
                ],
                ["-user", "-age"],
                True,
            ),
            list(
                reversed(
                    [
                        {"user": "fred", "age": 40},
                        {"user": "fred", "age": 30},
                        {"user": "barney", "age": 36},
                        {"user": "barney", "age": 26},
                    ]
                )
            ),
        ),
        (
            (
                {
                    1: {"user": "barney", "age": 36},
                    2: {"user": "fred", "age": 40},
                    3: {"user": "barney", "age": 26},
                    4: {"user": "fred", "age": 30},
                },
                ["user", "age"],
                True,
            ),
            list(
                reversed(
                    [
                        {"user": "barney", "age": 26},
                        {"user": "barney", "age": 36},
                        {"user": "fred", "age": 30},
                        {"user": "fred", "age": 40},
                    ]
                )
            ),
        ),
        (
            (
                [
                    {"user": "barney", "age": 36},
                    {"user": "fred", "age": 40},
                    {"user": "barney", "age": 26},
                    {"user": "fred", "age": 30},
                ],
                ["user", "age"],
                [False, True],
                True,
            ),
            list(
                reversed(
                    [
                        {"user": "fred", "age": 30},
                        {"user": "fred", "age": 40},
                        {"user": "barney", "age": 26},
                        {"user": "barney", "age": 36},
                    ]
                )
            ),
        ),
        (
            (
                [
                    {"user": "barney", "age": 36},
                    {"user": "fred", "age": 40},
                    {"user": "barney", "age": 26},
                    {"user": "fred", "age": 30},
                ],
                ["user", "age"],
                [False],
                True,
            ),
            list(
                reversed(
                    [
                        {"user": "fred", "age": 30},
                        {"user": "fred", "age": 40},
                        {"user": "barney", "age": 26},
                        {"user": "barney", "age": 36},
                    ]
                )
            ),
        ),
    ],
)
def test_order_by(case, expected):
    assert _.order_by(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], lambda item: item % 2), [[1, 3], [2]]),
        (([1.2, 2.3, 3.4], lambda item: math.floor(item) % 2), [[1.2, 3.4], [2.3]]),
        (
            (
                [
                    {"name": "barney", "age": 36},
                    {"name": "fred", "age": 40, "blocked": True},
                    {"name": "pebbles", "age": 1},
                ],
                {"age": 1},
            ),
            [
                [{"name": "pebbles", "age": 1}],
                [{"name": "barney", "age": 36}, {"name": "fred", "age": 40, "blocked": True}],
            ],
        ),
        (
            (
                [
                    {"name": "barney", "age": 36},
                    {"name": "fred", "age": 40, "blocked": True},
                    {"name": "pebbles", "age": 1},
                ],
                "blocked",
            ),
            [
                [{"name": "fred", "age": 40, "blocked": True}],
                [{"name": "barney", "age": 36}, {"name": "pebbles", "age": 1}],
            ],
        ),
    ],
)
def test_partition(case, expected):
    assert _.partition(*case) == expected


@parametrize(
    "case,expected",
    [
        (([{"name": "moe", "age": 40}, {"name": "larry", "age": 50}], "name"), ["moe", "larry"]),
        (
            (
                [
                    {"level1": {"level2": {"level3": {"value": 1}}}},
                    {"level1": {"level2": {"level3": {"value": 2}}}},
                    {"level1": {"level2": {"level3": {"value": 3}}}},
                    {"level1": {"level2": {"level3": {"value": 4}}}},
                    {"level1": {"level2": {}}},
                    {},
                ],
                "level1.level2.level3.value",
            ),
            [1, 2, 3, 4, None, None],
        ),
    ],
)
def test_pluck(case, expected):
    assert _.pluck(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], None), 1),
        (([1, 2, 3], fixtures.reduce_iteratee0), 6),
        (({"a": 1, "b": 2, "c": 3}, fixtures.reduce_iteratee1, {}), {"a": 3, "b": 6, "c": 9}),
    ],
)
def test_reduce_(case, expected):
    assert _.reduce_(*case) == expected


@parametrize("case,exception", [(([],), TypeError)])
def test_reduce_raise(case, exception):
    raised = False

    try:
        _.reduce_(*case)
    except exception:
        raised = True

    assert raised


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], None), 3),
        (([1, 2, 3], fixtures.reduce_iteratee0), 6),
        (([[0, 1], [2, 3], [4, 5]], fixtures.reduce_right_iteratee0), [4, 5, 2, 3, 0, 1]),
        (({"a": 1, "b": 2, "c": 3}, fixtures.reduce_iteratee1, {}), {"a": 3, "b": 6, "c": 9}),
    ],
)
def test_reduce_right(case, expected):
    assert _.reduce_right(*case) == expected


@parametrize("case,exception", [(([],), TypeError)])
def test_reduce_right_exception(case, exception):
    raised = False

    try:
        _.reduce_right(*case)
    except exception:
        raised = True

    assert raised


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], None), [1, 1]),
        (([1, 2, 3], fixtures.reduce_iteratee0), [3, 6]),
        (([1, 2, 3, 4, 5], fixtures.reduce_iteratee0, 0), [1, 3, 6, 10, 15]),
    ],
)
def test_reductions(case, expected):
    assert _.reductions(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], None), [3, 3]),
        (([1, 2, 3], fixtures.reduce_iteratee0), [5, 6]),
        (
            ([[0, 1], [2, 3], [4, 5]], fixtures.reduce_right_iteratee0),
            [[4, 5, 2, 3], [4, 5, 2, 3, 0, 1]],
        ),
    ],
)
def test_reductions_right(case, expected):
    assert _.reductions_right(*case) == expected


@parametrize(
    "case,expected",
    [
        (([0, True, False, None, 1, 2, 3],), [0, False, None]),
        (([1, 2, 3, 4, 5, 6], lambda num: num % 2 == 0), [1, 3, 5]),
        (
            (
                [
                    {"name": "barney", "age": 36, "blocked": False},
                    {"name": "fred", "age": 40, "blocked": True},
                ],
                "blocked",
            ),
            [{"name": "barney", "age": 36, "blocked": False}],
        ),
        (
            (
                [
                    {"name": "barney", "age": 36, "blocked": False},
                    {"name": "fred", "age": 40, "blocked": True},
                ],
                {"age": 36},
            ),
            [{"name": "fred", "age": 40, "blocked": True}],
        ),
    ],
)
def test_reject(case, expected):
    assert _.reject(*case) == expected


@parametrize(
    "case",
    [
        [1, 2, 3, 4, 5, 6],
    ],
)
def test_sample(case):
    assert _.sample(case) in case


@parametrize(
    "case",
    [
        ([1, 2, 3, 4, 5, 6], 2),
        ([1, 2, 3, 4, 5, 6], 3),
        ([1, 2, 3, 4, 5, 6], 4),
    ],
)
def test_sample_size(case):
    collection, n = case
    sample_n = _.sample_size(*case)

    assert isinstance(sample_n, list)
    assert len(sample_n) == min(n, len(collection))
    assert set(sample_n).issubset(collection)


@parametrize("case", [[1, 2, 3, 4, 5, 6], {"one": 1, "two": 2, "three": 3}])
def test_shuffle(case):
    shuffled = _.shuffle(case)

    assert len(shuffled) == len(case)

    if isinstance(case, dict):
        assert set(shuffled) == set(case.values())
    else:
        assert set(shuffled) == set(case)


@parametrize("case", [[1, 2, 3, 4, 5], {"1": 1, "2": 2, "3": 3}])
def test_size(case):
    assert _.size(case) == len(case)


@parametrize(
    "case,expected",
    [
        (([None, 0, "yes", False], bool), True),
        (([None, 0, "yes", False],), True),
        (
            (
                [
                    {"name": "apple", "organic": False, "type": "fruit"},
                    {"name": "carrot", "organic": True, "type": "vegetable"},
                ],
                "organic",
            ),
            True,
        ),
        (
            (
                [
                    {"name": "apple", "organic": False, "type": "fruit"},
                    {"name": "carrot", "organic": True, "type": "vegetable"},
                ],
                {"type": "meat"},
            ),
            False,
        ),
    ],
)
def test_some(case, expected):
    assert _.some(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], lambda x: math.sin(x)), [3, 1, 2]),
        (
            (
                [
                    {"name": "barney", "age": 36},
                    {"name": "fred", "age": 40},
                    {"name": "barney", "age": 26},
                    {"name": "fred", "age": 30},
                ],
                "age",
            ),
            [
                {"name": "barney", "age": 26},
                {"name": "fred", "age": 30},
                {"name": "barney", "age": 36},
                {"name": "fred", "age": 40},
            ],
        ),
        (({"a": 1, "b": 2, "c": 3}, lambda x: math.sin(x)), [3, 1, 2]),
        (([1, 2, 3], lambda x: math.sin(x), True), [2, 1, 3]),
        (
            (
                [
                    {"name": "barney", "age": 36},
                    {"name": "fred", "age": 40},
                    {"name": "barney", "age": 26},
                    {"name": "fred", "age": 30},
                ],
                "age",
                True,
            ),
            [
                {"name": "fred", "age": 40},
                {"name": "barney", "age": 36},
                {"name": "fred", "age": 30},
                {"name": "barney", "age": 26},
            ],
        ),
        (({"a": 1, "b": 2, "c": 3}, lambda x: math.sin(x), True), [2, 1, 3]),
    ],
)
def test_sort_by(case, expected):
    assert _.sort_by(*case) == expected


@parametrize(
    "case,expected",
    [
        (("cat",), ["c", "a", "t"]),
        ((b"cat",), ["c", "a", "t"]),
        ((u"cat",), [u"c", u"a", u"t"]),
        (("cat", False), ["cat"]),
        ((b"cat", False), [b"cat"]),
        ((u"cat", False), [u"cat"]),
        (({"a": 1, "b": 2, "c": 3},), [1, 2, 3]),
    ],
)
def test_to_list(case, expected):
    assert set(_.to_list(*case)) == set(expected)
