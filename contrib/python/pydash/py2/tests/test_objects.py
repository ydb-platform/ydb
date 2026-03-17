# -*- coding: utf-8 -*-

from argparse import Namespace
from collections import defaultdict, namedtuple
import datetime as dt

import pydash as _

from . import fixtures
from .fixtures import parametrize


today = 'today'


@parametrize(
    "case,expected",
    [
        (({"name": "fred"}, {"employer": "slate"}), {"name": "fred", "employer": "slate"}),
        (
            ({"name": "fred"}, {"employer": "slate"}, {"employer": "medium"}),
            {"name": "fred", "employer": "medium"},
        ),
    ],
)
def test_assign(case, expected):
    assert _.assign(*case) == expected


@parametrize(
    "case,expected",
    [
        (({"name": "fred"}, {"age": 26}, lambda obj, src: src + 1), {"name": "fred", "age": 27}),
    ],
)
def test_assign_with(case, expected):
    assert _.assign_with(*case) == expected


@parametrize(
    "case,expected",
    [
        (({"name": "fred", "greet": lambda: "Hello, world!"},), ["greet"]),
        ((["fred", lambda: "Hello, world!"],), [1]),
    ],
)
def test_callables(case, expected):
    assert _.callables(*case) == expected


@parametrize(
    "case",
    [
        {"a": {"d": 1}, "b": {"c": 2}},
        [{"a": {"d": 1}, "b": {"c": 2}}],
    ],
)
def test_clone(case):
    result = _.clone(case)

    assert result is not case

    for key, value in _.helpers.iterator(result):
        assert value is case[key]


@parametrize(
    "case,iteratee,expected",
    [
        ({"a": {"d": 1}, "b": {"c": 2}}, lambda v: v, {"a": {"d": 1}, "b": {"c": 2}}),
        (
            {"a": 1, "b": 2, "c": {"d": 3}},
            lambda v, k: v + 2 if isinstance(v, int) and k else None,
            {"a": 3, "b": 4, "c": {"d": 3}},
        ),
    ],
)
def test_clone_with(case, iteratee, expected):
    result = _.clone_with(case, iteratee)

    assert result == expected


@parametrize(
    "case",
    [
        {"a": {"d": 1}, "b": {"c": 2}},
        {"a": {"d": 1}, "b": {"c": 2}},
        [{"a": {"d": 1}, "b": {"c": 2}}],
    ],
)
def test_clone_deep(case):
    result = _.clone_deep(case)

    assert result is not case

    for key, value in _.helpers.iterator(result):
        assert value is not case[key]


@parametrize(
    "case,iteratee,expected",
    [
        ({"a": {"d": 1}, "b": {"c": 2}}, lambda v: v, {"a": {"d": 1}, "b": {"c": 2}}),
        (
            {"a": 1, "b": 2, "c": {"d": 3}},
            lambda v, k: v + 2 if isinstance(v, int) and k else None,
            {"a": 3, "b": 4, "c": {"d": 5}},
        ),
        (["a"], lambda a: None, ["a"]),
        ("a", lambda a: None, "a"),
    ],
)
def test_clone_deep_with(case, iteratee, expected):
    result = _.clone_deep_with(case, iteratee)

    assert result == expected


@parametrize(
    "case,expected",
    [
        (
            ({"name": "barney"}, {"name": "fred", "employer": "slate"}),
            {"name": "barney", "employer": "slate"},
        ),
    ],
)
def test_defaults(case, expected):
    assert _.defaults(*case) == expected


@parametrize(
    "case,expected",
    [
        (
            ({"user": {"name": "barney"}}, {"user": {"name": "fred", "age": 36}}),
            {"user": {"name": "barney", "age": 36}},
        ),
        (({}, {"a": {"b": ["c"]}}, {"a": {"b": ["d"]}}), {"a": {"b": ["c"]}}),
        (
            ({"a": {"b": [{"d": "e"}]}}, {"a": {"b": [{"d": "f"}]}}, {"a": {"b": [{"g": "h"}]}}),
            {"a": {"b": [{"d": "e", "g": "h"}]}},
        ),
        (
            (
                {"a": {"b": [{"d": "e"}]}},
                {"a": {"b": [{"d": "f"}, {"g": "h"}]}},
                {"a": {"b": [{"i": "j"}]}},
            ),
            {"a": {"b": [{"d": "e", "i": "j"}]}},
        ),
        (
            (
                {"a": {"b": [{"d": "e"}, {"x": "y"}]}},
                {"a": {"b": [{"d": "f"}, {"g": "h"}]}},
                {"a": {"b": [{"i": "j"}]}},
            ),
            {"a": {"b": [{"d": "e", "i": "j"}, {"x": "y", "g": "h"}]}},
        ),
    ],
)
def test_defaults_deep(case, expected):
    assert _.defaults_deep(*case) == expected


@parametrize(
    "case,expected",
    [
        ([1, 2, 3], {0: 1, 1: 2, 2: 3}),
        ({0: 1, 1: 2, 2: 3}, {0: 1, 1: 2, 2: 3}),
    ],
)
def test_to_dict(case, expected):
    assert _.to_dict(case) == expected


@parametrize(
    "case,expected",
    [
        ({"a": 1, "b": 2, "c": 3}, {1: "a", 2: "b", 3: "c"}),
        ([1, 2, 3], {1: 0, 2: 1, 3: 2}),
    ],
)
def test_invert(case, expected):
    assert _.invert(case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3],), {1: [0], 2: [1], 3: [2]}),
        (
            ({"first": "fred", "second": "barney", "third": "fred"},),
            {"fred": ["first", "third"], "barney": ["second"]},
        ),
        (({"a": 1, "b": 2}, lambda val: val * 2), {2: ["a"], 4: ["b"]}),
    ],
)
def test_invert_by(case, expected):
    result = _.invert_by(*case)
    for key in result:
        assert set(result[key]) == set(expected[key])


@parametrize(
    "case,expected",
    [
        (({"a": 1, "b": 2}, "get", "a"), 1),
        (({"a": {"b": {"c": [1, 2, 3, 3]}}}, "a.b.c.count", 3), 2),
        (({}, "count"), None),
    ],
)
def test_invoke(case, expected):
    assert _.invoke(*case) == expected


@parametrize(
    "case,expected",
    [
        # NOTE: The expected is a list of values but find_key returns only a single
        # value. However, since dicts do not have an order, it's unknown what the
        # "first" returned value will be.
        (
            (
                {
                    "barney": {"age": 36, "blocked": False},
                    "fred": {"age": 40, "blocked": True},
                    "pebbles": {"age": 1, "blocked": False},
                },
                lambda obj: obj["age"] < 40,
            ),
            ["pebbles", "barney"],
        ),
        (
            (
                {
                    "barney": {"age": 36, "blocked": False},
                    "fred": {"age": 40, "blocked": True},
                    "pebbles": {"age": 1, "blocked": False},
                },
            ),
            ["barney", "fred", "pebbles"],
        ),
        (([1, 2, 3],), [0]),
    ],
)
def test_find_key(case, expected):
    assert _.find_key(*case) in expected


@parametrize(
    "case,expected",
    [
        # NOTE: The expected is a list of values but find_last_key returns only a
        # single value. However, since dicts do not have an order, it's unknown
        # what the "first" returned value will be.
        (
            (
                {
                    "barney": {"age": 36, "blocked": False},
                    "fred": {"age": 40, "blocked": True},
                    "pebbles": {"age": 1, "blocked": False},
                },
                lambda obj: obj["age"] < 40,
            ),
            ["pebbles", "barney"],
        ),
        (
            (
                {
                    "barney": {"age": 36, "blocked": False},
                    "fred": {"age": 40, "blocked": True},
                    "pebbles": {"age": 1, "blocked": False},
                },
            ),
            ["barney", "fred", "pebbles"],
        ),
        (([1, 2, 3],), [2]),
    ],
)
def test_find_last_key(case, expected):
    assert _.find_last_key(*case) in expected


@parametrize(
    "case,expected",
    [
        (
            ({"name": "fred", "employer": "slate"}, fixtures.for_in_iteratee0),
            ({"name": "fredfred", "employer": "slateslate"},),
        ),
        (
            ({"name": "fred", "employer": "slate"}, fixtures.for_in_iteratee1),
            ({"name": "fredfred", "employer": "slate"}, {"name": "fred", "employer": "slateslate"}),
        ),
        (([1, 2, 3], fixtures.for_in_iteratee2), ([False, True, 3],)),
    ],
)
def test_for_in(case, expected):
    assert _.for_in(*case) in expected


@parametrize(
    "case,expected",
    [
        (
            ({"name": "fred", "employer": "slate"}, fixtures.for_in_iteratee0),
            ({"name": "fredfred", "employer": "slateslate"},),
        ),
        (
            ({"name": "fred", "employer": "slate"}, fixtures.for_in_iteratee1),
            ({"name": "fredfred", "employer": "slate"}, {"name": "fred", "employer": "slateslate"}),
        ),
        (([1, 2, 3], fixtures.for_in_iteratee2), ([1, True, "index:2"],)),
    ],
)
def test_for_in_right(case, expected):
    assert _.for_in_right(*case) in expected


@parametrize(
    "case,expected",
    [
        (({"one": {"two": {"three": 4}}}, "one.two"), {"three": 4}),
        (({"one": {"two": {"three": 4}}}, "one.two.three"), 4),
        (({"one": {"two": {"three": 4}}}, ["one", "two"]), {"three": 4}),
        (({"one": {"two": {"three": 4}}}, ["one", "two", "three"]), 4),
        (({"one": {"two": {"three": 4}}}, "one.four"), None),
        (({"one": {"two": {"three": 4}}}, "one.four.three", []), []),
        (({"one": {"two": {"three": 4}}}, "one.four.0.a", [{"a": 1}]), [{"a": 1}]),
        (({"one": {"two": {"three": [{"a": 1}]}}}, "one.four.three.0.a", []), []),
        (({"one": {"two": {"three": 4}}}, "one.four.three"), None),
        (({"one": {"two": {"three": [{"a": 1}]}}}, "one.four.three.0.a"), None),
        (({"one": {"two": {"three": 4}}}, "one.four.three", 2), 2),
        (({"one": {"two": {"three": [{"a": 1}]}}}, "one.four.three.0.a", 2), 2),
        (({"one": {"two": {"three": 4}}}, "one.four.three", {"test": "value"}), {"test": "value"}),
        (
            ({"one": {"two": {"three": [{"a": 1}]}}}, "one.four.three.0.a", {"test": "value"}),
            {"test": "value"},
        ),
        (({"one": {"two": {"three": 4}}}, "one.four.three", "haha"), "haha"),
        (({"one": {"two": {"three": [{"a": 1}]}}}, "one.four.three.0.a", "haha"), "haha"),
        (({"one": {"two": {"three": 4}}}, "five"), None),
        (({"one": ["two", {"three": [4, 5]}]}, ["one", 1, "three", 1]), 5),
        (({"one": ["two", {"three": [4, 5]}]}, "one.[1].three.[1]"), 5),
        (({"one": ["two", {"three": [4, 5]}]}, "one.1.three.1"), 5),
        ((["one", {"two": {"three": [4, 5]}}], "[1].two.three.[0]"), 4),
        ((["one", {"two": {"three": [4, [{"four": [5]}]]}}], "[1].two.three[1][0].four[0]"), 5),
        ((range(50), "[42]"), 42),
        (([[[[[[[[[[42]]]]]]]]]], "[0][0][0][0][0][0][0][0][0][0]"), 42),
        (([range(50)], "[0][42]"), 42),
        (({"a": [{"b": range(50)}]}, "a[0].b[42]"), 42),
        (
            ({"lev.el1": {"lev\\el2": {"level3": ["value"]}}}, "lev\\.el1.lev\\\\el2.level3.[0]"),
            "value",
        ),
        (({"one": ["hello", "there"]}, "one.bad.hello", []), []),
        (({"one": ["hello", None]}, "one.1.hello"), None),
        ((namedtuple("a", ["a", "b"])(1, 2), "a"), 1),
        ((namedtuple("a", ["a", "b"])(1, 2), 0), 1),
        ((namedtuple("a", ["a", "b"])({"c": {"d": 1}}, 2), "a.c.d"), 1),
        (({}, "update"), None),
        (([], "extend"), None),
        (({(1,): {(2,): 3}}, (1,)), {(2,): 3}),
        (({(1,): {(2,): 3}}, [(1,), (2,)]), 3),
        (({object: 1}, object), 1),
        (({object: {object: 1}}, [object, object]), 1),
        (({1: {"name": "John Doe"}}, "1.name"), "John Doe"),
    ],
)
def test_get(case, expected):
    assert _.get(*case) == expected


def test_get__should_not_populate_defaultdict():
    data = defaultdict(list)
    _.get(data, "a")
    assert data == {}


@parametrize(
    "case,expected",
    [
        (({"a": 1, "b": 2, "c": 3}, "b"), True),
        (([1, 2, 3], 0), True),
        (([1, 2, 3], 1), True),
        (([1, 2, 3], 3), False),
        (({"a": 1, "b": 2, "c": 3}, "b"), True),
        (([1, 2, 3], 0), True),
        (([1, 2, 3], 1), True),
        (([1, 2, 3], 3), False),
        (({"one": {"two": {"three": 4}}}, "one.two"), True),
        (({"one": {"two": {"three": 4}}}, "one.two.three"), True),
        (({"one": {"two": {"three": 4}}}, ["one", "two"]), True),
        (({"one": {"two": {"three": 4}}}, ["one", "two", "three"]), True),
        (({"one": {"two": {"three": 4}}}, "one.four"), False),
        (({"one": {"two": {"three": 4}}}, "five"), False),
        (({"one": {"two": {"three": 4}}}, "one.four.three"), False),
        (({"one": {"two": {"three": [{"a": 1}]}}}, "one.four.three.0.a"), False),
        (({"one": ["two", {"three": [4, 5]}]}, ["one", 1, "three", 1]), True),
        (({"one": ["two", {"three": [4, 5]}]}, "one.[1].three.[1]"), True),
        (({"one": ["two", {"three": [4, 5]}]}, "one.1.three.1"), True),
        ((["one", {"two": {"three": [4, 5]}}], "[1].two.three.[0]"), True),
        (({"lev.el1": {r"lev\el2": {"level3": ["value"]}}}, r"lev\.el1.lev\\el2.level3.[0]"), True),
    ],
)
def test_has(case, expected):
    assert _.has(*case) == expected


def test_has__should_not_populate_defaultdict():
    data = defaultdict(list)
    _.has(data, "a")
    assert data == {}


@parametrize("case,expected", [({"a": 1, "b": 2, "c": 3}, ["a", "b", "c"]), ([1, 2, 3], [0, 1, 2])])
def test_keys(case, expected):
    assert set(_.keys(case)) == set(expected)


@parametrize(
    "case,expected",
    [
        (({"a": 1, "b": 2, "c": 3}, lambda num: num * 3), {"a": 3, "b": 6, "c": 9}),
        (
            (
                {"fred": {"name": "fred", "age": 40}, "pebbles": {"name": "pebbles", "age": 1}},
                "age",
            ),
            {"fred": 40, "pebbles": 1},
        ),
    ],
)
def test_map_values(case, expected):
    assert _.map_values(*case) == expected


@parametrize(
    "case,expected",
    [
        (
            (
                {
                    "level1": {
                        "value": "value 1",
                        "level2": {"value": "value 2", "level3": {"value": "value 3"}},
                    }
                },
                lambda value, property_path: ".".join(property_path) + "==" + value,
            ),
            {
                "level1": {
                    "value": "level1.value==value 1",
                    "level2": {
                        "value": "level1.level2.value==value 2",
                        "level3": {"value": "level1.level2.level3.value==value 3"},
                    },
                }
            },
        ),
        (
            (
                [["value 1", [["value 2", ["value 3"]]]]],
                lambda value, property_path: (_.join(property_path, ".") + "==" + value),
            ),
            [["0.0==value 1", [["0.1.0.0==value 2", ["0.1.0.1.0==value 3"]]]]],
        ),
    ],
)
def test_map_values_deep(case, expected):
    assert _.map_values_deep(*case) == expected


@parametrize(
    "case,expected",
    [
        (
            (
                {"characters": [{"name": "barney"}, {"name": "fred"}]},
                {"characters": [{"age": 36}, {"age": 40}]},
            ),
            {"characters": [{"name": "barney", "age": 36}, {"name": "fred", "age": 40}]},
        ),
        (
            (
                {"characters": [{"name": "barney"}, {"name": "fred"}, {}]},
                {"characters": [{"age": 36}, {"age": 40}]},
            ),
            {"characters": [{"name": "barney", "age": 36}, {"name": "fred", "age": 40}, {}]},
        ),
        (
            (
                {"characters": [{"name": "barney"}, {"name": "fred"}]},
                {"characters": [{"age": 36}, {"age": 40}, {}]},
            ),
            {"characters": [{"name": "barney", "age": 36}, {"name": "fred", "age": 40}, {}]},
        ),
        (
            (
                {"characters": [{"name": "barney"}, {"name": "fred"}]},
                {"characters": [{"age": 36}, {"age": 40}]},
                {"characters": [{"score": 5}, {"score": 7}]},
            ),
            {
                "characters": [
                    {"name": "barney", "age": 36, "score": 5},
                    {"name": "fred", "age": 40, "score": 7},
                ]
            },
        ),
        (
            (
                {"characters": {"barney": {"age": 36}, "fred": {"score": 7}}},
                {"characters": {"barney": {"score": 5}, "fred": {"age": 40}}},
            ),
            {"characters": {"barney": {"age": 36, "score": 5}, "fred": {"age": 40, "score": 7}}},
        ),
        (
            (
                {"characters": {"barney": {"age": 36}, "fred": {"score": 7}}},
                {"characters": {"barney": [5], "fred": 7}},
            ),
            {"characters": {"barney": [5], "fred": 7}},
        ),
        (
            (
                {"characters": {"barney": {"age": 36}, "fred": {"score": 7}}},
                {"foo": {"barney": [5], "fred": 7}},
            ),
            {
                "characters": {"barney": {"age": 36}, "fred": {"score": 7}},
                "foo": {"barney": [5], "fred": 7},
            },
        ),
        (({"foo": {"bar": 1}}, {"foo": {}}), {"foo": {"bar": 1}}),
        (({},), {}),
        (([],), []),
        ((None,), None),
        ((None, {"a": 1}), None),
        ((None, None, None, {"a": 1}), None),
        (({"a": 1}, None), {"a": 1}),
        (({"a": 1}, None, None, None, {"b": 2}), {"a": 1, "b": 2}),
        (({"a": None}, None, None, None, {"b": None}), {"a": None, "b": None}),
    ],
)
def test_merge(case, expected):
    assert _.merge(*case) == expected


def test_merge_no_link_dict():
    case1 = {"foo": {"bar": None}}
    case2 = {"foo": {"bar": False}}
    result = _.merge({}, case1, case2)
    result["foo"]["bar"] = True

    assert case1 == {"foo": {"bar": None}}
    assert case2 == {"foo": {"bar": False}}


def test_merge_no_link_list():
    case = {"foo": [{}]}
    result = _.merge({}, case)
    result["foo"][0]["bar"] = True

    assert case == {"foo": [{}]}


@parametrize(
    "case,expected",
    [
        (
            (
                {"fruits": ["apple"], "vegetables": ["beet"]},
                {"fruits": ["banana"], "vegetables": ["carrot"]},
                lambda a, b: a + b if isinstance(a, list) else b,
            ),
            {"fruits": ["apple", "banana"], "vegetables": ["beet", "carrot"]},
        ),
    ],
)
def test_merge_with(case, expected):
    assert _.merge_with(*case) == expected


@parametrize(
    "case,expected",
    [
        (({"a": 1, "b": 2, "c": 3}, "a"), {"b": 2, "c": 3}),
        (({"a": 1, "b": 2, "c": 3}, "a", "b"), {"c": 3}),
        (({"a": 1, "b": 2, "c": 3}, ["a", "b"]), {"c": 3}),
        (({"a": 1, "b": 2, "c": 3}, ["a"], ["b"]), {"c": 3}),
        (([1, 2, 3],), {0: 1, 1: 2, 2: 3}),
        (([1, 2, 3], 0), {1: 2, 2: 3}),
        (([1, 2, 3], 0, 1), {2: 3}),
        (({"a": {"b": {"c": "d"}}, "e": "f"}, "a.b.c", "e"), {"a": {"b": {}}}),
        (({"a": [{"b": 1, "c": 2}, {"d": 3}]}, "a[0].c", "a[1].d"), {"a": [{"b": 1}, {}]}),
    ],
)
def test_omit(case, expected):
    assert _.omit(*case) == expected


@parametrize(
    "case,expected",
    [
        (({"a": 1, "b": 2, "c": 3}, ["a", "b"]), {"c": 3}),
        (({"a": 1, "b": 2, "c": 3}, lambda value, key: key == "a"), {"b": 2, "c": 3}),
        (([1, 2, 3],), {0: 1, 1: 2, 2: 3}),
        (([1, 2, 3], [0]), {1: 2, 2: 3}),
    ],
)
def test_omit_by(case, expected):
    assert _.omit_by(*case) == expected


@parametrize(
    "case,expected",
    [
        ((1,), 1),
        ((1.0,), 1),
        (("1",), 1),
        (("00001",), 1),
        ((13, 8), 11),
        (("0A",), 10),
        (("08",), 8),
        (("10",), 16),
        (("10", 10), 10),
        (("xyz",), None),
    ],
)
def test_parse_int(case, expected):
    assert _.parse_int(*case) == expected


@parametrize(
    "case,expected",
    [
        (({"a": 1, "b": 2, "c": 3}, "a"), {"a": 1}),
        (({"a": 1, "b": 2, "c": 3}, "a", "b"), {"a": 1, "b": 2}),
        (({"a": 1, "b": 2, "c": 3}, ["a", "b"]), {"a": 1, "b": 2}),
        (({"a": 1, "b": 2, "c": 3}, ["a"], ["b"]), {"a": 1, "b": 2}),
        (([1, 2, 3],), {}),
        (([1, 2, 3], 0), {0: 1}),
        ((fixtures.Object(a=1, b=2, c=3), "a"), {"a": 1}),
        ((fixtures.ItemsObject({"a": 1, "b": 2, "c": 3}), "a"), {"a": 1}),
        ((fixtures.IteritemsObject({"a": 1, "b": 2, "c": 3}), "a"), {"a": 1}),
        (({"a": {"b": 1, "c": 2, "d": 3}}, "a.b", "a.d"), {"a": {"b": 1, "d": 3}}),
        (
            ({"a": [{"b": 1}, {"c": 2}, {"d": 3}]}, "a[0]", "a[2]"),
            {"a": [{"b": 1}, None, {"d": 3}]},
        ),
    ],
)
def test_pick(case, expected):
    assert _.pick(*case) == expected


@parametrize(
    "case,expected",
    [
        (({"a": 1, "b": 2, "c": 3}, ["a", "b"]), {"a": 1, "b": 2}),
        (({"a": 1, "b": 2, "c": 3}, lambda value, key: key in ["a"]), {"a": 1}),
        (([1, 2, 3],), {0: 1, 1: 2, 2: 3}),
        (([1, 2, 3], [0]), {0: 1}),
        ((fixtures.Object(a=1, b=2, c=3), "a"), {"a": 1}),
        ((fixtures.ItemsObject({"a": 1, "b": 2, "c": 3}), "a"), {"a": 1}),
        ((fixtures.IteritemsObject({"a": 1, "b": 2, "c": 3}), "a"), {"a": 1}),
    ],
)
def test_pick_by(case, expected):
    assert _.pick_by(*case) == expected


@parametrize(
    "case,expected",
    [
        (({"a": 1, "b": 2}, {"a": "A", "b": "B"}), {"A": 1, "B": 2}),
        (({"a": 1, "b": 2}, {"a": "A"}), {"A": 1, "b": 2}),
        (({"a": 1, "b": 2}, {"c": "C", "b": "B"}), {"a": 1, "B": 2}),
    ],
)
def test_rename_keys(case, expected):
    assert _.rename_keys(*case) == expected


@parametrize(
    "case,expected",
    [
        (({}, ["one", "two", "three", "four"], 1), {"one": {"two": {"three": {"four": 1}}}}),
        (({}, "one.two.three.four", 1), {"one": {"two": {"three": {"four": 1}}}}),
        (
            ({"one": {"two": {}, "three": {}}}, ["one", "two", "three", "four"], 1),
            {"one": {"two": {"three": {"four": 1}}, "three": {}}},
        ),
        (
            ({"one": {"two": {}, "three": {}}}, "one.two.three.four", 1),
            {"one": {"two": {"three": {"four": 1}}, "three": {}}},
        ),
        (({}, "one", 1), {"one": 1}),
        (([], [0, 0, 0], 1), [[[1]]]),
        (([], "[0].[0].[0]", 1), [[[1]]]),
        (([1, 2, [3, 4, [5, 6]]], [2, 2, 1], 7), [1, 2, [3, 4, [5, 7]]]),
        (([1, 2, [3, 4, [5, 6]]], "[2].[2].[1]", 7), [1, 2, [3, 4, [5, 7]]]),
        (([1, 2, [3, 4, [5, 6]]], [2, 2, 2], 7), [1, 2, [3, 4, [5, 6, 7]]]),
        (([1, 2, [3, 4, [5, 6]]], "[2].[2].[2]", 7), [1, 2, [3, 4, [5, 6, 7]]]),
        (({}, "a.b[0].c", 1), {"a": {"b": [{"c": 1}]}}),
        (({}, "a.b[0][0].c", 1), {"a": {"b": [[{"c": 1}]]}}),
        (({}, "a", tuple), {"a": tuple}),
        (({}, r"a.b\.c.d", 1), {"a": {"b.c": {"d": 1}}}),
    ],
)
def test_set_(case, expected):
    assert _.set_(*case) == expected


@parametrize(
    "case,expected",
    [
        (({}, "[0][1]", "a", lambda: {}), {0: {1: "a"}}),
        (({}, "[0][1]", dict, lambda: {}), {0: {1: dict}}),
        ((Namespace(), "a.b", 5, lambda: Namespace()), Namespace(a=Namespace(b=5))),
        (
            (Namespace(a=Namespace(b=5)), "a.c.d", 55, lambda: Namespace()),
            Namespace(a=Namespace(b=5, c=Namespace(d=55))),
        ),
    ],
)
def test_set_with(case, expected):
    assert _.set_with(*case) == expected


@parametrize(
    "case,expected",
    [
        (("1",), True),
        (("0",), False),
        (("true",), True),
        (("True",), True),
        (("false",), False),
        (("False",), False),
        (("",), None),
        (("a",), None),
        ((0,), False),
        ((1,), True),
        (([],), False),
        ((True,), True),
        ((False,), False),
        ((None,), False),
        (("Truthy", ["truthy"]), True),
        (("Falsey", [], ["falsey"]), False),
        (("foobar", ["^[f]"]), True),
        (("ofobar", ["^[f]"]), None),
        (("foobar", [], [".+[r]$"]), False),
        (("foobra", [], [".+[r]$"]), None),
    ],
)
def test_to_boolean(case, expected):
    assert _.to_boolean(*case) is expected


@parametrize(
    "case,expected",
    [
        (1.4, 1),
        (1.9, 1),
        ("1.4", 1),
        ("1.9", 1),
        ("foo", 0),
        (None, 0),
        (True, 1),
        (False, 0),
        ({}, 0),
        ([], 0),
        ((), 0),
    ],
)
def test_to_integer(case, expected):
    assert _.to_integer(case) == expected


@parametrize(
    "case,expected",
    [(("2.556",), 3.0), (("2.556", 1), 2.6), (("999.999", -1), 990.0), (("foo",), None)],
)
def test_to_number(case, expected):
    assert _.to_number(*case) == expected


@parametrize(
    "case,expected",
    [
        ({"a": 1, "b": 2, "c": 3}, [["a", 1], ["b", 2], ["c", 3]]),
        ([1, 2, 3], [[0, 1], [1, 2], [2, 3]]),
    ],
)
def test_to_pairs(case, expected):
    assert dict(_.to_pairs(case)) == dict(expected)


@parametrize(
    "case,expected",
    [
        (1, "1"),
        (1.25, "1.25"),
        (True, "True"),
        ([1], "[1]"),
        ("d\xc3\xa9j\xc3\xa0 vu", "d\xc3\xa9j\xc3\xa0 vu"),
        ("", ""),
        (None, ""),
        (today, str(today)),
    ],
)
def test_to_string(case, expected):
    if case is today:
        case = dt.date.today()
        expected = str(case)
    assert _.to_string(case) == expected


@parametrize(
    "case,expected",
    [
        (
            ([1, 2, 3, 4, 5], lambda acc, value, key: acc.append((key, value))),
            [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)],
        ),
        (([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], fixtures.transform_iteratee0), [1, 9, 25]),
        (([1, 2, 3, 4, 5],), []),
    ],
)
def test_transform(case, expected):
    assert _.transform(*case) == expected


@parametrize(
    "case,expected",
    [
        (
            (
                {"rome": "Republic"},
                ["rome"],
                lambda value: "Empire" if value == "Republic" else value,
            ),
            {"rome": "Empire"},
        ),
        (({}, ["rome"], lambda value: "Empire" if value == "Republic" else value), {"rome": None}),
        (
            (
                {"earth": {"rome": "Republic"}},
                ["earth", "rome"],
                lambda value: "Empire" if value == "Republic" else value,
            ),
            {"earth": {"rome": "Empire"}},
        ),
    ],
)
def test_update(case, expected):
    assert _.update(*case) == expected


@parametrize(
    "case,expected",
    [
        (({}, "[0][1]", _.constant("a"), lambda *_: {}), {0: {1: "a"}}),
        (({}, "[0][1]", _.constant("a"), {}), {0: {1: "a"}}),
        (({}, "[0][1]", "a", {}), {0: {1: "a"}}),
    ],
)
def test_update_with(case, expected):
    assert _.update_with(*case) == expected


@parametrize(
    "obj,path,expected,new_obj",
    [
        ({"a": [{"b": {"c": 7}}]}, "a.0.b.c", True, {"a": [{"b": {}}]}),
        ([1, 2, 3], "1", True, [1, 3]),
        ([1, 2, 3], 1, True, [1, 3]),
        ([1, [2, 3]], [1, 1], True, [1, [2]]),
        ([1, 2, 3], "[0][0]", False, [1, 2, 3]),
        ([1, 2, 3], "[0][0][0]", False, [1, 2, 3]),
    ],
)
def test_unset(obj, path, expected, new_obj):
    assert _.unset(obj, path) == expected
    assert obj == new_obj


@parametrize("case,expected", [({"a": 1, "b": 2, "c": 3}, [1, 2, 3]), ([1, 2, 3], [1, 2, 3])])
def test_values(case, expected):
    assert set(_.values(case)) == set(expected)
