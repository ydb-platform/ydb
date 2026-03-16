# -*- coding: utf-8 -*-

import math

import pytest

import pydash as _

from .fixtures import parametrize


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5],), [[1], [2], [3], [4], [5]]),
        (([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]),
        (([1, 2, 3, 4, 5], 3), [[1, 2, 3], [4, 5]]),
        (([1, 2, 3, 4, 5], 4), [[1, 2, 3, 4], [5]]),
        (([1, 2, 3, 4, 5], 5), [[1, 2, 3, 4, 5]]),
        (([1, 2, 3, 4, 5], 6), [[1, 2, 3, 4, 5]]),
    ],
)
def test_chunk(case, expected):
    assert _.chunk(*case) == expected


@parametrize(
    "case,expected",
    [([0, 1, 2, 3], [1, 2, 3]), ([True, False, None, True, 1, "foo"], [True, True, 1, "foo"])],
)
def test_compact(case, expected):
    assert _.compact(case) == expected


@parametrize(
    "case,expected",
    [
        ((), []),
        (([],), []),
        (([1, 2, 3],), [1, 2, 3]),
        (([1, 2, 3], [4, 5, 6]), [1, 2, 3, 4, 5, 6]),
        (([1, 2, 3], [4, 5, 6], [7]), [1, 2, 3, 4, 5, 6, 7]),
        ((1, [2], 3, 4), [1, 2, 3, 4]),
    ],
)
def test_concat(case, expected):
    assert _.concat(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4],), [1, 2, 3, 4]),
        (([1, 2, 3, 4], []), [1, 2, 3, 4]),
        (([1, 2, 3, 4], [2, 4], [3, 5, 6]), [1]),
        (([1, 1, 1, 1], [2, 4], [3, 5, 6]), [1, 1, 1, 1]),
    ],
)
def test_difference(case, expected):
    assert _.difference(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4],), [1, 2, 3, 4]),
        (([1, 2, 3, 4], []), [1, 2, 3, 4]),
        (([{"a": 1}, {"a": 2, "b": 2}], [{"a": 1}], "a"), [{"a": 2, "b": 2}]),
    ],
)
def test_difference_by(case, expected):
    assert _.difference_by(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4],), [1, 2, 3, 4]),
        (([1, 2, 3, 4], []), [1, 2, 3, 4]),
        (
            ([{"a": 1}, {"a": 2, "b": 2}], [{"a": 1}], lambda item, other: item["a"] == other["a"]),
            [{"a": 2, "b": 2}],
        ),
    ],
)
def test_difference_with(case, expected):
    assert _.difference_with(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5],), [2, 3, 4, 5]),
        (([1, 2, 3, 4, 5], 1), [2, 3, 4, 5]),
        (([1, 2, 3, 4, 5], 2), [3, 4, 5]),
        (([1, 2, 3, 4, 5], 5), []),
        (([1, 2, 3, 4, 5], 6), []),
    ],
)
def test_drop(case, expected):
    assert _.drop(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5], lambda item: item < 3), [3, 4, 5]),
    ],
)
def test_drop_while(case, expected):
    assert _.drop_while(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5],), [1, 2, 3, 4]),
        (([1, 2, 3, 4, 5], 1), [1, 2, 3, 4]),
        (([1, 2, 3, 4, 5], 2), [1, 2, 3]),
        (([1, 2, 3, 4, 5], 5), []),
        (([1, 2, 3, 4, 5], 6), []),
    ],
)
def test_drop_right(case, expected):
    assert _.drop_right(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5], lambda item: item > 3), [1, 2, 3]),
    ],
)
def test_drop_right_while(case, expected):
    assert _.drop_right_while(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 2, 1, 5, 6, 5, 5, 5],), [2, 1, 5]),
        ((["A", "b", "C", "a", "B", "c"], lambda letter: letter.lower()), ["a", "B", "c"]),
    ],
)
def test_duplicates(case, expected):
    assert _.duplicates(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5], 0), [0, 0, 0, 0, 0]),
        (([1, 2, 3, 4, 5], 0, 2), [1, 2, 0, 0, 0]),
        (([1, 2, 3, 4, 5], 0, 2, 3), [1, 2, 0, 4, 5]),
        (([1, 2, 3, 4, 5], 0, 0, 5), [0, 0, 0, 0, 0]),
        (([1, 2, 3, 4, 5], 0, 0, 8), [0, 0, 0, 0, 0]),
        (([1, 2, 3, 4, 5], 0, 0, -1), [0, 0, 0, 0, 5]),
    ],
)
def test_fill(case, expected):
    array = case[0]
    assert _.fill(*case) == expected
    assert array == expected


@parametrize(
    "case,filter_by,expected",
    [
        (["apple", "banana", "beet"], lambda item: item.startswith("b"), 1),
        (
            [
                {"name": "apple", "type": "fruit"},
                {"name": "banana", "type": "fruit"},
                {"name": "beet", "type": "vegetable"},
            ],
            {"name": "banana"},
            1,
        ),
        (["apple", "banana", "beet"], lambda: False, -1),
    ],
)
def test_find_index(case, filter_by, expected):
    assert _.find_index(case, filter_by) == expected


@parametrize(
    "case,filter_by,expected",
    [
        (["apple", "banana", "beet"], lambda item: item.startswith("b"), 2),
        (
            [
                {"name": "apple", "type": "fruit"},
                {"name": "banana", "type": "fruit"},
                {"name": "beet", "type": "vegetable"},
            ],
            {"type": "fruit"},
            1,
        ),
        (["apple", "banana", "beet"], lambda: False, -1),
    ],
)
def test_find_last_index(case, filter_by, expected):
    assert _.find_last_index(case, filter_by) == expected


@parametrize(
    "case,expected",
    [
        ([1, ["2222"], [3, [[4]]]], [1, "2222", 3, [[4]]]),
    ],
)
def test_flatten(case, expected):
    assert _.flatten(case) == expected


@parametrize(
    "case,expected",
    [
        ([1, ["2222"], [3, [[4]]]], [1, "2222", 3, 4]),
    ],
)
def test_flatten_deep(case, expected):
    assert _.flatten_deep(case) == expected


@parametrize(
    "case,expected",
    [
        (([1, ["2222"], [3, [[4]]]],), [1, "2222", 3, [[4]]]),
        (([1, ["2222"], [3, [[4]]]], 1), [1, "2222", 3, [[4]]]),
        (([1, ["2222"], [3, [[4]]]], 2), [1, "2222", 3, [4]]),
        (([1, ["2222"], [3, [[4]]]], 3), [1, "2222", 3, 4]),
    ],
)
def test_flatten_depth(case, expected):
    assert _.flatten_depth(*case) == expected


@parametrize(
    "case,expected",
    [
        ([["a", 1], ["b", 2]], {"a": 1, "b": 2}),
        ([["a", 1], ["b", 2], ["c", 3]], {"a": 1, "b": 2, "c": 3}),
    ],
)
def test_from_pairs(case, expected):
    assert _.from_pairs(case) == expected


@parametrize("case,expected", [([1, 2, 3], 1), ([], None)])
def test_head(case, expected):
    assert _.head(case) == expected


@parametrize(
    "case,value,from_index,expected",
    [
        ([1, 2, 3, 1, 2, 3], 2, 0, 1),
        ([1, 2, 3, 1, 2, 3], 2, 3, 4),
        ([1, 1, 2, 2, 3, 3], 2, True, 2),
        ([1, 1, 2, 2, 3, 3], 4, 0, -1),
        ([1, 1, 2, 2, 3, 3], 2, 10, -1),
        ([1, 1, 2, 2, 3, 3], 0, 0, -1),
    ],
)
def test_index_of(case, value, from_index, expected):
    assert _.index_of(case, value, from_index) == expected


@parametrize("case,expected", [([1, 2, 3], [1, 2]), ([1], [])])
def test_initial(case, expected):
    assert _.initial(case) == expected


@parametrize(
    "case,expected",
    [
        (([[10, 20], [30, 40], [50, 60]], [1, 2, 3]), [10, 20, 1, 2, 3, 30, 40, 1, 2, 3, 50, 60]),
        (
            ([[[10, 20]], [[30, 40]], [50, [60]]], [1, 2, 3]),
            [[10, 20], 1, 2, 3, [30, 40], 1, 2, 3, 50, [60]],
        ),
    ],
)
def test_intercalate(case, expected):
    assert _.intercalate(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2], [3, 4]), [1, 3, 2, 4]),
        (([1, 2], [3, 4], [5, 6]), [1, 3, 5, 2, 4, 6]),
        (([1, 2], [3, 4, 5], [6]), [1, 3, 6, 2, 4, 5]),
        (([1, 2, 3], [4], [5, 6]), [1, 4, 5, 2, 6, 3]),
    ],
)
def test_interleave(case, expected):
    assert _.interleave(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], [101, 2, 1, 10], [2, 1]), [1, 2]),
        (([1, 1, 2, 2], [1, 1, 2, 2]), [1, 2]),
        (([1, 2, 3], [4]), []),
        (([1, 2, 3],), [1, 2, 3]),
        (([], [101, 2, 1, 10], [2, 1]), []),
        (([],), []),
    ],
)
def test_intersection(case, expected):
    assert _.intersection(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], [101, 2, 1, 10], [2, 1], None), [1, 2]),
        (([1, 2, 3], [4]), []),
        (([1, 2, 3],), [1, 2, 3]),
        (([], [101, 2, 1, 10], [2, 1]), []),
        (([],), []),
        (([1, 2, 3], [101, 2, 1, 10], [2, 1], lambda a: 1 if a < 10 else 0), [1]),
        (([{"a": 1}, {"a": 2}, {"a": 3}], [{"a": 2}], "a"), [{"a": 2}]),
    ],
)
def test_intersection_by(case, expected):
    assert _.intersection_by(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], [101, 2, 1, 10], [2, 1], None), [1, 2]),
        (([1, 2, 3], [4]), []),
        (([1, 2, 3],), [1, 2, 3]),
        (([], [101, 2, 1, 10], [2, 1]), []),
        (([],), []),
        (
            (["A", "b", "cC"], ["a", "cc"], ["A", "CC"], lambda a, b: a.lower() == b.lower()),
            ["A", "cC"],
        ),
    ],
)
def test_intersection_with(case, expected):
    assert _.intersection_with(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4], 10), [1, 10, 2, 10, 3, 10, 4]),
        (([1, 2, 3, 4], [0, 0, 0]), [1, [0, 0, 0], 2, [0, 0, 0], 3, [0, 0, 0], 4]),
        (
            ([[1, 2, 3], [4, 5, 6], [7, 8, 9]], [0, 0, 0]),
            [[1, 2, 3], [0, 0, 0], [4, 5, 6], [0, 0, 0], [7, 8, 9]],
        ),
    ],
)
def test_intersperse(case, expected):
    assert _.intersperse(*case) == expected


@parametrize("case,expected", [([1, 2, 3], 3), ([], None)])
def test_last(case, expected):
    assert _.last(case) == expected


@parametrize(
    "case,value,from_index,expected",
    [
        ([1, 2, 3, 1, 2, 3], 2, 0, -1),
        ([1, 2, 3, 1, 2, 3], 2, 3, 1),
        ([1, 2, 3, 1, 2, 3], 0, 0, -1),
        ([0, 1, 2, 3, 4, 5], 3, 0, -1),
        ([0, 1, 2, 3, 4, 5], 3, 1, -1),
        ([0, 1, 2, 3, 4, 5], 3, 2, -1),
        ([0, 1, 2, 3, 4, 5], 3, 3, 3),
        ([0, 1, 2, 3, 4, 5], 3, 4, 3),
        ([0, 1, 2, 3, 4, 5], 3, 5, 3),
        ([0, 1, 2, 3, 4, 5], 3, 6, 3),
        ([0, 1, 2, 3, 4, 5], 3, -1, 3),
        ([0, 1, 2, 3, 4, 5], 3, -2, 3),
        ([0, 1, 2, 3, 4, 5], 3, -3, 3),
        ([0, 1, 2, 3, 4, 5], 3, -4, -1),
        ([0, 1, 2, 3, 4, 5], 3, -5, -1),
        ([0, 1, 2, 3, 4, 5], 3, -6, -1),
        ([0, 1, 2, 3, 4, 5], 3, None, 3),
    ],
)
def test_last_index_of(case, value, from_index, expected):
    assert _.last_index_of(case, value, from_index) == expected


@parametrize(
    "case,expected",
    [(([1, 2, None, 4, None, 6], lambda x, i: ["{0}".format(i)] if x is None else []), ["2", "4"])],
)
def test_mapcat(case, expected):
    assert _.mapcat(*case) == expected


@parametrize(
    "case,pos,expected",
    [([11, 22, 33], 2, 33), ([11, 22, 33], 0, 11), ([11, 22, 33], -1, 33), ([11, 22, 33], 4, None)],
)
def test_nth(case, pos, expected):
    assert _.nth(case, pos) == expected


@parametrize(
    "case,expected,after",
    [
        (([1, 2, 3],), 3, [1, 2]),
        (([1, 2, 3], 0), 1, [2, 3]),
        (([1, 2, 3], 1), 2, [1, 3]),
    ],
)
def test_pop(case, expected, after):
    array = case[0]
    assert _.pop(*case) == expected
    assert array == after


@parametrize("case,values,expected", [([1, 2, 3, 1, 2, 3], [2, 3], [1, 1])])
def test_pull(case, values, expected):
    assert _.pull(case, *values) == expected


@parametrize(
    "case,values,expected",
    [
        ([1, 2, 3, 1, 2, 3], [2, 3], [1, 1]),
        ([1, 2, 3, 1, 2, 3], [1, 2, 3], []),
        ([1, 2, 3, 1, 2, 3], [1, 2, 3, 1, 2, 3], []),
    ],
)
def test_pull_all(case, values, expected):
    assert _.pull_all(case, values) == expected


@parametrize(
    "case,values,iteratee,expected",
    [
        ([1, 2, 3, 1, 2, 3], [2, 3], None, [1, 1]),
        ([1, 2, 3, 1, 2, 3], [2, 3], lambda item: item + 2, [1, 1]),
    ],
)
def test_pull_all_by(case, values, iteratee, expected):
    assert _.pull_all_by(case, values, iteratee) == expected


@parametrize(
    "case,values,iteratee,expected",
    [
        ([1, 2, 3, 1, 2, 3], [2, 3], None, [1, 1]),
        ([1, 2, 3, 1, 2, 3], [2, 3], lambda a, b: a == b, [1, 1]),
        ([1, 2, 3, 1, 2, 3], [2, 3], lambda a, b: a != b, []),
    ],
)
def test_pull_all_with(case, values, iteratee, expected):
    assert _.pull_all_with(case, values, iteratee) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 1, 2, 3], [2, 3]), [1, 2, 2, 3]),
        (([1, 2, 3, 1, 2, 3], [3, 2]), [1, 2, 2, 3]),
        (([1, 2, 3, 1, 2, 3], 3, 2), [1, 2, 2, 3]),
    ],
)
def test_pull_at(case, expected):
    assert _.pull_at(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], 4), [1, 2, 3, 4]),
        (([1, 2, 3], 4, 5), [1, 2, 3, 4, 5]),
        (([1, 2, 3], [4, 5], 6, [7, 8]), [1, 2, 3, [4, 5], 6, [7, 8]]),
    ],
)
def test_push(case, expected):
    assert _.push(*case) == expected


@parametrize(
    "case,filter_by,expected",
    [
        ([1, 2, 3, 4, 5, 6], lambda x: x % 2 == 0, [2, 4, 6]),
        ([1, 2, 3, 4], lambda x: x >= 3, [3, 4]),
    ],
)
def test_remove(case, filter_by, expected):
    original = list(case)
    assert _.remove(case, filter_by) == expected
    assert set(case).intersection(expected) == set([])
    assert set(original) == set(case + expected)


@parametrize(
    "case,expected",
    [
        ([1, 2, 3, 4], [4, 3, 2, 1]),
        ("abcdef", "fedcba"),
    ],
)
def test_reverse(case, expected):
    assert _.reverse(case) == expected


@parametrize(
    "case,expected,after",
    [
        ([1, 2, 3], 1, [2, 3]),
    ],
)
def test_shift(case, expected, after):
    assert _.shift(case) == expected
    assert case == after


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5], 0, 1), [1]),
        (([1, 2, 3, 4, 5], 1, 3), [2, 3]),
        (([1, 2, 3, 4, 5], 1, 4), [2, 3, 4]),
        (([1, 2, 3, 4, 5], 1, 5), [2, 3, 4, 5]),
        (([1, 2, 3, 4, 5], 0, -1), [1, 2, 3, 4]),
        (([1, 2, 3, 4, 5], 2), [3]),
        (([1, 2, 3, 4, 5], -1), [5]),
        (([1, 2, 3, 4, 5], -2), [4]),
        (([1, 2, 3, 4, 5], -3), [3]),
        (([1, 2, 3, 4, 5], -5), [1]),
    ],
)
def test_slice_(case, expected):
    assert _.slice_(*case) == expected


@parametrize(
    "case,expected",
    [
        (([2, 1, 3, 4, 6, 5],), [1, 2, 3, 4, 5, 6]),
        (([2, 1, 3, 4, 6, 5], None, None, True), [6, 5, 4, 3, 2, 1]),
        (([{"v": 2}, {"v": 3}, {"v": 1}], None, lambda x: x["v"]), [{"v": 1}, {"v": 2}, {"v": 3}]),
        (([2, 1, 3, 4, 6, 5], lambda a, b: -1 if a > b else 1), [6, 5, 4, 3, 2, 1]),
    ],
)
def test_sort(case, expected):
    array = case[0]
    assert _.sort(*case) == expected
    assert array == expected


def test_sort_comparator_key_exception():
    with pytest.raises(ValueError):
        _.sort([], comparator=lambda: None, key=lambda: None)


@parametrize(
    "case,expected",
    [
        (([4, 4, 5, 5, 6, 6], 5), 2),
        (([20, 30, 40, 40, 50], 40), 2),
        (([20, 30, 50], 40), 2),
        (([20, 30, 50], 10), 0),
    ],
)
def test_sorted_index(case, expected):
    assert _.sorted_index(*case) == expected


@parametrize(
    "case,expected",
    [
        (([{"x": 20}, {"x": 30}, {"x": 50}], {"x": 40}, "x"), 2),
        (
            (
                ["twenty", "thirty", "fifty"],
                "fourty",
                lambda x: {"twenty": 20, "thirty": 30, "fourty": 40, "fifty": 50}[x],
            ),
            2,
        ),
    ],
)
def test_sorted_index_by(case, expected):
    assert _.sorted_index_by(*case) == expected


@parametrize(
    "array,value,expected",
    [
        ([2, 3, 4, 10, 10], 10, 3),
        ([10, 10, 4, 2, 3], 11, -1),
    ],
)
def test_sorted_index_of(array, value, expected):
    assert _.sorted_index_of(array, value) == expected


@parametrize(
    "case,expected",
    [
        (([4, 4, 5, 5, 6, 6], 5), 4),
        (([20, 30, 40, 40, 50], 40), 4),
        (([20, 30, 50], 10), 0),
    ],
)
def test_sorted_last_index(case, expected):
    assert _.sorted_last_index(*case) == expected


@parametrize(
    "case,expected",
    [
        (([{"x": 20}, {"x": 30}, {"x": 50}], {"x": 40}, "x"), 2),
        (
            (
                ["twenty", "thirty", "fifty"],
                "fourty",
                lambda x: {"twenty": 20, "thirty": 30, "fourty": 40, "fifty": 50}[x],
            ),
            2,
        ),
    ],
)
def test_sorted_last_index_by(case, expected):
    assert _.sorted_last_index_by(*case) == expected


@parametrize(
    "array,value,expected",
    [
        ([2, 3, 4, 10, 10], 10, 4),
        ([10, 10, 4, 2, 3], 11, -1),
    ],
)
def test_sorted_last_index_of(array, value, expected):
    assert _.sorted_last_index_of(array, value) == expected


@parametrize(
    "case,expected", [([2, 2, 1, 0.5, 4], [0.5, 1, 2, 4]), ([4, -2, -2, 0.5, -1], [-2, -1, 0.5, 4])]
)
def test_sorted_uniq(case, expected):
    assert _.sorted_uniq(case) == expected


@parametrize(
    "case,iteratee,expected",
    [
        ([2.5, 3, 1, 2, 1.5], lambda num: math.floor(num), [1, 2.5, 3]),
        (["A", "b", "C", "a", "B", "c"], lambda letter: letter.lower(), ["A", "C", "b"]),
    ],
)
def test_sorted_uniq_by(case, iteratee, expected):
    assert _.sorted_uniq_by(case, iteratee) == expected


@parametrize(
    "case,expected,after",
    [
        (([1, 2, 3], 1, 0, "splice"), [], [1, "splice", 2, 3]),
        (([1, 2, 3], 1, 1, "splice"), [2], [1, "splice", 3]),
        (([1, 2, 3], 0, 2, "splice", "slice", "dice"), [1, 2], ["splice", "slice", "dice", 3]),
        (([1, 2, 3], 0), [1, 2, 3], []),
        (([1, 2, 3], 1), [2, 3], [1]),
    ],
)
def test_splice(case, expected, after):
    array = case[0]
    assert _.splice(*case) == expected
    assert array == after


@parametrize(
    "case,expected",
    [
        (("123", 1, 0, "splice"), "1splice23"),
    ],
)
def test_splice_string(case, expected):
    assert _.splice(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4, 5]]),
        (([1, 2, 3, 4, 5], 0), [[], [1, 2, 3, 4, 5]]),
    ],
)
def test_split_at(case, expected):
    assert _.split_at(*case) == expected


@parametrize("case,expected", [([1, 2, 3], [2, 3]), ([], [])])
def test_tail(case, expected):
    assert _.tail(case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5],), [1]),
        (([1, 2, 3, 4, 5], 1), [1]),
        (([1, 2, 3, 4, 5], 2), [1, 2]),
        (([1, 2, 3, 4, 5], 5), [1, 2, 3, 4, 5]),
        (([1, 2, 3, 4, 5], 6), [1, 2, 3, 4, 5]),
    ],
)
def test_take(case, expected):
    assert _.take(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5], lambda item: item < 3), [1, 2]),
    ],
)
def test_take_while(case, expected):
    assert _.take_while(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5],), [5]),
        (([1, 2, 3, 4, 5], 1), [5]),
        (([1, 2, 3, 4, 5], 2), [4, 5]),
        (([1, 2, 3, 4, 5], 5), [1, 2, 3, 4, 5]),
        (([1, 2, 3, 4, 5], 6), [1, 2, 3, 4, 5]),
    ],
)
def test_take_right(case, expected):
    assert _.take_right(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5], lambda item: item > 3), [4, 5]),
    ],
)
def test_take_right_while(case, expected):
    assert _.take_right_while(*case) == expected


@parametrize(
    "case,expected",
    [
        ([1, 2, 1, 3, 1], [1, 2, 3]),
        ([dict(a=1), dict(a=2), dict(a=1)], [dict(a=1), dict(a=2)]),
    ],
)
def test_uniq(case, expected):
    assert _.uniq(case) == expected


@parametrize(
    "case,iteratee,expected",
    [
        ([1, 2, 1.5, 3, 2.5], lambda num: math.floor(num), [1, 2, 3]),
        (
            [
                {"name": "banana", "type": "fruit"},
                {"name": "apple", "type": "fruit"},
                {"name": "beet", "type": "vegetable"},
                {"name": "beet", "type": "vegetable"},
                {"name": "carrot", "type": "vegetable"},
                {"name": "carrot", "type": "vegetable"},
            ],
            {"type": "vegetable"},
            [{"name": "banana", "type": "fruit"}, {"name": "beet", "type": "vegetable"}],
        ),
        (
            [{"x": 1, "y": 1}, {"x": 2, "y": 1}, {"x": 1, "y": 1}],
            "x",
            [{"x": 1, "y": 1}, {"x": 2, "y": 1}],
        ),
        (["A", "b", "C", "a", "B", "c"], lambda letter: letter.lower(), ["A", "b", "C"]),
    ],
)
def test_uniq_by(case, iteratee, expected):
    assert _.uniq_by(case, iteratee) == expected


@parametrize(
    "case,iteratee,expected",
    [
        ([1, 2, 3, 4, 5], lambda a, b: (a % 2) == (b % 2), [1, 2]),
        ([5, 4, 3, 2, 1], lambda a, b: (a % 2) == (b % 2), [5, 4]),
    ],
)
def test_uniq_with(case, iteratee, expected):
    assert _.uniq_with(case, iteratee) == expected


@parametrize(
    "case,expected",
    [(([1, 2, 3], [101, 2, 1, 10], [2, 1]), [1, 2, 3, 101, 10]), (([11, 22, 33],), [11, 22, 33])],
)
def test_union(case, expected):
    assert _.union(*case) == expected


@parametrize(
    "case,iteratee,expected",
    [
        (([1, 2, 3], [2, 3, 4]), lambda x: x % 10, [1, 2, 3, 4]),
        (([1, 2, 3], [2, 3, 4]), lambda x: x % 2, [1, 2]),
        (([1, 2, 3], [2, 3, 4], lambda x: x % 2), None, [1, 2]),
        (([11, 22, 33],), None, [11, 22, 33]),
    ],
)
def test_union_by(case, iteratee, expected):
    assert _.union_by(*case, iteratee=iteratee) == expected


@parametrize(
    "case,expected",
    [
        (([11, 22, 33], [22, 33, 44]), [11, 22, 33, 44]),
        (([11, 22, 33],), [11, 22, 33]),
        (([1, 2, 3], [2, 3, 4], lambda a, b: (a % 2) == (b % 2)), [1, 2]),
    ],
)
def test_union_with(case, expected):
    assert _.union_with(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], 4), [4, 1, 2, 3]),
        (([1, 2, 3], 4, 5), [4, 5, 1, 2, 3]),
        (([1, 2, 3], [4, 5], 6, [7, 8]), [[4, 5], 6, [7, 8], 1, 2, 3]),
    ],
)
def test_unshift(case, expected):
    assert _.unshift(*case) == expected
    assert case[0] == expected


@parametrize(
    "case,expected",
    [
        (
            [["moe", 30, True], ["larry", 40, False], ["curly", 35, True]],
            [["moe", "larry", "curly"], [30, 40, 35], [True, False, True]],
        )
    ],
)
def test_unzip(case, expected):
    assert _.unzip(case) == expected


@parametrize(
    "case,expected",
    [
        (([],), []),
        (([[1, 10, 100], [2, 20, 200]],), [[1, 2], [10, 20], [100, 200]]),
        (([[2, 4, 6], [2, 2, 2]], _.power), [4, 16, 36]),
    ],
)
def test_unzip_with(case, expected):
    assert _.unzip_with(*case) == expected


@parametrize("case,expected", [(([1, 2, 1, 0, 3, 1, 4], 0, 1), [2, 3, 4])])
def test_without(case, expected):
    assert _.without(*case) == expected


@parametrize(
    "case,expected",
    [(([1, 2, 3], [5, 2, 1, 4]), [3, 5, 4]), (([1, 2, 5], [2, 3, 5], [3, 4, 5]), [1, 4, 5])],
)
def test_xor(case, expected):
    assert _.xor(*case) == expected


@parametrize("case,expected", [(([1, 2, 3], [5, 4], lambda val: val % 3), [3])])
def test_xor_by(case, expected):
    assert _.xor_by(*case) == expected


@parametrize("case,expected", [(([1, 2, 3], [5, 4], lambda a, b: a <= b), [5, 4])])
def test_xor_with(case, expected):
    assert _.xor_with(*case) == expected


@parametrize(
    "case,expected",
    [
        (
            (["moe", "larry", "curly"], [30, 40, 35], [True, False, True]),
            [["moe", 30, True], ["larry", 40, False], ["curly", 35, True]],
        )
    ],
)
def test_zip_(case, expected):
    assert _.zip_(*case) == expected


@parametrize(
    "case,expected",
    [
        ((["moe", "larry"], [30, 40]), {"moe": 30, "larry": 40}),
        (([["moe", 30], ["larry", 40]],), {"moe": 30, "larry": 40}),
    ],
)
def test_zip_object(case, expected):
    assert _.zip_object(*case) == expected


@parametrize(
    "case,expected",
    [
        ((["a.b.c", "a.b.d"], [1, 2]), {"a": {"b": {"c": 1, "d": 2}}}),
        ((["a.b[0].c", "a.b[1].d"], [1, 2]), {"a": {"b": [{"c": 1}, {"d": 2}]}}),
    ],
)
def test_zip_object_deep(case, expected):
    assert _.zip_object_deep(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2],), [[1], [2]]),
        (([1, 2], [3, 4], _.add), [4, 6]),
    ],
)
def test_zip_with(case, expected):
    assert _.zip_with(*case) == expected
