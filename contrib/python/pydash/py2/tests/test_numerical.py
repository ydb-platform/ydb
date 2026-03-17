# -*- coding: utf-8 -*-

import pytest

import pydash as _

from .fixtures import parametrize


@parametrize(
    "case,expected",
    [
        ((5, 3), 8),
    ],
)
def test_add(case, expected):
    assert _.add(*case) == expected


@parametrize(
    "case,expected",
    [
        ((4.006,), 5),
        ((6.004, 2), 6.01),
        ((6040, -2), 6100),
        (([4.006, 6.004], 2), [4.01, 6.01]),
    ],
)
def test_ceil(case, expected):
    assert _.ceil(*case) == expected


@parametrize(
    "case,expected",
    [
        ((0, -1, 1), 0),
        ((1, -1, 1), 1),
        ((-1, -1, 1), -1),
        ((1, 1), 1),
        ((5, -1, 1), 1),
        ((-5, -1, 1), -1),
    ],
)
def test_clamp(case, expected):
    assert _.clamp(*case) == expected


@parametrize(
    "dividend,divisor,expected",
    [(10, 5, 2.0), (None, 1, 1.0), (None, None, 1.0), (1.5, 3, 0.5), (-10, 2, -5.0)],
)
def test_divide(dividend, divisor, expected):
    assert _.divide(dividend, divisor) == expected


@parametrize(
    "case,expected",
    [
        ((4.006,), 4),
        ((0.046, 2), 0.04),
        ((4060, -2), 4000),
        (([4.006, 0.046], 2), [4.0, 0.04]),
    ],
)
def test_floor(case, expected):
    assert _.floor(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3],), 3),
        (({"a": 3, "b": 2, "c": 1},), 3),
    ],
)
def test_max_(case, expected):
    assert _.max_(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3],), 3),
        (({"a": 3, "b": 2, "c": 1},), 3),
        ((["anaconda", "bison", "camel"], lambda x: len(x)), "anaconda"),
        (
            (
                [{"name": "barney", "age": 36}, {"name": "fred", "age": 40}],
                "age",
            ),
            {"name": "fred", "age": 40},
        ),
        (
            ([{"name": "barney", "age": 36}, {"name": "fred", "age": 40}], lambda c: c["age"]),
            {"name": "fred", "age": 40},
        ),
    ],
)
def test_max_by(case, expected):
    assert _.max_by(*case) == expected


@parametrize(
    "collection,default,expected",
    [([], -1, -1), ([1, 2, 3], -1, 3), ({}, -1, -1), ([], None, None), ({}, None, None)],
)
def test_max_default(collection, default, expected):
    assert _.max_(collection, default=default) == expected


@parametrize(
    "case,expected",
    [
        ([1, 2, 3, 4, 5], 3),
        ([0, 0.5, 1], 0.5),
    ],
)
def test_mean(case, expected):
    assert _.mean(case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5],), 3),
        (([{"b": 4}, {"b": 5}, {"b": 6}], "b"), 5),
        (([0, 0.5, 1],), 0.5),
        (({"one": {"a": 1}, "two": {"a": 2}, "three": {"a": 3}}, "a"), 2),
    ],
)
def test_mean_by(case, expected):
    assert _.mean_by(*case) == expected


@parametrize(
    "case,expected",
    [
        (([0, 0, 0, 0, 5],), 0),
        (([0, 0, 1, 2, 5],), 1),
        (([0, 0, 1, 2],), 0.5),
        (([0, 0, 1, 2, 3, 4],), 1.5),
    ],
)
def test_median(case, expected):
    assert _.median(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3],), 1),
        (({"a": 3, "b": 2, "c": 1},), 1),
    ],
)
def test_min_(case, expected):
    assert _.min_(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3],), 1),
        (({"a": 3, "b": 2, "c": 1},), 1),
        ((["anaconda", "bison", "cat"], lambda x: len(x)), "cat"),
        (
            (
                [{"name": "barney", "age": 36}, {"name": "fred", "age": 40}],
                "age",
            ),
            {"name": "barney", "age": 36},
        ),
        (
            ([{"name": "barney", "age": 36}, {"name": "fred", "age": 40}], lambda c: c["age"]),
            {"name": "barney", "age": 36},
        ),
    ],
)
def test_min_by(case, expected):
    assert _.min_by(*case) == expected


@parametrize(
    "collection,default,expected",
    [([], -1, -1), ([1, 2, 3], -1, 1), ({}, -1, -1), ([], None, None), ({}, None, None)],
)
def test_min_default(collection, default, expected):
    assert _.min_(collection, default=default) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5], 3), [2, 3, 4]),
        (([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3), [2, 3, 4, 5, 6, 7, 8, 9]),
        (([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 4), [2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5]),
    ],
)
def test_moving_mean(case, expected):
    assert _.moving_mean(*case) == expected


@parametrize(
    "multiplier,multiplicand,expected",
    [(10, 5, 50), (None, 1, 1), (None, None, 1), (1.5, 3, 4.5), (-10, 2, -20)],
)
def test_multiply(multiplier, multiplicand, expected):
    assert _.multiply(multiplier, multiplicand) == expected


@parametrize(
    "case,expected",
    [
        ((2, 3), 8),
        ((3, 4), 81),
        (([1, 2, 3, 4, 5], 2), [1, 4, 9, 16, 25]),
        (("junk", 2), None),
    ],
)
def test_power(case, expected):
    assert _.power(*case) == expected


@parametrize(
    "case,expected",
    [
        ((2.51,), 3),
        ((2.499,), 2),
        ((2.499, 2), 2.50),
        (([2.5, 2.499, 2.555], 2), [2.50, 2.50, 2.56]),
        (("junk",), None),
    ],
)
def test_round_(case, expected):
    assert _.round_(*case) == expected


@parametrize(
    "case,expected",
    [
        (([2, 5, 10], 1), [0.2, 0.5, 1]),
        (([1, 2, 5], 1), [0.2, 0.4, 1]),
        (([1, 2, 5], 5), [1, 2, 5]),
        (([1, 2, 5],), [0.2, 0.4, 1]),
    ],
)
def test_scale(case, expected):
    assert _.scale(*case) == expected


@parametrize(
    "case,expected",
    [
        (([0, 0], [5, 5]), 1),
        (([0, 0], [1, 10]), 10),
        (([0, 0], [0, 10]), float("inf")),
        (([0, 0], [10, 0]), 0),
    ],
)
def test_slope(case, expected):
    assert _.slope(*case) == expected


@parametrize(
    "case,expected",
    [
        ([1, 2, 3], (2.0 / 3.0) ** 0.5),
    ],
)
def test_std_deviation(case, expected):
    assert _.std_deviation(case) == expected


@parametrize(
    "minuend,subtrahend,expected",
    [
        (10, 4, 6),
        (-6, -4, -2),
        (4, -10, 14),
        (-10, 4, -14),
        ("10", "5", 5),
        (2, 0.5, 1.5),
        (None, None, 0),
    ],
)
def test_subtract(minuend, subtrahend, expected):
    assert _.subtract(minuend, subtrahend) == expected


@parametrize(
    "minuend,subtrahend",
    [
        ("abs", 4),
        (4, "abc"),
        ("abs", "abc"),
        ([1, 2, 3, 4], 4),
    ],
)
def test_subtract_exception(minuend, subtrahend):
    with pytest.raises(TypeError):
        _.subtract(minuend, subtrahend)


@parametrize(
    "case,expected",
    [
        ([1, 2, 3, 4, 5], 15),
        ([0, 14, 0.2], 14.2),
    ],
)
def test_sum_(case, expected):
    assert _.sum_(case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3, 4, 5], lambda a: a * 2), 30),
        (([{"b": 4}, {"b": 5}, {"b": 6}], "b"), 15),
        (({"one": {"a": 1}, "two": {"a": 2}, "three": {"a": 3}}, "a"), 6),
    ],
)
def test_sum_by(case, expected):
    assert _.sum_by(*case) == expected


@parametrize(
    "case,expected",
    [
        ([[1, 2, 3], [4, 5, 6], [7, 8, 9]], [[1, 4, 7], [2, 5, 8], [3, 6, 9]]),
    ],
)
def test_transpose(case, expected):
    assert _.transpose(case) == expected


@parametrize(
    "case,expected",
    [
        ([1, 2, 3], 2.0 / 3.0),
    ],
)
def test_variance(case, expected):
    assert _.variance(case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3],), [-1.225, 0.0, 1.225]),
        (([{"a": 1}, {"a": 2}, {"a": 3}], "a"), [-1.225, 0.0, 1.225]),
    ],
)
def test_zscore(case, expected):
    assert _.map_(_.zscore(*case), lambda v: round(v, 3)) == expected
