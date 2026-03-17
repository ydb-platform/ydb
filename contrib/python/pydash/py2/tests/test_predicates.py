# -*- coding: utf-8 -*-

import datetime
import decimal
import operator
import re

import pydash as _

from . import fixtures
from .fixtures import parametrize


@parametrize(
    "value,other,expected",
    [("a", "a", True), (None, None, True), (None, "", False), (1, str(1), False)],
)
def test_eq(value, other, expected):
    assert _.eq(value, other) == expected


@parametrize(
    "case,expected",
    [
        ((2, 1), True),
        ((2.5, 2.3), True),
        ((1, 2), False),
        ((2.3, 2.5), False),
        ((1, 1), False),
    ],
)
def test_gt(case, expected):
    assert _.gt(*case) == expected


@parametrize(
    "case,expected",
    [
        ((2, 1), True),
        ((2.5, 2.3), True),
        ((1, 2), False),
        ((2.3, 2.5), False),
        ((1, 1), True),
    ],
)
def test_gte(case, expected):
    assert _.gte(*case) == expected


@parametrize(
    "case,expected",
    [
        ((2, 1), False),
        ((2.5, 2.3), False),
        ((1, 2), True),
        ((2.3, 2.5), True),
        ((1, 1), False),
    ],
)
def test_lt(case, expected):
    assert _.lt(*case) == expected


@parametrize(
    "case,expected",
    [
        ((2, 1), False),
        ((2.5, 2.3), False),
        ((1, 2), True),
        ((2.3, 2.5), True),
        ((1, 1), True),
    ],
)
def test_lte(case, expected):
    assert _.lte(*case) == expected


@parametrize(
    "case,expected",
    [
        ((3, 2, 4), True),
        ((4, 8), True),
        ((4, 2), False),
        ((2, 2), False),
        ((1.2, 2), True),
        ((5.2, 4), False),
        (("", 5), False),
        ((2, ""), False),
        ((-1, -2, ""), True),
    ],
)
def test_in_range(case, expected):
    assert _.in_range(*case) == expected


@parametrize(
    "case,expected",
    [
        ([], True),
        ({}, True),
        ("", True),
        (0, False),
        (True, False),
    ],
)
def test_is_associative(case, expected):
    assert _.is_associative(case) == expected


@parametrize(
    "case,expected",
    [
        ("", True),
        ("\n", True),
        (" ", True),
        ("a", False),
    ],
)
def test_is_blank(case, expected):
    assert _.is_blank(case) == expected


@parametrize(
    "case,expected",
    [
        (True, True),
        (False, True),
        (0, False),
        ("", False),
    ],
)
def test_is_boolean(case, expected):
    assert _.is_boolean(case) == expected


@parametrize(
    "case,expected",
    [
        (list, True),
        (dict, True),
        ([].append, True),
        ({}.update, True),
        (1, True),
        (True, True),
        (None, True),
        (datetime, False),
        (parametrize, False),
        ("a", False),
        (lambda: None, False),
    ],
)
def test_is_builtin(case, expected):
    assert _.is_builtin(case) == expected


@parametrize(
    "case,expected",
    [
        (datetime.date.today(), True),
        (datetime.datetime.today(), True),
        ("2014-01-01", False),
        ("2014-01-01T00:00:00", False),
        (1, False),
    ],
)
def test_is_date(case, expected):
    assert _.is_date(case) == expected


@parametrize(
    "case,expected",
    [
        ([3, 2, 1], True),
        ([6, 5, 5], True),
        (0, True),
        ([5, 4, 4, 3, 1], True),
        ([5, 4, 4, 5, 4, 3], False),
    ],
)
def test_is_decreasing(case, expected):
    assert _.is_decreasing(case) == expected


@parametrize(
    "case,expected",
    [({}, True), ([], False), (1, False), ("a", False), (iter([]), False), (iter({}), False)],
)
def test_is_dict(case, expected):
    assert _.is_dict(case) == expected


@parametrize(
    "case,expected",
    [
        (True, True),
        (0, True),
        (123.45, True),
        ("", True),
        ({}, True),
        ([], True),
        (False, True),
        (None, True),
        ({"a": 1}, False),
        ([1], False),
        ("Hello", False),
        (["Hello", "World"], False),
    ],
)
def test_is_empty(case, expected):
    assert _.is_empty(case) == expected


@parametrize(
    "case,expected",
    [
        ((1, 1), True),
        ((1, 2), False),
        (("1", "1"), True),
        (("1", "2"), False),
        (([1], {"a": 1}), False),
        (({"a": 1}, {"a": 1}), True),
        (({"a": 1}, {"b": 1}), False),
        (([1, 2, 3], [1, 2, 3]), True),
        (([1, 2, 3], [1, 2]), False),
        (([1, 2], [1, 2, 3]), False),
        (([1, 2, 3], [1, 2]), False),
        (([1, 2], [1, 2, 3]), False),
        ((["hello", "goodbye"], ["hi", "goodbye"]), False),
    ],
)
def test_is_equal(case, expected):
    assert _.is_equal(*case) == expected


@parametrize(
    "case,expected",
    [
        ((1, 1, None), True),
        ((1, 2, None), False),
        (("1", "1", None), True),
        (("1", "2", None), False),
        (([1], {"a": 1}, None), False),
        (([1], {"a": 1}, lambda a, b: True), True),
        (({"a": 1}, {"a": 1}, None), True),
        (({"a": 1}, {"b": 1}, lambda a, b: None if isinstance(a, dict) else True), False),
        (([1, 2, 3], [1, 2, 3], None), True),
        (([1, 2, 3], [1, 2], None), False),
        (([1, 2], [1, 2, 3], None), False),
        (([1, 2, 3], [1, 2], lambda a, b: None if isinstance(a, list) else True), False),
        (([1, 2], [1, 2, 3], lambda a, b: None if isinstance(a, list) else True), False),
        ((["hello", "goodbye"], ["hi", "goodbye"], fixtures.is_equal_iteratee0), True),
    ],
)
def test_is_equal_with(case, expected):
    assert _.is_equal_with(*case) == expected


@parametrize("case,expected", [(Exception(), True), ({}, False), ([], False)])
def test_is_error(case, expected):
    assert _.is_error(case) == expected


@parametrize(
    "case,expected",
    [
        (2, True),
        (16, True),
        (1, False),
        (3.0, False),
        (3.5, False),
        (None, False),
    ],
)
def test_is_even(case, expected):
    assert _.is_even(case) == expected


@parametrize(
    "case,expected",
    [
        (1.0, True),
        (3.245, True),
        (1, False),
        (True, False),
        ("", False),
    ],
)
def test_is_float(case, expected):
    assert _.is_float(case) == expected


@parametrize(
    "case,expected",
    [
        (lambda x: x + 1, True),
        ("Hello, world!", False),
    ],
)
def test_is_function(case, expected):
    assert _.is_function(case) == expected


@parametrize(
    "case,expected",
    [
        ([1, 2, 3], True),
        ([5, 5, 6], True),
        (0, True),
        ([1, 2, 3, 4, 4, 5], True),
        ([1, 2, 3, 4, 4, 3], False),
    ],
)
def test_is_increasing(case, expected):
    assert _.is_increasing(case) == expected


@parametrize(
    "case,expected",
    [
        ([], True),
        ("", True),
        ((), True),
        ({}, False),
        (1, False),
        (None, False),
    ],
)
def test_is_indexed(case, expected):
    assert _.is_indexed(case) == expected


@parametrize(
    "case,expected",
    [
        ((1, int), True),
        ((1.0, float), True),
        (([], (list, str)), True),
        (([], dict), False),
        ((True, float), False),
    ],
)
def test_is_instance_of(case, expected):
    assert _.is_instance_of(*case) == expected


@parametrize(
    "case,expected",
    [
        (1, True),
        (2, True),
        (1.0, False),
        (True, False),
        (None, False),
        ([], False),
    ],
)
def test_is_integer(case, expected):
    assert _.is_integer(case) == expected


@parametrize(
    "case,expected",
    [
        ([], True),
        ({}, True),
        ((), True),
        ("a", True),
        (5, False),
        (None, False),
    ],
)
def test_is_iterable(case, expected):
    assert _.is_iterable(case) == expected


@parametrize(
    "case,expected",
    [
        ('{"one": 1, "two": {"three": "3"}, "four": [4]}', True),
        ({"one": 1, "two": {"three": "3"}, "four": [4]}, False),
        ("", False),
        (1, False),
        (True, False),
    ],
)
def test_is_json(case, expected):
    assert _.is_json(case) == expected


@parametrize(
    "case,expected",
    [
        ([1, 2, 3], True),
        ({}, False),
    ],
)
def test_is_list(case, expected):
    assert _.is_list(case) == expected


@parametrize(
    "case,expected",
    [
        (({"name": "fred", "age": 40}, {"age": 40}), True),
        (({"name": "fred", "age": 40}, {"age": 40, "active": True}), False),
        (([1, 2, 3], [1, 2]), True),
        (([1, 2, 3], [1, 2, 3, 4]), False),
        (({}, {}), True),
        (({"a": 1}, {}), True),
        (([], []), True),
        (([1], []), True),
    ],
)
def test_is_match(case, expected):
    assert _.is_match(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2], [2, 4], lambda a, b: None if isinstance(a, list) else b == a + a), True),
        (([1, 2], [2, 4], lambda a, b, key: a == b == key), False),
        (([0, 1], [0, 1], lambda a, b, key: a == b == key), True),
    ],
)
def test_is_match_with(case, expected):
    assert _.is_match_with(*case) == expected


@parametrize(
    "case,expected",
    [
        (([1, 2, 3], operator.le), True),
        (([3, 2, 1], operator.ge), True),
        (([1, 1, 2], operator.lt), False),
        (([3, 3, 2], operator.gt), False),
    ],
)
def test_is_monotone(case, expected):
    assert _.is_monotone(*case) == expected


@parametrize(
    "case,expected",
    [
        (0, False),
        (123456789123456789123456789, False),
        (123.45, False),
        (decimal.Decimal(1), False),
        ("1", True),
    ],
)
def test_is_nan(case, expected):
    assert _.is_nan(case) == expected


@parametrize(
    "case,expected",
    [
        (-1, True),
        (-1.25, True),
        (-0.1, True),
        (0, False),
        (1, False),
        (True, False),
        (False, False),
    ],
)
def test_is_negative(case, expected):
    assert _.is_negative(case) == expected


@parametrize(
    "case,expected",
    [
        (None, True),
        (0, False),
    ],
)
def test_is_none(case, expected):
    assert _.is_none(case) == expected


@parametrize(
    "case,expected",
    [
        (0, True),
        (123456789123456789123456789, True),
        (123.45, True),
        (decimal.Decimal(1), True),
        ("1", False),
        (True, False),
        (False, False),
    ],
)
def test_is_number(case, expected):
    assert _.is_number(case) == expected


@parametrize(
    "case,expected",
    [({}, True), ([], True), (1, False), ("a", False), (iter([]), False), (iter({}), False)],
)
def test_is_object(case, expected):
    assert _.is_object(case) == expected


@parametrize(
    "case,expected",
    [
        (1, True),
        (3.0, True),
        (3.5, True),
        (2, False),
        (16, False),
        (None, False),
    ],
)
def test_is_odd(case, expected):
    assert _.is_odd(case) == expected


@parametrize(
    "case,expected",
    [(1, True), (1.25, True), (0.1, True), (-1, False), (0, False), (True, False), (False, False)],
)
def test_is_positive(case, expected):
    assert _.is_positive(case) == expected


@parametrize(
    "case,expected",
    [
        (re.compile(""), True),
        ("", False),
        ("Hello, world!", False),
        (1, False),
        ({}, False),
        ([], False),
        (None, False),
    ],
)
def test_is_reg_exp(case, expected):
    assert _.is_reg_exp(case) == expected


@parametrize("case,expected", [(set(), True), ([1, 2, 3], False)])
def test_is_set(case, expected):
    assert _.is_set(case) == expected


@parametrize(
    "case,expected",
    [
        ([1, 2, 3], False),
        ([3, 2, 1], True),
        ([1, 1, 2], False),
        ([3, 3, 2], False),
    ],
)
def test_is_strictly_decreasing(case, expected):
    assert _.is_strictly_decreasing(case) == expected


@parametrize(
    "case,expected",
    [
        ([1, 2, 3], True),
        ([3, 2, 1], False),
        ([1, 1, 2], False),
        ([3, 3, 2], False),
    ],
)
def test_is_strictly_increasing(case, expected):
    assert _.is_strictly_increasing(case) == expected


@parametrize(
    "case,expected",
    [("", True), ("Hello, world!", True), (1, False), ({}, False), ([], False), (None, False)],
)
def test_is_string(case, expected):
    assert _.is_string(case) == expected


@parametrize(
    "case,expected",
    [
        ((), True),
        ([], False),
        ({}, False),
    ],
)
def test_is_tuple(case, expected):
    assert _.is_tuple(case) == expected


@parametrize(
    "case,expected",
    [
        (0, True),
        (0.0, False),
        ("", False),
        (True, False),
        (False, False),
    ],
)
def test_is_zero(case, expected):
    assert _.is_zero(case) == expected
