from __future__ import unicode_literals

import six
import pytest
import collections
import operator
import random

from flex.utils import (
    is_non_string_iterable,
    is_value_of_type,
    format_errors,
    get_type_for_value,
    cast_value_to_type,
    is_any_string_type,
    deep_equal,
)
from flex.functional import chain_reduce_partial
from flex.constants import (
    NULL,
    BOOLEAN,
    INTEGER,
    NUMBER,
    STRING,
    ARRAY,
    OBJECT,
)


#
# is_non_string_iterable
#
def test_list():
    assert is_non_string_iterable([])


def test_dict():
    assert is_non_string_iterable({})


def test_tuple():
    assert is_non_string_iterable(tuple())


def test_not_string():
    assert not is_non_string_iterable('is-a-string')


def test_not_bytes():
    if six.PY2:
        assert not is_non_string_iterable(six.binary_type('is-a-string'))
    else:
        assert not is_non_string_iterable(six.binary_type('is-a-string', encoding='utf-8'))


#
# is_value_of_type
#
def test_null_type():
    assert is_value_of_type(None, NULL)


def test_non_null_type():
    assert not is_value_of_type(False, NULL)
    assert not is_value_of_type('', NULL)
    assert not is_value_of_type([], NULL)


def test_boolean_type():
    assert is_value_of_type(True, BOOLEAN)
    assert is_value_of_type(False, BOOLEAN)


def test_non_boolean_types():
    assert not is_value_of_type(None, BOOLEAN)
    assert not is_value_of_type('1', BOOLEAN)
    assert not is_value_of_type(1, BOOLEAN)
    assert not is_value_of_type('0', BOOLEAN)
    assert not is_value_of_type(0, BOOLEAN)
    assert not is_value_of_type([], BOOLEAN)


def test_integer_type():
    assert is_value_of_type(0, INTEGER)
    assert is_value_of_type(-1, INTEGER)
    assert is_value_of_type(1, INTEGER)


def test_non_integer_type():
    assert not is_value_of_type(1.0, INTEGER)
    assert not is_value_of_type(True, INTEGER)
    assert not is_value_of_type(False, INTEGER)


def test_number_type():
    assert is_value_of_type(0, NUMBER)
    assert is_value_of_type(1, NUMBER)
    assert is_value_of_type(1.0, NUMBER)


def test_non_number_types():
    assert not is_value_of_type('0', NUMBER)
    assert not is_value_of_type(True, NUMBER)
    assert not is_value_of_type([], NUMBER)


def test_string_type():
    assert is_value_of_type('', STRING)
    if six.PY2:
        assert is_value_of_type(six.binary_type('string'), STRING)
    else:
        assert is_value_of_type(six.binary_type('string', encoding='utf-8'), STRING)
    assert is_value_of_type(six.text_type('string'), STRING)


def test_non_string_type():
    assert not is_value_of_type(1, STRING)
    assert not is_value_of_type(True, STRING)
    assert not is_value_of_type(None, STRING)
    assert not is_value_of_type([], STRING)


def test_array_type():
    assert is_value_of_type([], ARRAY)
    assert is_value_of_type(tuple(), ARRAY)


def test_non_array_types():
    assert not is_value_of_type({}, ARRAY)
    assert not is_value_of_type(1, ARRAY)
    assert not is_value_of_type('1234', ARRAY)


def test_object_types():
    assert is_value_of_type({}, OBJECT)


def test_non_object_types():
    assert not is_value_of_type([], OBJECT)
    assert not is_value_of_type(tuple(), OBJECT)


#
# format_errors tests
#
def test_string():
    messages = list(map(
        operator.methodcaller('replace', "u'", "'"),
        format_errors("error"),
    ))

    assert messages == ["'error'"]


def test_short_iterable():
    input_ = ["error-a", "error-b"]
    expected = [
        "0. 'error-a'",
        "1. 'error-b'",
    ]
    actual = list(map(
        operator.methodcaller('replace', "u'", "'"),
        format_errors(input_),
    ))

    assert set(actual) == set(expected)


def test_mapping_with_string_values():
    input_ = {
        'foo': 'bar',
        'bar': 'baz',
    }
    expected = [
        "'foo': 'bar'",
        "'bar': 'baz'",
    ]
    actual = list(map(
        operator.methodcaller('replace', "u'", "'"),
        format_errors(input_),
    ))

    assert set(actual) == set(expected)


def test_mapping_with_iterables():
    input_ = {
        'foo': ['bar', 'baz'],
        'bar': ['baz', 'foo'],
    }
    expected = [
        "'foo':",
        "    0. 'bar'",
        "    1. 'baz'",
        "'bar':",
        "    0. 'baz'",
        "    1. 'foo'",
    ]
    actual = list(map(
        operator.methodcaller('replace', "u'", "'"),
        format_errors(input_),
    ))

    assert set(actual) == set(expected)


def test_mapping_with_mappings():
    input_ = {
        'foo': {
            'bar': 'error-a',
            'baz': 'error-b',
        },
        'bar': {
            'baz': 'error-c',
            'foo': 'error-d',
        }
    }
    expected = [
        "'foo':",
        "    - 'bar': 'error-a'",
        "    - 'baz': 'error-b'",
        "'bar':",
        "    - 'baz': 'error-c'",
        "    - 'foo': 'error-d'",
    ]
    actual = list(map(
        operator.methodcaller('replace', "u'", "'"),
        format_errors(input_),
    ))

    assert set(actual) == set(expected)


def test_iterable_of_mappings():
    input_ = [
        {'foo': 'bar'},
        {'bar': ['baz', 'foo']},
    ]
    expected = [
        "0. 'foo': 'bar'",
        "1. 'bar':",
        "    0. 'baz'",
        "    1. 'foo'",
    ]
    actual = list(map(
        operator.methodcaller('replace', "u'", "'"),
        format_errors(input_),
    ))

    assert set(actual) == set(expected)


#
# get_type_for_value tests
#
@pytest.mark.parametrize(
    'value',
    (None,),
)
def test_get_type_for_null(value):
    assert get_type_for_value(value) == NULL


@pytest.mark.parametrize(
    'value',
    (True, False),
)
def test_get_type_for_boolean(value):
    assert get_type_for_value(value) == BOOLEAN


@pytest.mark.parametrize(
    'value',
    (0, 1, 2, -2),
)
def test_get_type_for_interger(value):
    assert get_type_for_value(value) == INTEGER


@pytest.mark.parametrize(
    'value',
    (0.0, 1.0, 2.0, -2.0),
)
def test_get_type_for_number(value):
    assert get_type_for_value(value) == NUMBER


@pytest.mark.parametrize(
    'value',
    ('', 'a', 'abc'),
)
def test_get_type_for_string(value):
    assert get_type_for_value(value) == STRING


@pytest.mark.parametrize(
    'value',
    (
        [],
        [1, 2, 3],
        [True, None, 'a', 4],
    ),
)
def test_get_type_for_array(value):
    assert get_type_for_value(value) == ARRAY


@pytest.mark.parametrize(
    'value',
    (
        {},
        {'a': 1},
        {'b': 2, 'c': 3},
    ),
)
def test_get_type_for_object(value):
    assert get_type_for_value(value) == OBJECT


@pytest.mark.parametrize(
    'value,type_,expected',
    (
        ('5', NUMBER, 5),
        ('8', INTEGER, 8),
        ('2.3', NUMBER, 2.3),
        (15, STRING, '15'),
        (12.5, STRING, '12.5'),
        (True, NUMBER, 1),
        (True, STRING, 'True'),
        (False, NUMBER, 0),
        (False, STRING, 'False'),
        ('1', BOOLEAN, True),
        ('true', BOOLEAN, True),
        ('True', BOOLEAN, True),
        ('', BOOLEAN, False),
        ('false', BOOLEAN, False),
        ('False', BOOLEAN, False),
        ('0', BOOLEAN, False),
        ('abc', ARRAY, ['a', 'b', 'c']),
        (collections.OrderedDict((('a', 1), ('b', 2))), ARRAY, ['a', 'b']),
    )
)
def test_casting_appropriate_values_to_type(value, type_, expected):
    assert cast_value_to_type(value, type_) == expected


@pytest.mark.parametrize(
    'value,type_',
    (
        ([], NUMBER),
        ('abc', NUMBER),
        (1, NULL),
        ('1.6', INTEGER),
        ('abc', NULL),
        ([], NULL),
        ({}, NULL),
        (True, NULL),
        (['a', 'b'], OBJECT),
    )
)
def test_casting_inappropriate_values_to_type(value, type_):
    with pytest.raises((TypeError, ValueError)):
        cast_value_to_type(value, type_)


#
# chain_reduce_partial tests
#
def test_chain_reduce_partial():
    def fn_a(v):
        return v * 3

    def fn_b(v):
        return v * 5

    fn_c = chain_reduce_partial(fn_a, fn_b)

    for _ in range(100):
        v = random.randint(-1000000, 1000000)
        assert fn_c(v) == fn_b(fn_a(v))


#
# is_any_string_type tests
#
def test_binary_type():
    assert is_any_string_type(six.binary_type(b'test'))


def test_text_type():
    assert is_any_string_type(six.text_type('test'))


def test_not_a_string_at_all():
    assert not is_any_string_type(1)


#
# deep_equal tests
#
@pytest.mark.parametrize(
    'left,right',
    (
        (1, True),
        (1.0, True),
        (1, 1.0),
        (0, False),
    )
)
def test_deep_equal_for_things_that_should_not_be_equal(left, right):
    assert not deep_equal(left, right)


@pytest.mark.parametrize(
    'left,right',
    (
        (six.binary_type(b'test'), six.text_type('test')),
    )
)
def test_deep_equal_for_things_that_should_be_equal(left, right):
    assert deep_equal(left, right)
