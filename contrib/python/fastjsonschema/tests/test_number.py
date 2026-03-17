import decimal

import pytest

from fastjsonschema import JsonSchemaValueException


@pytest.fixture(params=['number', 'integer'])
def number_type(request):
    return request.param


exc = JsonSchemaValueException('data must be {number_type}')
@pytest.mark.parametrize('value, expected', [
    (-5, -5),
    (0, 0),
    (5, 5),
    (None, exc),
    (True, exc),
    ('abc', exc),
    ([], exc),
    ({}, exc),
])
def test_number(asserter, number_type, value, expected):
    if isinstance(expected, JsonSchemaValueException):
        expected = JsonSchemaValueException(expected.message.format(number_type=number_type), value='{data}', name='data', definition='{definition}', rule='type')
    asserter({'type': number_type}, value, expected)


exc = JsonSchemaValueException('data must be smaller than or equal to 10', value='{data}', name='data', definition='{definition}', rule='maximum')
@pytest.mark.parametrize('value, expected', [
    (-5, -5),
    (5, 5),
    (9, 9),
    (10, 10),
    (11, exc),
    (20, exc),
])
def test_maximum(asserter, number_type, value, expected):
    asserter({
        'type': number_type,
        'maximum': 10,
    }, value, expected)


exc = JsonSchemaValueException('data must be smaller than 10', value='{data}', name='data', definition='{definition}', rule='maximum')
@pytest.mark.parametrize('value, expected', [
    (-5, -5),
    (5, 5),
    (9, 9),
    (10, exc),
    (11, exc),
    (20, exc),
])
def test_exclusive_maximum(asserter, number_type, value, expected):
    asserter({
        'type': number_type,
        'maximum': 10,
        'exclusiveMaximum': True,
    }, value, expected)


exc = JsonSchemaValueException('data must be bigger than or equal to 10', value='{data}', name='data', definition='{definition}', rule='minimum')
@pytest.mark.parametrize('value, expected', [
    (-5, exc),
    (9, exc),
    (10, 10),
    (11, 11),
    (20, 20),
])
def test_minimum(asserter, number_type, value, expected):
    asserter({
        'type': number_type,
        'minimum': 10,
    }, value, expected)


exc = JsonSchemaValueException('data must be bigger than 10', value='{data}', name='data', definition='{definition}', rule='minimum')
@pytest.mark.parametrize('value, expected', [
    (-5, exc),
    (9, exc),
    (10, exc),
    (11, 11),
    (20, 20),
])
def test_exclusive_minimum(asserter, number_type, value, expected):
    asserter({
        'type': number_type,
        'minimum': 10,
        'exclusiveMinimum': True,
    }, value, expected)


exc = JsonSchemaValueException('data must be multiple of 3', value='{data}', name='data', definition='{definition}', rule='multipleOf')
@pytest.mark.parametrize('value, expected', [
    (-4, exc),
    (-3, -3),
    (-2, exc),
    (-1, exc),
    (0, 0),
    (1, exc),
    (2, exc),
    (3, 3),
    (4, exc),
    (5, exc),
    (6, 6),
    (7, exc),
])
def test_multiple_of(asserter, number_type, value, expected):
    asserter({
        'type': number_type,
        'multipleOf': 3,
    }, value, expected)


exc = JsonSchemaValueException('data must be multiple of 0.0001', value='{data}', name='data', definition='{definition}', rule='multipleOf')
@pytest.mark.parametrize('value, expected', [
    (0.00751, exc),
    (0.0075, 0.0075),
])
def test_multiple_of_float(asserter, value, expected):
    asserter({
        'type': 'number',
        'multipleOf': 0.0001,
    }, value, expected)


exc = JsonSchemaValueException('data must be multiple of 0.01', value='{data}', name='data', definition='{definition}', rule='multipleOf')
@pytest.mark.parametrize('value, expected', [
    (0, 0),
    (0.01, 0.01),
    (0.1, 0.1),
    (19.01, 19.01),
    (0.001, exc),
    (19.001, exc),
])
def test_multiple_of_float_1_5(asserter, value, expected):
    asserter({
        'type': 'number',
        'multipleOf': 0.01,
    }, value, expected)


@pytest.mark.parametrize('value', (
    1.0,
    0.1,
    0.01,
    0.001,
))
def test_integer_is_not_number(asserter, value):
    asserter({
        'type': 'integer',
    }, value, JsonSchemaValueException('data must be integer', value='{data}', name='data', definition='{definition}', rule='type'))


@pytest.mark.parametrize('value', (
    1.0,
    0.1,
    0.01,
    0.001,
))
def test_number_allows_float(asserter, value):
    asserter({
        'type': 'number',
    }, value, value)



@pytest.mark.parametrize('value', (
    decimal.Decimal('1.0'),
    decimal.Decimal('0.1'),
    decimal.Decimal('0.01'),
    decimal.Decimal('0.001'),
))
def test_number_allows_decimal(asserter, value):
    asserter({
        'type': 'number',
    }, value, value)
