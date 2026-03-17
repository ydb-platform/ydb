import pytest

from fastjsonschema import JsonSchemaValueException


exc = JsonSchemaValueException('data must be one of [1, 2, \'a\', "b\'c"]', value='{data}', name='data', definition='{definition}', rule='enum')
@pytest.mark.parametrize('value, expected', [
    (1, 1),
    (2, 2),
    (12, exc),
    ('a', 'a'),
    ('aa', exc),
])
def test_enum(asserter, value, expected):
    asserter({'enum': [1, 2, 'a', "b'c"]}, value, expected)


exc = JsonSchemaValueException('data must be string or number', value='{data}', name='data', definition='{definition}', rule='type')
@pytest.mark.parametrize('value, expected', [
    (0, 0),
    (None, exc),
    (True, exc),
    ('abc', 'abc'),
    ([], exc),
    ({}, exc),
])
def test_types(asserter, value, expected):
    asserter({'type': ['string', 'number']}, value, expected)


@pytest.mark.parametrize('value, expected', [
    ('qwert', 'qwert'),
    ('qwertz', JsonSchemaValueException('data must be shorter than or equal to 5 characters', value='{data}', name='data', definition={'maxLength': 5}, rule='maxLength')),
])
def test_all_of(asserter, value, expected):
    asserter({'allOf': [
        {'type': 'string'},
        {'maxLength': 5},
    ]}, value, expected)


exc = JsonSchemaValueException('data cannot be validated by any definition', value='{data}', name='data', definition='{definition}', rule='anyOf')
@pytest.mark.parametrize('value, expected', [
    (0, 0),
    (None, exc),
    (True, exc),
    ('abc', 'abc'),
    ([], exc),
    ({}, exc),
])
def test_any_of(asserter, value, expected):
    asserter({'anyOf': [
        {'type': 'string'},
        {'type': 'number'},
    ]}, value, expected)


def exc(n):
    suffix = " ({} matches found)".format(n)
    return JsonSchemaValueException('data must be valid exactly by one definition' + suffix, value='{data}', name='data', definition='{definition}', rule='oneOf')

@pytest.mark.parametrize('value, expected', [
    (0, exc(2)),
    (2, exc(0)),
    (9, 9),
    (10, 10),
    (15, exc(2)),
])
def test_one_of(asserter, value, expected):
    asserter({'oneOf': [
        {'type': 'number', 'multipleOf': 5},
        {'type': 'number', 'multipleOf': 3},
    ]}, value, expected)


@pytest.mark.parametrize('value, expected', [
    (0, exc(2)),
    (2, exc(0)),
    (9, 9),
    (10, 10),
    (15, exc(2)),
])
def test_one_of_factorized(asserter, value, expected):
    asserter({
        'type': 'number',
        'oneOf': [
            {'multipleOf': 5},
            {'multipleOf': 3},
        ],
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    (0, JsonSchemaValueException('data must NOT match a disallowed definition', value='{data}', name='data', definition='{definition}', rule='not')),
    (True, True),
    ('abc', 'abc'),
    ([], []),
    ({}, {}),
])
def test_not(asserter, value, expected):
    asserter({'not': {'type': 'number'}}, value, expected)
