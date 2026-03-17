import pytest

from fastjsonschema import JsonSchemaValueException


BASE_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-07/schema',
    "if": {
        "const": 1
    },
}


@pytest.mark.parametrize('value, expected', [
    (1, 1),
    (2, 2),
])
def test_boolean_schema_true_in_then(asserter, value, expected):
    asserter({
        **BASE_SCHEMA,
        'then': True,
        'else': {
            'type': 'number'
        },
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    (1, JsonSchemaValueException(
        'data must not be there', value=1, name='data',
        definition=False, rule=None
    )),
    (2, 2),
])
def test_boolean_schema_false_in_then(asserter, value, expected):
    asserter({
        **BASE_SCHEMA,
        'then': False,
        'else': {
            'type': 'number'
        },
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    (1, 1),
    (2, 2),
])
def test_boolean_schema_true_in_else(asserter, value, expected):
    asserter({
        **BASE_SCHEMA,
        'then': {
            'type': 'number',
        },
        'else': True,
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    (1, 1),
    (2, JsonSchemaValueException(
        'data must not be there', value=2, name='data',
        definition=False, rule=None
    )),
])
def test_boolean_schema_false_in_else(asserter, value, expected):
    asserter({
        **BASE_SCHEMA,
        'then': {
            'type': 'number',
        },
        'else': False,
    }, value, expected)
