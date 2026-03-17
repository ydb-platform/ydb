import pytest

from fastjsonschema import JsonSchemaValueException


exc = JsonSchemaValueException('data must be array', value='{data}', name='data', definition='{definition}', rule='type')
@pytest.mark.parametrize('value, expected', [
    (0, exc),
    (None, exc),
    (True, exc),
    (False, exc),
    ('abc', exc),
    ([], []),
    ([1, 'a', True], [1, 'a', True]),
    ({}, exc),
])
def test_array(asserter, value, expected):
    asserter({'type': 'array'}, value, expected)


exc = JsonSchemaValueException('data must contain less than or equal to 1 items', value='{data}', name='data', definition='{definition}', rule='maxItems')
@pytest.mark.parametrize('value, expected', [
    ([], []),
    ([1], [1]),
    ([1, 1], exc),
    ([1, 2, 3], exc),
])
def test_max_items(asserter, value, expected):
    asserter({
        'type': 'array',
        'maxItems': 1,
    }, value, expected)


exc = JsonSchemaValueException('data must contain at least 2 items', value='{data}', name='data', definition='{definition}', rule='minItems')
@pytest.mark.parametrize('value, expected', [
    ([], exc),
    ([1], exc),
    ([1, 1], [1, 1]),
    ([1, 2, 3], [1, 2, 3]),
])
def test_min_items(asserter, value, expected):
    asserter({
        'type': 'array',
        'minItems': 2,
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ([], []),
    ([1], [1]),
    ([1, 1], JsonSchemaValueException('data must contain unique items', value='{data}', name='data', definition='{definition}', rule='uniqueItems')),
    ([1, 2, 3], [1, 2, 3]),
    ([True, False], [True, False]),
    ([True, True], JsonSchemaValueException('data must contain unique items', value='{data}', name='data', definition='{definition}', rule='uniqueItems')),
    (['abc', 'bce', 'hhh'], ['abc', 'bce', 'hhh']),
    (['abc', 'abc'], JsonSchemaValueException('data must contain unique items', value='{data}', name='data', definition='{definition}', rule='uniqueItems')),
    ([{'a': 'a'}, {'b': 'b'}], [{'a': 'a'}, {'b': 'b'}]),
    ([{'a': 'a'}, {'a': 'a'}], JsonSchemaValueException('data must contain unique items', value='{data}', name='data', definition='{definition}', rule='uniqueItems')),
    ([{'a': 'a', 'b': 'b'}, {'b': 'b', 'c': 'c'}], [{'a': 'a', 'b': 'b'}, {'b': 'b', 'c': 'c'}]),
    ([{'a': 'a', 'b': 'b'}, {'b': 'b', 'a': 'a'}], JsonSchemaValueException('data must contain unique items', value='{data}', name='data', definition='{definition}', rule='uniqueItems')),
    ([1, '1'], [1, '1']),
    ([{'a': 'b'}, "{'a': 'b'}"], [{'a': 'b'}, "{'a': 'b'}"]),
    ([[1, 2], [2, 1]], [[1, 2], [2, 1]]),
    ([[1, 2], [1, 2]], JsonSchemaValueException('data must contain unique items', value='{data}', name='data', definition='{definition}', rule='uniqueItems')),
    ([{'a': {'b': {'c': [1, 2]}}}, {'a': {'b': {'c': [1, 2]}}}], JsonSchemaValueException('data must contain unique items', value='{data}', name='data', definition='{definition}', rule='uniqueItems')),
    ([{'a': {'b': {'c': [2, 1]}}}, {'a': {'b': {'c': [1, 2]}}}], [{'a': {'b': {'c': [2, 1]}}}, {'a': {'b': {'c': [1, 2]}}}]),
])
def test_unique_items(asserter, value, expected):
    asserter({
        'type': 'array',
        'uniqueItems': True,
    }, value, expected)


def test_not_unique_items(asserter):
    value = [{'id': 1}, {'id': 1}]
    asserter({
        'type': 'array',
        'uniqueItems': False,
    }, value, value)


def test_min_and_unique_items(asserter):
    value = None
    asserter({
        'type': ['array', 'null'],
        'minItems': 1,
        'uniqueItems': True,
    }, value, value)


@pytest.mark.parametrize('value, expected', [
    ([], []),
    ([1], [1]),
    ([1, 'a'], JsonSchemaValueException('data[1] must be number', value='a', name='data[1]', definition={'type': 'number'}, rule='type')),
])
def test_items_all_same(asserter, value, expected):
    asserter({
        'type': 'array',
        'items': {'type': 'number'},
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ([], []),
    ([1], [1]),
    ([1, 'a'], [1, 'a']),
    ([1, 2], JsonSchemaValueException('data[1] must be string', value=2, name='data[1]', definition={'type': 'string'}, rule='type')),
    ([1, 'a', 2], [1, 'a', 2]),
    ([1, 'a', 'b'], [1, 'a', 'b']),
])
def test_different_items(asserter, value, expected):
    asserter({
        'type': 'array',
        'items': [
            {'type': 'number'},
            {'type': 'string'},
        ],
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ([], []),
    ([1], [1]),
    ([1, 'a'], [1, 'a']),
    ([1, 2], JsonSchemaValueException('data[1] must be string', value=2, name='data[1]', definition={'type': 'string'}, rule='type')),
    ([1, 'a', 2], JsonSchemaValueException('data[2] must be string', value=2, name='data[2]', definition={'type': 'string'}, rule='type')),
    ([1, 'a', 'b'], [1, 'a', 'b']),
])
def test_different_items_with_additional_items(asserter, value, expected):
    asserter({
        'type': 'array',
        'items': [
            {'type': 'number'},
            {'type': 'string'},
        ],
        'additionalItems': {'type': 'string'},
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ([], []),
    ([1], [1]),
    ([1, 'a'], [1, 'a']),
    ([1, 2], JsonSchemaValueException('data[1] must be string', value=2, name='data[1]', definition={'type': 'string'}, rule='type')),
    ([1, 'a', 2], JsonSchemaValueException('data must contain only specified items', value='{data}', name='data', definition='{definition}', rule='items')),
    ([1, 'a', 'b'], JsonSchemaValueException('data must contain only specified items', value='{data}', name='data', definition='{definition}', rule='items')),
])
def test_different_items_without_additional_items(asserter, value, expected):
    asserter({
        'type': 'array',
        'items': [
            {'type': 'number'},
            {'type': 'string'},
        ],
        'additionalItems': False,
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ((), ()),
    (('a',), ('a',)),
    (('a', 'b'), ('a', 'b')),
    (('a', 'b', 3), JsonSchemaValueException('data[2] must be string', value=3, name='data[2]',
                                        definition={'type': 'string'}, rule='type')),
])
def test_tuples_as_arrays(asserter, value, expected):
    asserter({
        '$schema': 'http://json-schema.org/draft-06/schema',
        'type': 'array',
        'items':
            {'type': 'string'},

    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ({'a': [], 'b': ()}, {'a': [], 'b': ()}),
    ({'a': (1, 2), 'b': (3, 4)}, {'a': (1, 2), 'b': (3, 4)}),
])
def test_mixed_arrays(asserter, value, expected):
    asserter({
        'type': 'object',
        'properties': {
            'a': {'type': 'array'},
            'b': {'type': 'array'},
        },
    }, value, expected)


def test_issue_114(asserter):
    """Prevent the faulty scheme to generate an empty for-loop."""
    schema = {
        "type": "object",
        "properties": {
            "a": {
                "type": "array",
                "items": {
                    "b": {
                        "type": "string"
                    }
                }
            }
        }
    }
    value = {"a": []}
    expected = value
    asserter(schema, value, expected)
