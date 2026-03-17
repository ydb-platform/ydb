import pytest

from fastjsonschema import JsonSchemaValueException, compile


definition = {
    'type': 'array',
    'items': [
        {
            'type': 'number',
            'maximum': 10,
            'exclusiveMaximum': True,
        },
        {
            'type': 'string',
            'enum': ['hello', 'world'],
        },
        {
            'type': 'array',
            'minItems': 1,
            'maxItems': 3,
            'items': [
                {'type': 'number'},
                {'type': 'string'},
                {'type': 'boolean'},
            ],
        },
        {
            'type': 'object',
            'required': ['a', 'b'],
            'minProperties': 3,
            'properties': {
                'a': {'type': ['null', 'string']},
                'b': {'type': ['null', 'string']},
                'c': {'type': ['null', 'string'], 'default': 'abc'}
            },
            'additionalProperties': {'type': 'string'},
        },
        {'not': {'type': ['null']}},
        {'oneOf': [
            {'type': 'number', 'multipleOf': 3},
            {'type': 'number', 'multipleOf': 5},
        ]},
    ],
}
@pytest.mark.parametrize('value, expected', [
    (
        [9, 'hello', [1, 'a', True], {'a': 'a', 'b': 'b', 'd': 'd'}, 42, 3],
        [9, 'hello', [1, 'a', True], {'a': 'a', 'b': 'b', 'c': 'abc', 'd': 'd'}, 42, 3],
    ),
    (
        [9, 'world', [1], {'a': 'a', 'b': 'b', 'd': 'd'}, 42, 3],
        [9, 'world', [1], {'a': 'a', 'b': 'b', 'c': 'abc', 'd': 'd'}, 42, 3],
    ),
    (
        (9, 'world', (1,), {'a': 'a', 'b': 'b', 'd': 'd'}, 42, 3),
        (9, 'world', (1,), {'a': 'a', 'b': 'b', 'c': 'abc', 'd': 'd'}, 42, 3),
    ),
    (
        [9, 'world', [1], {'a': 'a', 'b': 'b', 'c': 'xy'}, 42, 3],
        [9, 'world', [1], {'a': 'a', 'b': 'b', 'c': 'xy'}, 42, 3],
    ),
    (
        [9, 'world', [1], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5],
        [9, 'world', [1], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5],
    ),
    (
        [9, 'world', [1], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5, 'any'],
        [9, 'world', [1], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5, 'any'],
    ),
    (
        [10, 'world', [1], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5],
        JsonSchemaValueException('data[0] must be smaller than 10', value=10, name='data[0]', definition=definition['items'][0], rule='maximum'),
    ),
    (
        [9, 'xxx', [1], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5],
        JsonSchemaValueException('data[1] must be one of [\'hello\', \'world\']', value='xxx', name='data[1]', definition=definition['items'][1], rule='enum'),
    ),
    (
        [9, 'hello', [], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5],
        JsonSchemaValueException('data[2] must contain at least 1 items', value=[], name='data[2]', definition=definition['items'][2], rule='minItems'),
    ),
    (
        [9, 'hello', [1, 2, 3], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5],
        JsonSchemaValueException('data[2][1] must be string', value=2, name='data[2][1]', definition={'type': 'string'}, rule='type'),
    ),
    (
        [9, 'hello', [1], {'q': 'q', 'x': 'x', 'y': 'y'}, 'str', 5],
        JsonSchemaValueException('data[3] must contain [\'a\', \'b\'] properties', value={'q': 'q', 'x': 'x', 'y': 'y'}, name='data[3]', definition=definition['items'][3], rule='required'),
    ),
    (
        [9, 'hello', [1], {'a': 'a', 'x': 'x', 'y': 'y'}, 'str', 5],
        JsonSchemaValueException('data[3] must contain [\'b\'] properties', value={'a': 'a', 'x': 'x', 'y': 'y'}, name='data[3]', definition=definition['items'][3], rule='required'),
    ),
    (
        [9, 'hello', [1], {}, 'str', 5],
        JsonSchemaValueException('data[3] must contain at least 3 properties', value={}, name='data[3]', definition=definition['items'][3], rule='minProperties'),
    ),
    (
        [9, 'hello', [1], {'a': 'a', 'b': 'b', 'x': 'x'}, None, 5],
        JsonSchemaValueException('data[4] must NOT match a disallowed definition', value=None, name='data[4]', definition=definition['items'][4], rule='not'),
    ),
    (
        [9, 'hello', [1], {'a': 'a', 'b': 'b', 'x': 'x'}, 42, 15],
        JsonSchemaValueException('data[5] must be valid exactly by one definition (2 matches found)', value=15, name='data[5]', definition=definition['items'][5], rule='oneOf'),
    ),
])
def test_integration(asserter, value, expected):
    asserter(definition, value, expected)


def test_any_of_with_patterns(asserter):
    asserter({
        'type': 'object',
        'properties': {
            'hash': {
                'anyOf': [
                    {
                        'type': 'string',
                        'pattern': '^AAA'
                    },
                    {
                        'type': 'string',
                        'pattern': '^BBB'
                    }
                ]
            }
        }
    }, {
        'hash': 'AAAXXX',
    }, {
        'hash': 'AAAXXX',
    })


def test_swap_handlers():
    # Make sure that by swapping resolvers, the schemas do not get cached
    repo1 = {
        "sch://schema": {"type": "array"}
    }
    validator1 = compile({"$ref": "sch://schema"}, handlers={"sch": repo1.__getitem__})
    assert validator1([1, 2, 3]) is not None

    repo2 = {
        "sch://schema": {"type": "string"}
    }
    validator2 = compile({"$ref": "sch://schema"}, handlers={"sch": repo2.__getitem__})
    assert validator2("hello world") is not None
