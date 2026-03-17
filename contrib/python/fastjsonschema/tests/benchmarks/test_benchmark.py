import pytest

import fastjsonschema


JSON_SCHEMA = {
    'type': 'array',
    'items': [
        {
            'type': 'number',
            'exclusiveMaximum': 10,
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


fastjsonschema_validate = fastjsonschema.compile(JSON_SCHEMA)


@pytest.mark.benchmark(min_rounds=20)
@pytest.mark.parametrize('value', (
    [9, 'hello', [1, 'a', True], {'a': 'a', 'b': 'b', 'd': 'd'}, 42, 3],
    [9, 'world', [1, 'a', True], {'a': 'a', 'b': 'b', 'd': 'd'}, 42, 3],
    [9, 'world', [1, 'a', True], {'a': 'a', 'b': 'b', 'c': 'xy'}, 42, 3],
    [9, 'world', [1, 'a', True], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5],
))
def test_benchmark_ok_values(benchmark, value):
    @benchmark
    def f():
        fastjsonschema_validate(value)


@pytest.mark.benchmark(min_rounds=20)
@pytest.mark.parametrize('value', (
    [10, 'world', [1, 'a', True], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5],
    [9, 'xxx', [1, 'a', True], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5],
    [9, 'hello', [], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5],
    [9, 'hello', [1, 2, 3], {'a': 'a', 'b': 'b', 'c': 'xy'}, 'str', 5],
    [9, 'hello', [1, 'a', True], {'a': 'a', 'x': 'x', 'y': 'y'}, 'str', 5],
    [9, 'hello', [1, 'a', True], {}, 'str', 5],
    [9, 'hello', [1, 'a', True], {'a': 'a', 'b': 'b', 'x': 'x'}, None, 5],
    [9, 'hello', [1, 'a', True], {'a': 'a', 'b': 'b', 'x': 'x'}, 42, 15],
))
def test_benchmark_bad_values(benchmark, value):
    @benchmark
    def f():
        try:
            fastjsonschema_validate(value)
        except fastjsonschema.JsonSchemaValueException:
            pass
        else:
            pytest.fail('Exception is not raised')
