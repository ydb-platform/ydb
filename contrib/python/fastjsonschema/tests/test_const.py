import pytest

from fastjsonschema import JsonSchemaValueException


@pytest.mark.parametrize('value, testError', (
    ('foo', False),
    (42, False),
    (False, False),
    ([1, 2, 3], False),
    ('\'"', False),
    ('foo', True),
    ('\'"', False),
))
def test_const(asserter, value, testError):
    const = expected = value
    if testError:
        const = 'x'
        expected = JsonSchemaValueException('data must be same as const definition: x', value='{data}', name='data', definition='{definition}', rule='const')
    
    asserter({
        '$schema': 'http://json-schema.org/draft-06/schema',
        'const': const,
    }, value, expected)
