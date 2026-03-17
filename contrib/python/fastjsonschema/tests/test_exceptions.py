import pytest

from fastjsonschema import JsonSchemaValueException


@pytest.mark.parametrize('value, expected', [
    ('data', ['data']),
    ('data[0]', ['data', '0']),
    ('data.foo', ['data', 'foo']),
    ('data[1].bar', ['data', '1', 'bar']),
    ('data.foo[2]', ['data', 'foo', '2']),
    ('data.foo.bar[1][2]', ['data', 'foo', 'bar', '1', '2']),
    ('data[1][2].foo.bar', ['data', '1', '2', 'foo', 'bar']),
])
def test_exception_variable_path(value, expected):
    exc = JsonSchemaValueException('msg', name=value)
    assert exc.path == expected


@pytest.mark.parametrize('definition, rule, expected_rule_definition', [
    (None, None, None),
    ({}, None, None),
    ({'type': 'string'}, None, None),
    ({'type': 'string'}, 'unique', None),
    ({'type': 'string'}, 'type', 'string'),
    (None, 'type', None),
])
def test_exception_rule_definition(definition, rule, expected_rule_definition):
    exc = JsonSchemaValueException('msg', definition=definition, rule=rule)
    assert exc.rule_definition == expected_rule_definition
