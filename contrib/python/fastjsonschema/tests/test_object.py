import pytest

import fastjsonschema
from fastjsonschema import JsonSchemaDefinitionException, JsonSchemaValueException


exc = JsonSchemaValueException('data must be object', value='{data}', name='data', definition='{definition}', rule='type')
@pytest.mark.parametrize('value, expected', [
    (0, exc),
    (None, exc),
    (True, exc),
    (False, exc),
    ('abc', exc),
    ([], exc),
    ({}, {}),
    ({'x': 1, 'y': True}, {'x': 1, 'y': True}),
])
def test_object(asserter, value, expected):
    asserter({'type': 'object'}, value, expected)


@pytest.mark.parametrize('value, expected', [
    ({}, {}),
    ({'a': 1}, {'a': 1}),
    ({'a': 1, 'b': 2}, JsonSchemaValueException('data must contain less than or equal to 1 properties', value='{data}', name='data', definition='{definition}', rule='maxProperties')),
])
def test_max_properties(asserter, value, expected):
    asserter({
        'type': 'object',
        'maxProperties': 1,
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ({}, JsonSchemaValueException('data must contain at least 1 properties', value='{data}', name='data', definition='{definition}', rule='minProperties')),
    ({'a': 1}, {'a': 1}),
    ({'a': 1, 'b': 2}, {'a': 1, 'b': 2}),
])
def test_min_properties(asserter, value, expected):
    asserter({
        'type': 'object',
        'minProperties': 1,
    }, value, expected)


def make_exc(missing):
    return JsonSchemaValueException('data must contain {} properties'.format(missing), value='{data}', name='data', definition='{definition}', rule='required')
@pytest.mark.parametrize('value, expected', [
    ({}, make_exc(['a', 'b'])),
    ({'a': 1}, make_exc(['b'])),
    ({'a': 1, 'b': 2}, {'a': 1, 'b': 2}),
])
def test_required(asserter, value, expected):
    asserter({
        'type': 'object',
        'required': ['a', 'b'],
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ({}, {}),
    ({'a': 1}, {'a': 1}),
    ({'a': 1, 'b': ''}, {'a': 1, 'b': ''}),
    ({'a': 1, 'b': 2}, JsonSchemaValueException('data.b must be string', value=2, name='data.b', definition={'type': 'string'}, rule='type')),
    ({'a': 1, 'b': '', 'any': True}, {'a': 1, 'b': '', 'any': True}),
])
def test_properties(asserter, value, expected):
    asserter({
        'type': 'object',
        'properties': {
            'a': {'type': 'number'},
            'b': {'type': 'string'},
        },
    }, value, expected)


@pytest.mark.parametrize('definition', (
    {
        'properties': {
            'item': ['wrong'],
        },
    },
    {
        'properties': {
            'x': {
                'type': 'number',
            },
        },
        'required': ['x', 'y'],
        'additionalProperties': False,
    },
    {
        'properties': {
            'x': {
                'type': 'number',
            },
        },
        'required': ['x', 'x'],
    }
))
def test_invalid_properties(asserter, definition):
    with pytest.raises(JsonSchemaDefinitionException):
        fastjsonschema.compile(definition)


@pytest.mark.parametrize('definition', (
    {
        'properties': {
            'x': {
                'type': 'number',
            },
        },
        'patternProperties': {
            '^y': {
                'type': 'number',
            },
        },
        'required': ['x', 'y'],
        'additionalProperties': False,
    },
))
def test_valid_properties(asserter, definition):
    asserter(definition, {'x': 1, 'y': 2}, {'x': 1, 'y': 2})


@pytest.mark.parametrize('value, expected', [
    ({}, {}),
    ({'a': 1}, {'a': 1}),
    ({'a': 1, 'b': ''}, {'a': 1, 'b': ''}),
    ({'a': 1, 'b': 2}, JsonSchemaValueException('data.b must be string', value=2, name='data.b', definition={'type': 'string'}, rule='type')),
    ({'a': 1, 'b': '', 'additional': ''}, {'a': 1, 'b': '', 'additional': ''}),
    ({'a': 1, 'b': '', 'any': True}, JsonSchemaValueException('data.any must be string', value=True, name='data.any', definition={'type': 'string'}, rule='type')),
])
def test_properties_with_additional_properties(asserter, value, expected):
    asserter({
        'type': 'object',
        'properties': {
            'a': {'type': 'number'},
            'b': {'type': 'string'},
        },
        'additionalProperties': {'type': 'string'},
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ({}, {}),
    ({'a': 1}, {'a': 1}),
    ({'a': 1, 'b': ''}, {'a': 1, 'b': ''}),
    ({'a': 1, 'b': 2}, JsonSchemaValueException('data.b must be string', value=2, name='data.b', definition={'type': 'string'}, rule='type')),
    ({'a': 1, 'b': '', 'any': True}, JsonSchemaValueException('data must not contain {\'any\'} properties', value='{data}', name='data', definition='{definition}', rule='additionalProperties')),
    ({'cd': True}, JsonSchemaValueException('data must not contain {\'cd\'} properties', value='{data}', name='data', definition='{definition}', rule='additionalProperties')),
    ({'c_d': True}, {'c_d': True}),
])
def test_properties_without_additional_properties(asserter, value, expected):
    asserter({
        'type': 'object',
        'properties': {
            'a': {'type': 'number'},
            'b': {'type': 'string'},
            'c_d': {'type': 'boolean'},
        },
        'additionalProperties': False,
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ({}, {}),
    ({'a': 1}, {'a': 1}),
    ({'xa': 1}, {'xa': 1}),
    ({'xa': ''}, JsonSchemaValueException('data.xa must be number', value='', name='data.xa', definition={'type': 'number'}, rule='type')),
    ({'xbx': ''}, {'xbx': ''}),
])
def test_pattern_properties(asserter, value, expected):
    asserter({
        'type': 'object',
        'patternProperties': {
            'a': {'type': 'number'},
            'b': {'type': 'string'},
        },
        'additionalProperties': False,
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ({}, {}),
    ({'a': 1}, {'a': 1}),
    ({'b': True}, {'b': True}),
    ({'c': ''}, {'c': ''}),
    ({'d': 1}, JsonSchemaValueException('data.d must be string', value=1, name='data.d', definition={'type': 'string'}, rule='type')),
])
def test_additional_properties(asserter, value, expected):
    asserter({
        'type': 'object',
        "properties": {
            "a": {"type": "number"},
            "b": {"type": "boolean"},
        },
        "additionalProperties": {"type": "string"}
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ({}, {}),
    ({'a': 1}, {'a': 1}),
    ({'b': True}, {'b': True}),
    ({'c': ''}, {'c': ''}),
    ({'d': 1}, {'d': 1}),
    ({'e': {'test': 'x'}}, {'e': {'test': 'x'}}),
    ({'g': [1, 2, 3]}, {'g': [1, 2, 3]}),
    ({'h': False}, {'h': False}),
])
def test_any_additional_properties(asserter, value, expected):
    for obj in (True, {}):
        asserter({
            'type': 'object',
            "properties": {
                "a": {"type": "number"},
                "b": {"type": "boolean"},
            },
            "additionalProperties": obj
        }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ({'id': 1}, {'id': 1}),
    ({'id': 'a'}, JsonSchemaValueException('data.id must be integer', value='a', name='data.id', definition={'type': 'integer'}, rule='type')),
])
def test_object_with_id_property(asserter, value, expected):
    asserter({
        "type": "object",
        "properties": {
            "id": {"type": "integer"}
        }
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ({'$ref': 'ref://to.somewhere'}, {'$ref': 'ref://to.somewhere'}),
    ({'$ref': 1}, JsonSchemaValueException('data.$ref must be string', value=1, name='data.$ref', definition={'type': 'string'}, rule='type')),
])
def test_object_with_ref_property(asserter, value, expected):
    asserter({
        "type": "object",
        "properties": {
            "$ref": {"type": "string"}
        }
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ({}, {}),
    ({'foo': 'foo'}, JsonSchemaValueException('data missing dependency bar for foo', value={'foo': 'foo'}, name='data', definition='{definition}', rule='dependencies')),
    ({'foo': 'foo', 'bar': 'bar'}, {'foo': 'foo', 'bar': 'bar'}),
])
def test_dependencies(asserter, value, expected):
    asserter({
        'type': 'object',
        "properties": {
            "foo": {
                "type": "string"
            },
            "bar": {
                "type": "string"
            }
        },
        "dependencies": {
            "foo": ["bar"],
        },
    }, value, expected)


@pytest.mark.parametrize('value, expected', [
    ({"prop1": {"str": 1}}, JsonSchemaValueException('data.prop1.str must be string', value=1, name='data.prop1.str', definition={'type': 'string'}, rule='type')),
])
def test_full_name_after_ref(asserter, value, expected):
    asserter({
        "definitions": {
            "SomeType": {
                "type": "object",
                "properties": {
                    "str": {"type": "string"},
                },
            },
        },
        "type": "object",
        "properties": {
            "prop1": {"$ref": "#/definitions/SomeType"},
        }
    }, value, expected)
