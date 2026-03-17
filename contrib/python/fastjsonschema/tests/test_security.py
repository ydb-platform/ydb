import pytest

from fastjsonschema import JsonSchemaDefinitionException, compile


@pytest.mark.parametrize('schema', [
    {'type': 'validate(10)'},
    {'enum': 'validate(10)'},
    {'minLength': 'validate(10)'},
    {'maxLength': 'validate(10)'},
    {'minimum': 'validate(10)'},
    {'maximum': 'validate(10)'},
    {'multipleOf': 'validate(10)'},
    {'minItems': 'validate(10)'},
    {'maxItems': 'validate(10)'},
    {'minProperties': 'validate(10)'},
    {'maxProperties': 'validate(10)'},
    {'required': 'validate(10)'},
    {'exclusiveMinimum': 'validate(10)'},
    {'exclusiveMaximum': 'validate(10)'},
])
def test_not_generate_code_from_definition(schema):
    with pytest.raises(JsonSchemaDefinitionException):
        compile({
            '$schema': 'http://json-schema.org/draft-07/schema',
            **schema
        })


@pytest.mark.parametrize('schema,value', [
    ({'const': 'validate(10)'}, 'validate(10)'),
    ({'pattern': '" + validate("10") + "'}, '"  validate"10"  "'),
    ({'pattern': "' + validate('10') + '"}, '\'  validate\'10\'  \''),
    ({'pattern': "' + validate(\"10\") + '"}, '\'  validate"10"  \''),
    ({'properties': {
        'validate(10)': {'type': 'string'},
    }}, {'validate(10)': '10'}),
    ({'patternProperties': {
        'validate(10)': {'type': 'string'},
    }}, {'validate(10)': '10'}),
])
def test_generate_code_with_proper_variable_names(asserter, schema, value):
    asserter({
        '$schema': 'http://json-schema.org/draft-07/schema',
        **schema
    }, value, value)


def test_generate_code_without_overriding_variables(asserter):
    # We use variable name by property name. In the code is automatically generated
    # FOO_keys which could colide with keys parameter. Then the variable is reused and
    # for example additionalProperties feature is not working well. We need to make
    # sure the name not colide.
    value = {
        'keys': [1, 2, 3],
    }
    asserter({
        'type': 'object',
        'properties': {
            'keys': {'type': 'array'},
        },
        'additionalProperties': False,
    }, value, value)
