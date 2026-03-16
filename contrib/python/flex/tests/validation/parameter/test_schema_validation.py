import pytest

from flex.exceptions import ValidationError
from flex.loading.schema.paths.path_item.operation.parameters import (
    parameters_validator,
)
from flex.validation.parameter import (
    validate_parameters,
)
from flex.constants import (
    BODY,
    STRING,
    INTEGER,
    INT32,
)
from flex.error_messages import MESSAGES

from tests.utils import assert_message_in_errors


@pytest.mark.parametrize(
    'value,error_key,message_key',
    (
        ('not-a-uuid', 'format', 'invalid_uuid'), # not a valid uuid.
        (1234, 'type', 'invalid'), # not a string.
    ),
)
def test_parameter_schema_as_reference_validation_for_invalid_value(value, error_key, message_key):
    context = {
        'definitions': {'UUID': {'type': STRING, 'format': 'uuid'}},
    }
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': BODY,
            'description': 'id',
            'required': True,
            'schema': {'$ref': '#/definitions/UUID'},
        },
    ], context=context)
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, context=context)

    assert_message_in_errors(
        MESSAGES[error_key][message_key],
        err.value.messages,
        'id.{0}'.format(error_key),
    )


@pytest.mark.parametrize(
    'value,error_key,message_key',
    (
        ('not-a-uuid', 'format', 'invalid_uuid'), # not a valid uuid.
        (1234, 'type', 'invalid'), # not a string.
    ),
)
def test_parameter_schema_validation_for_invalid_value(value, error_key, message_key):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': BODY,
            'description': 'id',
            'required': True,
            'schema': {'type': STRING, 'format': 'uuid'},
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, context={})

    assert_message_in_errors(
        MESSAGES[error_key][message_key],
        err.value.messages,
        'id.{0}'.format(error_key),
    )


@pytest.mark.parametrize(
    'value',
    (
        1234,
        5678,
    ),
)
def test_local_parameter_values_override_schema(value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': BODY,
            'description': 'id',
            'required': True,
            'type': INTEGER,
            'format': INT32,
            'schema': {'type': STRING, 'format': 'uuid'},
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, context={})


@pytest.mark.parametrize(
    'value',
    (
        'http://www.example.com',
        'http://google.com',
        'https://facebook.com',
    ),
)
def test_parameter_schema_validation_on_valid_values(value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': BODY,
            'description': 'id',
            'required': True,
            'schema': {
                'type': STRING,
                'format': 'uri',
            },
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, context={})
