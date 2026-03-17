import pytest

from flex.exceptions import ValidationError
from flex.loading.schema.paths.path_item.operation.parameters import (
    parameters_validator,
)
from flex.validation.parameter import (
    validate_parameters,
)
from flex.constants import (
    PATH,
    INTEGER,
    NUMBER,
    STRING,
)
from flex.error_messages import MESSAGES

from tests.utils import assert_message_in_errors


@pytest.mark.parametrize(
    'type_,value',
    (
        (INTEGER, 1.0),
        (INTEGER, '1234'),
        (NUMBER, '1234'),
        (STRING, 1234),
    ),
)
def test_parameter_validation_enforces_type(type_, value):
    parameters = parameters_validator([
        {'name': 'id', 'in': PATH, 'description': 'id', 'type': type_, 'required': True},
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'id.type',
    )


@pytest.mark.parametrize(
    'type_,value',
    (
        (INTEGER, 1),
        (INTEGER, 0),
        (INTEGER, 1234),
        (NUMBER, 1),
        (NUMBER, 0),
        (NUMBER, 1234),
        (NUMBER, 1.5),
        (NUMBER, 0.5),
        (NUMBER, 12.34),
        (STRING, '1234'),
        (STRING, 'abcd'),
    ),
)
def test_parameter_validation_with_correct_type(type_, value):
    parameters = parameters_validator([
        {'name': 'id', 'in': PATH, 'description': 'id', 'type': type_, 'required': True},
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, {})
