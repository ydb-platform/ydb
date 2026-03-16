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
    NUMBER,
)
from flex.error_messages import MESSAGES

from tests.utils import assert_message_in_errors


@pytest.mark.parametrize(
    'divisor,value',
    (
        (3, 4),
        (2, 1),
        (0.19, 1.2),
        (7, 15),
    ),
)
def test_multiple_of_validation_for_invalid_values(divisor, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': NUMBER,
            'required': True,
            'multipleOf': divisor,
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_message_in_errors(
        MESSAGES['multiple_of']['invalid'],
        err.value.detail,
        'id.multipleOf',
    )


@pytest.mark.parametrize(
    'divisor,value',
    (
        (0.2, 1),
        (2, 10),
        (0.1, 10),
    ),
)
def test_multiple_of_validation_for_valid_multiples(divisor, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': NUMBER,
            'required': True,
            'multipleOf': divisor,
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, {})
