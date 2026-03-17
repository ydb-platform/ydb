import pytest

from flex.exceptions import ValidationError
from flex.loading.schema.paths.path_item.operation.parameters import (
    parameters_validator,
)
from flex.validation.parameter import (
    validate_parameters,
)
from flex.constants import (
    QUERY,
    INTEGER,
    ARRAY,
)
from flex.error_messages import MESSAGES

from tests.utils import assert_message_in_errors


def test_parameter_items_validation_on_invalid_array():
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': QUERY,
            'description': 'id',
            'items': {
                'type': INTEGER,
                'minimum': 0,
            },
            'type': ARRAY,
        },
    ])
    value = [1, 2, '3', -1, 4]
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, context={})

    assert_message_in_errors(
        MESSAGES['minimum']['invalid'],
        err.value.detail,
        'id.items.type',
    )
    assert_message_in_errors(
        MESSAGES['minimum']['invalid'],
        err.value.detail,
        'id.items.minimum',
    )


def test_parameter_items_validation_on_valid_value():
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': QUERY,
            'description': 'id',
            'items': {
                'type': INTEGER,
                'minimum': 0,
            },
            'type': ARRAY,
        },
    ])
    value = [1, 2, 3, 4, 5]
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, context={})
