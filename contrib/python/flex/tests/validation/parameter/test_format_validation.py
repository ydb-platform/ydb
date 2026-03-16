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
    UUID,
    DATE,
    DATETIME,
)
from flex.error_messages import MESSAGES

from tests.utils import assert_message_in_errors


@pytest.mark.parametrize(
    'format_,value,error_key',
    (
        (UUID, 'not-a-real-uuid', 'invalid_uuid'),
        (DATE, '2017-13-02', 'invalid_date'),  # Invalid month 13
        (DATETIME, '2011-13-18T10:29:47+03:00', 'invalid_datetime'),  # Invalid month 13
    )
)
def test_parameter_format_validation_on_invalid_values(format_, value, error_key):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description': 'id',
            'type': STRING,
            'required': True,
            'format': format_,
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_message_in_errors(
        MESSAGES['format'][error_key],
        err.value.detail,
        'id.format',
    )


@pytest.mark.parametrize(
    'format_,value',
    (
        (UUID, '536aa369-5b29-4367-b5a4-2696565f4e8a'),
        (DATE, '2017-02-03'),
        (DATETIME, '2011-12-18T10:29:47+03:00'),
    ),
)
def test_parameter_format_validation_succeeds_on_valid_values(format_, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description': 'id',
            'type': STRING,
            'required': True,
            'format': format_,
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, {})
