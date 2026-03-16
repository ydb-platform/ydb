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
    STRING,
)
from flex.error_messages import MESSAGES

from tests.utils import assert_message_in_errors


#
# pattern validation tests
#
@pytest.mark.parametrize(
    'pattern,value',
    (
        ("(?P<id>[0-9]+)", 'abcd'),  # Non digit in group
        ("[0-9]+", 'abcd'),  # Non digit not in group
    ),
)
def test_pattern_validation_with_invalid_values(pattern, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': STRING,
            'required': True,
            'pattern': pattern,
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_message_in_errors(
        MESSAGES['pattern']['invalid'],
        err.value.detail,
        'id.pattern',
    )


@pytest.mark.parametrize(
    'pattern,value',
    (
        ("(?P<id>[0-9]+)", '1234'),  # Non digit in group
        ("[0-9]+", '1234'),  # Non digit not in group
    ),
)
def test_pattern_validation_with_matching_values(pattern, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': STRING,
            'required': True,
            'pattern': pattern,
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, {})
