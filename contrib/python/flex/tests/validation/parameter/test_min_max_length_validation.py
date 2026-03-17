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

from tests.utils import assert_path_in_errors


#
# minimum validation tests
#
@pytest.mark.parametrize(
    'min_length,value',
    (
        (1, ''),
        (2, 'a'),
        (5, '1234'),
    ),
)
def test_minimum_length_validation_with_too_short_values(min_length, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': STRING,
            'required': True,
            'minLength': min_length,
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_path_in_errors(
        'id.minLength',
        err.value.detail,
    )



@pytest.mark.parametrize(
    'min_length,value',
    (
        (1, 'a'),
        (2, 'ab'),
        (2, '12345'),
        (5, '12345-abcde'),
    ),
)
def test_minimum_length_validation_with_valid_lengths(min_length, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': STRING,
            'required': True,
            'minLength': min_length,
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, {})


#
# maximum validation tests
#
@pytest.mark.parametrize(
    'max_length,value',
    (
        (1, 'ab'),
        (5, '123456'),
    ),
)
def test_maximum_length_validation_with_too_long_values(max_length, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': STRING,
            'required': True,
            'maxLength': max_length,
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_path_in_errors(
        'id.maxLength',
        err.value.detail,
    )



@pytest.mark.parametrize(
    'max_length,value',
    (
        (1, 'a'),
        (2, 'ab'),
        (2, '12'),
        (5, '12345'),
    ),
)
def test_maximum_length_validation_with_valid_lengths(max_length, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': STRING,
            'required': True,
            'maxLength': max_length,
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, {})
