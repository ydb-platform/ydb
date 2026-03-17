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


#
# minimum validation tests
#
@pytest.mark.parametrize(
    'minimum,value',
    (
        (0, -1),
        (-5, -6),
        (10, 9.999),
    ),
)
def test_minimum_validation_for_invalid_values(minimum, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': NUMBER,
            'required': True,
            'minimum': minimum,
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_message_in_errors(
        MESSAGES['minimum']['invalid'],
        err.value.detail,
        'id.minimum',
    )


@pytest.mark.parametrize(
    'minimum,value',
    (
        (0, -1),
        (0, 0),
        (-5, -6),
        (-5, -5),
        (10, 9.999),
        (10, 10),
    ),
)
def test_exclusive_minimum_validation_for_invalid_values(minimum, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': NUMBER,
            'required': True,
            'minimum': minimum,
            'exclusiveMinimum': True,
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_message_in_errors(
        MESSAGES['minimum']['invalid'],
        err.value.detail,
        'id.minimum',
    )


@pytest.mark.parametrize(
    'minimum,value',
    (
        (0, 0),
        (0, 1),
        (-5, -5),
        (-5, 5),
        (10, 15),
    ),
)
def test_minimum_validation_for_valid_values(minimum, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': NUMBER,
            'required': True,
            'minimum': minimum,
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, {})


@pytest.mark.parametrize(
    'minimum,value',
    (
        (-1, 1),
        (0, 0.00001),
        (10, 10.0001),
    ),
)
def test_exclusive_minimum_validation_for_valid_values(minimum, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': NUMBER,
            'required': True,
            'minimum': minimum,
            'exclusiveMinimum': True,
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
    'maximum,value',
    (
        (5, 6),
        (-5, -4),
        (10, 10.0001),
    ),
)
def test_maximum_validation_for_invalid_values(maximum, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': NUMBER,
            'required': True,
            'maximum': maximum,
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_message_in_errors(
        MESSAGES['maximum']['invalid'],
        err.value.detail,
        'id.maximum',
    )


@pytest.mark.parametrize(
    'maximum,value',
    (
        (5, 6),
        (5, 5),
        (-5, -4),
        (-5, -5),
        (10, 10.0001),
        (10, 10),
    ),
)
def test_exclusive_maximum_validation_for_invalid_values(maximum, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': NUMBER,
            'required': True,
            'maximum': maximum,
            'exclusiveMaximum': True,
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_message_in_errors(
        MESSAGES['maximum']['invalid'],
        err.value.detail,
        'id.maximum',
    )


@pytest.mark.parametrize(
    'maximum,value',
    (
        (0, 0),
        (1, 0),
        (-5, -5),
        (-5, -6),
        (10, 9),
    ),
)
def test_maximum_validation_for_valid_values(maximum, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': NUMBER,
            'required': True,
            'maximum': maximum,
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, {})


@pytest.mark.parametrize(
    'maximum,value',
    (
        (-1, -1.00001),
        (0, -0.000001),
        (10, 9),
    ),
)
def test_exclusive_maximum_validation_for_valid_values(maximum, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': NUMBER,
            'required': True,
            'maximum': maximum,
            'exclusiveMaximum': True,
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, {})
