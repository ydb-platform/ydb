import pytest
import os

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
    NUMBER,
    BOOLEAN,
    FLEX_DISABLE_X_NULLABLE
)
from flex.error_messages import MESSAGES

from tests.utils import assert_message_in_errors


#
# enum validation tests
#
@pytest.mark.parametrize(
    'enum,value',
    (
        ([True, False], 0),
        ([True, False], 1),
        ([True, False], ''),
        ([True, False], None),
        ([0, 1, 2, 3], True),
        ([0, 1, 2, 3], False),
        ([0, 1, 2, 3], '1'),
        ([0, 1, 2, 3], 4),
        ([0, 1, 2, 3], 1.0),
        ([0, 1, 2, 3], None),
        (['1', '2', 'a', 'b'], 'A'),
        (['1', '2', 'a', 'b'], 1),
        (['1', '2', 'a', 'b'], 2),
        (['1', '2', 'a', 'b'], None),
    ),
)
def test_enum_validation_with_invalid_values(enum, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description': 'id',
            'type': [STRING, NUMBER, BOOLEAN],
            'required': True,
            'enum': enum,
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_message_in_errors(
        MESSAGES['enum']['invalid'],
        err.value.detail,
        'id.enum',
    )


@pytest.mark.parametrize(
    'enum,value',
    (
        ([True, False], True),
        ([True, False], False),
        ([0, 1, 2, 3], 3),
        ([0, 1, 2, 3], 1),
        (['1', '2', 'a', 'b'], 'a'),
        (['1', '2', 'a', 'b'], '1'),
    ),
)
def test_enum_validation_with_allowed_values(enum, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description': 'id',
            'type': [STRING, NUMBER, BOOLEAN],
            'required': True,
            'enum': enum,
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, {})


@pytest.mark.parametrize(
    'enum,value',
    (
        ([True, False], True),
        ([True, False], None),
        ([0, 1, 2, 3], 1),
        ([0, 1, 2, 3], None),
        (['1', '2', 'a', 'b'], 'a'),
        (['1', '2', 'a', 'b'], None),
    ),
)
def test_nullable_enum_validation_with_allowed_values(enum, value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description': 'id',
            'type': [STRING, NUMBER, BOOLEAN],
            'required': True,
            'enum': enum,
            'x-nullable': True
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, {})


@pytest.mark.parametrize(
    'enum,value',
    (
        ([True, False], None),
        ([0, 1, 2, 3], None),
        (['1', '2', 'a', 'b'], None),
    ),
)
def test_nullable_enum_with_null_values_strict(enum, value, monkeypatch):

    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description': 'id',
            'type': [STRING, NUMBER, BOOLEAN],
            'required': True,
            'enum': enum,
            'x-nullable': True
        },
    ])
    parameter_values = {
        'id': value,
    }

    monkeypatch.setattr(os, 'environ', {FLEX_DISABLE_X_NULLABLE: '1'})
    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_message_in_errors(
        MESSAGES['enum']['invalid'],
        err.value.detail,
        'id.enum',
    )


@pytest.mark.parametrize(
    'enum,value',
    (
        ([True, False], 0),
        ([True, False], 1),
        ([True, False], ''),
        ([0, 1, 2, 3], True),
        ([0, 1, 2, 3], False),
        ([0, 1, 2, 3], '1'),
        ([0, 1, 2, 3], 4),
        ([0, 1, 2, 3], 1.0),
        (['1', '2', 'a', 'b'], 'A'),
        (['1', '2', 'a', 'b'], 1),
        (['1', '2', 'a', 'b'], 2),
    ),
)
def test_nullable_enum_with_invalid_values(enum, value):

    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description': 'id',
            'type': [STRING, NUMBER, BOOLEAN],
            'required': True,
            'enum': enum,
            'x-nullable': True
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_message_in_errors(
        MESSAGES['enum']['invalid'],
        err.value.detail,
        'id.enum',
    )
