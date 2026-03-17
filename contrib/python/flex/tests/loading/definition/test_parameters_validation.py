import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions import (
    definitions_validator,
)

from tests.factories import (
    ParameterFactory,
)
from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_parameters_definitions_are_not_required():
    context = {'deferred_references': set()}
    try:
        definitions_validator({}, context=context)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'parameters',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    ('abc', 1, 1.1, True, None, ['a', 'b']),
)
def test_parameters_definitions_type_validation_for_invalid_types(value):
    context = {'deferred_references': set()}
    with pytest.raises(ValidationError) as err:
        definitions_validator({'parameters': value}, context=context)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'parameters',
    )


def test_parameters_with_valid_array():
    context = {'deferred_references': set()}
    try:
        definitions_validator({
            'parameters': {},
        }, context=context)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'parameters',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    ('abc', 1, 1.1, True, None, [1, 2]),
)
def test_single_parameter_type_validation(value):
    context = {'deferred_references': set()}

    with pytest.raises(ValidationError) as err:
        definitions_validator({
            'parameters': {
                'SomeParameter': value,
            },
        }, context=context)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'parameters.SomeParameter',
    )


def test_basic_valid_parameter():
    context = {'deferred_references': set()}
    raw_parameter = ParameterFactory()
    try:
        definitions_validator({
            'parameters': {
                'SomeParameter': raw_parameter,
            },
        }, context=context)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'parameters',
        errors,
    )
