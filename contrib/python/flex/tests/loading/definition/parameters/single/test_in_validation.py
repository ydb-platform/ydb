import pytest

from flex.constants import (
    PARAMETER_IN_VALUES,
    PATH,
    BODY,
    QUERY,
    HEADER,
    FORM_DATA,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.parameters import (
    single_parameter_validator,
)

from tests.factories import ParameterFactory
from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_name_is_required():
    context = {'deferred_references': set()}
    with pytest.raises(ValidationError) as err:
        single_parameter_validator({}, context=context)

    assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
        'name',
    )


@pytest.mark.parametrize(
    'value',
    ([1, 2], None, {'a': 1}, True, 1, 1.1),
)
def test_in_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        single_parameter_validator({'in': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'in.type',
    )


def test_in_must_be_one_of_valid_values():
    with pytest.raises(ValidationError) as err:
        single_parameter_validator({'in': 'not-a-valid-in-value'})

    assert_message_in_errors(
        MESSAGES['enum']['invalid'],
        err.value.detail,
        'in.enum',
    )


@pytest.mark.parametrize(
    'value',
    PARAMETER_IN_VALUES,
)
def test_in_with_valid_values(value):
    try:
        single_parameter_validator({'in': value})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'in.enum',
        errors,
    )


def test_when_in_value_is_path_required_must_be_true():
    with pytest.raises(ValidationError) as err:
        single_parameter_validator(ParameterFactory(**{
            'in': PATH,
            'required': False,
        }))

    assert_message_in_errors(
        MESSAGES['required']['path_parameters_must_be_required'],
        err.value.detail,
        '^required',
    )


def test_when_in_value_is_body_a_schema_is_required():
    parameter = ParameterFactory(**{
        'in': BODY,
    })
    parameter.pop('schema', None)
    with pytest.raises(ValidationError) as err:
        single_parameter_validator(parameter)

    assert_message_in_errors(
        MESSAGES['schema']['body_parameters_must_include_a_schema'],
        err.value.detail,
        '^schema',
    )


@pytest.mark.parametrize(
    'in_',
    (QUERY, PATH, HEADER, FORM_DATA),
)
def test_when_in_value_is_not_body_type_is_required(in_):
    parameter = ParameterFactory(**{
        'in': in_,
    })
    parameter.pop('type', None)
    with pytest.raises(ValidationError) as err:
        single_parameter_validator(parameter)

    assert_message_in_errors(
        MESSAGES['type']['non_body_parameters_must_declare_a_type'],
        err.value.detail,
        '^type',
    )
