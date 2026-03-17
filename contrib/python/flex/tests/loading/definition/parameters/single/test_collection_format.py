import pytest

from flex.constants import (
    CSV,
    PATH,
    HEADER,
    QUERY,
    FORM_DATA,
    BODY,
    MULTI,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.parameters import (
    single_parameter_validator,
)

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
    assert_message_not_in_errors,
)
from tests.factories import ParameterFactory


def test_collection_format_is_not_required():
    try:
        single_parameter_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'collectionFormat',
        errors,
    )


def test_collection_format_defaults_to_csv():
    raw_parameter = ParameterFactory()
    raw_parameter.pop('collectionFormat', None)
    value = single_parameter_validator(raw_parameter)

    with pytest.raises(AssertionError):
        # TODO: how do we set a default in the return object.
        assert value.get('collectionFormat') == CSV


@pytest.mark.parametrize(
    'value',
    ([1, 2], None, {'a': 1}, True, 1, 1.1),
)
def test_collection_format_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        single_parameter_validator({'collectionFormat': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'collectionFormat.type',
    )


def test_collection_format_with_invalid_value():
    with pytest.raises(ValidationError) as err:
        single_parameter_validator({'collectionFormat': 'not-a-collection-format'})

    assert_message_in_errors(
        MESSAGES['enum']['invalid'],
        err.value.detail,
        'collectionFormat.enum',
    )


def test_collection_format_with_valid_values():
    try:
        single_parameter_validator({'collectionFormat': CSV})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'collectionFormat',
        errors,
    )


@pytest.mark.parametrize(
    'in_',
    (BODY, PATH, HEADER),
)
def test_multi_format_invalid_in_values(in_):
    parameter = ParameterFactory(**{
        'collectionFormat': MULTI,
        'in': in_,
    })
    with pytest.raises(ValidationError) as err:
        single_parameter_validator(parameter)

    assert_message_in_errors(
        MESSAGES['collection_format']['invalid_based_on_in_value'],
        err.value.detail,
        'collectionFormat',
    )


@pytest.mark.parametrize(
    'in_',
    (QUERY, FORM_DATA),
)
def test_multi_format_valid_in_values(in_):
    parameter = ParameterFactory(**{
        'collectionFormat': MULTI,
        'in': in_,
    })
    try:
        single_parameter_validator(parameter)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_message_not_in_errors(
        MESSAGES['collection_format']['invalid_based_on_in_value'],
        errors,
    )
