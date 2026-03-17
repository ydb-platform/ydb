import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_required_is_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('required', errors)


@pytest.mark.parametrize(
    'value',
    (True, False, None, {'a': 1}, 'abc', 1, 1.1),
)
def test_required_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'required': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'required.type',
    )


def test_required_with_invalid_sub_types():
    with pytest.raises(ValidationError) as err:
        schema_validator({'required': ['field-A', True, 'Field-B']})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'required.type',
    )


def test_required_for_valid_required():
    try:
        schema_validator({'required': ['field-A', 'field-B']})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('required', errors)
