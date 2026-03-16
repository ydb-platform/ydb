import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_format_is_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('format', errors)


@pytest.mark.parametrize(
    'value',
    ([1, 2], None, {'a': 1}, True, 1, 1.1),
)
def test_format_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'format': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'format.type',
    )


def test_format_for_valid_registered_format():
    try:
        schema_validator({'format': 'uuid'})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('format', errors)


def test_format_for_valid_unregistered_format():
    try:
        schema_validator({'format': 'not-a-registered-format'})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('format', errors)
