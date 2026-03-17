import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_enum_is_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('enum', errors)


@pytest.mark.parametrize(
    'value',
    (None, {'a': 1}, 1, 1.1, 'abc', True),
)
def test_enum_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'enum': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'enum.type',
    )


def test_enum_with_empty_array_is_invalid():
    with pytest.raises(ValidationError) as err:
        schema_validator({'enum': []})

    assert_message_in_errors(
        MESSAGES['min_items']['invalid'],
        err.value.detail,
        'enum.minItems',
    )


def test_enum_with_empty_array_is_invalid():
    try:
        schema_validator({'enum': [1, 2, 3]})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('enum', errors)

