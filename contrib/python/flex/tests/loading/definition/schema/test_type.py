import pytest

from flex.constants import (
    STRING,
    BOOLEAN,
    INTEGER,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_type_is_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('type', errors)


@pytest.mark.parametrize(
    'value',
    (None, {'a': 1}, True, 1, 1.1),
)
def test_type_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'type': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'type.type',
    )


def test_type_as_valid_array_of_types():
    try:
        schema_validator({
            'type': [STRING, INTEGER, BOOLEAN],
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('type', errors)


def test_type_with_invalid_multiple_types():
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'type': [STRING, 'not-a-valid-type', INTEGER, BOOLEAN],
        })

    assert_message_in_errors(
        MESSAGES['enum']['invalid'],
        err.value.detail,
        'type.enum',
    )


def test_type_as_valid_singular_type():
    try:
        schema_validator({
            'type': BOOLEAN,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('type', errors)


def test_type_with_invalid_single_type():
    with pytest.raises(ValidationError) as err:
        schema_validator({'type': 'not-a-valid-type'})

    assert_message_in_errors(
        MESSAGES['enum']['invalid'],
        err.value.detail,
        'type.enum',
    )
