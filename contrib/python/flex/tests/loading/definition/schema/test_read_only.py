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


def test_read_only_is_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('readOnly', errors)


@pytest.mark.parametrize(
    'value',
    (None, {'a': 1}, 1, 1.1, 'abc'),
)
def test_read_only_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'readOnly': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'readOnly.type',
    )


def test_read_only_with_valid_value():
    try:
        schema_validator({'readOnly': True})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('readOnly', errors)
