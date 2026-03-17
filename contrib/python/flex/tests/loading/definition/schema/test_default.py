import pytest

from flex.constants import (
    STRING,
    INTEGER,
    BOOLEAN,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
    assert_message_not_in_errors,
)


def test_default_is_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('default', errors)


def test_default_invalid_if_not_of_single_type():
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'type': STRING,
            'default': True,
        })

    assert_message_in_errors(
        MESSAGES['default']['invalid_type'],
        err.value.detail,
        'default',
    )


def test_default_invalid_if_not_of_multiple_types():
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'type': [STRING, INTEGER],
            'default': True,
        })

    assert_message_in_errors(
        MESSAGES['default']['invalid_type'],
        err.value.detail,
        'default',
    )


def test_default_valid_for_single_type():
    try:
        schema_validator({
            'type': BOOLEAN,
            'default': True,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('default', errors)
    assert_message_not_in_errors(MESSAGES['default']['invalid_type'], errors)


def test_default_valid_for_multiple_types():
    try:
        schema_validator({
            'type': [STRING, BOOLEAN, INTEGER],
            'default': True,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('default', errors)
    assert_message_not_in_errors(MESSAGES['default']['invalid_type'], errors)
