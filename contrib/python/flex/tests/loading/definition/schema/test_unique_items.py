import pytest

from flex.constants import (
    ARRAY,
    STRING,
    INTEGER,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_unique_items_are_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('uniqueItems', errors)


@pytest.mark.parametrize(
    'value',
    ([1, 2], None, {'a': 1}, 1, 1.1, 'abc'),
)
def test_unique_items_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'uniqueItems': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'uniqueItems.type',
    )


@pytest.mark.parametrize(
    'type_',
    (
        STRING,
        (INTEGER, STRING),
    ),
)
def test_type_validation_for_unique_items_with_invalid_types(type_):
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'uniqueItems': True,
            'type': type_,
        })

    assert_message_in_errors(
        MESSAGES['type']['invalid_type_for_unique_items'],
        err.value.detail,
        'type',
    )


@pytest.mark.parametrize(
    'type_',
    (
        ARRAY,
        (INTEGER, ARRAY, STRING),
    ),
)
def test_type_validation_for_unique_items_with_valid_types(type_):
    try:
        schema_validator({
            'uniqueItems': True,
            'type': type_,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('type', errors)


def test_unique_items_with_valid_value():
    try:
        schema_validator({'uniqueItems': True})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('uniqueItems', errors)
