import pytest

from flex.constants import (
    ARRAY,
    INTEGER,
    STRING,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_min_and_max_items_are_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('minItems', errors)
    assert_path_not_in_errors('maxItems', errors)


@pytest.mark.parametrize(
    'value',
    ([1, 2], None, {'a': 1}, True, 1.1, 'abc'),
)
def test_min_items_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'minItems': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'minItems.type',
    )


@pytest.mark.parametrize(
    'type_',
    (
        INTEGER,
        (INTEGER, STRING),
    ),
)
def test_type_validations_for_min_items_with_invalid_types(type_):
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'minItems': 5,
            'type': type_,
        })

    assert_message_in_errors(
        MESSAGES['type']['invalid_type_for_min_items'],
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
def test_type_validations_for_min_items_with_valid_types(type_):
    try:
        schema_validator({
            'minItems': 5,
            'type': type_,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('type', errors)


@pytest.mark.parametrize(
    'value',
    ([1, 2], None, {'a': 1}, True, 1.1, 'abc'),
)
def test_max_items_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'maxItems': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'maxItems.type',
    )


@pytest.mark.parametrize(
    'type_',
    (
        INTEGER,
        (INTEGER, STRING),
    ),
)
def test_type_validations_for_max_items_with_invalid_types(type_):
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'maxItems': 5,
            'type': type_,
        })

    assert_message_in_errors(
        MESSAGES['type']['invalid_type_for_max_items'],
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
def test_type_validations_for_max_items_with_valid_types(type_):
    try:
        schema_validator({
            'maxItems': 5,
            'type': type_,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('type', errors)


def test_max_items_must_be_gte_min_items():
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'minItems': 5,
            'maxItems': 4,
        })

    assert_message_in_errors(
        MESSAGES['max_items']['must_be_greater_than_min_items'],
        err.value.detail,
        'maxItems',
    )


def test_min_and_max_items_with_valid_values():
    try:
        schema_validator({
            'minItems': 4,
            'maxItems': 6,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('minItems', errors)
    assert_path_not_in_errors('maxItems', errors)
