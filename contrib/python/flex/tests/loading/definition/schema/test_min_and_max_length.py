import pytest

from flex.constants import (
    INTEGER,
    NUMBER,
    STRING,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_min_and_max_length_are_not_required():
    """
    Ensure that neither the `minLength` nor the `maxLength` fields of a schema are
    required.
    """
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('minLength', errors)
    assert_path_not_in_errors('maxLength', errors)


@pytest.mark.parametrize(
    'value',
    ('abc', [1, 2], None, {'a': 1}, True),
)
def test_min_length_for_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'minLength': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'minLength.type',
    )


@pytest.mark.parametrize(
    'type_',
    (
        INTEGER,
        (INTEGER, NUMBER),
    ),
)
def test_type_validation_for_min_length_for_invalid_types(type_):
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'minLength': 5,
            'type': type_,
        })

    assert_message_in_errors(
        MESSAGES['type']['invalid_type_for_min_length'],
        err.value.detail,
        'type',
    )


@pytest.mark.parametrize(
    'type_',
    (
        STRING,
        (INTEGER, STRING),
    ),
)
def test_type_validation_for_min_length_for_valid_types(type_):
    try:
        schema_validator({
            'minLength': 5,
            'type': type_,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'type',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    ('abc', [1, 2], None, {'a': 1}, True),
)
def test_max_length_for_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'maxLength': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'maxLength.type',
    )


@pytest.mark.parametrize(
    'type_',
    (
        INTEGER,
        (INTEGER, NUMBER),
    ),
)
def test_type_validation_for_max_length_for_invalid_types(type_):
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'maxLength': 5,
            'type': type_,
        })

    assert_message_in_errors(
        MESSAGES['type']['invalid_type_for_max_length'],
        err.value.detail,
        'type',
    )


@pytest.mark.parametrize(
    'type_',
    (
        STRING,
        (INTEGER, STRING),
    ),
)
def test_type_validation_for_max_length_for_valid_types(type_):
    try:
        schema_validator({
            'maxLength': 5,
            'type': type_,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'type',
        errors,
    )


def test_max_length_must_be_greater_than_or_equal_to_min_length():
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'maxLength': 8,
            'minLength': 9,
        })

    assert_message_in_errors(
        MESSAGES['max_length']['must_be_greater_than_min_length'],
        err.value.detail,
        'maxLength',
    )


def test_min_length_must_be_positive():
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'minLength': -1,
        })

    assert_message_in_errors(
        MESSAGES['minimum']['invalid'],
        err.value.detail,
        'minLength.minimum',
    )


def test_max_length_must_be_greater_than_0():
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'maxLength': 0,
        })

    assert_message_in_errors(
        MESSAGES['minimum']['invalid'],
        err.value.detail,
        'maxLength.minimum',
    )


def test_min_and_max_length_with_valid_values():
    try:
        schema_validator({
            'minLength': 8,
            'maxLength': 10,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('minLength', errors)
    assert_path_not_in_errors('maxLength', errors)
