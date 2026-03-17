import pytest

from flex.constants import (
    STRING,
    INTEGER,
    NUMBER,
    OBJECT,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_multiple_of_is_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('multipleOf', errors)


@pytest.mark.parametrize(
    'value',
    ('abc', [1, 2], None, {'a': 1}),
)
def test_multiple_of_for_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'multipleOf': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'multipleOf.type',
    )


@pytest.mark.parametrize(
    'type_',
    (
        OBJECT,
        (OBJECT, STRING),
    ),
)
def test_type_validation_for_multiple_of_for_invalid_types(type_):
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'multipleOf': 5,
            'type': type_,
        })

    assert_message_in_errors(
        MESSAGES['type']['invalid_type_for_multiple_of'],
        err.value.detail,
        'type',
    )


@pytest.mark.parametrize(
    'type_',
    (
        NUMBER,
        INTEGER,
        (INTEGER, NUMBER),
        (STRING, NUMBER, OBJECT),
    ),
)
def test_type_validation_for_multiple_of_for_valid_types(type_):
    try:
        schema_validator({
            'multipleOf': 5,
            'type': type_,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('type', errors)


@pytest.mark.parametrize(
    'value',
    (-1, -1.5),
)
def test_multiple_of_invalid_for_negative_numbers(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'multipleOf': value})

    assert_message_in_errors(
        MESSAGES['minimum']['invalid'],
        err.value.detail,
        'multipleOf.minimum',
    )


@pytest.mark.parametrize(
    'value',
    (1, 1.5),
)
def test_multiple_for_valid_values(value):
    try:
        schema_validator({'multipleOf': value})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('multipleOf', errors)
