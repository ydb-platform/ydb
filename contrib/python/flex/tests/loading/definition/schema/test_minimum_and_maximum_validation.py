import pytest

from flex.constants import (
    STRING,
    NULL,
    NUMBER,
    INTEGER,
    OBJECT,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_minimum_and_maximum_are_not_required():
    """
    Ensure that neither the `minimum` nor the `maximum` fields of a schema are
    required.
    """
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('minimum', errors)
    assert_path_not_in_errors('maximum', errors)


@pytest.mark.parametrize(
    'value',
    ('abc', [1, 2], None, {'a': 1}, True, False),
)
def test_minimum_for_invalid_types(value):
    """
    Ensure that the value of `minimum` is validated to be numeric.
    """
    with pytest.raises(ValidationError) as err:
        schema_validator({'minimum': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'minimum.type',
    )


@pytest.mark.parametrize(
    'types',
    (
        INTEGER,
        NUMBER,
        (INTEGER, NUMBER),
        (STRING, OBJECT, INTEGER, NULL),
    ),
)
def test_correct_types_for_minimum(types):
    try:
        schema_validator({
            'minimum': 7,
            'type': types,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('minimum', errors)
    assert_path_not_in_errors('type', errors)


@pytest.mark.parametrize(
    'types',
    (
        STRING,
        (NULL,),
        (OBJECT, STRING),
    ),
)
def test_no_correct_type_for_minimum(types):
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'minimum': 7,
            'type': types,
        })

    assert_message_in_errors(
        MESSAGES['type']['invalid_type_for_minimum'],
        err.value.detail,
        'type',
    )


@pytest.mark.parametrize(
    'value',
    ('abc', [1, 2], None, {'a': 1}, True, False),
)
def test_maximum_for_invalid_types(value):
    """
    Ensure that the value of `maximum` is validated to be numeric.
    """
    with pytest.raises(ValidationError) as err:
        schema_validator({'maximum': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'maximum.type',
    )


@pytest.mark.parametrize(
    'types',
    (
        INTEGER,
        NUMBER,
        (INTEGER, NUMBER),
        (STRING, OBJECT, INTEGER, NULL),
    ),
)
def test_correct_types_for_maximum(types):
    try:
        schema_validator({
            'maximum': 7,
            'type': types,
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('maximum', errors)
    assert_path_not_in_errors('type', errors)


@pytest.mark.parametrize(
    'types',
    (
        STRING,
        (NULL,),
        (OBJECT, STRING),
    ),
)
def test_no_correct_type_for_maximum(types):
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'maximum': 7,
            'type': types,
        })

    assert_message_in_errors(
        MESSAGES['type']['invalid_type_for_maximum'],
        err.value.detail,
        'type',
    )


def test_minimum_is_required_if_exclusive_provided():
    """
    Ensure that when `exclusiveMinimum` is set, that `minimum` is required.
    """
    with pytest.raises(ValidationError) as err:
        schema_validator(
            {'exclusiveMinimum': True},
        )

    assert_message_in_errors(
        MESSAGES['minimum']['exclusive_minimum_required_minimum'],
        err.value.detail,
        'minimum',
    )


def test_maximum_is_required_if_exclusive_provided():
    """
    Ensure that when `exclusiveMaximum` is set, that `maximum` is required.
    """
    with pytest.raises(ValidationError) as err:
        schema_validator(
            {'exclusiveMaximum': True},
        )

    assert_message_in_errors(
        MESSAGES['maximum']['exclusive_maximum_required_maximum'],
        err.value.detail,
        'maximum',
    )


def test_maximum_must_be_greater_than_minimum():
    """
    Test that the maximum value must be greater than or equal to the minimum.
    """
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'maximum': 10,
            'minimum': 11,
        })

    assert_message_in_errors(
        MESSAGES['maximum']['must_be_greater_than_minimum'],
        err.value.detail,
        'maximum',
    )


def test_exclusive_minimum_and_maximum_are_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('exclusiveMinimum', errors)
    assert_path_not_in_errors('exclusiveMaximum', errors)


@pytest.mark.parametrize(
    'value',
    ('abc', [1, 2], None, {'a': 1}, 1, 1.1),
)
def test_exclusive_minimum_for_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'exclusiveMinimum': value,
        })

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'exclusiveMinimum.type',
    )


@pytest.mark.parametrize(
    'value',
    ('abc', [1, 2], None, {'a': 1}, 1, 1.1),
)
def test_exclusive_maximum_for_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({
            'exclusiveMaximum': value,
        })

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'exclusiveMaximum.type',
    )


def test_exclusive_minimum_and_maximum_for_valid_values():
    try:
        schema_validator({'exclusiveMinimum': True, 'exclusiveMaximum': True})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('exclusiveMinimum', errors)
    assert_path_not_in_errors('exclusiveMaximum', errors)
