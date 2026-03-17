import pytest

from flex.constants import (
    STRING,
    INTEGER,
    NUMBER,
    BOOLEAN,
    ARRAY,
)
from flex.exceptions import (
    ValidationError,
)
from flex.loading.definitions.responses.single.headers.single import (
    single_header_validator,
)


def test_type_is_required(MESSAGES, msg_assertions):
    with pytest.raises(ValidationError) as err:
        single_header_validator({})

    msg_assertions.assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
        'required.type',
    )


@pytest.mark.parametrize(
    'value',
    (None, True, 1, 1.1, {'a': 'b'}),
)
def test_singular_type_type_validation(value, MESSAGES, msg_assertions):
    with pytest.raises(ValidationError) as err:
        single_header_validator({'type': value})

    msg_assertions.assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'type.type',
    )


def test_singular_type_with_unknown_type(msg_assertions, MESSAGES):
    with pytest.raises(ValidationError) as err:
        single_header_validator({'type': 'not-a-known-type'})

    msg_assertions.assert_message_in_errors(
        MESSAGES['enum']['invalid'],
        err.value.detail,
        'type',
    )


@pytest.mark.parametrize(
    'value',
    ([None], [True], [1], [1.1], [{'a': 'b'}]),
)
def test_multi_type_type_validation(value, MESSAGES, msg_assertions):
    with pytest.raises(ValidationError) as err:
        single_header_validator({'type': value})

    msg_assertions.assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'type.type',
    )


def test_multiple_type_with_unknown_type(msg_assertions, MESSAGES):
    with pytest.raises(ValidationError) as err:
        single_header_validator({'type': [STRING, 'not-a-known-type']})

    msg_assertions.assert_message_in_errors(
        MESSAGES['enum']['invalid'],
        err.value.detail,
        'type',
    )


@pytest.mark.parametrize(
    'value',
    (STRING, INTEGER, NUMBER, BOOLEAN, ARRAY),
)
def test_type_with_valid_singular_value(value, msg_assertions):
    try:
        single_header_validator({'type': value})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('type', errors)


@pytest.mark.parametrize(
    'value',
    (
        (STRING, INTEGER),
        (NUMBER, BOOLEAN, ARRAY),
    ),
)
def test_type_with_valid_multi_value(value, msg_assertions):
    try:
        single_header_validator({'type': value})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('type', errors)
