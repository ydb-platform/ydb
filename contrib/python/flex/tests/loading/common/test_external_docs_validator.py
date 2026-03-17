import pytest

from flex.exceptions import ValidationError
from flex.loading.common.external_docs import (
    external_docs_validator,
)


@pytest.mark.parametrize(
    'value',
    (None, True, 1, 1.1, 'abc', [1, 2, 3]),
)
def test_external_docs_type_validation(value, MESSAGES, msg_assertions):
    with pytest.raises(ValidationError) as err:
        external_docs_validator(value)

    msg_assertions.assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'type',
    )


def test_external_docs_type_validation_with_correct_type(msg_assertions):
    try:
        external_docs_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('type', errors)


def test_description_keyword_is_not_required(msg_assertions):
    try:
        external_docs_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('description', errors)


def test_url_keyword_is_required(msg_assertions, MESSAGES):
    with pytest.raises(ValidationError) as err:
        external_docs_validator({})

    msg_assertions.assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
        'url',
    )


def test_url_keyword_must_be_a_uri_formatted(msg_assertions, MESSAGES):
    with pytest.raises(ValidationError) as err:
        external_docs_validator({
            'url': 'not-a-valid-url',
        })

    msg_assertions.assert_message_in_errors(
        MESSAGES['format']['invalid_uri'],
        err.value.detail,
        'url.format',
    )


def test_valid_external_docs_object(msg_assertions):
    external_docs_validator({
        'url': 'http://www.example.com',
        'description': 'A test description',
    })
