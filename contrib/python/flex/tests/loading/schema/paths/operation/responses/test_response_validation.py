import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import (
    ValidationError,
)
from flex.loading.schema.paths.path_item.operation.responses import (
    responses_validator,
)


def test_description_is_required(msg_assertions):
    with pytest.raises(ValidationError) as err:
        responses_validator({
            200: {},
        })

    msg_assertions.assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
        '200.required.description',
    )


def test_response_as_reference_missing_description(msg_assertions):
    responses = {
        200: {
            '$ref': '#/responses/SomeResponse'
        },
    }
    context = {
        'responses': {
            'SomeResponse': {},
        },
    }
    try:
        responses_validator(responses, context=context)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('200', errors)


def test_with_description(msg_assertions):
    try:
        responses_validator({
            200: {'description': 'A Description'},
        })
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('200', errors)


def test_with_description_in_reference(msg_assertions):
    responses = {
        200: {'$ref': '#/responses/SomeResponse'},
    }
    context = {
        'responses': {
            'SomeResponse': {'description': 'A Description'},
        },
    }
    try:
        responses_validator(responses, context=context)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('200', errors)


def test_with_missing_reference(msg_assertions):
    responses = {
        200: {'$ref': '#/responses/UnknownReference'},
    }
    with pytest.raises(ValidationError) as err:
        responses_validator(responses, context={})

    msg_assertions.assert_message_in_errors(
        MESSAGES['reference']['undefined'],
        err.value.detail,
        '200.$ref',
    )
