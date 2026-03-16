import pytest

from flex.exceptions import ValidationError
from flex.validation.response import (
    validate_response,
)
from flex.error_messages import MESSAGES

from tests.factories import (
    ResponseFactory,
    SchemaFactory,
)

from tests.utils import assert_message_in_errors


#
#  produces mimetype validation.
#
def test_produces_validation_is_noop_when_produces_not_declared():
    """
    Test that the `produces` validation is a noop when no content types are
    declared.
    """
    response = ResponseFactory(
        content_type='application/json',
        url='http://www.example.com/get',
    )

    schema = SchemaFactory(
        paths={
            '/get': {'get': {'responses': {'200': {'description': 'Success'}}}},
        },
    )

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )


def test_produces_validation_valid_mimetype_from_global_definition():
    """
    Test that a response content_type that is in the global api produces
    definitions is valid.
    """
    response = ResponseFactory(
        content_type='application/json',
        url='http://www.example.com/get',
    )

    schema = SchemaFactory(
        produces=['application/json'],
        paths={
            '/get': {'get': {'responses': {'200': {'description': 'Success'}}}},
        },
    )

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )


def test_produces_validation_invalid_mimetype_from_global_definition():
    """
    Test that a response content_type that is in the global api produces
    definitions is valid.
    """
    response = ResponseFactory(
        content_type='application/json',
        url='http://www.example.com/get',
    )

    schema = SchemaFactory(
        produces=['application/xml'],
        paths={
            '/get': {'get': {'responses': {'200': {'description': 'Success'}}}},
        },
    )

    with pytest.raises(ValidationError):
        validate_response(
            response=response,
            request_method='get',
            schema=schema,
        )


def test_produces_validation_for_valid_mimetype_from_operation_definition():
    """
    Test that when `produces` is defined in an operation definition, that the
    local value is used in place of any global `produces` definition.
    """
    response = ResponseFactory(
        content_type='application/json',
        url='http://www.example.com/get',
    )

    schema = SchemaFactory(
        produces=['application/xml'],
        paths={
            '/get': {'get': {
                'responses': {'200': {'description': 'Success'}},
                'produces': ['application/json'],
            }},
        },
    )

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )


def test_produces_validation_for_invalid_mimetype_from_operation_definition():
    """
    Test the situation when the operation definition has overridden the global
    allowed mimetypes, that that the local value is used for validation.
    """
    response = ResponseFactory(
        content_type='application/xml',
        url='http://www.example.com/get',
    )

    schema = SchemaFactory(
        produces=['application/xml'],
        paths={
            '/get': {'get': {
                'responses': {'200': {'description': 'Success'}},
                'produces': ['application/json'],
            }},
        },
    )

    with pytest.raises(ValidationError) as err:
        validate_response(
            response=response,
            request_method='get',
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['content_type']['invalid'],
        err.value.detail,
        'body.produces',
    )
