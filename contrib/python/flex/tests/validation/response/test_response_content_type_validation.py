import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError

from flex.validation.response import (
    validate_response,
)

from tests.factories import (
    SchemaFactory,
    ResponseFactory,
)
from tests.utils import assert_message_in_errors


def test_response_content_type_validation():
    schema = SchemaFactory(
        produces=['application/json'],
        paths={
            '/get': {
                'get': {
                    'responses': {'200': {'description': 'Success'}},
                }
            },
        },
    )

    response = ResponseFactory(
        url='http://www.example.com/get',
        content_type='application/json',
    )

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )


def test_response_content_type_validation_when_no_content_type_specified():
    schema = SchemaFactory(
        produces=['application/json'],
        paths={
            '/get': {
                'get': {
                    'responses': {'200': {'description': 'Success'}},
                }
            },
        },
    )

    response = ResponseFactory(
        url='http://www.example.com/get',
        content_type=None,
    )

    # this is considered valid currently, but may change
    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )


def test_response_content_type_validation_ignores_parameters():
    schema = SchemaFactory(
        produces=['application/json'],
        paths={
            '/get': {
                'get': {
                    'responses': {'200': {'description': 'Success'}},
                }
            },
        },
    )

    response = ResponseFactory(
        url='http://www.example.com/get',
        content_type='application/json; charset=UTF-8',
    )

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )
