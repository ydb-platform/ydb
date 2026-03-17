import json

import pytest

from flex.exceptions import ValidationError
from flex.validation.request import (
    validate_request,
)
from flex.error_messages import MESSAGES
from flex.constants import (
    PATH,
    QUERY,
    STRING,
    INTEGER,
    BODY,
    POST,
    OBJECT,
)

from tests.factories import (
    SchemaFactory,
    RequestFactory,
)
from tests.utils import assert_message_in_errors


@pytest.fixture
def user_post_schema():
    schema = SchemaFactory(
        parameters = {
            'user': {
                'name': 'user',
                'in': BODY,
                'description': 'user',
                'required': True,
                'schema': {
                    'type': OBJECT,
                    'properties': {
                        'username': {'type': STRING},
                        'email': {'type': STRING, 'format': 'email'},
                        'age': {'type': INTEGER, 'x-nullable': True},
                    },
                },
            },
        },
        paths={
            '/post/': {
                'parameters': [
                    {'$ref': '#/parameters/user'},
                ],
                'post': {
                    'responses': {200: {'description': "Success"}},
                },
            },
        },
    )
    return schema


def test_request_body_parameter_validation_with_valid_value(user_post_schema):
    """
    Test validating the request body with a valid post.
    """
    request = RequestFactory(
        url='http://www.example.com/post/',
        content_type='application/json',
        body=json.dumps({'username': 'Test User', 'email': 'test@example.com'}),
        method=POST,
    )

    validate_request(
        request=request,
        schema=user_post_schema,
    )


def test_request_body_parameter_validation_with_nullable_field(user_post_schema):
    """
    Test validating the request body with a valid post.
    """
    request = RequestFactory(
        url='http://www.example.com/post/',
        content_type='application/json',
        body=json.dumps({'username': 'Test User', 'email': 'test@example.com', 'age': None}),
        method=POST,
    )

    validate_request(
        request=request,
        schema=user_post_schema,
    )


def test_request_body_parameter_validation_with_invalid_value(user_post_schema):
    """
    Test validating the request body with a valid post.
    """
    request = RequestFactory(
        url='http://www.example.com/post/',
        content_type='application/json',
        body=json.dumps({'username': 'Test User', 'email': 'test'}),
        method=POST,
    )

    with pytest.raises(ValidationError) as err:
        validate_request(
            request=request,
            schema=user_post_schema,
        )

    assert_message_in_errors(
        MESSAGES['format']['invalid_email'],
        err.value.detail,
    )
