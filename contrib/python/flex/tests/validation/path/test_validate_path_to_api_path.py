import pytest

from flex.exceptions import ValidationError
from flex.validation.common import (
    validate_path_to_api_path,
)
from flex.error_messages import MESSAGES
from flex.constants import (
    PATH,
    STRING,
    INTEGER,
)

from tests.factories import (
    SchemaFactory,
    RequestFactory,
)
from tests.utils import assert_message_in_errors


def test_basic_request_path_validation():
    context = SchemaFactory(
        paths={'/get': {}},
    )

    request = RequestFactory(url='http://www.example.com/get')
    path = validate_path_to_api_path(
        path=request.path,
        context=context,
        **context
    )
    assert path == '/get'


@pytest.mark.parametrize(
    'request_path',
    (
        '/get/',  # trailing slash.
        '/post',  # not declared at all
    )
)
def test_basic_request_path_validation_with_unspecified_paths(request_path):
    context = SchemaFactory(
        paths={'/get': {}},
    )

    url = 'http://www.example.com{0}'.format(request_path)

    request = RequestFactory(url=url)

    with pytest.raises(ValidationError) as err:
        validate_path_to_api_path(
            path=request.path,
            context=context,
            **context
        )

    assert_message_in_errors(
        MESSAGES['path']['no_matching_paths_found'],
        err.value.detail,
    )


def test_parametrized_string_path_validation():
    context = SchemaFactory(paths={
        '/get/{id}': {
            'parameters': [
                # One very plain id of type string.
                {'name': 'id', 'in': PATH, 'description': 'The id', 'type': STRING, 'required': True},
            ],
        }},
    )

    request = RequestFactory(url='http://www.example.com/get/25')
    path = validate_path_to_api_path(
        path=request.path,
        context=context,
        **context
    )
    assert path == '/get/{id}'


def test_parametrized_integer_path_validation():
    context = SchemaFactory(paths={
        '/get/{id}': {
            'parameters': [
                # One very plain id of type string.
                {'name': 'id', 'in': PATH, 'description': 'The id', 'type': INTEGER, 'required': True},
            ],
        }
    })

    request = RequestFactory(url='http://www.example.com/get/25')
    path = validate_path_to_api_path(
        path=request.path,
        context=context,
        **context
    )
    assert path == '/get/{id}'


def test_parametrized_path_with_multiple_prameters():
    context = SchemaFactory(paths={
        '/users/{username}/posts/{id}': {
            'parameters': [
                # One very plain id of type string.
                {'name': 'id', 'in': PATH, 'description': 'The id', 'type': INTEGER, 'required': True},
                {'name': 'username', 'in': PATH, 'description': 'The username', 'type': STRING, 'required': True},
            ],
        }
    })

    request = RequestFactory(url='http://www.example.com/users/john-smith/posts/47')
    path = validate_path_to_api_path(
        path=request.path,
        context=context,
        **context
    )
    assert path == '/users/{username}/posts/{id}'


def test_parametrized_path_with_parameter_definition_as_reference():
    context = SchemaFactory(
        paths={
            '/get/{id}': {
                'parameters': [
                    {'$ref': '#/parameters/id'}
                ],
            },
        },
        parameters={
            'id': {
                'name': 'id',
                'in': PATH,
                'description': 'The primary key',
                'type': INTEGER,
                'required': True,
            }
        }
    )

    request = RequestFactory(url='http://www.example.com/get/12345')
    path = validate_path_to_api_path(
        path=request.path,
        context=context,
        **context
    )
    assert path == '/get/{id}'
