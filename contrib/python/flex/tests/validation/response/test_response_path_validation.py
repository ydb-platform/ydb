import pytest

from flex.exceptions import ValidationError
from flex.validation.response import (
    validate_response,
)
from flex.error_messages import MESSAGES
from flex.constants import (
    PATH,
    INTEGER,
)

from tests.factories import (
    SchemaFactory,
    ResponseFactory,
)
from tests.utils import (
    assert_message_in_errors,
)


def test_response_validation_with_invalid_path():
    """
    Test that request validation detects request paths that are not declared
    in the schema.
    """
    schema = SchemaFactory()
    assert not schema['paths']

    response = ResponseFactory(url='http://www.example.com/not-an-api-path')

    with pytest.raises(ValidationError) as err:
        validate_response(
            response=response,
            request_method='get',
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['path']['no_matching_paths_found'],
        err.value.detail,
        'path',
    )


def test_response_validation_with_valid_path():
    """
    Test that response validation is able to match api paths.
    """
    schema = SchemaFactory(
        paths={
            '/get': {
                'get': {'responses': {'200': {'description': 'Success'}}},
            },
        }
    )

    response = ResponseFactory(url='http://www.example.com/get')

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )


def test_response_validation_with_valid_path_and_base_path():
    """
    Test that response validation is able to match api paths even when there is
    a base path.
    """
    schema = SchemaFactory(
        basePath='/api/v1',
        paths={
            '/get': {
                'get': {'responses': {'200': {'description': 'Success'}}},
            },
        }
    )

    response = ResponseFactory(url='http://www.example.com/api/v1/get')

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )


def test_response_validation_with_parametrized_path():
    """
    Test that request validation finds and validates parametrized paths.
    """
    schema = SchemaFactory(
        paths={
            '/get/{id}': {
                'get': {'responses': {'200': {'description': 'Success'}}},
                'parameters': [
                    {
                        'name': 'id',
                        'in': PATH,
                        'description': 'The Primary Key',
                        'type': INTEGER,
                        'required': True,
                    }
                ]
            },
        }
    )

    response = ResponseFactory(url='http://www.example.com/get/1234')

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )


def test_response_validation_with_parametrized_path_and_api_base_path():
    """
    Test that request validation finds and validates parametrized paths even
    when the api has a base path.
    """
    schema = SchemaFactory(
        basePath='/api/v1',
        paths={
            '/get/{id}': {
                'get': {'responses': {'200': {'description': 'Success'}}},
                'parameters': [
                    {
                        'name': 'id',
                        'in': PATH,
                        'description': 'The Primary Key',
                        'type': INTEGER,
                        'required': True,
                    }
                ]
            },
        }
    )

    response = ResponseFactory(url='http://www.example.com/api/v1/get/1234')

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )


def test_response_validation_with_parametrized_path_and_invalid_value():
    """
    Test that request validation does type checking on path parameters.  Ensure
    that the value in the path is validated.
    """
    schema = SchemaFactory(
        paths={
            '/get/{id}': {
                'get': {'responses': {'200': {'description': 'Success'}}},
                'parameters': [
                    {
                        'name': 'id',
                        'in': PATH,
                        'description': 'The Primary Key',
                        'type': INTEGER,
                        'required': True,
                    }
                ]
            },
        }
    )

    response = ResponseFactory(url='http://www.example.com/get/abc')

    with pytest.raises(ValidationError) as err:
        validate_response(
            response=response,
            request_method='get',
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['path']['no_matching_paths_found'],
        err.value.detail,
        'path',
    )


def test_response_validation_with_parameter_as_reference():
    """
    Test that request validation finds and validates parametrized paths when
    the parameter is a reference.
    """
    schema = SchemaFactory(
        paths={
            '/get/{id}': {
                'get': {'responses': {'200': {'description': 'Success'}}},
                'parameters': [
                    {'$ref': '#/parameters/id'},
                ]
            },
        },
        parameters={
            'id': {
                'name': 'id',
                'in': PATH,
                'description': 'The Primary Key',
                'type': INTEGER,
                'required': True,
            }
        },
    )

    response = ResponseFactory(url='http://www.example.com/get/1234')

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )


def test_response_validation_with_parameter_as_reference_for_invalid_value():
    """
    Test that request validation finds and validates parametrized paths when
    the parameter is a reference.  Ensure that it detects invalid types.
    """
    schema = SchemaFactory(
        paths={
            '/get/{id}': {
                'get': {'responses': {'200': {'description': 'Success'}}},
                'parameters': [
                    {'$ref': '#/parameters/id'},
                ]
            },
        },
        parameters={
            'id': {
                'name': 'id',
                'in': PATH,
                'description': 'The Primary Key',
                'type': INTEGER,
                'required': True,
            }
        },
    )

    response = ResponseFactory(url='http://www.example.com/get/abc')

    with pytest.raises(ValidationError) as err:
        validate_response(
            response=response,
            request_method='get',
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['path']['no_matching_paths_found'],
        err.value.detail,
        'path',
    )


def test_response_validation_with_parametrized_path_and_simple_api_base_path():
    """
    Test that request validation finds and validates parametrized paths even
    when the api has a base path being /.
    """
    schema = SchemaFactory(
        basePath='/',
        paths={
            '/api/v1/get/{id}': {
                'get': {'responses': {'200': {'description': 'Success'}}},
                'parameters': [
                    {
                        'name': 'id',
                        'in': PATH,
                        'description': 'The Primary Key',
                        'type': INTEGER,
                        'required': True,
                    }
                ]
            },
        }
    )

    response = ResponseFactory(url='http://www.example.com/api/v1/get/1234')

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )


def test_response_validation_with_parametrized_path_and_base_path_slash_end():
    """
    Test that request validation finds and validates parametrized paths even
    when the api has a base path being /.
    """
    schema = SchemaFactory(
        basePath='/api/v1/',
        paths={
            '/get/{id}': {
                'get': {'responses': {'200': {'description': 'Success'}}},
                'parameters': [
                    {
                        'name': 'id',
                        'in': PATH,
                        'description': 'The Primary Key',
                        'type': INTEGER,
                        'required': True,
                    }
                ]
            },
        }
    )

    response = ResponseFactory(url='http://www.example.com/api/v1/get/1234')

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )