import pytest

from flex.exceptions import ValidationError
from flex.validation.request import (
    validate_request,
)
from flex.error_messages import MESSAGES
from flex.constants import (
    PATH,
    INTEGER,
)

from tests.factories import (
    SchemaFactory,
    RequestFactory,
)
from tests.utils import assert_message_in_errors


def test_request_validation_with_invalid_request_path():
    """
    Test that request validation detects request paths that are not declared
    in the schema.
    """
    schema = SchemaFactory()
    assert not schema['paths']

    request = RequestFactory(url='http://www.example.com/not-an-api-path')

    with pytest.raises(ValidationError) as err:
        validate_request(
            request=request,
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['path']['no_matching_paths_found'],
        err.value.detail,
        'path',
    )


def test_request_validation_with_valid_path():
    """
    Test that request validation is able to match api paths.
    """
    schema = SchemaFactory(
        paths={
            '/get': {
                'get': {'responses': {'200': {'description': 'Success'}}},
            },
        },
    )

    request = RequestFactory(url='http://www.example.com/get')

    validate_request(
        request=request,
        schema=schema,
    )


def test_request_validation_with_parametrized_path():
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

    request = RequestFactory(url='http://www.example.com/get/1234')

    validate_request(
        request=request,
        schema=schema,
    )


def test_request_validation_with_parametrized_path_with_base_path():
    """
    Test that request validation finds and validates parametrized paths.
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

    request = RequestFactory(url='http://www.example.com/api/v1/get/1234')

    validate_request(
        request=request,
        schema=schema,
    )


def test_request_validation_with_parametrized_path_with_invalid_value():
    """
    Test that request validation finds and validates parametrized paths.
    Ensure that it does validation on the values.
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

    request = RequestFactory(url='http://www.example.com/get/abcd')

    with pytest.raises(ValidationError) as err:
        validate_request(
            request=request,
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['path']['no_matching_paths_found'],
        err.value.detail,
        'path',
    )


def test_request_validation_with_parameter_as_reference():
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

    request = RequestFactory(url='http://www.example.com/get/1234')

    validate_request(
        request=request,
        schema=schema,
    )


def test_request_validation_with_valid_path_and_base_path():
    """
    Test that request validation is able to match api paths that also have a
    base api path.
    """
    schema = SchemaFactory(
        basePath='/api/v1',
        paths={
            '/get': {
                'get': {
                    'responses': {'200': {'description': "Success"}},
                },
            },
        },
    )

    request = RequestFactory(url='http://www.example.com/api/v1/get')

    validate_request(
        request=request,
        schema=schema,
    )


def test_request_validation_with_parameter_as_reference_for_invalid_value():
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

    request = RequestFactory(url='http://www.example.com/get/abc')

    with pytest.raises(ValidationError) as err:
        validate_request(
            request=request,
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['path']['no_matching_paths_found'],
        err.value.detail,
        'path',
    )
