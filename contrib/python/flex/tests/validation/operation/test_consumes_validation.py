import pytest

from flex.exceptions import ValidationError
from flex.validation.operation import (
    construct_operation_validators,
    validate_operation,
)

from tests.factories import (
    RequestFactory,
    ResponseFactory,
    SchemaFactory,
)


#
# consumes validation
#
def test_consumes_validation_valid_mimetype_from_global_definition():
    """
    Test that a request content_type that is in the global api consumes
    definitions is valid.
    """
    request = RequestFactory(content_type='application/json')
    response = ResponseFactory(request=request)

    schema = SchemaFactory(
        consumes=['application/json'],
        paths={
            '/get': {'get': {'responses': {'200': {'description': 'Success'}}}},
        },
    )

    validators = construct_operation_validators(
        api_path='/get',
        path_definition=schema['paths']['/get'],
        operation_definition=schema['paths']['/get']['get'],
        context=schema,
    )
    validate_operation(response.request, validators)


def test_consumes_validation_invalid_mimetype_from_global_definition():
    """
    Test that a request content_type that is in the global api consumes
    definitions is valid.
    """
    request = RequestFactory(content_type='application/json')
    response = ResponseFactory(request=request)

    schema = SchemaFactory(
        consumes=['text/xml', 'application/xml'],
        paths={
            '/get': {'get': {'responses': {'200': {'description': 'Success'}}}},
        },
    )

    validators = construct_operation_validators(
        api_path='/get',
        path_definition=schema['paths']['/get'],
        operation_definition=schema['paths']['/get']['get'],
        context=schema,
    )
    with pytest.raises(ValidationError):
        validate_operation(response.request, validators)


def test_consumes_validation_for_valid_mimetype_from_operation_definition():
    """
    Test that when `consumes` is defined in an operation definition, that the
    local value is used in place of any global `consumes` definition.
    """
    request = RequestFactory(content_type='application/json')
    response = ResponseFactory(request=request)

    schema = SchemaFactory(
        consumes=['application/xml'],
        paths={
            '/get': {'get': {
                'responses': {'200': {'description': 'Success'}},
                'consumes': ['application/json'],
            }},
        },
    )

    validators = construct_operation_validators(
        api_path='/get',
        path_definition=schema['paths']['/get'],
        operation_definition=schema['paths']['/get']['get'],
        context=schema,
    )
    validate_operation(response.request, validators)


def test_consumes_validation_for_invalid_mimetype_from_operation_definition():
    """
    Test the situation when the operation definition has overridden the global
    allowed mimetypes, that that the local value is used for validation.
    """
    request = RequestFactory(content_type='application/xml')
    response = ResponseFactory(request=request)

    schema = SchemaFactory(
        consumes=['application/xml'],
        paths={
            '/get': {'get': {
                'responses': {'200': {'description': 'Success'}},
                'consumes': ['application/json'],
            }},
        },
    )

    validators = construct_operation_validators(
        api_path='/get',
        path_definition=schema['paths']['/get'],
        operation_definition=schema['paths']['/get']['get'],
        context=schema,
    )
    with pytest.raises(ValidationError):
        validate_operation(response.request, validators)
