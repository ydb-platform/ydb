import pytest

from flex.exceptions import ValidationError
from flex.validation.request import (
    validate_request,
)
from flex.error_messages import MESSAGES
from flex.constants import (
    ARRAY,
    BOOLEAN,
    CSV,
    INTEGER,
    PATH,
    PIPES,
    QUERY,
    SSV,
    STRING,
    TSV,
)

from tests.factories import (
    SchemaFactory,
    RequestFactory,
)
from tests.utils import assert_message_in_errors


def test_request_parameter_validation():
    """
    Test that request validation does parameter validation.  This is largely a
    smoke test to ensure that parameter validation is wired into request
    validation correctly.
    """
    schema = SchemaFactory(
        paths={
            '/get/{id}/': {
                'parameters': [
                    {
                        'name': 'id',
                        'in': PATH,
                        'description': 'id',
                        'required': True,
                        'type': STRING,
                        'format': 'uuid',
                    },
                    {
                        'name': 'page',
                        'in': QUERY,
                        'type': INTEGER,
                    },
                ],
                'get': {
                    'responses': {200: {'description': "Success"}},
                },
            },
        },
    )

    request = RequestFactory(url='http://www.example.com/get/32/?page=abcd')

    with pytest.raises(ValidationError) as err:
        validate_request(
            request=request,
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['format']['invalid_uuid'],
        err.value.detail,
        'method.parameters.path.id.format',
    )

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'query.page.type',
    )


def test_request_parameter_validation_with_base_path():
    """
    Test that path parameter validation works even when there is a base path in
    the api.
    """
    schema = SchemaFactory(
        basePath='/api/v1',
        paths={
            '/get/{id}/': {
                'parameters': [
                    {
                        'name': 'id',
                        'in': PATH,
                        'description': 'id',
                        'required': True,
                        'type': STRING,
                    },
                ],
                'get': {
                    'responses': {200: {'description': "Success"}},
                },
            },
        },
    )

    request = RequestFactory(url='http://www.example.com/api/v1/get/32/')

    validate_request(
        request=request,
        schema=schema,
    )


@pytest.mark.parametrize(
    'type_,value',
    (
        (BOOLEAN, 'true'),
        (INTEGER, '123'),
    )
)
def test_request_parameter_validation_typecasting(type_, value):
    """
    Test that request validation does parameter validation for all parameters that require
    typecasting since query params are generally treated as strings.
    """
    schema = SchemaFactory(
        paths={
            '/get/': {
                'parameters': [
                    {
                        'name': 'id',
                        'in': QUERY,
                        'type': type_,
                    }
                ],
                'get': {
                    'responses': {"200": {'description': "Success"}},
                },
            },
        },
    )

    request = RequestFactory(url='http://www.example.com/get/?id={}'.format(value))

    validate_request(
        request=request,
        schema=schema,
    )


@pytest.mark.parametrize(
    'format_,value',
    (
        (CSV, '1,2,3'),
        (SSV, '1 2 3'),
        #(TSV, '1\t2\t3'),
        (PIPES, '1|2|3'),
    ),
)
def test_request_parameter_array_extraction(format_, value):
    schema = SchemaFactory(
        paths={
            '/get/': {
                'get': {
                    'responses': {'200': {'description': "Success"}},
                    'parameters': [
                        {
                            'name': 'id',
                            'in': QUERY,
                            'type': ARRAY,
                            'collectionFormat': format_,
                            'minItems': 3,
                            'maxItems': 3,
                            'items': {
                                'type': INTEGER,
                                'minimum': 1,
                                'maximum': 3,
                            },
                        },
                    ],
                },
            },
        },
    )

    request = RequestFactory(url='http://www.example.com/get/?id={}'.format(value))

    validate_request(
        request=request,
        schema=schema,
    )
