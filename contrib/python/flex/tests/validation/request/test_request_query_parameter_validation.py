from flex.validation.request import (
    validate_request,
)
from flex.error_messages import MESSAGES
from flex.constants import (
    PATH,
    QUERY,
    STRING,
    INTEGER,
    ARRAY
)

from tests.factories import (
    SchemaFactory,
    RequestFactory,
)
from tests.utils import assert_message_in_errors


def test_request_query_parameter_validation_with_no_declared_parameters():
    """
    Ensure that when a query parameter is present with no definition within the
    schema that it is ignored.  We need to have at least one parameter at play
    to trigger parameter validation to happen for this endpoint.
    """
    schema = SchemaFactory(
        parameters = {
            'id': {
                'name': 'id',
                'in': PATH,
                'description': 'id',
                'required': True,
                'type': INTEGER,
            },
        },
        paths={
            '/get/{id}/': {
                'parameters': [
                    {'$ref': '#/parameters/id'},
                ],
                'get': {
                    'responses': {200: {'description': "Success"}},
                },
            },
        },
    )

    request = RequestFactory(url='http://www.example.com/get/3/?page=3')

    validate_request(
        request=request,
        schema=schema,
    )


def test_request_query_parameter_validation_with_array_by_multi():
    """
    Ensure that when a query parameter is present with no definition within the
    schema that it is ignored.  We need to have at least one parameter at play
    to trigger parameter validation to happen for this endpoint.
    """
    schema = SchemaFactory(
        parameters = {
            'status': {
                'name': 'status',
                'in': QUERY,
                'description': 'status',
                'required': True,
                'type': ARRAY,
                "items": {
                    "type": "string",
                    "enum": [
                        "available",
                        "pending",
                        "sold"
                    ],
                    "default": "available"
                },
                "collectionFormat": "multi"
            },
        },
        paths={
            '/get': {
                'parameters': [
                    {'$ref': '#/parameters/status'},
                ],
                'get': {
                    'responses': {200: {'description': "Success"}},
                },
            },
        },
    )

    request = RequestFactory(url='http://www.example.com/get?status=sold')

    validate_request(
        request=request,
        schema=schema,
    )
