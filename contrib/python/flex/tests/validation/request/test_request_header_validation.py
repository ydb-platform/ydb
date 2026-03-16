import pytest

from flex.validation.request import (
    validate_request,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.constants import (
    INTEGER,
    HEADER,
    ARRAY,
    CSV,
    SSV,
    TSV,
    PIPES,
)

from tests.factories import (
    SchemaFactory,
    RequestFactory,
)
from tests.utils import assert_message_in_errors


def test_request_header_validation():
    schema = SchemaFactory(
        paths={
            '/get/': {
                'get': {
                    'responses': {'200': {'description': "Success"}},
                    'parameters': [
                        {
                            'name': 'Authorization',
                            'in': HEADER,
                            'type': INTEGER,
                        }
                    ]
                },
            },
        },
    )

    request = RequestFactory(
        url='http://www.example.com/get/',
        headers={'Authorization': 'abc'},
    )

    with pytest.raises(ValidationError) as err:
        validate_request(
            request=request,
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'method.parameters.headers.Authorization.type',
    )

    # integers within strings since headers are strings + undeclared parameters:
    request = RequestFactory(
        url='http://www.example.com/get/?foo=1',
        headers={'Authorization': '123'},
    )

    validate_request(
        request=request,
        schema=schema,
    )

@pytest.mark.parametrize(
    'format_,value',
    (
        (CSV, '1,2,3'),
        (SSV, '1 2 3'),
        (TSV, '1\t2\t3'),
        (PIPES, '1|2|3'),
    ),
)
def test_request_header_array_extraction(format_, value):
    schema = SchemaFactory(
        paths={
            '/get/': {
                'get': {
                    'responses': {200: {'description': "Success"}},
                    'parameters': [
                        {
                            'name': 'Authorization',
                            'in': HEADER,
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

    request = RequestFactory(
        url='http://www.example.com/get/',
        headers={'Authorization': value},
    )

    validate_request(
        request=request,
        schema=schema,
    )
