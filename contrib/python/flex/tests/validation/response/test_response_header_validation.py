import pytest

from flex.exceptions import ValidationError
from flex.validation.response import (
    validate_response,
)
from flex.error_messages import MESSAGES
from flex.constants import (
    INTEGER,
    NUMBER,
    BOOLEAN,
    STRING,
)

from tests.factories import (
    SchemaFactory,
    ResponseFactory,
)
from tests.utils import assert_message_in_errors


def test_response_header_validation():
    """
    Test basic validation of response headers.
    """
    schema = SchemaFactory(
        paths={
            '/get': {
                'get': {
                    'responses': {'200': {
                        'description': "Success",
                        'headers': {
                            'Foo': {'type': INTEGER},
                        }
                    }},
                },
            },
        },
    )

    response = ResponseFactory(
        url='http://www.example.com/get',
        headers={'Foo': 'abc'},
    )

    with pytest.raises(ValidationError) as err:
        validate_response(
            response=response,
            request_method='get',
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'body.headers.Foo.type',
    )


@pytest.mark.parametrize(
    'type_,value',
    (
        (INTEGER, '3'),
        (NUMBER, '3.3'),
        (BOOLEAN, 'true'),
        (BOOLEAN, 'True'),
        (BOOLEAN, '1'),
        (BOOLEAN, 'false'),
        (BOOLEAN, 'False'),
        (BOOLEAN, '0'),
        (STRING, 'abcd'),
    )
)
def test_response_header_validation_for_non_strings(type_, value):
    schema = SchemaFactory(
        paths={
            '/get': {
                'get': {
                    'responses': {'200': {
                        'description': "Success",
                        'headers': {
                            'Foo': {'type': type_},
                        }
                    }},
                },
            },
        },
    )

    response = ResponseFactory(
        url='http://www.example.com/get',
        headers={'Foo': value},
    )

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )
