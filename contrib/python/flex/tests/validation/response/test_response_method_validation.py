import pytest

from flex.exceptions import ValidationError
from flex.validation.response import (
    validate_response,
)
from flex.error_messages import MESSAGES
from flex.constants import (
    GET,
    POST,
)

from tests.factories import (
    SchemaFactory,
    ResponseFactory,
)
from tests.utils import (
    assert_message_in_errors,
)


def test_response_validation_with_invalid_method():
    """
    Test that response validation detects not defined method.
    """
    schema = SchemaFactory(
        paths={
            '/get': {
                GET: {'responses': {'200': {'description': 'Success'}}},
            },
        }
    )

    response = ResponseFactory(url='http://www.example.com/get')

    with pytest.raises(ValidationError) as err:
        validate_response(
            response=response,
            request_method=POST,
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['request']['invalid_method'],
        err.value.detail,
        'method',
    )
