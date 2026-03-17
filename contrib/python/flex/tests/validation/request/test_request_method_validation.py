import pytest

from flex.validation.request import (
    validate_request,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError

from tests.factories import (
    SchemaFactory,
    RequestFactory,
)
from tests.utils import assert_message_in_errors


def test_request_validation_with_invalid_operation_on_path():
    """
    Test that request validation detects request paths that are not declared
    in the schema.
    """
    schema = SchemaFactory(
        paths={
            '/post': {
                'post': {},
            },
        },
    )

    request = RequestFactory(url='http://www.example.com/post')

    with pytest.raises(ValidationError) as err:
        validate_request(
            request=request,
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['request']['invalid_method'],
        err.value.detail,
        'method',
    )
