import json
import pytest

from flex.exceptions import ValidationError
from flex.validation.response import (
    validate_response,
)
from flex.constants import (
    PATH,
    INTEGER,
    OBJECT,
)

from tests.factories import (
    SchemaFactory,
    ResponseFactory,
)

from flex.error_messages import MESSAGES

from tests.utils import assert_message_in_errors


def test_response_reference():
    schema = SchemaFactory(
            paths={
                '/get': {
                    'get': {'responses': {
                            '200':
                                { '$ref': "#/responses/NumberResponse"},
                            }
                    }
                },
            },
            definitions={
                'Number': {
                    'type': INTEGER,
                },
            },
            responses={
                'NumberResponse':
                {'description': 'Success', 'schema': {'$ref': '#/definitions/Number'}},
            },
    )

    response = ResponseFactory(
            url='http://www.example.com/get',
            status_code=200,
            content_type='application/json',
            content=json.dumps(None),
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
            'body.schema.type',
    )
