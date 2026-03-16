import pytest

from flex.exceptions import ValidationError
from flex.validation.operation import (
    construct_operation_validators,
    validate_operation,
)
from flex.constants import (
    PATH,
    QUERY,
    STRING,
    INTEGER,
)
from flex.error_messages import MESSAGES

from tests.utils import assert_message_in_errors
from tests.factories import (
    RequestFactory,
    SchemaFactory,
)


#
#  path parameter validation.
#
def test_operation_parameter_validation_uses_correct_parameter_definitions():
    """
    Validation of a request's parameters involves merging the parameter
    definitions from the path definition as well as the operation definition.
    Ensure that this merging is happening and with the correct precendence to
    override path level parameters with any defined at the operation level.

    This test also serves as a *smoke* test to see that parameter validation is
    working as expected.
    """
    schema = SchemaFactory(
        produces=['application/json'],
        paths={
            '/get/{username}/posts/{id}/': {
                'parameters': [
                    {
                        'name': 'username',
                        'in': PATH,
                        'description': 'username',
                        'required': True,
                        'type': STRING,
                    },
                    {
                        'name': 'id',
                        'in': PATH,
                        'description': 'id',
                        'required': True,
                        'type': INTEGER,
                    },
                ],
                'get': {
                    'responses': {'200': {'description': 'Success'}},
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
                    ]
                },
            },
        },
    )


    request = RequestFactory(url='http://www.example.com/get/fernando/posts/45/')

    api_path = '/get/{username}/posts/{id}/'
    path_definition = schema['paths']['/get/{username}/posts/{id}/']
    operation_definition = schema['paths']['/get/{username}/posts/{id}/']['get']

    validators = construct_operation_validators(
        api_path=api_path,
        path_definition=path_definition,
        operation_definition=operation_definition,
        context=schema,
    )

    with pytest.raises(ValidationError) as err:
        validate_operation(request, validators)

    assert_message_in_errors(
        MESSAGES['format']['invalid_uuid'],
        err.value.detail,
        'parameters.path.id.format',
    )
