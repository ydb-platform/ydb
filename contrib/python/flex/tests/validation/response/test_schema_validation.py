import json
import pytest

from flex.exceptions import ValidationError
from flex.constants import (
    INTEGER,
    OBJECT,
    ARRAY,
    STRING,
)
from flex.validation.response import (
    validate_response,
)
from flex.error_messages import MESSAGES

from tests.factories import (
    SchemaFactory,
    ResponseFactory,
)
from tests.utils import assert_message_in_errors


def test_basic_response_body_schema_validation_with_invalid_value():
    schema = SchemaFactory(
        paths={
            '/get': {
                'get': {
                    'responses': {
                        '200': {
                            'description': 'Success',
                            'schema': {'type': INTEGER},
                        }
                    },
                },
            },
        },
    )

    response = ResponseFactory(
        url='http://www.example.com/get',
        status_code=200,
        content_type='application/json',
        content=json.dumps('not-an-integer'),
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


def test_basic_response_body_schema_validation_with_nullable_value():
    """
    Ensure objects marked with x-nullable: true attribute are treated as nullable.
    """
    schema = SchemaFactory(
        paths={
            '/get': {
                'get': {
                    'responses': {
                        '200': {
                            'description': 'Success',
                            'schema': {
                                'type': INTEGER,
                                'x-nullable': True
                            },
                        }
                    },
                },
            },
        },
    )

    response = ResponseFactory(
        url='http://www.example.com/get',
        status_code=200,
        content_type='application/json',
        content=json.dumps(None),
    )

    validate_response(
        response=response,
        request_method='get',
        schema=schema,
    )


def test_basic_response_body_schema_validation_with_type_mismatch():
    """
    Ensure that when the expected response type is an object, and some other
    type is provided, that schema validation does not break since internally it
    will try to pull keys off of the value.
    """
    schema = SchemaFactory(
        paths={
            '/get': {
                'get': {
                    'responses': {
                        '200': {
                            'description': 'Success',
                            'schema': {
                                'type': OBJECT,
                                'required': ['id', 'name'],
                                'properties': {
                                    'id': {'type': INTEGER},
                                    'name': {'type': STRING},
                                },
                            },
                        }
                    },
                },
            },
        },
    )

    response = ResponseFactory(
        url='http://www.example.com/get',
        status_code=200,
        content_type='application/json',
        content=json.dumps([{'id': 3, 'name': 'Bob'}]),
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


def test_response_body_schema_validation_with_items_as_reference():
    """
    Ensure that when the expected response type is an object, and some other
    type is provided, that schema validation does not break since internally it
    will try to pull keys off of the value.
    """
    schema = SchemaFactory(
        definitions={
            'User': {
                'required': [
                    'id',
                    'name',
                ],
                'properties': {
                    'id': {
                        'type': INTEGER,
                    },
                    'name': {
                        'enum': ('bob', 'joe'),
                    },
                },
            },
            'UserList': {
                'type': OBJECT,
                'required': [
                    'results',
                ],
                'properties': {
                    'results': {
                        'type': ARRAY,
                        'items':{
                            '$ref': '#/definitions/User',
                        },
                    },
                },
            },
        },
        paths={
            '/get': {
                'get': {
                    'responses': {
                        '200': {
                            'description': 'Success',
                            'schema': {
                                '$ref': '#/definitions/UserList',
                            },
                        }
                    },
                },
            },
        },
    )

    response = ResponseFactory(
        url='http://www.example.com/get',
        status_code=200,
        content_type='application/json',
        content=json.dumps({'results': [{'id': 3, 'name': 'billy'}]}),
    )

    with pytest.raises(ValidationError) as err:
        validate_response(
            response=response,
            request_method='get',
            schema=schema,
        )

    assert_message_in_errors(
        MESSAGES['enum']['invalid'],
        err.value.detail,
        'body.schema.enum',
    )
