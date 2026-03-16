
import pytest

from flex.exceptions import ValidationError
from flex.constants import (
    STRING,
    INTEGER,
)
from flex.error_messages import MESSAGES

from tests.utils import (
    generate_validator_from_schema,
    assert_message_in_errors
)


def test_allof_simple():
    # example taken from http://spacetelescope.github.io/understanding-json-schema/reference/combining.html
    schema = {
        "allOf": [
            {"type": STRING},
            {"maxLength": 5},
        ]
    }
    validator = generate_validator_from_schema(schema)
    validator("short")

    with pytest.raises(ValidationError) as err:
        validator("too long")

    assert_message_in_errors(
        MESSAGES['max_length']['invalid'],
        err.value.detail,
    )


def test_allof_complex():
    schema_without_allof = {
        'type': 'object',
        'properties': {
            'name': {
                'type': STRING,
            },
            'age': {
                'type': INTEGER,
            }
        }
    }

    schema_with_allof = {
        'type': 'object',
        'allOf': [
            {
                'properties': {
                    'name': {
                        'type': STRING,
                    }
                }
            },
            {
                'properties': {
                    'age': {
                        'type': INTEGER,
                    }
                }
            },
        ]
    }

    good_data = dict(name="foo", age=42)
    bad_data = dict(name="foo", age="bar")

    validator_without_allof = generate_validator_from_schema(schema_without_allof)
    validator_with_allof = generate_validator_from_schema(schema_with_allof)

    validator_without_allof(good_data)
    validator_with_allof(good_data)

    with pytest.raises(ValidationError):
        validator_without_allof(bad_data)

    with pytest.raises(ValidationError):
        validator_with_allof(bad_data)


def test_allof_simple_multiple_failures():
    # example taken from http://spacetelescope.github.io/understanding-json-schema/reference/combining.html
    schema = {
        "allOf": [
            {"type": STRING},
            {"type": INTEGER},
        ]
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(True)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
    )

    assert len(err.value.detail['allOf']) == 2


def test_allof_ref_duplicate_failure():
    # example taken from https://github.com/pipermerriam/flex/issues/146
    schema_with_allof = {
        'type': 'object',
        'allOf': [
            {
                '$ref': '#/definitions/UserBase'
            },
            {
                'properties': {
                    'user_id': {
                        'type': 'string',
                        'maxLength': 128
                    }
                },
                'required': ['user_id']
            }
        ]
    }
    context = {
        'definitions': {
            'UserBase': {
                'type': 'object',
                'properties': {
                    'name': {
                        'type': 'string',
                        'maxLength': 100
                    }
                },
                'required': ['name']
            }
        }
    }

    data = {'user_id': 'asdf'}

    validator = generate_validator_from_schema(schema_with_allof, context=context)

    with pytest.raises(ValidationError):
        validator(data)

    with pytest.raises(ValidationError):
        validator(data)
