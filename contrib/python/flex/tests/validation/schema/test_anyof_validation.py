
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


def test_anyof_simple():
    # example taken from http://spacetelescope.github.io/understanding-json-schema/reference/combining.html
    schema = {
        "anyOf": [
            {"type": STRING, "maxLength": 5},
            {"type": INTEGER, "minimum": 0},
        ]
    }
    validator = generate_validator_from_schema(schema)
    validator("short")
    validator(42)

    with pytest.raises(ValidationError) as err:
        validator("too long")

    assert_message_in_errors(
        MESSAGES['max_length']['invalid'],
        err.value.detail,
    )

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
    )

    with pytest.raises(ValidationError) as err:
        validator(-42)

    assert_message_in_errors(
        MESSAGES['minimum']['invalid'],
        err.value.detail,
    )

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
    )


def test_anyof_complex():
    schema_without_anyof = {
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

    schema_with_anyof = {
        'type': 'object',
        'anyOf': [
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

    validator_without_anyof = generate_validator_from_schema(schema_without_anyof)
    validator_with_anyof = generate_validator_from_schema(schema_with_anyof)

    validator_without_anyof(good_data)
    validator_with_anyof(good_data)

    with pytest.raises(ValidationError):
        validator_without_anyof(bad_data)

    validator_with_anyof(bad_data)
