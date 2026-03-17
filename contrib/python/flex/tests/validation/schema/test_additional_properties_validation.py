
import pytest

from flex.exceptions import ValidationError
from flex.constants import (
    STRING,
    OBJECT,
)

from tests.utils import generate_validator_from_schema


def test_additional_properties_validation_with_no_extra_properties():
    schema = {
        'properties': {
            'name': {
                'type': STRING,
            },
        },
        'additionalProperties': False,
    }

    validator = generate_validator_from_schema(schema)

    validator({
        'name': 'Bob',
    })


def test_additional_properties_validation_with_bad_extra_properties():
    schema = {
        'properties': {
            'name': {
                'type': STRING,
            },
        },
        'additionalProperties': False,
    }

    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError):
        validator({
            'name': 'Bob',
            'age': 10,
        })
