import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_properties_is_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('properties', errors)


@pytest.mark.parametrize(
    'value',
    (None, 1, 1.1, [1, 2], 'abc', True),
)
def test_properties_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'properties': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'properties.type',
    )


def test_properties_applies_schema_validation():
    with pytest.raises(ValidationError) as err:
        schema_validator(
            {'properties': {
                'TestProperty': {'required': 'invalid-type'},
            }},
        )

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'properties.TestProperty.required',
    )
