import pytest

from flex.exceptions import (
    ValidationError,
)
from flex.error_messages import (
    MESSAGES,
)
from flex.constants import (
    OBJECT,
)

from tests.utils import (
    generate_validator_from_schema,
    assert_message_in_errors,
    assert_message_not_in_errors,
    assert_path_not_in_errors,
)


def test_field_declared_as_required():
    schema = {
        'type': OBJECT,
        'required': [
            'field-A',
            # 'field-B',
        ],
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator({})

    assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
        'required.field-A',
    )
    assert_path_not_in_errors(
        'required.field-B',
        err.value.detail,
    )


def test_field_declared_as_required_with_field_present_is_valid():
    schema = {
        'type': OBJECT,
        'required': [
            'field-A',
            # 'field-B',
        ],
    }
    validator = generate_validator_from_schema(schema)

    try:
        validator({'field-A': 'present'})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'required.field-A',
        errors,
    )
    assert_path_not_in_errors(
        'required.field-B',
        errors,
    )
