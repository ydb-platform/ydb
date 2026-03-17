import pytest

from flex.exceptions import (
    ValidationError,
)
from flex.error_messages import (
    MESSAGES,
)
from flex.constants import (
    INTEGER,
    STRING,
    EMPTY,
)

from tests.utils import (
    generate_validator_from_schema,
    assert_error_message_equal,
)


#
# Integration style tests for PropertiesSerializer type validation.
#
def test_non_integer_type_raises_error():
    schema = {
        'type': INTEGER,
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator('1')

    assert 'type' in err.value.messages[0]
    assert_error_message_equal(
        err.value.messages[0]['type'][0],
        MESSAGES['type']['invalid'],
    )


def test_string_type_raises_error():
    schema = {
        'type': INTEGER,
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator('abcd')

    assert 'type' in err.value.messages[0]
    assert_error_message_equal(
        err.value.messages[0]['type'][0],
        MESSAGES['type']['invalid'],
    )


def test_integer_type_valid():
    schema = {
        'type': INTEGER,
    }
    validator = generate_validator_from_schema(schema)

    validator(1)


@pytest.mark.parametrize(
    'value',
    (1, '1'),
)
def test_multi_type(value):
    schema = {
        'type': [INTEGER, STRING],
    }
    validator = generate_validator_from_schema(schema)
    validator(value)


@pytest.mark.parametrize(
    'value',
    (None, True, False, [], {}),
)
def test_invalid_multi_type(value):
    schema = {
        'type': [INTEGER, STRING],
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(value)

    assert 'type' in err.value.messages[0]
    assert_error_message_equal(
        err.value.messages[0]['type'][0],
        MESSAGES['type']['invalid'],
    )


def test_type_validation_is_noop_when_not_required_and_not_present():
    schema = {
        'type': [INTEGER, STRING],
    }

    validator = generate_validator_from_schema(schema)

    validator(EMPTY)
