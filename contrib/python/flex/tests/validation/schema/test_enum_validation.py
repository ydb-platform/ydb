import six

import pytest

from flex.exceptions import ValidationError
from flex.constants import EMPTY
from flex.error_messages import MESSAGES

from tests.utils import (
    generate_validator_from_schema,
    assert_error_message_equal,
)


#
# minLength validation tests
#
@pytest.mark.parametrize(
    'letters',
    ('a', 'b', True, 1, 2),
)
def test_enum_with_valid_array(letters):
    schema = {
        'enum': [2, 1, 'a', 'b', 'c', True, False],
    }
    validator = generate_validator_from_schema(schema)

    validator(letters)


@pytest.mark.parametrize(
    'letters',
    (None, 1, 0, 2, 'a'),
)
def test_enum_with_invalid_items(letters):
    schema = {
        'enum': [True, False, 1.0, 2.0, 'A'],
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(letters)

    assert_error_message_equal(
        err.value.messages[0]['enum'][0],
        MESSAGES['enum']['invalid'],
    )


def test_enum_noop_when_not_required_and_field_not_present():
    schema = {
        'enum': [True, False, 1.0, 2.0, 'A'],
    }
    validator = generate_validator_from_schema(schema)

    validator(EMPTY)


@pytest.mark.parametrize(
    'enum_value,value',
    (
        (six.text_type('test'), six.text_type('test')),
        (six.text_type('test'), b'test'),
        (b'test', six.text_type('test')),
        (b'test', b'test'),
    )
)
def test_enum_disperate_text_types(enum_value, value):
    schema = {
        'enum': [enum_value],
    }
    validator = generate_validator_from_schema(schema)

    validator(value)
