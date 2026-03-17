import pytest

from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import (
    ARRAY,
    EMPTY,
)

from tests.utils import (
    generate_validator_from_schema,
    assert_error_message_equal,
)


#
# minLength validation tests
#
@pytest.mark.parametrize(
    'letters',
    (
        ['a', 'b', 'c'],
        ['a', 'b', 'c', 'd'],
    ),
)
def test_valid_array_with_min_items(letters):
    schema = {
        'type': ARRAY,
        'minItems': 3,
    }
    validator = generate_validator_from_schema(schema)

    validator(letters)


@pytest.mark.parametrize(
    'letters',
    (
        [],
        ['a'],
        ['a', 'b'],
    ),
)
def test_min_items_with_too_short_array(letters):
    schema = {
        'type': ARRAY,
        'minItems': 3,
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(letters)

    assert 'minItems' in err.value.messages[0]
    assert_error_message_equal(
        err.value.messages[0]['minItems'][0],
        MESSAGES['min_items']['invalid'],
    )


def test_min_items_allows_empty_when_not_required_and_not_present():
    schema = {
        'type': ARRAY,
        'minItems': 3,
    }
    validator = generate_validator_from_schema(schema)

    validator(EMPTY)


#
# maxItems validation tests
#
@pytest.mark.parametrize(
    'letters',
    (
        [],
        ['a', 'b'],
        ['a', 'b', 'c'],
    ),
)
def test_valid_array_with_max_items(letters):
    schema = {
        'type': ARRAY,
        'maxItems': 3,
    }
    validator = generate_validator_from_schema(schema)

    validator(letters)


@pytest.mark.parametrize(
    'letters',
    (
        ['a', 'b', 'c', 'd'],
        ['a', 'b', 'c', 'd', 'e'],
    ),
)
def test_max_items_with_too_long_array(letters):
    schema = {
        'type': ARRAY,
        'maxItems': 3,
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(letters)

    assert 'maxItems' in err.value.messages[0]
    assert_error_message_equal(
        err.value.messages[0]['maxItems'][0],
        MESSAGES['max_items']['invalid'],
    )


def test_max_items_allows_empty_when_not_required_and_not_present():
    schema = {
        'type': ARRAY,
        'maxItems': 3,
    }
    validator = generate_validator_from_schema(schema)

    validator(EMPTY)
