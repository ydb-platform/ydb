import pytest

from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import (
    NUMBER,
    EMPTY,
)

from tests.utils import (
    generate_validator_from_schema,
    assert_error_message_equal,
)


#
# minimum and exclusiveMinimum tests
#
@pytest.mark.parametrize(
    'width',
    (5, 5.0, 6),
)
def test_inclusive_minimum_validation_with_valid_numbers(width):
    schema = {
        'type': NUMBER,
        'minimum': 5,
    }
    validator = generate_validator_from_schema(schema)

    validator(width)


@pytest.mark.parametrize(
    'width',
    (-5, 0, 4.999),
)
def test_inclusive_minimum_validation_with_invalid_numbers(width):
    schema = {
        'type': NUMBER,
        'minimum': 5,
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(width)

    assert 'minimum' in err.value.messages[0]
    assert_error_message_equal(
        err.value.messages[0]['minimum'][0],
        MESSAGES['minimum']['invalid'],
    )


@pytest.mark.parametrize(
    'width',
    (5.00001, 6, 10),
)
def test_exclusive_minimum_validation_with_valid_numbers(width):
    schema = {
        'type': NUMBER,
        'minimum': 5,
        'exclusiveMinimum': True,
    }
    validator = generate_validator_from_schema(schema)

    validator(width)


@pytest.mark.parametrize(
    'width',
    (5, 4.99999, 0),
)
def test_exclusive_minimum_validation_with_invalid_numbers(width):
    schema = {
        'type': NUMBER,
        'minimum': 5,
        'exclusiveMinimum': True,
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(width)

    assert 'minimum' in err.value.messages[0]
    assert_error_message_equal(
        err.value.messages[0]['minimum'][0],
        MESSAGES['minimum']['invalid'],
    )


def test_minimum_noop_when_not_required_or_present():
    schema = {
        'type': NUMBER,
        'minimum': 5,
    }
    validator = generate_validator_from_schema(schema)

    validator(EMPTY)


#
# maximum and exclusiveMaximum tests
#
@pytest.mark.parametrize(
    'width',
    (5, 5.0, 0, -5),
)
def test_inclusive_maximum_validation_with_valid_numbers(width):
    schema = {
        'type': NUMBER,
        'maximum': 5,
    }
    validator = generate_validator_from_schema(schema)

    validator(width)


@pytest.mark.parametrize(
    'width',
    (6, 10, 5.000001),
)
def test_inclusive_maximum_validation_with_invalid_numbers(width):
    schema = {
        'type': NUMBER,
        'maximum': 5,
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(width)

    assert 'maximum' in err.value.messages[0]
    assert_error_message_equal(
        err.value.messages[0]['maximum'][0],
        MESSAGES['maximum']['invalid'],
    )


@pytest.mark.parametrize(
    'width',
    (4.99999, 0, -5),
)
def test_exclusive_maximum_validation_with_valid_numbers(width):
    schema = {
        'type': NUMBER,
        'maximum': 5,
        'exclusiveMaximum': True,
    }
    validator = generate_validator_from_schema(schema)

    validator(width)


@pytest.mark.parametrize(
    'width',
    (5, 5.00001, 10),
)
def test_exclusive_maximum_validation_with_invalid_numbers(width):
    schema = {
        'type': NUMBER,
        'maximum': 5,
        'exclusiveMaximum': True,
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(width)

    assert 'maximum' in err.value.messages[0]
    assert_error_message_equal(
        err.value.messages[0]['maximum'][0],
        MESSAGES['maximum']['invalid'],
    )


def test_maximum_noop_when_not_required_or_present():
    schema = {
        'type': NUMBER,
        'maximum': 5,
    }
    validator = generate_validator_from_schema(schema)

    validator(EMPTY)
