import pytest

from flex.exceptions import ValidationError
from flex.constants import (
    STRING,
    EMPTY,
)

from tests.utils import (
    generate_validator_from_schema,
)


#
# minLength validation tests
#
@pytest.mark.parametrize(
    'zipcode',
    ('80205', '80205-1234'),
)
def test_minimum_length_with_valid_string(zipcode):
    schema = {
        'type': STRING,
        'minLength': 5,
    }
    validator = generate_validator_from_schema(schema)

    validator(zipcode)


@pytest.mark.parametrize(
    'zipcode',
    ('8020', 'abcd', ''),
)
def test_minimum_length_with_too_short_string(zipcode):
    schema = {
        'type': STRING,
        'minLength': 5,
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError):
        validator(zipcode)


def test_minimum_length_is_noop_when_not_required_and_not_present():
    schema = {
        'type': STRING,
        'minLength': 5,
    }
    validator = generate_validator_from_schema(schema)

    validator(EMPTY)


#
# maxLength validation tests
#
@pytest.mark.parametrize(
    'zipcode',
    ('80205', '1234567890', ''),
)
def test_maximum_length_with_valid_string(zipcode):
    schema = {
        'type': STRING,
        'maxLength': 10,
    }
    validator = generate_validator_from_schema(schema)

    validator(zipcode)


@pytest.mark.parametrize(
    'zipcode',
    ('12345-12345', 'abcde-fghijkl'),
)
def test_maximum_length_with_too_long_string(zipcode):
    schema = {
        'type': STRING,
        'maxLength': 10,
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError):
        validator(zipcode)


def test_maximum_length_is_noop_when_not_required_and_not_present():
    schema = {
        'type': STRING,
        'maxLength': 5,
    }
    validator = generate_validator_from_schema(schema)

    validator(EMPTY)
