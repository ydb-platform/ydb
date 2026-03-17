import re
import pytest

from flex.exceptions import ValidationError
from flex.constants import (
    STRING,
    EMPTY,
)

from tests.utils import generate_validator_from_schema


ZIPCODE_REGEX = '^[0-9]{5}(?:[-\s][0-9]{4})?$'


@pytest.mark.parametrize(
    'zipcode',
    ('80205', '80205-1234', '80205 1234'),
)
def test_zipcode_regex(zipcode):
    assert re.compile(ZIPCODE_REGEX).match(zipcode)


@pytest.mark.parametrize(
    'zipcode',
    (
        '8020',  # too short
        '802051234',  # no separator
        '8020X',  # non digit character
        '80205-123X',  # non digit character
    ),
)
def test_zipcode_regex_on_bad_zips(zipcode):
    assert not re.compile(ZIPCODE_REGEX).match(zipcode)


@pytest.mark.parametrize(
    'zipcode',
    ('80205', '80205-1234', '80205 1234'),
)
def test_pattern_on_good_strings(zipcode):
    schema = {
        'type': STRING,
        'pattern': ZIPCODE_REGEX,
    }
    validator = generate_validator_from_schema(schema)

    validator(zipcode)


@pytest.mark.parametrize(
    'zipcode',
    (
        '8020',  # too short
        '802051234',  # no separator
        '8020X',  # non digit character
        '80205-123X',  # non digit character
    ),
)
def test_pattern_on_non_matching_strings(zipcode):
    schema = {
        'type': STRING,
        'pattern': ZIPCODE_REGEX,
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError):
        validator(zipcode)


def test_pattern_is_noop_when_not_required_and_not_present():
    schema = {
        'type': STRING,
        'pattern': ZIPCODE_REGEX,
    }
    validator = generate_validator_from_schema(schema)

    validator(EMPTY)
