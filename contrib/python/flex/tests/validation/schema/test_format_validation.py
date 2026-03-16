import pytest

from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import EMPTY

from tests.utils import (
    generate_validator_from_schema,
    assert_error_message_equal,
)


#
# minLength validation tests
#
@pytest.mark.parametrize(
    'when',
    (
        '2011-08-18T10:29:47+03:00',
        '2017-01-02T12:34:56Z',
    ),
)
def test_date_time_format_validation(when):
    schema = {
        'format': 'date-time',
    }
    validator = generate_validator_from_schema(schema)

    validator(when)


@pytest.mark.parametrize(
    'when',
    (
        '2017-02-03',
    ),
)
def test_date_format_validation(when):
    schema = {
        'format': 'date',
    }
    validator = generate_validator_from_schema(schema)

    validator(when)


@pytest.mark.parametrize(
    'when',
    (
        'not-a-date-at-all',  # not a date at all
        '02-03-2017',
        '02/03/2017',
        '2017-02-0318T12:34:56',  # valid date-time, not date
    ),
)
def test_date_with_invalid_dates_strings(when):
    schema = {
        'format': 'date',
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(when)

    assert 'format' in err.value.messages[0]
    assert_error_message_equal(
        err.value.messages[0]['format'][0],
        MESSAGES['format']['invalid_date'],
    )


@pytest.mark.parametrize(
    'when',
    (
        'not-a-date-at-all',  # not a date at all
        '2011-13-18T10:29:47+03:00',  # Invalid month 13
        '2011-08-32T10:29:47+03:00',  # Invalid day 32
        '2011-08-18T25:29:47+03:00',  # Invalid hour 25
        '2011-08-18T10:65:47+03:00',  # Invalid minute 65
        '2011-08-18T10:29:65+03:00',  # Invalid second 65
        '2011-08-18T10:29:65+25:00',  # Invalid offset 25 hours
    ),
)
def test_date_time_with_invalid_dates_strings(when):
    schema = {
        'format': 'date-time',
    }
    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(when)

    assert 'format' in err.value.messages[0]
    assert_error_message_equal(
        err.value.messages[0]['format'][0],
        MESSAGES['format']['invalid_datetime'],
    )


def test_date_is_noop_when_not_present_or_required():
    schema = {
        'format': 'date',
    }
    validator = generate_validator_from_schema(schema)

    validator(EMPTY)


def test_date_time_is_noop_when_not_present_or_required():
    schema = {
        'format': 'date-time',
    }
    validator = generate_validator_from_schema(schema)

    validator(EMPTY)
