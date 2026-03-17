import pytest
import uuid

from flex.exceptions import ValidationError
from flex.formats import (
    date_time_format_validator,
    uuid_format_validator,
    int32_validator,
    int64_validator,
    email_validator,
)


#
# date_time_format_validator tests
#
@pytest.mark.parametrize(
    'value',
    (1, True, None, 2.0, [], {}),
)
def test_date_time_format_validator_skips_non_string_types(value):
    date_time_format_validator(value)


@pytest.mark.parametrize(
    'value',
    (
        # XXX: due transfering to iso8601 commented cases are valid
        # '2017',  # Not a full date
        # '2017-01-02T12:34:56',  # No TZ part
        '2011-13-18T10:29:47+03:00',  # Invalid month 13
        '2011-08-32T10:29:47+03:00',  # Invalid day 32
        '2011-08-18T25:29:47+03:00',  # Invalid hour 25
        '2011-08-18T10:65:47+03:00',  # Invalid minute 65
        '2011-08-18T10:29:65+03:00',  # Invalid second 65
        '2011-08-18T10:29:65+25:00',  # Invalid offset 25 hours
    )
)
def test_date_time_format_validator_detects_invalid_values(value):
    with pytest.raises(ValidationError):
        date_time_format_validator(value)


@pytest.mark.parametrize(
    'value',
    (
        '2011-08-18T10:29:47+03:00',
        '1985-04-12T23:20:50.52Z',
        '1996-12-19T16:39:57-08:00',
        # Leap second should be valid but strict_rfc339 doesn't correctly parse.
        pytest.param('1990-12-31T23:59:60Z', marks=pytest.mark.xfail),
        # Leap second should be valid but strict_rfc339 doesn't correctly parse.
        pytest.param('1990-12-31T15:59:60-08:00', marks=pytest.mark.xfail),
        # Weird netherlands time from strange 1909 law.
        '1937-01-01T12:00:27.87+00:20',
    )
)
def test_date_time_format_validator_with_valid_dateties(value):
    date_time_format_validator(value)


#
# uuid format tests
#
def test_uuid1_matches():
    for _ in range(100):
        uuid_format_validator(str(uuid.uuid1()))

def test_uuid3_matches():
    for _ in range(100):
        uuid_format_validator(str(uuid.uuid3(uuid.uuid4(), 'test-4')))
        uuid_format_validator(str(uuid.uuid3(uuid.uuid1(), 'test-1')))

def test_uuid4_matches():
    for _ in range(100):
        uuid_format_validator(str(uuid.uuid4()))

def test_uuid5_matches():
    for _ in range(100):
        uuid_format_validator(str(uuid.uuid5(uuid.uuid4(), 'test-4')))
        uuid_format_validator(str(uuid.uuid5(uuid.uuid1(), 'test-1')))


MAX_INT32 = 2 ** 31 - 1
MIN_INT32 = -1 * 2 ** 31
MAX_INT64 = 2 ** 63 - 1
MIN_INT64 = -1 * 2 ** 63


#
#  int32 and int64 format tests.
#
@pytest.mark.parametrize(
    'n',
    (MIN_INT32, -1, 0, 1, MAX_INT32),
)
def test_int32_with_in_range_number(n):
    int32_validator(n)


@pytest.mark.parametrize(
    'n',
    (MIN_INT32 - 1, MAX_INT32 + 1),
)
def test_int32_with_out_of_range_number(n):
    with pytest.raises(ValidationError):
        int32_validator(n)


@pytest.mark.parametrize(
    'n',
    (MIN_INT64, -1, 0, 1, MAX_INT64),
)
def test_int64_with_in_range_number(n):
    int64_validator(n)


@pytest.mark.parametrize(
    'n',
    (MIN_INT64 - 1, MAX_INT64 + 1),
)
def test_int64_with_out_of_range_number(n):
    with pytest.raises(ValidationError):
        int64_validator(n)


#
# email validators
#
@pytest.mark.parametrize(
    'email_address',
    (
        'test@example.com',
        'test+extra@example.com',
    ),
)
def test_email_validation_with_valid_email_addresses(email_address):
    email_validator(email_address)


@pytest.mark.parametrize(
    'email_address',
    (
        'example.com',
        'www.example.com',
        'not-an-email-address-at-all',
    ),
)
def test_email_validation_with_invalid_email_addresses(email_address):
    with pytest.raises(ValidationError):
        email_validator(email_address)
