"""
Test cases for the isodatetime module.
"""

from datetime import datetime

import pytest

from isodate import (
    DATE_BAS_COMPLETE,
    DATE_BAS_ORD_COMPLETE,
    DATE_BAS_WEEK_COMPLETE,
    DATE_EXT_COMPLETE,
    DATE_EXT_ORD_COMPLETE,
    DATE_EXT_WEEK_COMPLETE,
    TIME_BAS_COMPLETE,
    TIME_BAS_MINUTE,
    TIME_EXT_COMPLETE,
    TIME_EXT_MINUTE,
    TZ_BAS,
    TZ_EXT,
    TZ_HOUR,
    UTC,
    FixedOffset,
    ISO8601Error,
    datetime_isoformat,
    parse_datetime,
)

# the following list contains tuples of ISO datetime strings and the expected
# result from the parse_datetime method. A result of None means an ISO8601Error
# is expected.
TEST_CASES = [
    (
        "19850412T1015",
        datetime(1985, 4, 12, 10, 15),
        DATE_BAS_COMPLETE + "T" + TIME_BAS_MINUTE,
        "19850412T1015",
    ),
    (
        "1985-04-12T10:15",
        datetime(1985, 4, 12, 10, 15),
        DATE_EXT_COMPLETE + "T" + TIME_EXT_MINUTE,
        "1985-04-12T10:15",
    ),
    (
        "1985102T1015Z",
        datetime(1985, 4, 12, 10, 15, tzinfo=UTC),
        DATE_BAS_ORD_COMPLETE + "T" + TIME_BAS_MINUTE + TZ_BAS,
        "1985102T1015Z",
    ),
    (
        "1985-102T10:15Z",
        datetime(1985, 4, 12, 10, 15, tzinfo=UTC),
        DATE_EXT_ORD_COMPLETE + "T" + TIME_EXT_MINUTE + TZ_EXT,
        "1985-102T10:15Z",
    ),
    (
        "1985W155T1015+0400",
        datetime(1985, 4, 12, 10, 15, tzinfo=FixedOffset(4, 0, "+0400")),
        DATE_BAS_WEEK_COMPLETE + "T" + TIME_BAS_MINUTE + TZ_BAS,
        "1985W155T1015+0400",
    ),
    (
        "1985-W15-5T10:15+04",
        datetime(
            1985,
            4,
            12,
            10,
            15,
            tzinfo=FixedOffset(4, 0, "+0400"),
        ),
        DATE_EXT_WEEK_COMPLETE + "T" + TIME_EXT_MINUTE + TZ_HOUR,
        "1985-W15-5T10:15+04",
    ),
    (
        "1985-W15-5T10:15-0430",
        datetime(
            1985,
            4,
            12,
            10,
            15,
            tzinfo=FixedOffset(-4, -30, "-0430"),
        ),
        DATE_EXT_WEEK_COMPLETE + "T" + TIME_EXT_MINUTE + TZ_BAS,
        "1985-W15-5T10:15-0430",
    ),
    (
        "1985-W15-5T10:15+04:45",
        datetime(
            1985,
            4,
            12,
            10,
            15,
            tzinfo=FixedOffset(4, 45, "+04:45"),
        ),
        DATE_EXT_WEEK_COMPLETE + "T" + TIME_EXT_MINUTE + TZ_EXT,
        "1985-W15-5T10:15+04:45",
    ),
    (
        "20110410T101225.123000Z",
        datetime(2011, 4, 10, 10, 12, 25, 123000, tzinfo=UTC),
        DATE_BAS_COMPLETE + "T" + TIME_BAS_COMPLETE + ".%f" + TZ_BAS,
        "20110410T101225.123000Z",
    ),
    (
        "2012-10-12T08:29:46.069178Z",
        datetime(2012, 10, 12, 8, 29, 46, 69178, tzinfo=UTC),
        DATE_EXT_COMPLETE + "T" + TIME_EXT_COMPLETE + ".%f" + TZ_BAS,
        "2012-10-12T08:29:46.069178Z",
    ),
    (
        "2012-10-12T08:29:46.691780Z",
        datetime(2012, 10, 12, 8, 29, 46, 691780, tzinfo=UTC),
        DATE_EXT_COMPLETE + "T" + TIME_EXT_COMPLETE + ".%f" + TZ_BAS,
        "2012-10-12T08:29:46.691780Z",
    ),
    (
        "2012-10-30T08:55:22.1234567Z",
        datetime(2012, 10, 30, 8, 55, 22, 123456, tzinfo=UTC),
        DATE_EXT_COMPLETE + "T" + TIME_EXT_COMPLETE + ".%f" + TZ_BAS,
        "2012-10-30T08:55:22.123456Z",
    ),
    (
        "2012-10-30T08:55:22.1234561Z",
        datetime(2012, 10, 30, 8, 55, 22, 123456, tzinfo=UTC),
        DATE_EXT_COMPLETE + "T" + TIME_EXT_COMPLETE + ".%f" + TZ_BAS,
        "2012-10-30T08:55:22.123456Z",
    ),
    (
        "2014-08-18 14:55:22.123456Z",
        None,
        DATE_EXT_COMPLETE + "T" + TIME_EXT_COMPLETE + ".%f" + TZ_BAS,
        "2014-08-18T14:55:22.123456Z",
    ),
]


@pytest.mark.parametrize("datetimestring, expected, format, output", TEST_CASES)
def test_parse(datetimestring, expected, format, output):
    """
    Parse an ISO datetime string and compare it to the expected value.
    """
    if expected is None:
        with pytest.raises(ISO8601Error):
            parse_datetime(datetimestring)
    else:
        assert parse_datetime(datetimestring) == expected


@pytest.mark.parametrize("datetimestring, expected, format, output", TEST_CASES)
def test_format(datetimestring, expected, format, output):
    """
    Take datetime object and create ISO string from it.
    This is the reverse test to test_parse.
    """
    if expected is None:
        with pytest.raises(AttributeError):
            datetime_isoformat(expected, format)
    else:
        assert datetime_isoformat(expected, format) == output
