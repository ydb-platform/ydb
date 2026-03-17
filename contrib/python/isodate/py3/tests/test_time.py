"""
Test cases for the isotime module.
"""

from datetime import time

import pytest

from isodate import (
    TIME_BAS_COMPLETE,
    TIME_BAS_MINUTE,
    TIME_EXT_COMPLETE,
    TIME_EXT_MINUTE,
    TIME_HOUR,
    TZ_BAS,
    TZ_EXT,
    TZ_HOUR,
    UTC,
    FixedOffset,
    ISO8601Error,
    parse_time,
    time_isoformat,
)

# the following list contains tuples of ISO time strings and the expected
# result from the parse_time method. A result of None means an ISO8601Error
# is expected.
TEST_CASES = [
    ("232050", time(23, 20, 50), TIME_BAS_COMPLETE + TZ_BAS),
    ("23:20:50", time(23, 20, 50), TIME_EXT_COMPLETE + TZ_EXT),
    ("2320", time(23, 20), TIME_BAS_MINUTE),
    ("23:20", time(23, 20), TIME_EXT_MINUTE),
    ("23", time(23), TIME_HOUR),
    ("232050,5", time(23, 20, 50, 500000), None),
    ("23:20:50.5", time(23, 20, 50, 500000), None),
    # test precision
    ("15:33:42.123456", time(15, 33, 42, 123456), None),
    ("15:33:42.1234564", time(15, 33, 42, 123456), None),
    ("15:33:42.1234557", time(15, 33, 42, 123455), None),
    (
        "10:59:59.9999999Z",
        time(10, 59, 59, 999999, tzinfo=UTC),
        None,
    ),  # TIME_EXT_COMPLETE + TZ_EXT),
    ("2320,8", time(23, 20, 48), None),
    ("23:20,8", time(23, 20, 48), None),
    ("23,3", time(23, 18), None),
    ("232030Z", time(23, 20, 30, tzinfo=UTC), TIME_BAS_COMPLETE + TZ_BAS),
    ("2320Z", time(23, 20, tzinfo=UTC), TIME_BAS_MINUTE + TZ_BAS),
    ("23Z", time(23, tzinfo=UTC), TIME_HOUR + TZ_BAS),
    ("23:20:30Z", time(23, 20, 30, tzinfo=UTC), TIME_EXT_COMPLETE + TZ_EXT),
    ("23:20Z", time(23, 20, tzinfo=UTC), TIME_EXT_MINUTE + TZ_EXT),
    (
        "152746+0100",
        time(15, 27, 46, tzinfo=FixedOffset(1, 0, "+0100")),
        TIME_BAS_COMPLETE + TZ_BAS,
    ),
    (
        "152746-0500",
        time(15, 27, 46, tzinfo=FixedOffset(-5, 0, "-0500")),
        TIME_BAS_COMPLETE + TZ_BAS,
    ),
    (
        "152746+01",
        time(15, 27, 46, tzinfo=FixedOffset(1, 0, "+01:00")),
        TIME_BAS_COMPLETE + TZ_HOUR,
    ),
    (
        "152746-05",
        time(15, 27, 46, tzinfo=FixedOffset(-5, -0, "-05:00")),
        TIME_BAS_COMPLETE + TZ_HOUR,
    ),
    (
        "15:27:46+01:00",
        time(15, 27, 46, tzinfo=FixedOffset(1, 0, "+01:00")),
        TIME_EXT_COMPLETE + TZ_EXT,
    ),
    (
        "15:27:46-05:00",
        time(15, 27, 46, tzinfo=FixedOffset(-5, -0, "-05:00")),
        TIME_EXT_COMPLETE + TZ_EXT,
    ),
    (
        "15:27:46+01",
        time(15, 27, 46, tzinfo=FixedOffset(1, 0, "+01:00")),
        TIME_EXT_COMPLETE + TZ_HOUR,
    ),
    (
        "15:27:46-05",
        time(15, 27, 46, tzinfo=FixedOffset(-5, -0, "-05:00")),
        TIME_EXT_COMPLETE + TZ_HOUR,
    ),
    (
        "15:27:46-05:30",
        time(15, 27, 46, tzinfo=FixedOffset(-5, -30, "-05:30")),
        TIME_EXT_COMPLETE + TZ_EXT,
    ),
    (
        "15:27:46-0545",
        time(15, 27, 46, tzinfo=FixedOffset(-5, -45, "-0545")),
        TIME_EXT_COMPLETE + TZ_BAS,
    ),
    ("1:17:30", None, TIME_EXT_COMPLETE),
]


@pytest.mark.parametrize("timestring, expectation, format", TEST_CASES)
def test_parse(timestring, expectation, format):
    """
    Parse an ISO time string and compare it to the expected value.
    """
    if expectation is None:
        with pytest.raises(ISO8601Error):
            parse_time(timestring)
    else:
        assert parse_time(timestring) == expectation


@pytest.mark.parametrize("timestring, expectation, format", TEST_CASES)
def test_format(timestring, expectation, format):
    """
    Take time object and create ISO string from it.
    This is the reverse test to test_parse.
    """
    if expectation is None:
        with pytest.raises(AttributeError):
            time_isoformat(expectation, format)
    elif format is not None:
        assert time_isoformat(expectation, format) == timestring
