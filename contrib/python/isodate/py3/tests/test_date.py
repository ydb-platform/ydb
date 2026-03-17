"""
Test cases for the isodate module.
"""

from datetime import date

import pytest

from isodate import (
    DATE_BAS_COMPLETE,
    DATE_BAS_MONTH,
    DATE_BAS_ORD_COMPLETE,
    DATE_BAS_WEEK,
    DATE_BAS_WEEK_COMPLETE,
    DATE_CENTURY,
    DATE_EXT_COMPLETE,
    DATE_EXT_MONTH,
    DATE_EXT_ORD_COMPLETE,
    DATE_EXT_WEEK,
    DATE_EXT_WEEK_COMPLETE,
    DATE_YEAR,
    ISO8601Error,
    date_isoformat,
    parse_date,
)

# the following list contains tuples of ISO date strings and the expected
# result from the parse_date method. A result of None means an ISO8601Error
# is expected. The test cases are grouped into dates with 4 digit years
# and 6 digit years.
TEST_CASES = {
    # yeardigits = 4
    (4, "19", date(1901, 1, 1), DATE_CENTURY),
    (4, "1985", date(1985, 1, 1), DATE_YEAR),
    (4, "1985-04", date(1985, 4, 1), DATE_EXT_MONTH),
    (4, "198504", date(1985, 4, 1), DATE_BAS_MONTH),
    (4, "1985-04-12", date(1985, 4, 12), DATE_EXT_COMPLETE),
    (4, "19850412", date(1985, 4, 12), DATE_BAS_COMPLETE),
    (4, "1985102", date(1985, 4, 12), DATE_BAS_ORD_COMPLETE),
    (4, "1985-102", date(1985, 4, 12), DATE_EXT_ORD_COMPLETE),
    (4, "1985W155", date(1985, 4, 12), DATE_BAS_WEEK_COMPLETE),
    (4, "1985-W15-5", date(1985, 4, 12), DATE_EXT_WEEK_COMPLETE),
    (4, "1985W15", date(1985, 4, 8), DATE_BAS_WEEK),
    (4, "1985-W15", date(1985, 4, 8), DATE_EXT_WEEK),
    (4, "1989-W15", date(1989, 4, 10), DATE_EXT_WEEK),
    (4, "1989-W15-5", date(1989, 4, 14), DATE_EXT_WEEK_COMPLETE),
    (4, "1-W1-1", None, DATE_BAS_WEEK_COMPLETE),
    # yeardigits = 6
    (6, "+0019", date(1901, 1, 1), DATE_CENTURY),
    (6, "+001985", date(1985, 1, 1), DATE_YEAR),
    (6, "+001985-04", date(1985, 4, 1), DATE_EXT_MONTH),
    (6, "+001985-04-12", date(1985, 4, 12), DATE_EXT_COMPLETE),
    (6, "+0019850412", date(1985, 4, 12), DATE_BAS_COMPLETE),
    (6, "+001985102", date(1985, 4, 12), DATE_BAS_ORD_COMPLETE),
    (6, "+001985-102", date(1985, 4, 12), DATE_EXT_ORD_COMPLETE),
    (6, "+001985W155", date(1985, 4, 12), DATE_BAS_WEEK_COMPLETE),
    (6, "+001985-W15-5", date(1985, 4, 12), DATE_EXT_WEEK_COMPLETE),
    (6, "+001985W15", date(1985, 4, 8), DATE_BAS_WEEK),
    (6, "+001985-W15", date(1985, 4, 8), DATE_EXT_WEEK),
}


@pytest.mark.parametrize("yeardigits,datestring,expected,_", TEST_CASES)
def test_parse(yeardigits, datestring, expected, _):
    if expected is None:
        with pytest.raises(ISO8601Error):
            parse_date(datestring, yeardigits)
    else:
        result = parse_date(datestring, yeardigits)
        assert result == expected


@pytest.mark.parametrize("yeardigits, datestring, expected, format", TEST_CASES)
def test_format(yeardigits, datestring, expected, format):
    """
    Take date object and create ISO string from it.
    This is the reverse test to test_parse.
    """
    if expected is None:
        with pytest.raises(AttributeError):
            date_isoformat(expected, format, yeardigits)
    else:
        assert date_isoformat(expected, format, yeardigits) == datestring
