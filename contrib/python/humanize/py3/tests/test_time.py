"""Tests for time humanizing."""

from __future__ import annotations

import datetime as dt
import typing

import pytest
from freezegun import freeze_time

import humanize
from humanize import time

ONE_DAY_DELTA = dt.timedelta(days=1)

# In seconds
ONE_MICROSECOND = 1 / 1000000
FOUR_MICROSECONDS = 4 / 1000000
ONE_MILLISECOND = 1 / 1000
FOUR_MILLISECONDS = 4 / 1000
MICROSECONDS_101_943 = 101943 / 1000000  # 101.94 milliseconds
MILLISECONDS_1_337 = 1337 / 1000  # 1.337 seconds
ONE_HOUR = 3600
ONE_DAY = 24 * ONE_HOUR
ONE_YEAR = 365.25 * ONE_DAY
FROZEN_DATE = "2010-02-02"

with freeze_time(FROZEN_DATE):
    NOW = dt.datetime.now()
    NOW_UTC = dt.datetime.now(tz=dt.timezone.utc)
    NOW_UTC_PLUS_01_00 = dt.datetime.now(tz=dt.timezone(offset=dt.timedelta(hours=1)))
    TODAY = dt.date.today()
    TOMORROW = TODAY + ONE_DAY_DELTA
    YESTERDAY = TODAY - ONE_DAY_DELTA


class FakeDate:
    def __init__(self, year: int, month: int, day: int) -> None:
        self.year, self.month, self.day = year, month, day

    def __str__(self) -> str:
        return f"{self.year}-{self.month}-{self.day}"


VALUE_ERROR_TEST = FakeDate(290149024, 2, 2)
OVERFLOW_ERROR_TEST = FakeDate(120390192341, 2, 2)


def assert_equal_datetime(dt1: dt.datetime, dt2: dt.datetime) -> None:
    assert (dt1 - dt2).seconds == 0


def assert_equal_timedelta(td1: dt.timedelta, td2: dt.timedelta) -> None:
    assert td1.days == td2.days
    assert td1.seconds == td2.seconds


# These are not considered "public" interfaces, but require tests anyway.


def test_date_and_delta() -> None:
    now = dt.datetime.now()
    td = dt.timedelta
    int_tests = (3, 29, 86399, 86400, 86401 * 30)
    date_tests = [now - td(seconds=x) for x in int_tests]
    td_tests = [td(seconds=x) for x in int_tests]
    results = [(now - td(seconds=x), td(seconds=x)) for x in int_tests]
    for t in (int_tests, date_tests, td_tests):
        for arg, result in zip(t, results):
            date, d = time._date_and_delta(arg)
            assert_equal_datetime(date, result[0])
            assert_equal_timedelta(d, result[1])
    assert time._date_and_delta("NaN") == (None, "NaN")


# Tests for the public interface of humanize.time


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (dt.timedelta(days=7), "7 days"),
        (dt.timedelta(days=31), "31 days"),
        (dt.timedelta(days=230), "230 days"),
        (dt.timedelta(days=400), "1 year, 35 days"),
    ],
)
def test_naturaldelta_nomonths(test_input: dt.timedelta, expected: str) -> None:
    assert humanize.naturaldelta(test_input, months=False) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (0, "a moment"),
        (1, "a second"),
        (23.5, "23 seconds"),
        (30, "30 seconds"),
        (dt.timedelta(microseconds=13), "a moment"),
        (dt.timedelta(minutes=1, seconds=29), "a minute"),
        (dt.timedelta(minutes=1, seconds=30), "2 minutes"),
        (dt.timedelta(minutes=1, seconds=59), "2 minutes"),
        (dt.timedelta(minutes=2), "2 minutes"),
        (dt.timedelta(minutes=59), "59 minutes"),
        (dt.timedelta(minutes=59, seconds=30), "an hour"),
        (dt.timedelta(hours=1, minutes=29), "an hour"),
        # Round to nearest, ties to even.
        # See https://en.wikipedia.org/wiki/IEEE_754#Rounding_rules
        (dt.timedelta(hours=1, minutes=30), "2 hours"),
        (dt.timedelta(hours=2, minutes=30), "2 hours"),
        (dt.timedelta(hours=3, minutes=30), "4 hours"),
        (dt.timedelta(hours=23, minutes=50, seconds=50), "a day"),
        (dt.timedelta(days=1), "a day"),
        (dt.timedelta(days=500), "1 year, 4 months"),
        (dt.timedelta(days=365 * 2 + 35), "2 years"),
        (dt.timedelta(seconds=1), "a second"),
        (dt.timedelta(seconds=30), "30 seconds"),
        (dt.timedelta(days=364), "a year"),
        (dt.timedelta(days=365 + 364), "2 years"),
        # regression tests for bugs in post-release humanize
        (dt.timedelta(days=10000), "27 years"),
        (dt.timedelta(days=365 + 35), "1 year, 1 month"),
        (dt.timedelta(days=365 * 2 + 65), "2 years"),
        (dt.timedelta(days=365 + 4), "1 year, 4 days"),
        (dt.timedelta(days=35), "a month"),
        (dt.timedelta(days=65), "2 months"),
        (dt.timedelta(days=9), "9 days"),
        (dt.timedelta(days=365), "a year"),
        (dt.timedelta(days=365 * 1_141), "1,141 years"),
        ("NaN", "NaN"),  # Returns non-numbers unchanged.
        # largest possible timedelta
        (dt.timedelta(days=999_999_999), "2,739,726 years"),
    ],
)
def test_naturaldelta(test_input: float | dt.timedelta, expected: str) -> None:
    assert humanize.naturaldelta(test_input) == expected
    if not isinstance(test_input, str):
        assert humanize.naturaldelta(-test_input) == expected


@freeze_time(FROZEN_DATE)
@pytest.mark.parametrize(
    "test_input, expected",
    [
        (NOW, "now"),
        (NOW - dt.timedelta(seconds=1), "a second ago"),
        (NOW - dt.timedelta(seconds=30), "30 seconds ago"),
        (NOW - dt.timedelta(minutes=1, seconds=29), "a minute ago"),
        (NOW - dt.timedelta(minutes=1, seconds=30), "2 minutes ago"),
        (NOW - dt.timedelta(minutes=2), "2 minutes ago"),
        (NOW - dt.timedelta(hours=1, minutes=29, seconds=30), "an hour ago"),
        (NOW - dt.timedelta(hours=1, minutes=30, seconds=30), "2 hours ago"),
        (NOW - dt.timedelta(hours=23, minutes=29, seconds=50), "23 hours ago"),
        (NOW - dt.timedelta(hours=23, minutes=50, seconds=50), "a day ago"),
        (NOW - dt.timedelta(days=1), "a day ago"),
        (NOW - dt.timedelta(days=500), "1 year, 4 months ago"),
        (NOW - dt.timedelta(days=365 * 2 + 35), "2 years ago"),
        (NOW + dt.timedelta(seconds=1), "a second from now"),
        (NOW + dt.timedelta(seconds=30), "30 seconds from now"),
        (NOW + dt.timedelta(minutes=1, seconds=29), "a minute from now"),
        (NOW + dt.timedelta(minutes=1, seconds=30), "2 minutes from now"),
        (NOW + dt.timedelta(minutes=2), "2 minutes from now"),
        (NOW + dt.timedelta(hours=1, minutes=29, seconds=30), "an hour from now"),
        (NOW + dt.timedelta(hours=1, minutes=30, seconds=30), "2 hours from now"),
        (NOW + dt.timedelta(hours=23, minutes=29, seconds=50), "23 hours from now"),
        (NOW + dt.timedelta(hours=23, minutes=50, seconds=50), "a day from now"),
        (NOW + dt.timedelta(days=1), "a day from now"),
        (NOW + dt.timedelta(days=500), "1 year, 4 months from now"),
        (NOW + dt.timedelta(days=365 * 2 + 35), "2 years from now"),
        # regression tests for bugs in post-release humanize
        (NOW + dt.timedelta(days=10000), "27 years from now"),
        (NOW - dt.timedelta(days=365 + 35), "1 year, 1 month ago"),
        (dt.timedelta(days=-10000), "27 years from now"),
        (dt.timedelta(days=365 + 35), "1 year, 1 month ago"),
        (22.5, "22 seconds ago"),
        (23.5, "24 seconds ago"),
        (23.9, "24 seconds ago"),
        (30, "30 seconds ago"),
        (NOW - dt.timedelta(days=365 * 2 + 65), "2 years ago"),
        (NOW - dt.timedelta(days=365 + 4), "1 year, 4 days ago"),
        ("NaN", "NaN"),
    ],
)
def test_naturaltime(
    test_input: dt.datetime | dt.timedelta | float, expected: str
) -> None:
    assert humanize.naturaltime(test_input) == expected


@freeze_time(FROZEN_DATE)
@pytest.mark.parametrize(
    "test_input, expected",
    [
        (NOW, "now"),
        (NOW - dt.timedelta(seconds=1), "a second ago"),
        (NOW - dt.timedelta(seconds=30), "30 seconds ago"),
        (NOW - dt.timedelta(minutes=1, seconds=29), "a minute ago"),
        (NOW - dt.timedelta(minutes=1, seconds=30), "2 minutes ago"),
        (NOW - dt.timedelta(minutes=2), "2 minutes ago"),
        (NOW - dt.timedelta(hours=1, minutes=29, seconds=30), "an hour ago"),
        (NOW - dt.timedelta(hours=1, minutes=30, seconds=30), "2 hours ago"),
        (NOW - dt.timedelta(hours=23, minutes=50, seconds=50), "a day ago"),
        (NOW - dt.timedelta(days=1), "a day ago"),
        (NOW - dt.timedelta(days=17), "17 days ago"),
        (NOW - dt.timedelta(days=47), "47 days ago"),
        (NOW - dt.timedelta(days=500), "1 year, 135 days ago"),
        (NOW - dt.timedelta(days=365 * 2 + 35), "2 years ago"),
        (NOW + dt.timedelta(seconds=1), "a second from now"),
        (NOW + dt.timedelta(seconds=30), "30 seconds from now"),
        (NOW + dt.timedelta(minutes=1, seconds=29), "a minute from now"),
        (NOW + dt.timedelta(minutes=1, seconds=30), "2 minutes from now"),
        (NOW + dt.timedelta(minutes=2), "2 minutes from now"),
        (NOW + dt.timedelta(hours=1, minutes=29, seconds=30), "an hour from now"),
        (NOW + dt.timedelta(hours=1, minutes=30, seconds=30), "2 hours from now"),
        (NOW + dt.timedelta(hours=23, minutes=29, seconds=50), "23 hours from now"),
        (NOW + dt.timedelta(hours=23, minutes=50, seconds=50), "a day from now"),
        (NOW + dt.timedelta(days=1), "a day from now"),
        (NOW + dt.timedelta(days=500), "1 year, 135 days from now"),
        (NOW + dt.timedelta(days=365 * 2 + 35), "2 years from now"),
        # regression tests for bugs in post-release humanize
        (NOW + dt.timedelta(days=10000), "27 years from now"),
        (NOW - dt.timedelta(days=365 + 35), "1 year, 35 days ago"),
        (dt.timedelta(days=-10000), "27 years from now"),
        (dt.timedelta(days=365 + 35), "1 year, 35 days ago"),
        (22.5, "22 seconds ago"),
        (23.5, "24 seconds ago"),
        (30, "30 seconds ago"),
        (NOW - dt.timedelta(days=365 * 2 + 65), "2 years ago"),
        (NOW - dt.timedelta(days=365 + 4), "1 year, 4 days ago"),
        ("NaN", "NaN"),
    ],
)
def test_naturaltime_nomonths(
    test_input: dt.datetime | dt.timedelta | float, expected: str
) -> None:
    assert humanize.naturaltime(test_input, months=False) == expected


@freeze_time(FROZEN_DATE)
@pytest.mark.parametrize(
    "test_args, expected",
    [
        ([TODAY], "today"),
        ([TOMORROW], "tomorrow"),
        ([YESTERDAY], "yesterday"),
        ([dt.date(TODAY.year, 3, 5)], "Mar 05"),
        (["02/26/1984"], "02/26/1984"),
        ([dt.date(1982, 6, 27), "%Y.%m.%d"], "1982.06.27"),
        ([None], "None"),
        (["Not a date at all."], "Not a date at all."),
        ([VALUE_ERROR_TEST], str(VALUE_ERROR_TEST)),
        ([OVERFLOW_ERROR_TEST], str(OVERFLOW_ERROR_TEST)),
    ],
)
def test_naturalday(test_args: list[typing.Any], expected: str) -> None:
    assert humanize.naturalday(*test_args) == expected


@freeze_time(FROZEN_DATE)
@pytest.mark.parametrize(
    "test_input, expected",
    [
        (TODAY, "today"),
        (TOMORROW, "tomorrow"),
        (YESTERDAY, "yesterday"),
        (dt.date(TODAY.year, 3, 5), "Mar 05"),
        (dt.date(1982, 6, 27), "Jun 27 1982"),
        (None, "None"),
        ("Not a date at all.", "Not a date at all."),
        (VALUE_ERROR_TEST, str(VALUE_ERROR_TEST)),
        (OVERFLOW_ERROR_TEST, str(OVERFLOW_ERROR_TEST)),
        (dt.date(2009, 2, 2), "Feb 02 2009"),
        (dt.date(2009, 3, 2), "Mar 02 2009"),
        (dt.date(2009, 4, 2), "Apr 02 2009"),
        (dt.date(2009, 5, 2), "May 02 2009"),
        (dt.date(2009, 6, 2), "Jun 02 2009"),
        (dt.date(2009, 7, 2), "Jul 02 2009"),
        (dt.date(2009, 8, 2), "Aug 02 2009"),
        (dt.date(2009, 9, 2), "Sep 02 2009"),
        (dt.date(2009, 10, 2), "Oct 02"),
        (dt.date(2009, 11, 2), "Nov 02"),
        (dt.date(2009, 12, 2), "Dec 02"),
        (dt.date(2010, 1, 2), "Jan 02"),
        (dt.date(2010, 2, 2), "today"),
        (dt.date(2010, 3, 2), "Mar 02"),
        (dt.date(2010, 4, 2), "Apr 02"),
        (dt.date(2010, 5, 2), "May 02"),
        (dt.date(2010, 6, 2), "Jun 02"),
        (dt.date(2010, 7, 2), "Jul 02"),
        (dt.date(2010, 8, 2), "Aug 02 2010"),
        (dt.date(2010, 9, 2), "Sep 02 2010"),
        (dt.date(2010, 10, 2), "Oct 02 2010"),
        (dt.date(2010, 11, 2), "Nov 02 2010"),
        (dt.date(2010, 12, 2), "Dec 02 2010"),
        (dt.date(2011, 1, 2), "Jan 02 2011"),
        (dt.date(2011, 2, 2), "Feb 02 2011"),
    ],
)
def test_naturaldate(test_input: dt.date, expected: str) -> None:
    assert humanize.naturaldate(test_input) == expected


@pytest.mark.parametrize(
    "seconds, expected",
    [
        (ONE_MICROSECOND, "a moment"),
        (FOUR_MICROSECONDS, "a moment"),
        (ONE_MILLISECOND, "a moment"),
        (FOUR_MILLISECONDS, "a moment"),
        (2, "2 seconds"),
        (4, "4 seconds"),
        (ONE_HOUR + FOUR_MILLISECONDS, "an hour"),
        (ONE_DAY + FOUR_MILLISECONDS, "a day"),
        (ONE_YEAR + FOUR_MICROSECONDS, "a year"),
    ],
)
def test_naturaldelta_minimum_unit_default(seconds: float, expected: str) -> None:
    # Arrange
    delta = dt.timedelta(seconds=seconds)

    # Act / Assert
    assert humanize.naturaldelta(delta) == expected


@pytest.mark.parametrize(
    "minimum_unit, seconds, expected",
    [
        ("seconds", ONE_MICROSECOND, "a moment"),
        ("seconds", FOUR_MICROSECONDS, "a moment"),
        ("seconds", ONE_MILLISECOND, "a moment"),
        ("seconds", FOUR_MILLISECONDS, "a moment"),
        ("seconds", MICROSECONDS_101_943, "a moment"),  # 0.10194 s
        ("seconds", MILLISECONDS_1_337, "a second"),  # 1.337 s
        ("seconds", 2, "2 seconds"),
        ("seconds", 4, "4 seconds"),
        ("seconds", ONE_HOUR + FOUR_MILLISECONDS, "an hour"),
        ("seconds", ONE_DAY + FOUR_MILLISECONDS, "a day"),
        ("seconds", ONE_YEAR + FOUR_MICROSECONDS, "a year"),
        ("milliseconds", FOUR_MICROSECONDS, "0 milliseconds"),
        ("milliseconds", ONE_MILLISECOND, "1 millisecond"),
        ("milliseconds", FOUR_MILLISECONDS, "4 milliseconds"),
        ("milliseconds", MICROSECONDS_101_943, "101 milliseconds"),  # 101.94 ms
        ("milliseconds", MILLISECONDS_1_337, "a second"),  # 1,337 ms
        ("milliseconds", 2, "2 seconds"),
        ("milliseconds", 4, "4 seconds"),
        ("milliseconds", ONE_HOUR + FOUR_MILLISECONDS, "an hour"),
        ("milliseconds", ONE_YEAR + FOUR_MICROSECONDS, "a year"),
        ("microseconds", ONE_MICROSECOND, "1 microsecond"),
        ("microseconds", FOUR_MICROSECONDS, "4 microseconds"),
        ("microseconds", FOUR_MILLISECONDS, "4 milliseconds"),
        ("microseconds", MICROSECONDS_101_943, "101 milliseconds"),  # 101,940 µs
        ("microseconds", MILLISECONDS_1_337, "a second"),  # 1,337,000 µs
        ("microseconds", 2, "2 seconds"),
        ("microseconds", 4, "4 seconds"),
        ("microseconds", ONE_HOUR + FOUR_MILLISECONDS, "an hour"),
        ("microseconds", ONE_DAY + FOUR_MILLISECONDS, "a day"),
        ("microseconds", ONE_YEAR + FOUR_MICROSECONDS, "a year"),
    ],
)
def test_naturaldelta_minimum_unit_explicit(
    minimum_unit: str, seconds: float, expected: str
) -> None:
    # Arrange
    delta = dt.timedelta(seconds=seconds)

    # Act / Assert
    assert humanize.naturaldelta(delta, minimum_unit=minimum_unit) == expected
    assert humanize.naturaldelta(seconds, minimum_unit=minimum_unit) == expected


@freeze_time(FROZEN_DATE)
@pytest.mark.parametrize(
    "seconds, expected",
    [
        (ONE_MICROSECOND, "now"),
        (FOUR_MICROSECONDS, "now"),
        (ONE_MILLISECOND, "now"),
        (FOUR_MILLISECONDS, "now"),
        (2, "2 seconds ago"),
        (4, "4 seconds ago"),
        (ONE_HOUR + FOUR_MILLISECONDS, "an hour ago"),
        (ONE_DAY + FOUR_MILLISECONDS, "a day ago"),
        (ONE_YEAR + FOUR_MICROSECONDS, "a year ago"),
    ],
)
def test_naturaltime_minimum_unit_default(seconds: float, expected: str) -> None:
    # Arrange
    datetime = NOW - dt.timedelta(seconds=seconds)

    # Act / Assert
    assert humanize.naturaltime(datetime) == expected


@freeze_time(FROZEN_DATE)
@pytest.mark.parametrize(
    "minimum_unit, seconds, expected",
    [
        ("seconds", ONE_MICROSECOND, "now"),
        ("seconds", FOUR_MICROSECONDS, "now"),
        ("seconds", ONE_MILLISECOND, "now"),
        ("seconds", FOUR_MILLISECONDS, "now"),
        ("seconds", MICROSECONDS_101_943, "now"),  # 0.10194 s
        ("seconds", MILLISECONDS_1_337, "a second ago"),  # 1.337 s
        ("seconds", 2, "2 seconds ago"),
        ("seconds", 4, "4 seconds ago"),
        ("seconds", ONE_HOUR + FOUR_MILLISECONDS, "an hour ago"),
        ("seconds", ONE_DAY + FOUR_MILLISECONDS, "a day ago"),
        ("seconds", ONE_YEAR + FOUR_MICROSECONDS, "a year ago"),
        ("milliseconds", FOUR_MICROSECONDS, "0 milliseconds ago"),
        ("milliseconds", ONE_MILLISECOND, "1 millisecond ago"),
        ("milliseconds", FOUR_MILLISECONDS, "4 milliseconds ago"),
        ("milliseconds", MICROSECONDS_101_943, "101 milliseconds ago"),  # 101.94 ms
        ("milliseconds", MILLISECONDS_1_337, "a second ago"),  # 1,337 ms
        ("milliseconds", 2, "2 seconds ago"),
        ("milliseconds", 4, "4 seconds ago"),
        ("milliseconds", ONE_HOUR + FOUR_MILLISECONDS, "an hour ago"),
        ("milliseconds", ONE_YEAR + FOUR_MICROSECONDS, "a year ago"),
        ("microseconds", ONE_MICROSECOND, "1 microsecond ago"),
        ("microseconds", FOUR_MICROSECONDS, "4 microseconds ago"),
        ("microseconds", FOUR_MILLISECONDS, "4 milliseconds ago"),
        ("microseconds", MICROSECONDS_101_943, "101 milliseconds ago"),  # 101,940 µs
        ("microseconds", MILLISECONDS_1_337, "a second ago"),  # 1,337,000 µs
        ("microseconds", 2, "2 seconds ago"),
        ("microseconds", 4, "4 seconds ago"),
        ("microseconds", ONE_HOUR + FOUR_MILLISECONDS, "an hour ago"),
        ("microseconds", ONE_DAY + FOUR_MILLISECONDS, "a day ago"),
        ("microseconds", ONE_YEAR + FOUR_MICROSECONDS, "a year ago"),
    ],
)
def test_naturaltime_minimum_unit_explicit(
    minimum_unit: str, seconds: float, expected: str
) -> None:
    # Arrange
    datetime = NOW - dt.timedelta(seconds=seconds)

    # Act / Assert
    assert humanize.naturaltime(datetime, minimum_unit=minimum_unit) == expected


@freeze_time(FROZEN_DATE)
@pytest.mark.parametrize(
    "test_input, expected",
    [
        (NOW_UTC, "now"),
        (NOW_UTC - dt.timedelta(seconds=1), "a second ago"),
        (NOW_UTC - dt.timedelta(seconds=30), "30 seconds ago"),
        (NOW_UTC - dt.timedelta(minutes=1, seconds=29), "a minute ago"),
        (NOW_UTC - dt.timedelta(minutes=1, seconds=30), "2 minutes ago"),
        (NOW_UTC - dt.timedelta(minutes=2), "2 minutes ago"),
        (NOW_UTC - dt.timedelta(hours=1, minutes=29, seconds=30), "an hour ago"),
        (NOW_UTC - dt.timedelta(hours=1, minutes=30, seconds=30), "2 hours ago"),
        (NOW_UTC - dt.timedelta(hours=23, minutes=29, seconds=50), "23 hours ago"),
        (NOW_UTC - dt.timedelta(hours=23, minutes=50, seconds=50), "a day ago"),
        (NOW_UTC - dt.timedelta(days=1), "a day ago"),
        (NOW_UTC - dt.timedelta(days=500), "1 year, 4 months ago"),
        (NOW_UTC - dt.timedelta(days=365 * 2 + 35), "2 years ago"),
        (NOW_UTC + dt.timedelta(seconds=1), "a second from now"),
        (NOW_UTC + dt.timedelta(seconds=30), "30 seconds from now"),
        (NOW_UTC + dt.timedelta(minutes=1, seconds=29), "a minute from now"),
        (NOW_UTC + dt.timedelta(minutes=1, seconds=30), "2 minutes from now"),
        (NOW_UTC + dt.timedelta(minutes=2), "2 minutes from now"),
        (NOW_UTC + dt.timedelta(hours=1, minutes=29, seconds=30), "an hour from now"),
        (NOW_UTC + dt.timedelta(hours=1, minutes=30, seconds=30), "2 hours from now"),
        (NOW_UTC + dt.timedelta(hours=23, minutes=29, seconds=50), "23 hours from now"),
        (NOW_UTC + dt.timedelta(hours=23, minutes=50, seconds=50), "a day from now"),
        (NOW_UTC + dt.timedelta(days=1), "a day from now"),
        (NOW_UTC + dt.timedelta(days=500), "1 year, 4 months from now"),
        (NOW_UTC + dt.timedelta(days=365 * 2 + 35), "2 years from now"),
        # regression tests for bugs in post-release humanize
        (NOW_UTC + dt.timedelta(days=10000), "27 years from now"),
        (NOW_UTC - dt.timedelta(days=365 + 35), "1 year, 1 month ago"),
        (NOW_UTC - dt.timedelta(days=365 * 2 + 65), "2 years ago"),
        (NOW_UTC - dt.timedelta(days=365 + 4), "1 year, 4 days ago"),
    ],
)
def test_naturaltime_timezone(test_input: dt.datetime, expected: str) -> None:
    assert humanize.naturaltime(test_input) == expected


@freeze_time(FROZEN_DATE)
@pytest.mark.parametrize(
    "test_input, expected",
    [
        (NOW_UTC, "now"),
        (NOW_UTC - dt.timedelta(seconds=1), "a second ago"),
        (NOW_UTC - dt.timedelta(seconds=30), "30 seconds ago"),
        (NOW_UTC - dt.timedelta(minutes=1, seconds=29), "a minute ago"),
        (NOW_UTC - dt.timedelta(minutes=1, seconds=30), "2 minutes ago"),
        (NOW_UTC - dt.timedelta(minutes=2), "2 minutes ago"),
        (NOW_UTC - dt.timedelta(hours=1, minutes=29, seconds=30), "an hour ago"),
        (NOW_UTC - dt.timedelta(hours=1, minutes=30, seconds=30), "2 hours ago"),
        (NOW_UTC - dt.timedelta(hours=23, minutes=29, seconds=50), "23 hours ago"),
        (NOW_UTC - dt.timedelta(hours=23, minutes=50, seconds=50), "a day ago"),
        (NOW_UTC - dt.timedelta(days=1), "a day ago"),
        (NOW_UTC - dt.timedelta(days=500), "1 year, 4 months ago"),
        (NOW_UTC - dt.timedelta(days=365 * 2 + 35), "2 years ago"),
        (NOW_UTC + dt.timedelta(seconds=1), "a second from now"),
        (NOW_UTC + dt.timedelta(seconds=30), "30 seconds from now"),
        (NOW_UTC + dt.timedelta(minutes=1, seconds=29), "a minute from now"),
        (NOW_UTC + dt.timedelta(minutes=1, seconds=30), "2 minutes from now"),
        (NOW_UTC + dt.timedelta(minutes=2), "2 minutes from now"),
        (NOW_UTC + dt.timedelta(hours=1, minutes=29, seconds=30), "an hour from now"),
        (NOW_UTC + dt.timedelta(hours=1, minutes=30, seconds=30), "2 hours from now"),
        (NOW_UTC + dt.timedelta(hours=23, minutes=29, seconds=50), "23 hours from now"),
        (NOW_UTC + dt.timedelta(hours=23, minutes=50, seconds=50), "a day from now"),
        (NOW_UTC + dt.timedelta(days=1), "a day from now"),
        (NOW_UTC + dt.timedelta(days=500), "1 year, 4 months from now"),
        (NOW_UTC + dt.timedelta(days=365 * 2 + 35), "2 years from now"),
        # regression tests for bugs in post-release humanize
        (NOW_UTC + dt.timedelta(days=10000), "27 years from now"),
        (NOW_UTC - dt.timedelta(days=365 + 35), "1 year, 1 month ago"),
        (NOW_UTC - dt.timedelta(days=365 * 2 + 65), "2 years ago"),
        (NOW_UTC - dt.timedelta(days=365 + 4), "1 year, 4 days ago"),
    ],
)
def test_naturaltime_timezone_when(test_input: dt.datetime, expected: str) -> None:
    assert humanize.naturaltime(test_input, when=NOW_UTC) == expected


@pytest.mark.parametrize(
    "val, min_unit, expected",
    [
        (dt.timedelta(microseconds=1), "microseconds", "1 microsecond"),
        (dt.timedelta(microseconds=2), "microseconds", "2 microseconds"),
        (dt.timedelta(microseconds=1000), "microseconds", "1 millisecond"),
        (dt.timedelta(microseconds=2000), "microseconds", "2 milliseconds"),
        (dt.timedelta(seconds=1), "seconds", "1 second"),
        (1, "seconds", "1 second"),
        (2, "seconds", "2 seconds"),
        (60, "seconds", "1 minute"),
        (120, "seconds", "2 minutes"),
        (3600, "seconds", "1 hour"),
        (3600 * 2, "seconds", "2 hours"),
        (3600 * 24, "seconds", "1 day"),
        (3600 * 24 * 2, "seconds", "2 days"),
        (3600 * 24 * 365, "seconds", "1 year"),
        (3600 * 24 * 365 * 2, "seconds", "2 years"),
        (3600 * 24 * 365 * 1_963, "seconds", "1,963 years"),
    ],
)
def test_precisedelta_one_unit_enough(
    val: dt.timedelta | float, min_unit: str, expected: str
) -> None:
    assert humanize.precisedelta(val, minimum_unit=min_unit) == expected


@pytest.mark.parametrize(
    "val, min_unit, expected",
    [
        (
            dt.timedelta(microseconds=1001),
            "microseconds",
            "1 millisecond and 1 microsecond",
        ),
        (
            dt.timedelta(microseconds=2002),
            "microseconds",
            "2 milliseconds and 2 microseconds",
        ),
        (
            dt.timedelta(seconds=1, microseconds=2),
            "microseconds",
            "1 second and 2 microseconds",
        ),
        (
            dt.timedelta(hours=4, seconds=3, microseconds=2),
            "microseconds",
            "4 hours, 3 seconds and 2 microseconds",
        ),
        (
            dt.timedelta(days=5, hours=4, seconds=3, microseconds=2),
            "microseconds",
            "5 days, 4 hours, 3 seconds and 2 microseconds",
        ),
        (
            dt.timedelta(days=370, hours=4, seconds=3, microseconds=2),
            "microseconds",
            "1 year, 5 days, 4 hours, 3 seconds and 2 microseconds",
        ),
        (
            dt.timedelta(days=370, microseconds=2),
            "microseconds",
            "1 year, 5 days and 2 microseconds",
        ),
        (
            dt.timedelta(days=370, seconds=2),
            "microseconds",
            "1 year, 5 days and 2 seconds",
        ),
        (
            dt.timedelta(seconds=0.01),
            "minutes",
            "0 minutes",
        ),
        (dt.timedelta(days=31), "seconds", "1 month"),
        (dt.timedelta(days=32), "seconds", "1 month and 1 day"),
        (dt.timedelta(days=62), "seconds", "2 months and 1 day"),
        (dt.timedelta(days=92), "seconds", "3 months"),
        (dt.timedelta(days=31), "days", "1 month"),
        (dt.timedelta(days=32), "days", "1 month and 1 day"),
        (dt.timedelta(days=62), "days", "2 months and 1 day"),
        (dt.timedelta(days=92), "days", "3 months"),
    ],
)
def test_precisedelta_multiple_units(
    val: dt.timedelta | float, min_unit: str, expected: str
) -> None:
    assert humanize.precisedelta(val, minimum_unit=min_unit) == expected


@pytest.mark.parametrize(
    "val, min_unit, fmt, expected",
    [
        (
            dt.timedelta(microseconds=1001),
            "milliseconds",
            "%0.4f",
            "1.0010 milliseconds",
        ),
        (
            dt.timedelta(microseconds=2002),
            "milliseconds",
            "%0.4f",
            "2.0020 milliseconds",
        ),
        (dt.timedelta(microseconds=2002), "milliseconds", "%0.2f", "2 milliseconds"),
        (
            dt.timedelta(seconds=1, microseconds=230000),
            "seconds",
            "%0.2f",
            "1.23 seconds",
        ),
        (
            dt.timedelta(hours=4, seconds=3, microseconds=200000),
            "seconds",
            "%0.2f",
            "4 hours and 3.20 seconds",
        ),
        (
            dt.timedelta(days=5, hours=4, seconds=30 * 60),
            "seconds",
            "%0.2f",
            "5 days, 4 hours and 30 minutes",
        ),
        (
            dt.timedelta(days=5, hours=4, seconds=30 * 60),
            "hours",
            "%0.2f",
            "5 days and 4.50 hours",
        ),
        (dt.timedelta(days=5, hours=4, seconds=30 * 60), "days", "%0.2f", "5.19 days"),
        # 1 month is 30.5 days but remainder is always rounded down.
        (dt.timedelta(days=31), "days", "%d", "1 month"),
        (dt.timedelta(days=31), "days", "%.0f", "1 month"),
        (dt.timedelta(days=32), "days", "%d", "1 month and 1 day"),
        (dt.timedelta(days=32), "days", "%.0f", "1 month and 1 day"),
        (dt.timedelta(days=62), "days", "%d", "2 months and 1 day"),
        (dt.timedelta(days=92), "days", "%d", "3 months"),
        (dt.timedelta(days=120), "months", "%0.2f", "3.93 months"),
        (dt.timedelta(days=183), "years", "%0.1f", "0.5 years"),
        (0.01, "seconds", "%0.3f", "0.010 seconds"),
        # 31 seconds will be truncated to 0 with %d and rounded to the nearest
        # number with %.0f, ie. 1
        (31, "minutes", "%d", "0 minutes"),
        (31, "minutes", "%0.0f", "1 minute"),
        (60 + 29.99, "minutes", "%d", "1 minute"),
        (60 + 29.99, "minutes", "%.0f", "1 minute"),
        (60 + 30, "minutes", "%d", "1 minute"),
        # 30 sec is 0.5 minutes. Round to nearest, ties away from zero.
        # See https://en.wikipedia.org/wiki/IEEE_754#Rounding_rules
        (60 + 30, "minutes", "%.0f", "2 minutes"),
        (60 * 60 + 30.99, "minutes", "%.0f", "1 hour"),
        (60 * 60 + 31, "minutes", "%.0f", "1 hour and 1 minute"),
        (
            ONE_DAY - MILLISECONDS_1_337,
            "seconds",
            "%.1f",
            "23 hours, 59 minutes and 58.7 seconds",
        ),
        (
            ONE_DAY - ONE_MILLISECOND,
            "seconds",
            "%.4f",
            "23 hours, 59 minutes and 59.9990 seconds",
        ),
        (91500, "hours", "%0.0f", "1 day and 1 hour"),
        # Because we use a format to round, we will end up with 9 hours.
        (9 * 60 * 60 - 1, "minutes", "%0.0f", "9 hours"),
        (dt.timedelta(days=30.99999), "minutes", "%0.0f", "1 month"),
        # We round at the hour. We end up with 12.5 hours. It's a tie, so round to the
        # nearest even number which is 12, thus we round down.
        (
            dt.timedelta(days=30.5 * 3, minutes=30),
            "hours",
            "%0.0f",
            "2 months, 30 days and 12 hours",
        ),
        (dt.timedelta(days=10, hours=6), "days", "%0.2f", "10.25 days"),
        (dt.timedelta(days=30.55), "days", "%0.1f", "30.6 days"),
        (dt.timedelta(microseconds=999.5), "microseconds", "%0.0f", "1 millisecond"),
        (dt.timedelta(milliseconds=999.5), "milliseconds", "%0.0f", "1 second"),
        (dt.timedelta(seconds=59.5), "seconds", "%0.0f", "1 minute"),
        (dt.timedelta(minutes=59.5), "minutes", "%0.0f", "1 hour"),
        (dt.timedelta(days=364), "months", "%0.0f", "1 year"),
    ],
)
def test_precisedelta_custom_format(
    val: dt.timedelta | float, min_unit: str, fmt: str, expected: str
) -> None:
    assert humanize.precisedelta(val, minimum_unit=min_unit, format=fmt) == expected


@pytest.mark.parametrize(
    "val, min_unit, suppress, expected",
    [
        (
            dt.timedelta(microseconds=1200),
            "microseconds",
            [],
            "1 millisecond and 200 microseconds",
        ),
        (
            dt.timedelta(microseconds=1200),
            "microseconds",
            ["milliseconds"],
            "1200 microseconds",
        ),
        (
            dt.timedelta(microseconds=1200),
            "microseconds",
            ["microseconds"],
            "1.20 milliseconds",
        ),
        (
            dt.timedelta(seconds=1, microseconds=200),
            "microseconds",
            ["seconds"],
            "1000 milliseconds and 200 microseconds",
        ),
        (
            dt.timedelta(seconds=1, microseconds=200000),
            "microseconds",
            ["milliseconds"],
            "1 second and 200000 microseconds",
        ),
        (
            dt.timedelta(seconds=1, microseconds=200000),
            "microseconds",
            ["milliseconds", "microseconds"],
            "1.20 seconds",
        ),
        (
            dt.timedelta(hours=4, seconds=30, microseconds=200),
            "microseconds",
            ["microseconds"],
            "4 hours, 30 seconds and 0.20 milliseconds",
        ),
        (
            dt.timedelta(hours=4, seconds=30, microseconds=200),
            "microseconds",
            ["seconds"],
            "4 hours, 30000 milliseconds and 200 microseconds",
        ),
        (
            dt.timedelta(hours=4, seconds=30, microseconds=200),
            "microseconds",
            ["seconds", "milliseconds"],
            "4 hours and 30000200 microseconds",
        ),
        (
            dt.timedelta(hours=4, seconds=30, microseconds=200),
            "microseconds",
            ["hours"],
            "240 minutes, 30 seconds and 200 microseconds",
        ),
        (
            dt.timedelta(hours=4, seconds=30, microseconds=200),
            "microseconds",
            ["hours", "seconds", "milliseconds", "microseconds"],
            "240.50 minutes",
        ),
    ],
)
def test_precisedelta_suppress_units(
    val: dt.timedelta | float, min_unit: str, suppress: list[str], expected: str
) -> None:
    assert (
        humanize.precisedelta(val, minimum_unit=min_unit, suppress=suppress) == expected
    )


def test_precisedelta_bogus_call() -> None:
    assert humanize.precisedelta(None) == "None"

    with pytest.raises(
        ValueError,
        match="Minimum unit is suppressed and no suitable replacement was found",
    ):
        humanize.precisedelta(1, minimum_unit="years", suppress=["years"])

    with pytest.raises(ValueError, match="Minimum unit 'years' not supported"):
        humanize.naturaldelta(1, minimum_unit="years")


def test_time_unit() -> None:
    years, minutes = time.Unit["YEARS"], time.Unit["MINUTES"]
    assert minutes < years
    assert years > minutes

    with pytest.raises(TypeError):
        _ = years < "foo"


@pytest.mark.parametrize(
    "fmt, value, expected",
    [
        ("%.2f", 1.011, 1.01),
        ("%.0f", 1.01, 1.0),
        ("%.0f", 1.5, 2.0),
        ("%10.0f", 1.01, 1.0),
        ("%i", 1.01, 1),
        # Surprising rounding with %d. It does not truncate for all values...
        ("%d", 1.999999999999999, 1),
        ("%d", 1.9999999999999999, 2),
    ],
)
def test_rounding_by_fmt(fmt: str, value: float, expected: float) -> None:
    assert time._rounding_by_fmt(fmt, value) == pytest.approx(expected)
