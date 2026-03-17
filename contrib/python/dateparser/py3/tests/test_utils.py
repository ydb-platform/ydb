import itertools
from datetime import datetime

import pytest
from parameterized import param, parameterized
from pytz import UnknownTimeZoneError, utc

from dateparser.conf import settings
from dateparser.utils import (
    apply_timezone,
    apply_timezone_from_settings,
    find_date_separator,
    get_last_day_of_month,
    get_next_leap_year,
    get_previous_leap_year,
    localize_timezone,
    registry,
)
from tests import BaseTestCase


class TestUtils(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.date_format = None
        self.result = None

    def given_date_format(self, date_format):
        self.date_format = date_format

    def when_date_separator_is_parsed(self):
        self.result = find_date_separator(self.date_format)

    def then_date_separator_is(self, sep):
        self.assertEqual(self.result, sep)

    @staticmethod
    def make_class_without_get_keys():
        class SomeClass:
            pass

        some_class = SomeClass
        return some_class

    @parameterized.expand(
        [
            param(date_format=fmt.format(sep=sep), expected_sep=sep)
            for (fmt, sep) in itertools.product(
                ["%d{sep}%m{sep}%Y", "%d{sep}%m{sep}%Y %H:%M"], ["/", ".", "-", ":"]
            )
        ]
    )
    def test_separator_extraction(self, date_format, expected_sep):
        self.given_date_format(date_format)
        self.when_date_separator_is_parsed()
        self.then_date_separator_is(expected_sep)

    @parameterized.expand(
        [
            param(datetime(2015, 12, 12), timezone="UTC", zone="UTC"),
            param(datetime(2015, 12, 12), timezone="Asia/Karachi", zone="Asia/Karachi"),
            param(datetime(2015, 12, 12, tzinfo=utc), timezone="UTC", zone="UTC"),
        ]
    )
    def test_localize_timezone_function(self, date, timezone, zone):
        tzaware_dt = localize_timezone(date, timezone)
        self.assertEqual(tzaware_dt.tzinfo.zone, zone)

    @parameterized.expand(
        [
            param(datetime(2015, 12, 12), timezone="UTB"),
            param(datetime(2015, 12, 12), timezone="Asia/Karach"),
        ]
    )
    def test_localize_timezone_function_raise_error(self, date, timezone):
        self.assertRaises(UnknownTimeZoneError, localize_timezone, date, timezone)

    @parameterized.expand(
        [
            param(datetime(2015, 12, 12), timezone="UTC+3", zone=r"UTC\+03:00"),
        ]
    )
    def test_localize_timezone_function_exception(self, date, timezone, zone):
        tzaware_dt = localize_timezone(date, timezone)
        self.assertEqual(tzaware_dt.tzinfo._StaticTzInfo__name, zone)

    @parameterized.expand(
        [
            param(
                datetime(2015, 12, 12, 10, 12),
                timezone="Asia/Karachi",
                expected=datetime(2015, 12, 12, 15, 12),
            ),
            param(
                datetime(2015, 12, 12, 10, 12),
                timezone="-0500",
                expected=datetime(2015, 12, 12, 5, 12),
            ),
        ]
    )
    def test_apply_timezone_function(self, date, timezone, expected):
        result = apply_timezone(date, timezone)
        result = result.replace(tzinfo=None)
        self.assertEqual(expected, result)

    @parameterized.expand(
        [
            param(
                datetime(2015, 12, 12, 10, 12),
                timezone="Asia/Karachi",
                expected=datetime(2015, 12, 12, 15, 12),
            ),
            param(
                datetime(2015, 12, 12, 10, 12),
                timezone="-0500",
                expected=datetime(2015, 12, 12, 5, 12),
            ),
        ]
    )
    def test_apply_timezone_from_settings_function(self, date, timezone, expected):
        result = apply_timezone_from_settings(
            date, settings.replace(**{"TO_TIMEZONE": timezone, "TIMEZONE": "UTC"})
        )
        self.assertEqual(expected, result)

    @parameterized.expand(
        [
            param(
                datetime(2015, 12, 12, 10, 12), expected=datetime(2015, 12, 12, 10, 12)
            ),
        ]
    )
    def test_apply_timezone_from_settings_function_none_settings(self, date, expected):
        result = apply_timezone_from_settings(date, None)
        self.assertEqual(expected, result)

    @parameterized.expand(
        [
            param(
                datetime(2015, 12, 12, 10, 12),
            ),
            param(
                datetime(2015, 12, 12, 10, 12),
            ),
        ]
    )
    def test_apply_timezone_from_settings_function_should_return_tz(self, date):
        result = apply_timezone_from_settings(
            date, settings.replace(**{"RETURN_AS_TIMEZONE_AWARE": True})
        )
        self.assertTrue(bool(result.tzinfo))

    def test_registry_when_get_keys_not_implemented(self):
        cl = self.make_class_without_get_keys()
        self.assertRaises(NotImplementedError, registry, cl)

    @parameterized.expand(
        [
            param(2111, 1, 31),
            param(1999, 2, 28),  # normal year
            param(1996, 2, 29),  # leap and not centurial year
            param(2000, 2, 29),  # leap and centurial year
            param(1700, 2, 28),  # no leap and centurial year (exception)
            param(2020, 3, 31),
            param(1987, 4, 30),
            param(1000, 5, 31),
            param(1534, 6, 30),
            param(1777, 7, 31),
            param(1234, 8, 31),
            param(1678, 9, 30),
            param(1947, 10, 31),
            param(2015, 11, 30),
            param(2300, 12, 31),
        ]
    )
    def test_get_last_day_of_month(self, year, month, expected_last_day):
        assert get_last_day_of_month(year, month) == expected_last_day


@pytest.mark.parametrize(
    "year,expected_previous_leap_year",
    [
        (2020, 2016),
        (2000, 1996),  # leap and centurial year
        (2104, 2096),  # missing no leap centurial year
        (1704, 1696),
        (2396, 2392),
        (0, -4),  # even if this is not a valid year, it is the expected result
    ],
)
def test_get_previous_leap_year(year, expected_previous_leap_year):
    assert get_previous_leap_year(year) == expected_previous_leap_year


@pytest.mark.parametrize(
    "year,expected_next_leap_year",
    [
        (2020, 2024),
        (1996, 2000),  # leap and centurial year
        (2096, 2104),  # missing no leap centurial year
        (1696, 1704),
        (2396, 2400),
        (0, 4),
    ],
)
def test_get_next_leap_year(year, expected_next_leap_year):
    assert get_next_leap_year(year) == expected_next_leap_year
