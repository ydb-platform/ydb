#!/usr/bin/env python

import datetime as real_datetime
import os
import unittest
from collections import OrderedDict
from datetime import datetime, timedelta
from datetime import timezone as dttz
from itertools import product
from time import tzset
from unittest.mock import Mock, patch

import pytest
from parameterized import param, parameterized

import dateparser
from dateparser import date
from dateparser.conf import Settings
from dateparser.date import DateData
from tests import BaseTestCase


class TestDateRangeFunction(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.result = NotImplemented

    @parameterized.expand(
        [
            param(
                begin=datetime(2014, 6, 15),
                end=datetime(2014, 6, 25),
                expected_length=10,
            )
        ]
    )
    def test_date_range(self, begin, end, expected_length):
        self.when_date_range_generated(begin, end)
        self.then_range_length_is(expected_length)
        self.then_all_dates_in_range_are_present(begin, end)
        self.then_range_is_in_ascending_order()

    @parameterized.expand(
        [
            param(
                begin=datetime(2014, 4, 15),
                end=datetime(2014, 6, 25),
                expected_months=[(2014, 4), (2014, 5), (2014, 6)],
            ),
            param(
                begin=datetime(2014, 4, 25),
                end=datetime(2014, 5, 5),
                expected_months=[(2014, 4), (2014, 5)],
            ),
            param(
                begin=datetime(2014, 4, 5),
                end=datetime(2014, 4, 25),
                expected_months=[(2014, 4)],
            ),
            param(
                begin=datetime(2014, 4, 25),
                end=datetime(2014, 6, 5),
                expected_months=[(2014, 4), (2014, 5), (2014, 6)],
            ),
        ]
    )
    def test_one_date_for_each_month(self, begin, end, expected_months):
        self.when_date_range_generated(begin, end, months=1)
        self.then_expected_months_are(expected_months)

    @parameterized.expand(
        [
            "year",
            "month",
            "week",
            "day",
            "hour",
            "minute",
            "second",
        ]
    )
    def test_should_reject_easily_mistaken_dateutil_arguments(self, invalid_period):
        self.when_date_range_generated(
            begin=datetime(2014, 6, 15),
            end=datetime(2014, 6, 25),
            **{invalid_period: 1},
        )
        self.then_period_was_rejected(invalid_period)

    def when_date_range_generated(self, begin, end, **size):
        try:
            self.result = list(date.date_range(begin, end, **size))
        except Exception as error:
            self.error = error

    def then_expected_months_are(self, expected):
        self.assertEqual(expected, [(d.year, d.month) for d in self.result])

    def then_range_length_is(self, expected_length):
        self.assertEqual(expected_length, len(self.result))

    def then_all_dates_in_range_are_present(self, begin, end):
        date_under_test = begin
        while date_under_test < end:
            self.assertIn(date_under_test, self.result)
            date_under_test += timedelta(days=1)

    def then_range_is_in_ascending_order(self):
        for i in range(len(self.result) - 1):
            self.assertLess(self.result[i], self.result[i + 1])

    def then_period_was_rejected(self, period):
        self.then_error_was_raised(ValueError, ["Invalid argument: {}".format(period)])


class TestGetIntersectingPeriodsFunction(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.result = NotImplemented

    @parameterized.expand(
        [param(low=datetime(2014, 6, 15), high=datetime(2014, 6, 16), length=1)]
    )
    def test_date_arguments_and_date_range_with_default_post_days(
        self, low, high, length
    ):
        self.when_intersecting_period_calculated(low, high, period_size="day")
        self.then_all_dates_in_range_are_present(begin=low, end=high)
        self.then_date_range_length_is(length)

    @parameterized.expand(
        [
            param(
                low=datetime(2014, 4, 15),
                high=datetime(2014, 6, 25),
                expected_results=[
                    datetime(2014, 4, 1),
                    datetime(2014, 5, 1),
                    datetime(2014, 6, 1),
                ],
            ),
            param(
                low=datetime(2014, 4, 25),
                high=datetime(2014, 5, 5),
                expected_results=[datetime(2014, 4, 1), datetime(2014, 5, 1)],
            ),
            param(
                low=datetime(2014, 4, 5),
                high=datetime(2014, 4, 25),
                expected_results=[datetime(2014, 4, 1)],
            ),
            param(
                low=datetime(2014, 4, 25),
                high=datetime(2014, 6, 5),
                expected_results=[
                    datetime(2014, 4, 1),
                    datetime(2014, 5, 1),
                    datetime(2014, 6, 1),
                ],
            ),
            param(
                low=datetime(2014, 4, 25),
                high=datetime(2014, 4, 25),
                expected_results=[],
            ),
            param(
                low=datetime(2014, 12, 31),
                high=datetime(2015, 1, 1),
                expected_results=[datetime(2014, 12, 1)],
            ),
        ]
    )
    def test_dates_in_intersecting_period_should_use_first_day_when_period_is_month(
        self, low, high, expected_results
    ):
        self.when_intersecting_period_calculated(low, high, period_size="month")
        self.then_results_are(expected_results)

    @parameterized.expand(
        [
            param(
                low=datetime(2012, 4, 18),
                high=datetime(2014, 9, 22),
                expected_results=[
                    datetime(2012, 1, 1, 0, 0),
                    datetime(2013, 1, 1, 0, 0),
                    datetime(2014, 1, 1, 0, 0),
                ],
            ),
            param(
                low=datetime(2013, 8, 5),
                high=datetime(2014, 5, 15),
                expected_results=[
                    datetime(2013, 1, 1, 0, 0),
                    datetime(2014, 1, 1, 0, 0),
                ],
            ),
            param(
                low=datetime(2008, 4, 5),
                high=datetime(2010, 1, 1),
                expected_results=[
                    datetime(2008, 1, 1, 0, 0),
                    datetime(2009, 1, 1, 0, 0),
                ],
            ),
            param(
                low=datetime(2014, 1, 1),
                high=datetime(2016, 8, 22),
                expected_results=[
                    datetime(2014, 1, 1, 0, 0),
                    datetime(2015, 1, 1, 0, 0),
                    datetime(2016, 1, 1, 0, 0),
                ],
            ),
            param(
                low=datetime(2001, 7, 11),
                high=datetime(2001, 10, 16),
                expected_results=[datetime(2001, 1, 1, 0, 0)],
            ),
            param(
                low=datetime(2017, 1, 1), high=datetime(2017, 1, 1), expected_results=[]
            ),
        ]
    )
    def test_dates_in_intersecting_period_should_use_first_month_and_first_day_when_period_is_year(
        self, low, high, expected_results
    ):
        self.when_intersecting_period_calculated(low, high, period_size="year")
        self.then_results_are(expected_results)

    @parameterized.expand(
        [
            param(
                low=datetime(2014, 4, 15),
                high=datetime(2014, 5, 15),
                period_size="month",
                expected_results=[datetime(2014, 4, 1), datetime(2014, 5, 1)],
            ),
            param(
                low=datetime(2014, 10, 30, 4, 30),
                high=datetime(2014, 11, 7, 5, 20),
                period_size="week",
                expected_results=[datetime(2014, 10, 27), datetime(2014, 11, 3)],
            ),
            param(
                low=datetime(2014, 8, 13, 13, 21),
                high=datetime(2014, 8, 14, 14, 7),
                period_size="day",
                expected_results=[datetime(2014, 8, 13), datetime(2014, 8, 14)],
            ),
            param(
                low=datetime(2014, 5, 11, 22, 4),
                high=datetime(2014, 5, 12, 0, 5),
                period_size="hour",
                expected_results=[
                    datetime(2014, 5, 11, 22, 0),
                    datetime(2014, 5, 11, 23, 0),
                    datetime(2014, 5, 12, 0, 0),
                ],
            ),
            param(
                low=datetime(2014, 4, 25, 11, 11, 11),
                high=datetime(2014, 4, 25, 11, 12, 11),
                period_size="minute",
                expected_results=[
                    datetime(2014, 4, 25, 11, 11, 0),
                    datetime(2014, 4, 25, 11, 12, 0),
                ],
            ),
            param(
                low=datetime(2014, 12, 31, 23, 59, 58, 500),
                high=datetime(2014, 12, 31, 23, 59, 59, 600),
                period_size="second",
                expected_results=[
                    datetime(2014, 12, 31, 23, 59, 58, 0),
                    datetime(2014, 12, 31, 23, 59, 59, 0),
                ],
            ),
        ]
    )
    def test_periods(self, low, high, period_size, expected_results):
        self.when_intersecting_period_calculated(low, high, period_size=period_size)
        self.then_results_are(expected_results)

    @parameterized.expand(
        [
            param("years"),
            param("months"),
            param("days"),
            param("hours"),
            param("minutes"),
            param("seconds"),
            param("microseconds"),
            param("some_period"),
        ]
    )
    def test_should_reject_easily_mistaken_dateutil_arguments(self, period_size):
        self.when_intersecting_period_calculated(
            low=datetime(2014, 6, 15),
            high=datetime(2014, 6, 25),
            period_size=period_size,
        )
        self.then_error_was_raised(ValueError, ["Invalid period: " + str(period_size)])

    @parameterized.expand(
        [
            param(
                low=datetime(2014, 4, 15),
                high=datetime(2014, 4, 14),
                period_size="month",
            ),
            param(
                low=datetime(2014, 4, 25),
                high=datetime(2014, 4, 25),
                period_size="month",
            ),
        ]
    )
    def test_empty_period(self, low, high, period_size):
        self.when_intersecting_period_calculated(low, high, period_size)
        self.then_period_is_empty()

    def when_intersecting_period_calculated(self, low, high, period_size):
        try:
            self.result = list(
                date.get_intersecting_periods(low, high, period=period_size)
            )
        except Exception as error:
            self.error = error

    def then_results_are(self, expected_results):
        self.assertEqual(expected_results, self.result)

    def then_date_range_length_is(self, size):
        self.assertEqual(size, len(self.result))

    def then_all_dates_in_range_are_present(self, begin, end):
        date_under_test = begin
        while date_under_test < end:
            self.assertIn(date_under_test, self.result)
            date_under_test += timedelta(days=1)

    def then_period_is_empty(self):
        self.assertEqual([], self.result)


class TestParseWithFormatsFunction(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.result = NotImplemented

    @parameterized.expand(
        [
            param(date_string="yesterday", date_formats=["%Y-%m-%d"]),
        ]
    )
    def test_date_with_not_matching_format_is_not_parsed(
        self, date_string, date_formats
    ):
        self.when_date_is_parsed_with_formats(date_string, date_formats)
        self.then_date_was_not_parsed()

    @parameterized.expand(
        [
            param(
                date_string="25-03-14",
                date_formats=["%d-%m-%y"],
                expected_result=datetime(2014, 3, 25),
            ),
        ]
    )
    def test_should_parse_date(self, date_string, date_formats, expected_result):
        self.when_date_is_parsed_with_formats(date_string, date_formats)
        self.then_date_was_parsed()
        self.then_parsed_period_is("day")
        self.then_parsed_date_is(expected_result)

    @parameterized.expand(
        [
            param(
                date_string="09.16",
                date_formats=["%m.%d"],
                expected_month=9,
                expected_day=16,
            ),
        ]
    )
    def test_should_use_current_year_for_dates_without_year(
        self, date_string, date_formats, expected_month, expected_day
    ):
        self.given_now(2015, 2, 4)
        self.when_date_is_parsed_with_formats(date_string, date_formats)
        self.then_date_was_parsed()
        self.then_parsed_period_is("day")
        self.then_parsed_date_is(datetime(2015, expected_month, expected_day))

    @parameterized.expand(
        [
            param(
                date_string="August 2014",
                date_formats=["%B %Y"],
                expected_year=2014,
                expected_month=8,
                today_day=12,
                prefer_day_of_month="first",
                expected_day=1,
            ),
            param(
                date_string="August 2014",
                date_formats=["%B %Y"],
                expected_year=2014,
                expected_month=8,
                today_day=12,
                prefer_day_of_month="last",
                expected_day=31,
            ),
            param(
                date_string="August 2014",
                date_formats=["%B %Y"],
                expected_year=2014,
                expected_month=8,
                today_day=12,
                prefer_day_of_month="current",
                expected_day=12,
            ),
        ]
    )
    def test_should_use_correct_day_from_settings_for_dates_without_day(
        self,
        date_string,
        date_formats,
        expected_year,
        expected_month,
        today_day,
        prefer_day_of_month,
        expected_day,
    ):
        self.given_now(2014, 8, today_day)
        settings_mod = Settings()
        settings_mod.PREFER_DAY_OF_MONTH = prefer_day_of_month
        self.when_date_is_parsed_with_formats(date_string, date_formats, settings_mod)
        self.then_date_was_parsed()
        self.then_parsed_period_is("month")
        self.then_parsed_date_is(
            datetime(year=expected_year, month=expected_month, day=expected_day)
        )

    @parameterized.expand(
        [
            param(
                date_string="2014",
                date_formats=["%Y"],
                expected_year=2014,
                prefer_month_of_year="first",
                current_month=7,
                expected_month=1,
                expected_day=1,
            ),
            param(
                date_string="2014",
                date_formats=["%Y"],
                expected_year=2014,
                prefer_month_of_year="current",
                current_month=7,
                expected_month=7,
                expected_day=1,
            ),
            param(
                date_string="2014",
                date_formats=["%Y"],
                expected_year=2014,
                prefer_month_of_year="last",
                current_month=7,
                expected_month=12,
                expected_day=1,
            ),
        ]
    )
    def test_should_use_correct_month_from_settings_for_dates_without_month(
        self,
        date_string,
        date_formats,
        expected_year,
        prefer_month_of_year,
        current_month,
        expected_month,
        expected_day,
    ):
        self.given_now(2014, current_month, 1)
        settings_mod = Settings()
        settings_mod.PREFER_MONTH_OF_YEAR = prefer_month_of_year
        self.when_date_is_parsed_with_formats(date_string, date_formats, settings_mod)
        self.then_date_was_parsed()
        self.then_parsed_period_is("year")
        self.then_parsed_date_is(
            datetime(year=expected_year, month=expected_month, day=expected_day)
        )

    @parameterized.expand(
        [
            param(
                date_string="2014",
                date_formats=["%Y"],
                current_day=15,
                current_month=4,
                prefer_day_of_month="last",
                prefer_month_of_year="last",
                expected_year=2014,
                expected_month=12,
                expected_day=31,
            )
        ]
    )
    def test_should_use_correct_day_n_month_from_settings_for_dates_without_day_n_month(
        self,
        date_string,
        date_formats,
        current_day,
        current_month,
        prefer_day_of_month,
        prefer_month_of_year,
        expected_year,
        expected_month,
        expected_day,
    ):
        self.given_now(2014, current_month, current_day)
        settings_mod = Settings()
        settings_mod.PREFER_DAY_OF_MONTH = prefer_day_of_month
        settings_mod.PREFER_MONTH_OF_YEAR = prefer_month_of_year
        self.when_date_is_parsed_with_formats(date_string, date_formats, settings_mod)
        self.then_date_was_parsed()
        self.then_parsed_period_is("year")
        self.then_parsed_date_is(
            datetime(year=expected_year, month=expected_month, day=expected_day)
        )

    def given_now(self, year, month, day, **time):
        now = datetime(year, month, day, **time)
        datetime_mock = Mock(wraps=datetime)
        datetime_mock.utcnow = Mock(return_value=now)
        datetime_mock.now = Mock(return_value=now)
        datetime_mock.today = Mock(return_value=now)
        self.add_patch(patch("dateparser.date.datetime", new=datetime_mock))
        self.add_patch(patch("dateparser.utils.datetime", new=datetime_mock))

    def when_date_is_parsed_with_formats(
        self, date_string, date_formats, custom_settings=None
    ):
        self.result = date.parse_with_formats(
            date_string, date_formats, custom_settings or Settings()
        )

    def then_date_was_not_parsed(self):
        self.assertIsNotNone(self.result)
        self.assertIsNone(self.result["date_obj"])

    def then_date_was_parsed(self):
        self.assertIsNotNone(self.result)
        self.assertIsNotNone(self.result["date_obj"])

    def then_parsed_date_is(self, date_obj):
        self.assertEqual(date_obj.date(), self.result["date_obj"].date())

    def then_parsed_period_is(self, period):
        self.assertEqual(period, self.result["period"])


class TestDateDataParser(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.parser = NotImplemented
        self.result = NotImplemented
        self.multiple_results = NotImplemented

    @parameterized.expand(
        [
            param("10:04am EDT"),
        ]
    )
    def test_time_without_date_should_use_today(self, date_string):
        self.given_parser(settings={"RELATIVE_BASE": datetime(2020, 7, 19)})
        self.when_date_string_is_parsed(date_string)
        self.then_date_was_parsed()
        self.then_parsed_date_is(datetime(2020, 7, 19).date())

    @parameterized.expand(
        [
            # Today
            param("today", days_ago=0),
            param("Today", days_ago=0),
            param("TODAY", days_ago=0),
            param("Сегодня", days_ago=0),
            param("Hoje", days_ago=0),
            param("Oggi", days_ago=0),
            # Yesterday
            param("yesterday", days_ago=1),
            param(" Yesterday \n", days_ago=1),
            param("Ontem", days_ago=1),
            param("Ieri", days_ago=1),
            param("вчера", days_ago=1),
            param("снощи", days_ago=1),
            # Day before yesterday
            param("the day before yesterday", days_ago=2),
            param("The DAY before Yesterday", days_ago=2),
            param("Anteontem", days_ago=2),
            param("Avant-hier", days_ago=2),
            param("вчера", days_ago=1),
            param("снощи", days_ago=1),
        ]
    )
    def test_temporal_nouns_are_parsed(self, date_string, days_ago):
        self.given_parser()
        self.when_date_string_is_parsed(date_string)
        self.then_date_was_parsed()
        self.then_date_is_n_days_ago(days=days_ago)

    def test_should_not_assume_language_too_early(self):
        dates_to_parse = OrderedDict(
            [
                ("07/07/2014", datetime(2014, 7, 7).date()),  # any language
                ("07.jul.2014 | 12:52", datetime(2014, 7, 7).date()),  # en, es, pt, nl
                ("07.ago.2014 | 12:52", datetime(2014, 8, 7).date()),  # es, it, pt
                (
                    "07.feb.2014 | 12:52",
                    datetime(2014, 2, 7).date(),
                ),  # en, de, es, it, nl, ro
                ("07.ene.2014 | 12:52", datetime(2014, 1, 7).date()),
            ]
        )  # es

        self.given_parser(
            restrict_to_languages=["en", "de", "fr", "it", "pt", "nl", "ro", "es", "ru"]
        )
        self.when_multiple_dates_are_parsed(dates_to_parse.keys())
        self.then_all_results_were_parsed()
        self.then_parsed_dates_are(list(dates_to_parse.values()))

    @parameterized.expand(
        [
            param(date_string="11 Marzo, 2014", locale="es"),
            param(date_string="13 Septiembre, 2014", locale="es"),
            param(date_string="Сегодня", locale="ru"),
            param(date_string="Avant-hier", locale="fr"),
            param(date_string="Anteontem", locale="pt"),
            param(date_string="ธันวาคม 11, 2014, 08:55:08 PM", locale="th"),
            param(date_string="Anteontem", locale="pt"),
            param(date_string="14 aprilie 2014", locale="ro"),
            param(date_string="11 Ağustos, 2014", locale="tr"),
            param(date_string="pon 16. čer 2014 10:07:43", locale="cs"),
            param(date_string="24 януари 2015г.", locale="bg"),
        ]
    )
    def test_returned_detected_locale_should_be(self, date_string, locale):
        self.given_parser()
        self.when_date_string_is_parsed(date_string)
        self.then_detected_locale(locale)

    def test_try_previous_locales_false(self):
        self.given_parser(try_previous_locales=False)
        self.when_date_string_is_parsed("Mañana")  # es
        self.then_detected_locale("es")
        self.when_date_string_is_parsed("2020-05-01")
        self.then_detected_locale("en")

    def test_try_previous_locales_true(self):
        self.given_parser(try_previous_locales=True)
        self.when_date_string_is_parsed("Mañana")  # es
        self.then_detected_locale("es")
        self.when_date_string_is_parsed("2020-05-01")
        self.then_detected_locale("es")

    def test_try_previous_locales_order_deterministic(self):
        self.given_parser(try_previous_locales=True)
        self.when_date_string_is_parsed("Mañana")  # es
        self.then_detected_locale("es")
        self.when_date_string_is_parsed("Понедельник")  # ru
        self.then_detected_locale("ru")
        self.then_previous_locales_is(["es", "ru"])

    @parameterized.expand(
        [
            param("2014-10-09T17:57:39+00:00"),
        ]
    )
    def test_get_date_data_should_not_strip_timezone_info(self, date_string):
        self.given_parser()
        self.when_date_string_is_parsed(date_string)
        self.then_date_was_parsed()
        self.then_parsed_date_has_timezone()

    @parameterized.expand(
        [
            param(
                date_string="14 giu 13",
                date_formats=["%y %B %d"],
                expected_result=datetime(2014, 6, 13),
            ),
            param(
                date_string="14_luglio_15",
                date_formats=["%y_%B_%d"],
                expected_result=datetime(2014, 7, 15),
            ),
            param(
                date_string="14_LUGLIO_15",
                date_formats=["%y_%B_%d"],
                expected_result=datetime(2014, 7, 15),
            ),
            param(
                date_string="10.01.2016, 20:35",
                date_formats=["%d.%m.%Y, %H:%M"],
                expected_result=datetime(2016, 1, 10, 20, 35),
            ),
        ]
    )
    def test_parse_date_using_format(self, date_string, date_formats, expected_result):
        self.given_local_tz_offset(0)
        self.given_parser()
        self.when_date_string_is_parsed(date_string, date_formats)
        self.then_date_was_parsed()
        self.then_period_is("day")
        self.then_parsed_datetime_is(expected_result)

    @parameterized.expand(
        [
            param(
                date_string="11/09/2007", date_formats={"date_formats": ["%d/%m/%Y"]}
            ),
            param(date_string="16.09.03 11:55", date_formats=111),
            param(date_string="08-01-1998", date_formats=12.56),
        ]
    )
    def test_parsing_date_using_invalid_type_date_format_must_raise_error(
        self, date_string, date_formats
    ):
        self.given_local_tz_offset(0)
        self.given_parser()
        self.when_date_string_is_parsed(date_string, date_formats)
        self.then_error_was_raised(
            TypeError,
            [
                "Date formats should be list, tuple or set of strings",
                "'{}' object is not iterable".format(type(date_formats).__name__),
            ],
        )

    @parameterized.expand(
        [
            param(date_string={"date": "12/11/1998"}),
            param(date_string=[2017, 12, 1]),
            param(date_string=2018),
            param(date_string=12.2000),
            param(date_string=datetime(year=2009, month=12, day=7)),
        ]
    )
    def test_parsing_date_using_invalid_type_date_string_must_raise_error(
        self, date_string
    ):
        self.given_parser()
        self.when_date_string_is_parsed(date_string)
        self.then_error_was_raised(TypeError, ["Input type must be str"])

    @parameterized.expand(
        [
            param(
                date_string="2014/11/17 14:56 EDT",
                expected_result=datetime(2014, 11, 17, 18, 56),
            ),
        ]
    )
    def test_parse_date_with_timezones_not_using_formats(
        self, date_string, expected_result
    ):
        self.given_parser(settings={"TO_TIMEZONE": "UTC"})
        self.when_date_string_is_parsed(date_string)
        self.then_date_was_parsed()
        self.then_period_is("day")
        self.result["date_obj"] = self.result["date_obj"].replace(tzinfo=None)
        self.then_parsed_datetime_is(expected_result)

    @parameterized.expand(
        [
            param(
                date_string="2014/11/17 14:56 EDT",
                date_formats=["%Y/%m/%d %H:%M EDT"],
                expected_result=datetime(2014, 11, 17, 14, 56),
            ),
        ]
    )
    def test_parse_date_with_timezones_using_formats_ignore_timezone(
        self, date_string, date_formats, expected_result
    ):
        self.given_local_tz_offset(0)
        self.given_parser()
        self.when_date_string_is_parsed(date_string, date_formats)
        self.then_date_was_parsed()
        self.then_period_is("day")
        self.then_parsed_datetime_is(expected_result)

    @parameterized.expand(
        [
            param(
                date_string="08-08-2014\xa018:29",
                expected_result=datetime(2014, 8, 8, 18, 29),
            ),
        ]
    )
    def test_should_parse_with_no_break_space_in_dates(
        self, date_string, expected_result
    ):
        self.given_parser()
        self.when_date_string_is_parsed(date_string)
        self.then_date_was_parsed()
        self.then_period_is("day")
        self.then_parsed_datetime_is(expected_result)

    @parameterized.expand(
        [
            param(
                date_string="12 jan 1876",
                expected_result=(datetime(1876, 1, 12, 0, 0), "day", "en"),
            ),
            param(
                date_string="02/09/16",
                expected_result=(datetime(2016, 2, 9, 0, 0), "day", "en"),
            ),
            param(
                date_string="10 giu 2018",
                expected_result=(datetime(2018, 6, 10, 0, 0), "day", "it"),
            ),
        ]
    )
    def test_get_date_tuple(self, date_string, expected_result):
        self.given_parser()
        self.when_get_date_tuple_is_called(date_string)
        self.then_returned_tuple_is(expected_result)

    @parameterized.expand(
        [
            param(
                "Mo",
                datetime(2025, 7, 28, 0, 0),
            ),
            param(
                "Tu",
                datetime(2025, 7, 29, 0, 0),
            ),
            param(
                "We",
                datetime(2025, 7, 30, 0, 0),
            ),
            param(
                "Th",
                datetime(2025, 7, 31, 0, 0),
            ),
            param(
                "Fr",
                datetime(2025, 8, 1, 0, 0),
            ),
            param(
                "Sa",
                datetime(2025, 7, 26, 0, 0),
            ),
            param(
                "Su",
                datetime(2025, 7, 27, 0, 0),
            ),
        ]
    )
    def test_short_weekday_names(self, date_string, expected):
        if "Mo" in date_string:
            pytest.xfail(
                "Known bug: 'Mo' is being interpreted as a month instead of a weekday and needs to be fixed."
            )

        self.given_parser(["en"])
        self.given_now(2025, 8, 1)
        self.when_date_string_is_parsed(date_string)
        self.then_parsed_datetime_is(expected)

    def given_now(self, year, month, day, **time):
        now = real_datetime.datetime(year, month, day, **time)

        # Patch the datetime *class* in each target module
        class DateParserDateTime(real_datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                return now.replace(tzinfo=tz) if tz else now

            @classmethod
            def utcnow(cls):
                return now

            @classmethod
            def today(cls):
                return now

        self.add_patch(patch("dateparser.date.datetime", DateParserDateTime))
        self.add_patch(patch("dateparser.parser.datetime", DateParserDateTime))

    def given_parser(self, restrict_to_languages=None, **params):
        self.parser = date.DateDataParser(languages=restrict_to_languages, **params)

    def given_local_tz_offset(self, offset):
        self.add_patch(
            patch.object(
                dateparser.timezone_parser,
                "local_tz_offset",
                new=timedelta(seconds=3600 * offset),
            )
        )

    def when_date_string_is_parsed(self, date_string, date_formats=None):
        try:
            self.result = self.parser.get_date_data(date_string, date_formats)
        except Exception as error:
            self.error = error

    def when_multiple_dates_are_parsed(self, date_strings):
        self.multiple_results = []
        for date_string in date_strings:
            try:
                result = self.parser.get_date_data(date_string)
            except Exception as error:
                result = error
            finally:
                self.multiple_results.append(result)

    def when_get_date_tuple_is_called(self, date_string):
        self.result = self.parser.get_date_tuple(date_string)

    def then_date_was_parsed(self):
        self.assertIsNotNone(self.result["date_obj"])

    def then_date_locale(self):
        self.assertIsNotNone(self.result["locale"])

    def then_date_is_n_days_ago(self, days):
        today = datetime.now().date()
        expected_date = today - timedelta(days=days)
        self.assertEqual(expected_date, self.result["date_obj"].date())

    def then_all_results_were_parsed(self):
        self.assertNotIn(None, self.multiple_results)

    def then_parsed_dates_are(self, expected_dates):
        self.assertEqual(
            expected_dates,
            [result["date_obj"].date() for result in self.multiple_results],
        )

    def then_detected_locale(self, locale):
        self.assertEqual(locale, self.result["locale"])

    def then_period_is(self, day):
        self.assertEqual(day, self.result["period"])

    def then_parsed_datetime_is(self, expected_datetime):
        self.assertEqual(expected_datetime, self.result["date_obj"])

    def then_parsed_date_is(self, expected_date):
        self.assertEqual(expected_date, self.result["date_obj"].date())

    def then_parsed_date_has_timezone(self):
        self.assertTrue(hasattr(self.result["date_obj"], "tzinfo"))

    def then_returned_tuple_is(self, expected_tuple):
        self.assertEqual(expected_tuple, self.result)

    def then_previous_locales_is(self, expected_list):
        self.assertEqual(
            expected_list, [locale.shortname for locale in self.parser.previous_locales]
        )


class TestParserInitialization(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.parser = NotImplemented

    @parameterized.expand(
        [
            param(languages="en"),
            param(languages={"languages": ["en", "he", "it"]}),
        ]
    )
    def test_error_raised_for_invalid_languages_argument(self, languages):
        self.when_parser_is_initialized(languages=languages)
        self.then_error_was_raised(
            TypeError,
            ["languages argument must be a list (%r given)" % type(languages)],
        )

    @parameterized.expand(
        [
            param(locales="en-001"),
            param(locales={"locales": ["zh-Hant-HK", "ha-NE", "se-SE"]}),
        ]
    )
    def test_error_raised_for_invalid_locales_argument(self, locales):
        self.when_parser_is_initialized(locales=locales)
        self.then_error_was_raised(
            TypeError, ["locales argument must be a list (%r given)" % type(locales)]
        )

    @parameterized.expand(
        [
            param(region=["AW", "BE"]),
            param(region=150),
        ]
    )
    def test_error_raised_for_invalid_region_argument(self, region):
        self.when_parser_is_initialized(region=region)
        self.then_error_was_raised(
            TypeError, ["region argument must be str (%r given)" % type(region)]
        )

    @parameterized.expand(
        [
            param(try_previous_locales=["ar-OM", "pt-PT", "fr-CG", "uk"]),
            param(try_previous_locales="uk"),
            param(try_previous_locales={"try_previous_locales": True}),
            param(try_previous_locales=0),
        ]
    )
    def test_error_raised_for_invalid_try_previous_locales_argument(
        self, try_previous_locales
    ):
        self.when_parser_is_initialized(try_previous_locales=try_previous_locales)
        self.then_error_was_raised(
            TypeError,
            [
                "try_previous_locales argument must be a boolean (%r given)"
                % type(try_previous_locales)
            ],
        )

    @parameterized.expand(
        [
            param(use_given_order=["da", "pt", "ja", "sv"]),
            param(use_given_order="uk"),
            param(use_given_order={"use_given_order": True}),
            param(use_given_order=1),
        ]
    )
    def test_error_raised_for_invalid_use_given_order_argument(self, use_given_order):
        self.when_parser_is_initialized(
            locales=["en", "es"], use_given_order=use_given_order
        )
        self.then_error_was_raised(
            TypeError,
            [
                "use_given_order argument must be a boolean (%r given)"
                % type(use_given_order)
            ],
        )

    def test_error_is_raised_when_use_given_order_is_True_and_locales_and_languages_is_None(
        self,
    ):
        self.when_parser_is_initialized(use_given_order=True)
        self.then_error_was_raised(
            ValueError,
            ["locales or languages must be given if use_given_order is True"],
        )

    def test_no_error_for_order_with_languages_without_locales(self):
        self.when_parser_is_initialized(languages=["en", "fr"], use_given_order=True)

    def when_parser_is_initialized(
        self,
        languages=None,
        locales=None,
        region=None,
        try_previous_locales=True,
        use_given_order=False,
    ):
        try:
            self.parser = date.DateDataParser(
                languages=languages,
                locales=locales,
                region=region,
                try_previous_locales=try_previous_locales,
                use_given_order=use_given_order,
            )
        except Exception as error:
            self.error = error


class TestSanitizeDate(BaseTestCase):
    def test_remove_year_in_russian(self):
        self.assertEqual(date.sanitize_date("2005г."), "2005")
        self.assertEqual(date.sanitize_date("2005г. 20:37"), "2005 20:37")
        self.assertEqual(date.sanitize_date("2005 г."), "2005")
        self.assertEqual(date.sanitize_date("2005 г. 15:24"), "2005 15:24")
        self.assertEqual(date.sanitize_date("Авг."), "Авг")

    def test_sanitize_date_colons(self):
        self.assertEqual(date.sanitize_date("2019:"), "2019")
        self.assertEqual(date.sanitize_date("31/07/2019:"), "31/07/2019")


class TestDateLocaleParser(BaseTestCase):
    def setUp(self):
        super().setUp()

    @parameterized.expand(
        [
            param(
                date_data=DateData(date_obj=datetime(1999, 10, 1, 0, 0))
            ),  # missing period
            param(date_data=DateData(period="day")),  # missing date_obj
            param(
                date_data={"date": datetime(2007, 1, 22, 0, 0), "period": "day"}
            ),  # wrong format (dict)
            param(
                date_data=DateData(date_obj=datetime(2020, 10, 29, 0, 0), period="hour")
            ),  # wrong period
            param(
                date_data=[datetime(2007, 1, 22, 0, 0), "day"]
            ),  # wrong format (list)
            param(date_data=DateData(date_obj=None, period="day")),  # date_obj is None
            param(
                date_data=DateData(date_obj="29/05/1994", period="day")
            ),  # wrong date_obj format
        ]
    )
    def test_is_valid_date_data(self, date_data):
        self.given_parser(
            language=["en"],
            date_string="10 jan 2000",
            date_formats=None,
            settings=Settings(),
        )
        self.when_date_object_is_validated(date_data)
        self.then_date_object_is_invalid()

    def given_parser(self, language, date_string, date_formats, settings):
        self.parser = date._DateLocaleParser(
            language, date_string, date_formats, settings
        )

    def when_date_object_is_validated(self, date_data):
        self.is_valid_date_obj = self.parser._is_valid_date_data(date_data)

    def then_date_object_is_invalid(self):
        self.assertFalse(self.is_valid_date_obj)


class TestTimestampParser(BaseTestCase):
    def given_parser(self, **params):
        self.parser = date.DateDataParser(**params)

    def given_tzstr(self, tzstr):
        # Save the existing value
        self.old_tzstr = os.environ["TZ"] if "TZ" in os.environ else None

        # Overwrite the value, or remove it
        if tzstr is not None:
            os.environ["TZ"] = tzstr
        elif "TZ" in os.environ:
            del os.environ["TZ"]

        # Call tzset
        tzset()

    def reset_tzstr(self):
        # If we never set it with given_tzstr, don't bother resetting it
        if not hasattr(self, "old_tzstr"):
            return

        # Restore the old value, or remove it if null
        if self.old_tzstr is not None:
            os.environ["TZ"] = self.old_tzstr
        elif "TZ" in os.environ:
            del os.environ["TZ"]

        # Remove the local attribute
        del self.old_tzstr

        # Restore the old timezone behavior
        tzset()

    def test_timestamp_in_milliseconds(self):
        self.assertEqual(
            date.get_date_from_timestamp("1570308760263", None),
            datetime.fromtimestamp(1570308760).replace(microsecond=263000),
        )

    @parameterized.expand(
        product(
            ["1570308760"],
            ["EDT", "EST", "PDT", "PST", "UTC", "local"],
            [None, "EDT", "EST", "PDT", "PST", "UTC"],
            ["EST5EDT4", "UTC0", "PST8PDT7", None],
        )
    )
    def test_timestamp_with_different_timestr(
        self, timestamp, timezone, to_timezone, tzstr
    ):
        settings = {
            "RETURN_AS_TIMEZONE_AWARE": True,
            "TIMEZONE": timezone,
        }

        # is TO_TIMEZONE supposed to be allowed to be False, or None ???
        if to_timezone is not None:
            settings["TO_TIMEZONE"] = to_timezone

        self.given_parser(settings=settings)

        self.given_tzstr(tzstr)

        self.assertEqual(
            self.parser.get_date_data(timestamp)["date_obj"],
            datetime.fromtimestamp(int(timestamp), dttz.utc),
        )

        self.reset_tzstr()

    def test_timestamp_in_microseconds(self):
        self.assertEqual(
            date.get_date_from_timestamp("1570308760263111", None),
            datetime.fromtimestamp(1570308760).replace(microsecond=263111),
        )

    @parameterized.expand(
        [
            param(
                input_timestamp="-1570308760",
                negative=True,
                result=datetime.fromtimestamp(-1570308760),
            ),
            param(input_timestamp="-1570308760", negative=False, result=None),
            param(input_timestamp="1570308760", negative=True, result=None),
            param(
                input_timestamp="1570308760",
                negative=False,
                result=datetime.fromtimestamp(1570308760),
            ),
        ]
    )
    def test_timestamp_with_negative(self, input_timestamp, negative, result):
        self.assertEqual(
            date.get_date_from_timestamp(input_timestamp, None, negative=negative),
            result,
        )

    @parameterized.expand(
        [
            param(date_string="15703087602631"),
            param(date_string="157030876026xx"),
            param(date_string="1570308760263x"),
            param(date_string="157030876026311"),
            param(date_string="15703087602631x"),
            param(date_string="15703087602631xx"),
            param(date_string="15703087602631111"),
            param(date_string="1570308760263111x"),
            param(date_string="1570308760263111xx"),
            param(date_string="1570308760263111222"),
        ]
    )
    def test_timestamp_with_wrong_length(self, date_string):
        self.assertEqual(date.get_date_from_timestamp(date_string, None), None)


if __name__ == "__main__":
    unittest.main()
