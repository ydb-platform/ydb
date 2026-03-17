import locale
from parameterized import parameterized, param
from datetime import datetime
from unittest import SkipTest

from tests import BaseTestCase
from dateparser.utils.strptime import strptime


class TestStrptime(BaseTestCase):
    def setUp(self):
        super(TestStrptime, self).setUp()

    def given_system_locale_is(self, locale_str):
        try:
            locale.setlocale(locale.LC_ALL, locale_str)
        except locale.Error:
            raise SkipTest('Locale {} is not installed'.format(locale_str))

    def when_date_string_is_parsed(self, date_string, fmt):
        try:
            self.result = strptime(date_string, fmt)
        except ValueError as e:
            self.result = e

    def when_date_string_is_parsed_using_datetime_strptime(self, date_string, fmt):
        try:
            self.result = datetime.strptime(date_string, fmt)
        except ValueError as e:
            self.result = e

    def then_date_object_is(self, expected):
        assert self.result == expected

    def then_date_object_is_instance_of(self, expected):
        assert isinstance(self.result, expected)

    # Monkey patching with module copying works in 'ya py'

    # Patching w/o module copying works for all tests except
    # test_parsing_date_should_fail_using_datetime_strptime_if_locale_is_non_english,
    # which should use non-modified datetime.strptime.

    # fr_FR is not present on machines by default.

    # @parameterized.expand([
    #     param('21 January 2010', '%d %B %Y', expected=datetime(2010, 1, 21, 0, 0)),
    #     param('2 Mar 2010', '%d %b %Y', expected=datetime(2010, 3, 2, 0, 0)),
    #     param('12 December 10 10:30', '%d %B %y %H:%M', expected=datetime(2010, 12, 12, 10, 30)),
    #     param('12 December 10 22:41', '%d %B %y %H:%M', expected=datetime(2010, 12, 12, 22, 41)),
    #     param('12 February 2016 11:41', '%d %B %Y %I:%M', expected=datetime(2016, 2, 12, 11, 41)),
    #     param('21 Jan 2010', '%d %b %Y', expected=datetime(2010, 1, 21, 0, 0)),
    #     param('12 Dec 10 10:30', '%d %b %y %H:%M', expected=datetime(2010, 12, 12, 10, 30)),
    #     param('12 Feb 2016 11:41', '%d %b %Y %I:%M', expected=datetime(2016, 2, 12, 11, 41)),
    # ])
    # def test_dates_with_months_are_parsed_if_locale_is_non_english(self, date_string, fmt, expected):
    #     self.given_system_locale_is('fr_FR.UTF-8')
    #     self.when_date_string_is_parsed(date_string, fmt)
    #     self.then_date_object_is(expected)

    # @parameterized.expand([
    #     param('Monday 21 January 2010', '%A %d %B %Y', expected=datetime(2010, 1, 21, 0, 0)),
    #     param('Tue 2 Mar 2010', '%a %d %b %Y', expected=datetime(2010, 3, 2, 0, 0)),
    #     param('Friday 12 December 10 10:30', '%A %d %B %y %H:%M', expected=datetime(2010, 12, 12, 10, 30)),
    #     param('Wed 12 December 10 22:41', '%a %d %B %y %H:%M', expected=datetime(2010, 12, 12, 22, 41)),
    #     param('Thu 12 February 2016 11:41', '%a %d %B %Y %I:%M', expected=datetime(2016, 2, 12, 11, 41)),
    # ])
    # def test_dates_with_days_are_parsed_if_locale_is_non_english(self, date_string, fmt, expected):
    #     self.given_system_locale_is('fr_FR.UTF-8')
    #     self.when_date_string_is_parsed(date_string, fmt)
    #     self.then_date_object_is(expected)

    # def test_parsing_date_should_fail_using_datetime_strptime_if_locale_is_non_english(self):
    #     self.given_system_locale_is('fr_FR.UTF-8')
    #     self.when_date_string_is_parsed_using_datetime_strptime('21 february 2010', '%d %B %Y')
    #     self.then_date_object_is_instance_of(ValueError)

    @parameterized.expand([
        param('12 Dec 10 10:30:55.1', '%d %b %y %H:%M:%S.%f', expected=datetime(2010, 12, 12, 10, 30, 55, 100000)),
        param('12 Dec 10 10:30:55.10', '%d %b %y %H:%M:%S.%f', expected=datetime(2010, 12, 12, 10, 30, 55, 100000)),
        param('12 Dec 10 10:30:55.100', '%d %b %y %H:%M:%S.%f', expected=datetime(2010, 12, 12, 10, 30, 55, 100000)),
        param('12 Dec 10 10:30:55.1000', '%d %b %y %H:%M:%S.%f', expected=datetime(2010, 12, 12, 10, 30, 55, 100000)),
        param('12 Dec 10 10:30:55.100000', '%d %b %y %H:%M:%S.%f', expected=datetime(2010, 12, 12, 10, 30, 55, 100000)),
        param('12 Dec 10 10:30:55.000001', '%d %b %y %H:%M:%S.%f', expected=datetime(2010, 12, 12, 10, 30, 55, 1)),
        param('12 Dec 10 10:30:55.000011', '%d %b %y %H:%M:%S.%f', expected=datetime(2010, 12, 12, 10, 30, 55, 11)),
        param('12 Dec 10 10:30:55.000111', '%d %b %y %H:%M:%S.%f', expected=datetime(2010, 12, 12, 10, 30, 55, 111)),
        param('12 Feb 2016 11:41:23', '%d %b %Y %I:%M:%S', expected=datetime(2016, 2, 12, 11, 41, 23)),
        param('11 Dec 10 10:30:2011.999999', '%y %b %S %H:%M:%Y.%f', expected=datetime(2011, 12, 1, 10, 30, 10, 999999)),
    ])
    def test_microseconds_are_parsed_correctly(self, date_string, fmt, expected):
        self.when_date_string_is_parsed(date_string, fmt)
        self.then_date_object_is(expected)
