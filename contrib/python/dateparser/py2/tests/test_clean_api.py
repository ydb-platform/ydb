# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from datetime import date, datetime

from parameterized import parameterized, param

import dateparser
from tests import BaseTestCase


class TestParseFunction(BaseTestCase):
    def setUp(self):
        super(TestParseFunction, self).setUp()
        self.result = NotImplemented

    @parameterized.expand([
        param(date_string="24 de Janeiro de 2014", expected_date=date(2014, 1, 24)),
        param(date_string="2 de Enero de 2013", expected_date=date(2013, 1, 2)),
        param(date_string="January 25, 2014", expected_date=date(2014, 1, 25)),
    ])
    def test_parse_dates_in_different_languages(self, date_string, expected_date):
        self.when_date_is_parsed(date_string)
        self.then_parsed_date_is(expected_date)

    @parameterized.expand([
        param(date_string="24 de Janeiro de 2014", languages=['pt'], expected_date=date(2014, 1, 24)),
    ])
    def test_dates_which_match_languages_are_parsed(self, date_string, languages, expected_date):
        self.when_date_is_parsed(date_string, languages=languages)
        self.then_parsed_date_is(expected_date)

    @parameterized.expand([
        param(date_string="January 24, 2014", languages=['pt']),
    ])
    def test_dates_which_do_not_match_languages_are_not_parsed(self, date_string, languages):
        self.when_date_is_parsed(date_string, languages=languages)
        self.then_date_was_not_parsed()

    @parameterized.expand([
        param(date_string="24 de Janeiro de 2014", locales=['pt-TL'], expected_date=date(2014, 1, 24)),
    ])
    def test_dates_which_match_locales_are_parsed(self, date_string, locales, expected_date):
        self.when_date_is_parsed(date_string, locales=locales)
        self.then_parsed_date_is(expected_date)

    @parameterized.expand([
        param(date_string="January 24, 2014", locales=['pt-AO']),
    ])
    def test_dates_which_do_not_match_locales_are_not_parsed(self, date_string, locales):
        self.when_date_is_parsed(date_string, locales=locales)
        self.then_date_was_not_parsed()

    def when_date_is_parsed(self, date_string, languages=None, locales=None):
        self.result = dateparser.parse(date_string, languages=languages, locales=locales)

    def then_parsed_date_is(self, expected_date):
        self.assertEqual(self.result, datetime.combine(expected_date, datetime.min.time()))

    def then_date_was_not_parsed(self):
        self.assertIsNone(self.result)
