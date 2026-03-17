# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from datetime import datetime
import six
import unittest

from parameterized import parameterized, param

# umalqurra does not support Python3 yet
# see https://github.com/tytkal/python-hijiri-ummalqura/pull/5
# let's skip these tests under Python3 for now
try:
    from dateparser.calendars.hijri import HijriCalendar
except ImportError:
    if not six.PY2:
        pass
    else:
        raise

from tests import BaseTestCase


@unittest.skipUnless(six.PY2, "umalqurra does not work under Python3 yet.")
class TestHijriParser(BaseTestCase):

    def setUp(self):
        super(TestHijriParser, self).setUp()
        self.result = NotImplemented
        self.date_string = NotImplemented
        self.parser = NotImplemented
        self.translated = NotImplemented

    def when_date_is_given(self, dt_string, date_formats, languages):
        self.date_string = dt_string
        self.parser = HijriCalendar(dt_string)
        self.result = self.parser.get_date()

    def then_parsed_datetime_is(self, dt):
        self.assertEqual(dt, self.result['date_obj'])

    @parameterized.expand([
        param(dt_string="14-09-1432 هـ, 09:40 صباحاً", dt_obj=datetime(2011, 8, 14, 9, 40)),
        param(dt_string="20-02-1430 هـ, 07:21 صباحاً", dt_obj=datetime(2009, 2, 15, 7, 21)),
        param(dt_string="11-08-1434 هـ, 09:38 صباحاً", dt_obj=datetime(2013, 6, 20, 9, 38)),
        param(dt_string=" 17-01-1437 هـ 08:30 مساءً", dt_obj=datetime(2015, 10, 30, 20, 30)),
        param(dt_string="29-02-1433 هـ, 06:22 صباحاً", dt_obj=datetime(2012, 1, 23, 6, 22)),
        param(dt_string="30-02-1433", dt_obj=datetime(2012, 1, 24)),
        param(dt_string="04-03-1433 هـ, 10:08 مساءً", dt_obj=datetime(2012, 1, 27, 22, 8)),
    ])
    def test_datetime_parsing(self, dt_string, dt_obj,
                              date_formats=None, languages=None):
        from dateparser.conf import settings
        settings.DATE_ORDER = 'DMY'
        self.when_date_is_given(dt_string, date_formats, languages)
        self.then_parsed_datetime_is(dt_obj)
        settings.DATE_ORDER = 'MDY'
