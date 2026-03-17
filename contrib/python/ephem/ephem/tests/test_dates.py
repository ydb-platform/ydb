#!/usr/bin/env python

import unittest, time
from datetime import datetime, tzinfo, timedelta

from ephem import Date, localtime, to_timezone, UTC

millisecond = 1.0 / 24.0 / 60.0 / 60.0 / 1e3

class CET(tzinfo):
    """central european time without daylight saving time"""
    def utcoffset(self, dt):
        return timedelta(hours=1) + self.dst(dt)
    def dst(self, dt):
        return timedelta(0)

# Determine whether dates behave reasonably.

class DateTests(unittest.TestCase):

    def setUp(self):
        self.date = Date('2004/09/04 00:17:15.8')

    def test_date_constructor(self):

        def construct_and_compare(args1, args2):
            d1, d2 = Date(args1), Date(args2)
            self.assertTrue(abs(d1 - d2) < millisecond,
                            'dates not equal:\n %r = date%r\n %r = date%r'
                            % (d1.tuple(), args1, d2.tuple(), args2))

        std = '2004/09/04 00:17:15.8'
        pairs = [
            [std, '2004.67489614324023472'],
            [std, '   2004.67489614324023472 '],
            [std, '2004/9/4.0119884259259257'],
            [std, ' 2004/9/4.0119884259259257  '],
            [std, '2004/9/4 0.28772222222222221'],
            [std, ' 2004/9/4 0.28772222222222221 '],
            [std, '2004/9/4 0:17.263333333333332'],
            [std, '    2004/9/4 0:17.263333333333332  '],
            [std, '2004/9/4 0:17:15.8'],
            [std, '  2004/9/4 0:17:15.8 '],
            [std, '  2004-9-4 0:17:15.8 '],
            ['2004', (2004,)],
            ['  2004 ', (2004,)],
            ['2004/09', (2004, 9)],
            [' 2004/09  ', (2004, 9)],
            [std, (2004, 9, 4.0119884259259257)],
            [std, (2004, 9, 4, 0.28772222222222221)],
            [std, (2004, 9, 4, 0, 17.263333333333332)],
            [std, (2004, 9, 4, 0, 17, 15.8)],
            [std, (datetime(2004, 9, 4, 0, 17, 15, 800000))],
            ]
        for arg1, arg2 in pairs:
            construct_and_compare(arg1, arg2)
            if type(arg2) is str:
                construct_and_compare(arg1, '  %s  ' % arg2)

    def test_date_parser_error_message(self):
        with self.assertRaises(ValueError) as e:
            Date('bad string')
        self.assertEqual(
            str(e.exception),
            "your date string 'bad string' does"
            " not look like a year/month/day optionally"
            " followed by hours:minutes:seconds",
        )

    def test_year_zero(self):
        # I would have thought the year would be 0, but it looks like
        # libastro considers 1 BC to be the year -1?
        self.assertEqual(str(Date('0')), '-1/1/1 00:00:00')

    def test_date_string_value(self):
        self.assertEqual(str(self.date), '2004/9/4 00:17:16')

    def test_date_triple_value(self):
        self.assertEqual(self.date.triple(), (2004, 9, 4.0119884259256651))

    def test_date_tuple_value(self):
        self.assertEqual(self.date.tuple(),
                         (2004, 9, 4, 0, 17, 15.8))

    def test_another_tuple_value(self):
        #d = Date((1994, 7, 16, 20, 15, 0))
        d = Date(34530.34375)
        self.assertEqual(d.tuple(), (1994, 7, 16, 20, 15, 0))

    def test_tuple_that_rounded_to_negative_seconds(self):  # Github issue 223
        d = Date(44417.49999991596)
        self.assertEqual(d.tuple(), (2021, 8, 10, 23, 59, 59.992739))

    def test_localtime_modern(self):
        if time.timezone == 18000: # test only works in Eastern time zone
            self.assertEqual(localtime(Date('2009/6/23 8:47')),
                             datetime(2009, 6, 23, 4, 47, 0))

    def test_timezone_aware_utc(self):
        timezoned_date = to_timezone(self.date, UTC)
        self.assertEqual(timezoned_date.tzinfo, UTC)
        self.assertEqual(timezoned_date.hour, 0)
        self.assertEqual(timezoned_date.minute, 17)
        self.assertEqual(timezoned_date.second, 15)
        self.assertEqual(timezoned_date.day, 4)
        self.assertEqual(timezoned_date.month, 9)
        self.assertEqual(timezoned_date.year, 2004)

    def test_timezone_aware_cet(self):
        cet = CET()
        timezoned_date = to_timezone(self.date, cet)
        self.assertEqual(timezoned_date.tzinfo, cet)
        self.assertEqual(timezoned_date.hour, 1)
        self.assertEqual(timezoned_date.minute, 17)
        self.assertEqual(timezoned_date.second, 15)
        self.assertEqual(timezoned_date.day, 4)
        self.assertEqual(timezoned_date.month, 9)
        self.assertEqual(timezoned_date.year, 2004)

    # I am commenting this out for now because I am not sure that I can
    # fix it without either writing an entirely new time module for
    # PyEphem, or making PyEphem depend on another Python module - which
    # would be its first-ever external dependency.

    def OFF_test_localtime_premodern(self):
        if time.timezone == 18000: # test only works in Eastern time zone
            self.assertEqual(localtime(Date('1531/8/24 2:49')),
                             datetime(1957, 10, 4, 15, 28, 34, 4))
