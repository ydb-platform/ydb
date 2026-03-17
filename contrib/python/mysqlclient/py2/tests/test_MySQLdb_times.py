import mock
import unittest
from time import gmtime
from datetime import time, date, datetime, timedelta

from MySQLdb import times

import warnings
warnings.simplefilter("ignore")


class TestX_or_None(unittest.TestCase):
    def test_date_or_none(self):
        assert times.Date_or_None('1969-01-01') == date(1969, 1, 1)
        assert times.Date_or_None('2015-01-01') == date(2015, 1, 1)
        assert times.Date_or_None('2015-12-13') == date(2015, 12, 13)

        assert times.Date_or_None('') is None
        assert times.Date_or_None('fail') is None
        assert times.Date_or_None('2015-12') is None
        assert times.Date_or_None('2015-12-40') is None
        assert times.Date_or_None('0000-00-00') is None

    def test_time_or_none(self):
        assert times.Time_or_None('00:00:00') == time(0, 0)
        assert times.Time_or_None('01:02:03') == time(1, 2, 3)
        assert times.Time_or_None('01:02:03.123456') == time(1, 2, 3, 123456)

        assert times.Time_or_None('') is None
        assert times.Time_or_None('fail') is None
        assert times.Time_or_None('24:00:00') is None
        assert times.Time_or_None('01:02:03.123456789') is None

    def test_datetime_or_none(self):
        assert times.DateTime_or_None('1000-01-01') == date(1000, 1, 1)
        assert times.DateTime_or_None('2015-12-13') == date(2015, 12, 13)
        assert times.DateTime_or_None('2015-12-13 01:02') == datetime(2015, 12, 13, 1, 2)
        assert times.DateTime_or_None('2015-12-13T01:02') == datetime(2015, 12, 13, 1, 2)
        assert times.DateTime_or_None('2015-12-13 01:02:03') == datetime(2015, 12, 13, 1, 2, 3)
        assert times.DateTime_or_None('2015-12-13T01:02:03') == datetime(2015, 12, 13, 1, 2, 3)
        assert times.DateTime_or_None('2015-12-13 01:02:03.123') == datetime(2015, 12, 13, 1, 2, 3, 123000)
        assert times.DateTime_or_None('2015-12-13 01:02:03.000123') == datetime(2015, 12, 13, 1, 2, 3, 123)
        assert times.DateTime_or_None('2015-12-13 01:02:03.123456') == datetime(2015, 12, 13, 1, 2, 3, 123456)
        assert times.DateTime_or_None('2015-12-13T01:02:03.123456') == datetime(2015, 12, 13, 1, 2, 3, 123456)

        assert times.DateTime_or_None('') is None
        assert times.DateTime_or_None('fail') is None
        assert times.DateTime_or_None('0000-00-00 00:00:00') is None
        assert times.DateTime_or_None('0000-00-00 00:00:00.000000') is None
        assert times.DateTime_or_None('2015-12-13T01:02:03.123456789') is None

    def test_timedelta_or_none(self):
        assert times.TimeDelta_or_None('-1:0:0') == timedelta(0, -3600)
        assert times.TimeDelta_or_None('1:0:0') == timedelta(0, 3600)
        assert times.TimeDelta_or_None('12:55:30') == timedelta(0, 46530)
        assert times.TimeDelta_or_None('12:55:30.123456') == timedelta(0, 46530, 123456)
        assert times.TimeDelta_or_None('12:55:30.123456789') == timedelta(0, 46653, 456789)
        assert times.TimeDelta_or_None('12:55:30.123456789123456') == timedelta(1429, 37719, 123456)

        assert times.TimeDelta_or_None('') is None
        assert times.TimeDelta_or_None('0') is None
        assert times.TimeDelta_or_None('fail') is None


class TestTicks(unittest.TestCase):
    @mock.patch('MySQLdb.times.localtime', side_effect=gmtime)
    def test_date_from_ticks(self, mock):
        assert times.DateFromTicks(0) == date(1970, 1, 1)
        assert times.DateFromTicks(1430000000) == date(2015, 4, 25)

    @mock.patch('MySQLdb.times.localtime', side_effect=gmtime)
    def test_time_from_ticks(self, mock):
        assert times.TimeFromTicks(0) == time(0, 0, 0)
        assert times.TimeFromTicks(1431100000) == time(15, 46, 40)
        assert times.TimeFromTicks(1431100000.123) == time(15, 46, 40)

    @mock.patch('MySQLdb.times.localtime', side_effect=gmtime)
    def test_timestamp_from_ticks(self, mock):
        assert times.TimestampFromTicks(0) == datetime(1970, 1, 1, 0, 0, 0)
        assert times.TimestampFromTicks(1430000000) == datetime(2015, 4, 25, 22, 13, 20)
        assert times.TimestampFromTicks(1430000000.123) == datetime(2015, 4, 25, 22, 13, 20)


class TestToLiteral(unittest.TestCase):
    def test_datetime_to_literal(self):
        assert times.DateTime2literal(datetime(2015, 12, 13), '') == b"'2015-12-13 00:00:00'"
        assert times.DateTime2literal(datetime(2015, 12, 13, 11, 12, 13), '') == b"'2015-12-13 11:12:13'"
        assert times.DateTime2literal(datetime(2015, 12, 13, 11, 12, 13, 123456), '') == b"'2015-12-13 11:12:13.123456'"

    def test_datetimedelta_to_literal(self):
        d = datetime(2015, 12, 13, 1, 2, 3) - datetime(2015, 12, 13, 1, 2, 2)
        assert times.DateTimeDelta2literal(d, '') == b"'0 0:0:1'"


class TestFormat(unittest.TestCase):
    def test_format_timedelta(self):
        d = datetime(2015, 1, 1) - datetime(2015, 1, 1)
        assert times.format_TIMEDELTA(d) == '0 0:0:0'

        d = datetime(2015, 1, 1, 10, 11, 12) - datetime(2015, 1, 1, 8, 9, 10)
        assert times.format_TIMEDELTA(d) == '0 2:2:2'

        d = datetime(2015, 1, 1, 10, 11, 12) - datetime(2015, 1, 1, 11, 12, 13)
        assert times.format_TIMEDELTA(d) == '-1 22:58:59'

    def test_format_timestamp(self):
        assert times.format_TIMESTAMP(datetime(2015, 2, 3)) == '2015-02-03 00:00:00'
        assert times.format_TIMESTAMP(datetime(2015, 2, 3, 17, 18, 19)) == '2015-02-03 17:18:19'
        assert times.format_TIMESTAMP(datetime(15, 2, 3, 17, 18, 19)) == '0015-02-03 17:18:19'
