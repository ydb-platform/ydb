#
# Copyright (C) 2007-2011 Edgewall Software, 2013-2025 the Babel team
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution. The terms
# are also available at https://github.com/python-babel/babel/blob/master/LICENSE.
#
# This software consists of voluntary contributions made by many
# individuals. For the exact contribution history, see the revision
# history and logs, available at https://github.com/python-babel/babel/commits/master/.

import calendar
from datetime import date, datetime, time, timedelta

import freezegun
import pytest

from babel import Locale, dates
from babel.dates import NO_INHERITANCE_MARKER, UTC, _localize, parse_pattern
from babel.util import FixedOffsetTimezone


class DateTimeFormatTestCase:

    def test_quarter_format(self):
        d = date(2006, 6, 8)
        fmt = dates.DateTimeFormat(d, locale='en_US')
        assert fmt['Q'] == '2'
        assert fmt['QQQQ'] == '2nd quarter'
        assert fmt['q'] == '2'
        assert fmt['qqqq'] == '2nd quarter'
        d = date(2006, 12, 31)
        fmt = dates.DateTimeFormat(d, locale='en_US')
        assert fmt['qqq'] == 'Q4'
        assert fmt['qqqqq'] == '4'
        assert fmt['QQQ'] == 'Q4'
        assert fmt['QQQQQ'] == '4'

    def test_month_context(self):
        d = date(2006, 2, 8)
        assert dates.DateTimeFormat(d, locale='mt_MT')['MMMMM'] == 'F'  # narrow format
        assert dates.DateTimeFormat(d, locale='mt_MT')['LLLLL'] == 'Fr'  # narrow standalone

    def test_abbreviated_month_alias(self):
        d = date(2006, 3, 8)
        assert dates.DateTimeFormat(d, locale='de_DE')['LLL'] == 'Mär'

    def test_week_of_year_first(self):
        d = date(2006, 1, 8)
        assert dates.DateTimeFormat(d, locale='de_DE')['w'] == '1'
        assert dates.DateTimeFormat(d, locale='en_US')['ww'] == '02'

    def test_week_of_year_first_with_year(self):
        d = date(2006, 1, 1)
        fmt = dates.DateTimeFormat(d, locale='de_DE')
        assert fmt['w'] == '52'
        assert fmt['YYYY'] == '2005'

    def test_week_of_year_last(self):
        d = date(2006, 12, 26)
        assert dates.DateTimeFormat(d, locale='de_DE')['w'] == '52'
        assert dates.DateTimeFormat(d, locale='en_US')['w'] == '52'

    def test_week_of_year_last_us_extra_week(self):
        d = date(2005, 12, 26)
        assert dates.DateTimeFormat(d, locale='de_DE')['w'] == '52'
        assert dates.DateTimeFormat(d, locale='en_US')['w'] == '53'

    def test_week_of_year_de_first_us_last_with_year(self):
        d = date(2018, 12, 31)
        fmt = dates.DateTimeFormat(d, locale='de_DE')
        assert fmt['w'] == '1'
        assert fmt['YYYY'] == '2019'
        fmt = dates.DateTimeFormat(d, locale='en_US')
        assert fmt['w'] == '53'
        assert fmt['yyyy'] == '2018'

    def test_week_of_month_first(self):
        d = date(2006, 1, 8)
        assert dates.DateTimeFormat(d, locale='de_DE')['W'] == '1'
        assert dates.DateTimeFormat(d, locale='en_US')['W'] == '2'

    def test_week_of_month_last(self):
        d = date(2006, 1, 29)
        assert dates.DateTimeFormat(d, locale='de_DE')['W'] == '4'
        assert dates.DateTimeFormat(d, locale='en_US')['W'] == '5'

    def test_day_of_year(self):
        d = date(2007, 4, 1)
        assert dates.DateTimeFormat(d, locale='en_US')['D'] == '91'

    def test_day_of_year_works_with_datetime(self):
        d = datetime(2007, 4, 1)
        assert dates.DateTimeFormat(d, locale='en_US')['D'] == '91'

    def test_day_of_year_first(self):
        d = date(2007, 1, 1)
        assert dates.DateTimeFormat(d, locale='en_US')['DDD'] == '001'

    def test_day_of_year_last(self):
        d = date(2007, 12, 31)
        assert dates.DateTimeFormat(d, locale='en_US')['DDD'] == '365'

    def test_day_of_week_in_month(self):
        d = date(2007, 4, 15)
        assert dates.DateTimeFormat(d, locale='en_US')['F'] == '3'

    def test_day_of_week_in_month_first(self):
        d = date(2007, 4, 1)
        assert dates.DateTimeFormat(d, locale='en_US')['F'] == '1'

    def test_day_of_week_in_month_last(self):
        d = date(2007, 4, 29)
        assert dates.DateTimeFormat(d, locale='en_US')['F'] == '5'

    def test_local_day_of_week(self):
        d = date(2007, 4, 1)  # a sunday
        assert dates.DateTimeFormat(d, locale='de_DE')['e'] == '7'  # monday is first day of week
        assert dates.DateTimeFormat(d, locale='en_US')['ee'] == '01'  # sunday is first day of week
        assert dates.DateTimeFormat(d, locale='ar_BH')['ee'] == '02'  # saturday is first day of week

        d = date(2007, 4, 2)  # a monday
        assert dates.DateTimeFormat(d, locale='de_DE')['e'] == '1'  # monday is first day of week
        assert dates.DateTimeFormat(d, locale='en_US')['ee'] == '02'  # sunday is first day of week
        assert dates.DateTimeFormat(d, locale='ar_BH')['ee'] == '03'  # saturday is first day of week

    def test_local_day_of_week_standalone(self):
        d = date(2007, 4, 1)  # a sunday
        assert dates.DateTimeFormat(d, locale='de_DE')['c'] == '7'  # monday is first day of week
        assert dates.DateTimeFormat(d, locale='en_US')['c'] == '1'  # sunday is first day of week
        assert dates.DateTimeFormat(d, locale='ar_BH')['c'] == '2'  # saturday is first day of week

        d = date(2007, 4, 2)  # a monday
        assert dates.DateTimeFormat(d, locale='de_DE')['c'] == '1'  # monday is first day of week
        assert dates.DateTimeFormat(d, locale='en_US')['c'] == '2'  # sunday is first day of week
        assert dates.DateTimeFormat(d, locale='ar_BH')['c'] == '3'  # saturday is first day of week

    def test_pattern_day_of_week(self):
        dt = datetime(2016, 2, 6)
        fmt = dates.DateTimeFormat(dt, locale='en_US')
        assert fmt['c'] == '7'
        assert fmt['ccc'] == 'Sat'
        assert fmt['cccc'] == 'Saturday'
        assert fmt['ccccc'] == 'S'
        assert fmt['cccccc'] == 'Sa'
        assert fmt['e'] == '7'
        assert fmt['ee'] == '07'
        assert fmt['eee'] == 'Sat'
        assert fmt['eeee'] == 'Saturday'
        assert fmt['eeeee'] == 'S'
        assert fmt['eeeeee'] == 'Sa'
        assert fmt['E'] == 'Sat'
        assert fmt['EE'] == 'Sat'
        assert fmt['EEE'] == 'Sat'
        assert fmt['EEEE'] == 'Saturday'
        assert fmt['EEEEE'] == 'S'
        assert fmt['EEEEEE'] == 'Sa'
        fmt = dates.DateTimeFormat(dt, locale='uk')
        assert fmt['c'] == '6'
        assert fmt['e'] == '6'
        assert fmt['ee'] == '06'

    def test_fractional_seconds(self):
        t = time(8, 3, 9, 799)
        assert dates.DateTimeFormat(t, locale='en_US')['S'] == '0'
        t = time(8, 3, 1, 799)
        assert dates.DateTimeFormat(t, locale='en_US')['SSSS'] == '0008'
        t = time(8, 3, 1, 34567)
        assert dates.DateTimeFormat(t, locale='en_US')['SSSS'] == '0346'
        t = time(8, 3, 1, 345678)
        assert dates.DateTimeFormat(t, locale='en_US')['SSSSSS'] == '345678'
        t = time(8, 3, 1, 799)
        assert dates.DateTimeFormat(t, locale='en_US')['SSSSS'] == '00080'

    def test_fractional_seconds_zero(self):
        t = time(15, 30, 0)
        assert dates.DateTimeFormat(t, locale='en_US')['SSSS'] == '0000'

    def test_milliseconds_in_day(self):
        t = time(15, 30, 12, 345000)
        assert dates.DateTimeFormat(t, locale='en_US')['AAAA'] == '55812345'

    def test_milliseconds_in_day_zero(self):
        d = time(0, 0, 0)
        assert dates.DateTimeFormat(d, locale='en_US')['AAAA'] == '0000'

    def test_timezone_rfc822(self, timezone_getter):
        tz = timezone_getter('Europe/Berlin')
        t = _localize(tz, datetime(2015, 1, 1, 15, 30))
        assert dates.DateTimeFormat(t, locale='de_DE')['Z'] == '+0100'

    def test_timezone_gmt(self, timezone_getter):
        tz = timezone_getter('Europe/Berlin')
        t = _localize(tz, datetime(2015, 1, 1, 15, 30))
        assert dates.DateTimeFormat(t, locale='de_DE')['ZZZZ'] == 'GMT+01:00'

    def test_timezone_name(self, timezone_getter):
        tz = timezone_getter('Europe/Paris')
        dt = _localize(tz, datetime(2007, 4, 1, 15, 30))
        assert dates.DateTimeFormat(dt, locale='fr_FR')['v'] == 'heure : France'

    def test_timezone_location_format(self, timezone_getter):
        tz = timezone_getter('Europe/Paris')
        dt = _localize(tz, datetime(2007, 4, 1, 15, 30))
        assert dates.DateTimeFormat(dt, locale='fr_FR')['VVVV'] == 'heure : France'

    def test_timezone_walltime_short(self, timezone_getter):
        tz = timezone_getter('Europe/Paris')
        t = time(15, 30, tzinfo=tz)
        assert dates.DateTimeFormat(t, locale='fr_FR')['v'] == 'heure : France'

    def test_timezone_walltime_long(self, timezone_getter):
        tz = timezone_getter('Europe/Paris')
        t = time(15, 30, tzinfo=tz)
        assert dates.DateTimeFormat(t, locale='fr_FR')['vvvv'] == 'heure d’Europe centrale'

    def test_hour_formatting(self):
        locale = 'en_US'
        t = time(0, 0, 0)
        assert dates.format_time(t, 'h a', locale=locale) == '12 AM'
        assert dates.format_time(t, 'H', locale=locale) == '0'
        assert dates.format_time(t, 'k', locale=locale) == '24'
        assert dates.format_time(t, 'K a', locale=locale) == '0 AM'
        t = time(12, 0, 0)
        assert dates.format_time(t, 'h a', locale=locale) == '12 PM'
        assert dates.format_time(t, 'H', locale=locale) == '12'
        assert dates.format_time(t, 'k', locale=locale) == '12'
        assert dates.format_time(t, 'K a', locale=locale) == '0 PM'


class FormatDateTestCase:

    def test_with_time_fields_in_pattern(self):
        with pytest.raises(AttributeError):
            dates.format_date(date(2007, 4, 1), "yyyy-MM-dd HH:mm", locale='en_US')

    def test_with_time_fields_in_pattern_and_datetime_param(self):
        with pytest.raises(AttributeError):
            dates.format_date(datetime(2007, 4, 1, 15, 30), "yyyy-MM-dd HH:mm", locale='en_US')

    def test_with_day_of_year_in_pattern_and_datetime_param(self):
        # format_date should work on datetimes just as well (see #282)
        d = datetime(2007, 4, 1)
        assert dates.format_date(d, 'w', locale='en_US') == '14'


class FormatDatetimeTestCase:

    def test_with_float(self, timezone_getter):
        UTC = timezone_getter('UTC')
        d = datetime(2012, 4, 1, 15, 30, 29, tzinfo=UTC)
        epoch = float(calendar.timegm(d.timetuple()))
        formatted_string = dates.format_datetime(epoch, format='long', locale='en_US')
        assert formatted_string == 'April 1, 2012 at 3:30:29 PM UTC'

    def test_timezone_formats_los_angeles(self, timezone_getter):
        tz = timezone_getter('America/Los_Angeles')
        dt = _localize(tz, datetime(2016, 1, 13, 7, 8, 35))
        assert dates.format_datetime(dt, 'z', locale='en') == 'PST'
        assert dates.format_datetime(dt, 'zz', locale='en') == 'PST'
        assert dates.format_datetime(dt, 'zzz', locale='en') == 'PST'
        assert dates.format_datetime(dt, 'zzzz', locale='en') == 'Pacific Standard Time'
        assert dates.format_datetime(dt, 'Z', locale='en') == '-0800'
        assert dates.format_datetime(dt, 'ZZ', locale='en') == '-0800'
        assert dates.format_datetime(dt, 'ZZZ', locale='en') == '-0800'
        assert dates.format_datetime(dt, 'ZZZZ', locale='en') == 'GMT-08:00'
        assert dates.format_datetime(dt, 'ZZZZZ', locale='en') == '-08:00'
        assert dates.format_datetime(dt, 'OOOO', locale='en') == 'GMT-08:00'
        assert dates.format_datetime(dt, 'VV', locale='en') == 'America/Los_Angeles'
        assert dates.format_datetime(dt, 'VVV', locale='en') == 'Los Angeles'
        assert dates.format_datetime(dt, 'X', locale='en') == '-08'
        assert dates.format_datetime(dt, 'XX', locale='en') == '-0800'
        assert dates.format_datetime(dt, 'XXX', locale='en') == '-08:00'
        assert dates.format_datetime(dt, 'XXXX', locale='en') == '-0800'
        assert dates.format_datetime(dt, 'XXXXX', locale='en') == '-08:00'
        assert dates.format_datetime(dt, 'x', locale='en') == '-08'
        assert dates.format_datetime(dt, 'xx', locale='en') == '-0800'
        assert dates.format_datetime(dt, 'xxx', locale='en') == '-08:00'
        assert dates.format_datetime(dt, 'xxxx', locale='en') == '-0800'
        assert dates.format_datetime(dt, 'xxxxx', locale='en') == '-08:00'

    def test_timezone_formats_utc(self, timezone_getter):
        tz = timezone_getter('UTC')
        dt = _localize(tz, datetime(2016, 1, 13, 7, 8, 35))
        assert dates.format_datetime(dt, 'Z', locale='en') == '+0000'
        assert dates.format_datetime(dt, 'ZZ', locale='en') == '+0000'
        assert dates.format_datetime(dt, 'ZZZ', locale='en') == '+0000'
        assert dates.format_datetime(dt, 'ZZZZ', locale='en') == 'GMT+00:00'
        assert dates.format_datetime(dt, 'ZZZZZ', locale='en') == 'Z'
        assert dates.format_datetime(dt, 'OOOO', locale='en') == 'GMT+00:00'
        assert dates.format_datetime(dt, 'VV', locale='en') == 'Etc/UTC'
        assert dates.format_datetime(dt, 'VVV', locale='en') == 'UTC'
        assert dates.format_datetime(dt, 'X', locale='en') == 'Z'
        assert dates.format_datetime(dt, 'XX', locale='en') == 'Z'
        assert dates.format_datetime(dt, 'XXX', locale='en') == 'Z'
        assert dates.format_datetime(dt, 'XXXX', locale='en') == 'Z'
        assert dates.format_datetime(dt, 'XXXXX', locale='en') == 'Z'
        assert dates.format_datetime(dt, 'x', locale='en') == '+00'
        assert dates.format_datetime(dt, 'xx', locale='en') == '+0000'
        assert dates.format_datetime(dt, 'xxx', locale='en') == '+00:00'
        assert dates.format_datetime(dt, 'xxxx', locale='en') == '+0000'
        assert dates.format_datetime(dt, 'xxxxx', locale='en') == '+00:00'

    def test_timezone_formats_kolkata(self, timezone_getter):
        tz = timezone_getter('Asia/Kolkata')
        dt = _localize(tz, datetime(2016, 1, 13, 7, 8, 35))
        assert dates.format_datetime(dt, 'zzzz', locale='en') == 'India Standard Time'
        assert dates.format_datetime(dt, 'ZZZZ', locale='en') == 'GMT+05:30'
        assert dates.format_datetime(dt, 'ZZZZZ', locale='en') == '+05:30'
        assert dates.format_datetime(dt, 'OOOO', locale='en') == 'GMT+05:30'
        assert dates.format_datetime(dt, 'VV', locale='en') == 'Asia/Calcutta'
        assert dates.format_datetime(dt, 'VVV', locale='en') == 'Kolkata'
        assert dates.format_datetime(dt, 'X', locale='en') == '+0530'
        assert dates.format_datetime(dt, 'XX', locale='en') == '+0530'
        assert dates.format_datetime(dt, 'XXX', locale='en') == '+05:30'
        assert dates.format_datetime(dt, 'XXXX', locale='en') == '+0530'
        assert dates.format_datetime(dt, 'XXXXX', locale='en') == '+05:30'
        assert dates.format_datetime(dt, 'x', locale='en') == '+0530'
        assert dates.format_datetime(dt, 'xx', locale='en') == '+0530'
        assert dates.format_datetime(dt, 'xxx', locale='en') == '+05:30'
        assert dates.format_datetime(dt, 'xxxx', locale='en') == '+0530'
        assert dates.format_datetime(dt, 'xxxxx', locale='en') == '+05:30'


class FormatTimeTestCase:

    def test_with_naive_datetime_and_tzinfo(self, timezone_getter):
        assert dates.format_time(
            datetime(2007, 4, 1, 15, 30),
            'long',
            tzinfo=timezone_getter('US/Eastern'),
            locale='en',
        ) == '11:30:00 AM EDT'

    def test_with_float(self, timezone_getter):
        tz = timezone_getter('UTC')
        d = _localize(tz, datetime(2012, 4, 1, 15, 30, 29))
        epoch = float(calendar.timegm(d.timetuple()))
        assert dates.format_time(epoch, format='long', locale='en_US') == '3:30:29 PM UTC'

    def test_with_date_fields_in_pattern(self):
        with pytest.raises(AttributeError):
            dates.format_time(datetime(2007, 4, 1), 'yyyy-MM-dd HH:mm', locale='en')

    def test_with_date_fields_in_pattern_and_datetime_param(self):
        with pytest.raises(AttributeError):
            dates.format_time(datetime(2007, 4, 1, 15, 30), "yyyy-MM-dd HH:mm", locale='en_US')


class FormatTimedeltaTestCase:

    def test_zero_seconds(self):
        td = timedelta(seconds=0)
        assert dates.format_timedelta(td, locale='en') == '0 seconds'
        assert dates.format_timedelta(td, locale='en', format='short') == '0 sec'
        assert dates.format_timedelta(td, granularity='hour', locale='en') == '0 hours'
        assert dates.format_timedelta(td, granularity='hour', locale='en', format='short') == '0 hr'

    def test_small_value_with_granularity(self):
        td = timedelta(seconds=42)
        assert dates.format_timedelta(td, granularity='hour', locale='en') == '1 hour'
        assert dates.format_timedelta(td, granularity='hour', locale='en', format='short') == '1 hr'

    def test_direction_adding(self):
        td = timedelta(hours=1)
        assert dates.format_timedelta(td, locale='en', add_direction=True) == 'in 1 hour'
        assert dates.format_timedelta(-td, locale='en', add_direction=True) == '1 hour ago'

    def test_format_narrow(self):
        assert dates.format_timedelta(timedelta(hours=1), locale='en', format='narrow') == '1h'
        assert dates.format_timedelta(timedelta(hours=-2), locale='en', format='narrow') == '2h'

    def test_format_invalid(self):
        for format in (None, '', 'bold italic'):
            with pytest.raises(TypeError):
                dates.format_timedelta(timedelta(hours=1), format=format)


class TimeZoneAdjustTestCase:

    def _utc(self):
        class EvilFixedOffsetTimezone(FixedOffsetTimezone):

            def localize(self, dt, is_dst=False):
                raise NotImplementedError()
        UTC = EvilFixedOffsetTimezone(0, 'UTC')
        # This is important to trigger the actual bug (#257)
        assert hasattr(UTC, 'normalize') is False
        return UTC

    def test_can_format_time_with_custom_timezone(self):
        # regression test for #257
        utc = self._utc()
        t = datetime(2007, 4, 1, 15, 30, tzinfo=utc)
        formatted_time = dates.format_time(t, 'long', tzinfo=utc, locale='en')
        assert formatted_time == '3:30:00 PM UTC'


def test_get_period_names():
    assert dates.get_period_names(locale='en_US')['am'] == 'AM'


def test_get_day_names():
    assert dates.get_day_names('wide', locale='en_US')[1] == 'Tuesday'
    assert dates.get_day_names('short', locale='en_US')[1] == 'Tu'
    assert dates.get_day_names('abbreviated', locale='es')[1] == 'mar'
    de = dates.get_day_names('narrow', context='stand-alone', locale='de_DE')
    assert de[1] == 'D'


def test_get_month_names():
    assert dates.get_month_names('wide', locale='en_US')[1] == 'January'
    assert dates.get_month_names('abbreviated', locale='es')[1] == 'ene'
    de = dates.get_month_names('narrow', context='stand-alone', locale='de_DE')
    assert de[1] == 'J'


def test_get_quarter_names():
    assert dates.get_quarter_names('wide', locale='en_US')[1] == '1st quarter'
    assert dates.get_quarter_names('abbreviated', locale='de_DE')[1] == 'Q1'
    assert dates.get_quarter_names('narrow', locale='de_DE')[1] == '1'


def test_get_era_names():
    assert dates.get_era_names('wide', locale='en_US')[1] == 'Anno Domini'
    assert dates.get_era_names('abbreviated', locale='de_DE')[1] == 'n. Chr.'


def test_get_date_format():
    us = dates.get_date_format(locale='en_US')
    assert us.pattern == 'MMM d, y'
    de = dates.get_date_format('full', locale='de_DE')
    assert de.pattern == 'EEEE, d. MMMM y'


def test_get_datetime_format():
    assert dates.get_datetime_format(locale='en_US') == '{1}, {0}'


def test_get_time_format():
    assert dates.get_time_format(locale='en_US').pattern == 'h:mm:ss\u202fa'
    assert (dates.get_time_format('full', locale='de_DE').pattern ==
            'HH:mm:ss zzzz')


def test_get_timezone_gmt(timezone_getter):
    dt = datetime(2007, 4, 1, 15, 30)
    assert dates.get_timezone_gmt(dt, locale='en') == 'GMT+00:00'
    assert dates.get_timezone_gmt(dt, locale='en', return_z=True) == 'Z'
    assert dates.get_timezone_gmt(dt, locale='en', width='iso8601_short') == '+00'
    tz = timezone_getter('America/Los_Angeles')
    dt = _localize(tz, datetime(2007, 4, 1, 15, 30))
    assert dates.get_timezone_gmt(dt, locale='en') == 'GMT-07:00'
    assert dates.get_timezone_gmt(dt, 'short', locale='en') == '-0700'
    assert dates.get_timezone_gmt(dt, locale='en', width='iso8601_short') == '-07'
    assert dates.get_timezone_gmt(dt, 'long', locale='fr_FR') == 'UTC-07:00'


def test_get_timezone_location(timezone_getter):
    tz = timezone_getter('America/St_Johns')
    assert (dates.get_timezone_location(tz, locale='de_DE') ==
            "Kanada (St. John\u2019s) (Ortszeit)")
    assert (dates.get_timezone_location(tz, locale='en') ==
            'Canada (St. John’s) Time')
    assert (dates.get_timezone_location(tz, locale='en', return_city=True) ==
            'St. John’s')

    tz = timezone_getter('America/Mexico_City')
    assert (dates.get_timezone_location(tz, locale='de_DE') ==
            'Mexiko (Mexiko-Stadt) (Ortszeit)')

    tz = timezone_getter('Europe/Berlin')
    assert (dates.get_timezone_location(tz, locale='de_DE') ==
            'Deutschland (Berlin) (Ortszeit)')


@pytest.mark.parametrize(
    "tzname, params, expected",
    [
        ("America/Los_Angeles", {"locale": "en_US"}, "Pacific Time"),
        ("America/Los_Angeles", {"width": "short", "locale": "en_US"}, "PT"),
        ("Europe/Berlin", {"locale": "de_DE"}, "Mitteleurop\xe4ische Zeit"),
        ("Europe/Berlin", {"locale": "pt_BR"}, "Hor\xe1rio da Europa Central"),
        ("America/St_Johns", {"locale": "de_DE"}, "Neufundland-Zeit"),
        (
            "America/Los_Angeles",
            {"locale": "en", "width": "short", "zone_variant": "generic"},
            "PT",
        ),
        (
            "America/Los_Angeles",
            {"locale": "en", "width": "short", "zone_variant": "standard"},
            "PST",
        ),
        (
            "America/Los_Angeles",
            {"locale": "en", "width": "short", "zone_variant": "daylight"},
            "PDT",
        ),
        (
            "America/Los_Angeles",
            {"locale": "en", "width": "long", "zone_variant": "generic"},
            "Pacific Time",
        ),
        (
            "America/Los_Angeles",
            {"locale": "en", "width": "long", "zone_variant": "standard"},
            "Pacific Standard Time",
        ),
        (
            "America/Los_Angeles",
            {"locale": "en", "width": "long", "zone_variant": "daylight"},
            "Pacific Daylight Time",
        ),
        ("Europe/Berlin", {"locale": "en_US"}, "Central European Time"),
    ],
)
def test_get_timezone_name_tzinfo(timezone_getter, tzname, params, expected):
    tz = timezone_getter(tzname)
    assert dates.get_timezone_name(tz, **params) == expected


@pytest.mark.parametrize("timezone_getter", ["pytz.timezone"], indirect=True)
@pytest.mark.parametrize(
    "tzname, params, expected",
    [
        ("America/Los_Angeles", {"locale": "en_US"}, "Pacific Standard Time"),
        (
            "America/Los_Angeles",
            {"locale": "en_US", "return_zone": True},
            "America/Los_Angeles",
        ),
        ("America/Los_Angeles", {"width": "short", "locale": "en_US"}, "PST"),
    ],
)
def test_get_timezone_name_time_pytz(timezone_getter, tzname, params, expected):
    """pytz (by design) can't determine if the time is in DST or not,
    so it will always return Standard time"""
    dt = time(15, 30, tzinfo=timezone_getter(tzname))
    assert dates.get_timezone_name(dt, **params) == expected


def test_get_timezone_name_misc(timezone_getter):
    localnow = datetime.now(timezone_getter('UTC')).astimezone(dates.LOCALTZ)
    assert (dates.get_timezone_name(None, locale='en_US') ==
            dates.get_timezone_name(localnow, locale='en_US'))

    assert (dates.get_timezone_name('Europe/Berlin', locale='en_US') == "Central European Time")

    assert (dates.get_timezone_name(1400000000, locale='en_US', width='short') == "Unknown Region (UTC) Time")
    assert (dates.get_timezone_name(time(16, 20), locale='en_US', width='short') == "UTC")


def test_format_date():
    d = date(2007, 4, 1)
    assert dates.format_date(d, locale='en_US') == 'Apr 1, 2007'
    assert (dates.format_date(d, format='full', locale='de_DE') ==
            'Sonntag, 1. April 2007')
    assert (dates.format_date(d, "EEE, MMM d, ''yy", locale='en') ==
            "Sun, Apr 1, '07")


def test_format_datetime(timezone_getter):
    dt = datetime(2007, 4, 1, 15, 30)
    assert (dates.format_datetime(dt, locale='en_US') ==
            'Apr 1, 2007, 3:30:00\u202fPM')

    full = dates.format_datetime(
        dt, 'full',
        tzinfo=timezone_getter('Europe/Paris'),
        locale='fr_FR',
    )
    assert full == (
        'dimanche 1 avril 2007, 17:30:00 heure '
        'd’été d’Europe centrale'
    )
    custom = dates.format_datetime(
        dt, "yyyy.MM.dd G 'at' HH:mm:ss zzz",
        tzinfo=timezone_getter('US/Eastern'),
        locale='en',
    )
    assert custom == '2007.04.01 AD at 11:30:00 EDT'


def test_format_time(timezone_getter):
    t = time(15, 30)
    assert dates.format_time(t, locale='en_US') == '3:30:00\u202fPM'
    assert dates.format_time(t, format='short', locale='de_DE') == '15:30'

    assert (dates.format_time(t, "hh 'o''clock' a", locale='en') ==
            "03 o'clock PM")

    paris = timezone_getter('Europe/Paris')
    eastern = timezone_getter('US/Eastern')

    t = _localize(paris, datetime(2007, 4, 1, 15, 30))
    fr = dates.format_time(t, format='full', tzinfo=paris, locale='fr_FR')
    assert fr == '15:30:00 heure d’été d’Europe centrale'

    custom = dates.format_time(t, "hh 'o''clock' a, zzzz", tzinfo=eastern, locale='en')
    assert custom == "09 o'clock AM, Eastern Daylight Time"

    with freezegun.freeze_time("2023-01-01"):
        t = time(15, 30)
        paris = dates.format_time(t, format='full', tzinfo=paris, locale='fr_FR')
        assert paris == '15:30:00 heure normale d’Europe centrale'

        us_east = dates.format_time(t, format='full', tzinfo=eastern, locale='en_US')
        assert us_east == '3:30:00\u202fPM Eastern Standard Time'


def test_format_skeleton(timezone_getter):
    dt = datetime(2007, 4, 1, 15, 30)
    assert (dates.format_skeleton('yMEd', dt, locale='en_US') == 'Sun, 4/1/2007')
    assert (dates.format_skeleton('yMEd', dt, locale='th') == 'อา. 1/4/2007')

    assert (dates.format_skeleton('EHm', dt, locale='en') == 'Sun 15:30')
    assert (dates.format_skeleton('EHm', dt, tzinfo=timezone_getter('Asia/Bangkok'), locale='th') == 'อา. 22:30 น.')


@pytest.mark.parametrize(('skeleton', 'expected'), [
    ('Hmz', 'Hmv'),
    ('kmv', 'Hmv'),
    ('Kmv', 'hmv'),
    ('Hma', 'Hm'),
    ('Hmb', 'Hm'),
    ('zkKab', 'vHh'),
])
def test_match_skeleton_alternate_characters(skeleton, expected):
    # https://github.com/unicode-org/icu/blob/5e22f0076ec9b55056cd8a84e9ef370632f44174/icu4j/main/core/src/main/java/com/ibm/icu/text/DateIntervalInfo.java#L1090-L1102
    assert dates.match_skeleton(skeleton, (expected,)) == expected


def test_format_timedelta():
    assert (dates.format_timedelta(timedelta(weeks=12), locale='en_US')
            == '3 months')
    assert (dates.format_timedelta(timedelta(seconds=1), locale='es')
            == '1 segundo')

    assert (dates.format_timedelta(timedelta(hours=3), granularity='day',
                                   locale='en_US')
            == '1 day')

    assert (dates.format_timedelta(timedelta(hours=23), threshold=0.9,
                                   locale='en_US')
            == '1 day')
    assert (dates.format_timedelta(timedelta(hours=23), threshold=1.1,
                                   locale='en_US')
            == '23 hours')


def test_parse_date():
    assert dates.parse_date('4/1/04', locale='en_US') == date(2004, 4, 1)
    assert dates.parse_date('01.04.2004', locale='de_DE') == date(2004, 4, 1)
    assert dates.parse_date('2004-04-01', locale='sv_SE', format='short') == date(2004, 4, 1)


def test_parse_date_custom_format():
    assert dates.parse_date('1.4.2024', format='dd.mm.yyyy') == date(2024, 4, 1)
    assert dates.parse_date('2024.4.1', format='yyyy.mm.dd') == date(2024, 4, 1)
    # Dates that look like ISO 8601 should use the custom format as well:
    assert dates.parse_date('2024-04-01', format='yyyy.dd.mm') == date(2024, 1, 4)


@pytest.mark.parametrize('input, expected', [
    # base case, fully qualified time
    ('15:30:00', time(15, 30)),
    # test digits
    ('15:30', time(15, 30)),
    ('3:30', time(3, 30)),
    ('00:30', time(0, 30)),
    # test am parsing
    ('03:30 am', time(3, 30)),
    ('3:30:21 am', time(3, 30, 21)),
    ('3:30 am', time(3, 30)),
    # test pm parsing
    ('03:30 pm', time(15, 30)),
    ('03:30 pM', time(15, 30)),
    ('03:30 Pm', time(15, 30)),
    ('03:30 PM', time(15, 30)),
    # test hour-only parsing
    ('4 pm', time(16, 0)),
])
def test_parse_time(input, expected):
    assert dates.parse_time(input, locale='en_US') == expected


def test_parse_time_no_seconds_in_format():
    # parse time using a time format which does not include seconds
    locale = 'cs_CZ'
    fmt = 'short'
    assert dates.get_time_format(format=fmt, locale=locale).pattern == 'H:mm'
    assert dates.parse_time('9:30', locale=locale, format=fmt) == time(9, 30)


def test_parse_time_alternate_characters(monkeypatch):
    # 'K' can be used as an alternative to 'H'
    def get_time_format(*args, **kwargs):
        return parse_pattern('KK:mm:ss')
    monkeypatch.setattr(dates, 'get_time_format', get_time_format)

    assert dates.parse_time('9:30') == time(9, 30)


def test_parse_date_alternate_characters(monkeypatch):
    # 'l' can be used as an alternative to 'm'
    def get_date_format(*args, **kwargs):
        return parse_pattern('yyyy-ll-dd')
    monkeypatch.setattr(dates, 'get_time_format', get_date_format)

    assert dates.parse_date('2024-10-20') == date(2024, 10, 20)


def test_parse_time_custom_format():
    assert dates.parse_time('15:30:00', format='HH:mm:ss') == time(15, 30)
    assert dates.parse_time('00:30:15', format='ss:mm:HH') == time(15, 30)


@pytest.mark.parametrize('case', ['', 'a', 'aaa'])
@pytest.mark.parametrize('func', [dates.parse_date, dates.parse_time])
def test_parse_errors(case, func):
    with pytest.raises(dates.ParseError):
        func(case, locale='en_US')


def test_datetime_format_get_week_number():
    format = dates.DateTimeFormat(date(2006, 1, 8), Locale.parse('de_DE'))
    assert format.get_week_number(6) == 1

    format = dates.DateTimeFormat(date(2006, 1, 8), Locale.parse('en_US'))
    assert format.get_week_number(6) == 2


def test_parse_pattern():
    assert dates.parse_pattern("MMMMd").format == '%(MMMM)s%(d)s'
    assert (dates.parse_pattern("MMM d, yyyy").format ==
            '%(MMM)s %(d)s, %(yyyy)s')
    assert (dates.parse_pattern("H:mm' Uhr 'z").format ==
            '%(H)s:%(mm)s Uhr %(z)s')
    assert dates.parse_pattern("hh' o''clock'").format == "%(hh)s o'clock"


def test_lithuanian_long_format():
    assert (
        dates.format_date(date(2015, 12, 10), locale='lt_LT', format='long') ==
        '2015 m. gruodžio 10 d.'
    )


def test_zh_TW_format():
    # Refs GitHub issue #378
    assert dates.format_time(datetime(2016, 4, 8, 12, 34, 56), locale='zh_TW') == '中午12:34:56'


def test_format_current_moment():
    frozen_instant = datetime.now(UTC)
    with freezegun.freeze_time(time_to_freeze=frozen_instant):
        assert dates.format_datetime(locale="en_US") == dates.format_datetime(frozen_instant, locale="en_US")


@pytest.mark.all_locales
def test_no_inherit_metazone_marker_never_in_output(locale, timezone_getter):
    # See: https://github.com/python-babel/babel/issues/428
    tz = timezone_getter('America/Los_Angeles')
    t = _localize(tz, datetime(2016, 1, 6, 7))
    assert NO_INHERITANCE_MARKER not in dates.format_time(t, format='long', locale=locale)
    assert NO_INHERITANCE_MARKER not in dates.get_timezone_name(t, width='short', locale=locale)


def test_no_inherit_metazone_formatting(timezone_getter):
    # See: https://github.com/python-babel/babel/issues/428
    tz = timezone_getter('America/Los_Angeles')
    t = _localize(tz, datetime(2016, 1, 6, 7))
    assert dates.format_time(t, format='long', locale='en_US') == "7:00:00\u202fAM PST"
    assert dates.format_time(t, format='long', locale='en_GB') == "07:00:00 Pacific Standard Time"
    assert dates.get_timezone_name(t, width='short', locale='en_US') == "PST"
    assert dates.get_timezone_name(t, width='short', locale='en_GB') == "Pacific Standard Time"


def test_russian_week_numbering():
    # See https://github.com/python-babel/babel/issues/485
    v = date(2017, 1, 1)
    assert dates.format_date(v, format='YYYY-ww', locale='ru_RU') == '2016-52'  # This would have returned 2017-01 prior to CLDR 32
    assert dates.format_date(v, format='YYYY-ww', locale='de_DE') == '2016-52'


def test_week_numbering_isocalendar():
    locale = Locale.parse('de_DE')
    assert locale.first_week_day == 0
    assert locale.min_week_days == 4

    def week_number(value):
        return dates.format_date(value, format="YYYY-'W'ww-e", locale=locale)

    for year in range(1991, 2010):
        for delta in range(-9, 9):
            value = date(year, 1, 1) + timedelta(days=delta)
            expected = '%04d-W%02d-%d' % value.isocalendar()
            assert week_number(value) == expected


def test_week_numbering_monday_mindays_4():
    locale = Locale.parse('de_DE')
    assert locale.first_week_day == 0
    assert locale.min_week_days == 4

    def week_number(value):
        value = date.fromisoformat(value)
        return dates.format_date(value, format="YYYY-'W'ww-e", locale=locale)

    def week_of_month(value):
        value = date.fromisoformat(value)
        return dates.format_date(value, format='W', locale=locale)

    assert week_number('2003-01-01') == '2003-W01-3'
    assert week_number('2003-01-06') == '2003-W02-1'
    assert week_number('2003-12-28') == '2003-W52-7'
    assert week_number('2003-12-29') == '2004-W01-1'
    assert week_number('2003-12-31') == '2004-W01-3'
    assert week_number('2004-01-01') == '2004-W01-4'
    assert week_number('2004-01-05') == '2004-W02-1'
    assert week_number('2004-12-26') == '2004-W52-7'
    assert week_number('2004-12-27') == '2004-W53-1'
    assert week_number('2004-12-31') == '2004-W53-5'
    assert week_number('2005-01-01') == '2004-W53-6'
    assert week_number('2005-01-03') == '2005-W01-1'
    assert week_number('2005-01-10') == '2005-W02-1'
    assert week_number('2005-12-31') == '2005-W52-6'
    assert week_number('2006-01-01') == '2005-W52-7'
    assert week_number('2006-01-02') == '2006-W01-1'
    assert week_number('2006-01-09') == '2006-W02-1'
    assert week_number('2006-12-31') == '2006-W52-7'
    assert week_number('2007-01-01') == '2007-W01-1'
    assert week_number('2007-12-30') == '2007-W52-7'
    assert week_number('2007-12-31') == '2008-W01-1'
    assert week_number('2008-01-01') == '2008-W01-2'
    assert week_number('2008-01-07') == '2008-W02-1'
    assert week_number('2008-12-28') == '2008-W52-7'
    assert week_number('2008-12-29') == '2009-W01-1'
    assert week_number('2008-12-31') == '2009-W01-3'
    assert week_number('2009-01-01') == '2009-W01-4'
    assert week_number('2009-01-05') == '2009-W02-1'
    assert week_number('2009-12-27') == '2009-W52-7'
    assert week_number('2009-12-28') == '2009-W53-1'
    assert week_number('2009-12-31') == '2009-W53-4'
    assert week_number('2010-01-01') == '2009-W53-5'
    assert week_number('2010-01-03') == '2009-W53-7'
    assert week_number('2010-01-04') == '2010-W01-1'

    assert week_of_month('2003-01-01') == '1'  # Wed
    assert week_of_month('2003-02-28') == '4'
    assert week_of_month('2003-05-01') == '1'  # Thu
    assert week_of_month('2003-08-01') == '0'  # Fri
    assert week_of_month('2003-12-31') == '5'
    assert week_of_month('2004-01-01') == '1'  # Thu
    assert week_of_month('2004-02-29') == '4'


def test_week_numbering_monday_mindays_1():
    locale = Locale.parse('tr_TR')
    assert locale.first_week_day == 0
    assert locale.min_week_days == 1

    def week_number(value):
        value = date.fromisoformat(value)
        return dates.format_date(value, format="YYYY-'W'ww-e", locale=locale)

    def week_of_month(value):
        value = date.fromisoformat(value)
        return dates.format_date(value, format='W', locale=locale)

    assert week_number('2003-01-01') == '2003-W01-3'
    assert week_number('2003-01-06') == '2003-W02-1'
    assert week_number('2003-12-28') == '2003-W52-7'
    assert week_number('2003-12-29') == '2004-W01-1'
    assert week_number('2003-12-31') == '2004-W01-3'
    assert week_number('2004-01-01') == '2004-W01-4'
    assert week_number('2004-01-05') == '2004-W02-1'
    assert week_number('2004-12-26') == '2004-W52-7'
    assert week_number('2004-12-27') == '2005-W01-1'
    assert week_number('2004-12-31') == '2005-W01-5'
    assert week_number('2005-01-01') == '2005-W01-6'
    assert week_number('2005-01-03') == '2005-W02-1'
    assert week_number('2005-12-25') == '2005-W52-7'
    assert week_number('2005-12-26') == '2006-W01-1'
    assert week_number('2005-12-31') == '2006-W01-6'
    assert week_number('2006-01-01') == '2006-W01-7'
    assert week_number('2006-01-02') == '2006-W02-1'
    assert week_number('2006-12-31') == '2006-W53-7'
    assert week_number('2007-01-01') == '2007-W01-1'
    assert week_number('2007-01-08') == '2007-W02-1'
    assert week_number('2007-12-30') == '2007-W52-7'
    assert week_number('2007-12-31') == '2008-W01-1'
    assert week_number('2008-01-01') == '2008-W01-2'
    assert week_number('2008-01-07') == '2008-W02-1'
    assert week_number('2008-12-28') == '2008-W52-7'
    assert week_number('2008-12-29') == '2009-W01-1'
    assert week_number('2008-12-31') == '2009-W01-3'
    assert week_number('2009-01-01') == '2009-W01-4'
    assert week_number('2009-01-05') == '2009-W02-1'
    assert week_number('2009-12-27') == '2009-W52-7'
    assert week_number('2009-12-28') == '2010-W01-1'
    assert week_number('2009-12-31') == '2010-W01-4'
    assert week_number('2010-01-01') == '2010-W01-5'
    assert week_number('2010-01-03') == '2010-W01-7'
    assert week_number('2010-01-04') == '2010-W02-1'

    assert week_of_month('2003-01-01') == '1'  # Wed
    assert week_of_month('2003-02-28') == '5'
    assert week_of_month('2003-06-01') == '1'  # Sun
    assert week_of_month('2003-12-31') == '5'
    assert week_of_month('2004-01-01') == '1'  # Thu
    assert week_of_month('2004-02-29') == '5'


def test_week_numbering_sunday_mindays_1():
    locale = Locale.parse('en_US')
    assert locale.first_week_day == 6
    assert locale.min_week_days == 1

    def week_number(value):
        value = date.fromisoformat(value)
        return dates.format_date(value, format="YYYY-'W'ww-e", locale=locale)

    def week_of_month(value):
        value = date.fromisoformat(value)
        return dates.format_date(value, format='W', locale=locale)

    assert week_number('2003-01-01') == '2003-W01-4'
    assert week_number('2003-01-05') == '2003-W02-1'
    assert week_number('2003-12-27') == '2003-W52-7'
    assert week_number('2003-12-28') == '2004-W01-1'
    assert week_number('2003-12-31') == '2004-W01-4'
    assert week_number('2004-01-01') == '2004-W01-5'
    assert week_number('2004-01-04') == '2004-W02-1'
    assert week_number('2004-12-25') == '2004-W52-7'
    assert week_number('2004-12-26') == '2005-W01-1'
    assert week_number('2004-12-31') == '2005-W01-6'
    assert week_number('2005-01-01') == '2005-W01-7'
    assert week_number('2005-01-02') == '2005-W02-1'
    assert week_number('2005-12-24') == '2005-W52-7'
    assert week_number('2005-12-25') == '2005-W53-1'
    assert week_number('2005-12-31') == '2005-W53-7'
    assert week_number('2006-01-01') == '2006-W01-1'
    assert week_number('2006-01-08') == '2006-W02-1'
    assert week_number('2006-12-30') == '2006-W52-7'
    assert week_number('2006-12-31') == '2007-W01-1'
    assert week_number('2007-01-01') == '2007-W01-2'
    assert week_number('2007-01-07') == '2007-W02-1'
    assert week_number('2007-12-29') == '2007-W52-7'
    assert week_number('2007-12-30') == '2008-W01-1'
    assert week_number('2007-12-31') == '2008-W01-2'
    assert week_number('2008-01-01') == '2008-W01-3'
    assert week_number('2008-01-06') == '2008-W02-1'
    assert week_number('2008-12-27') == '2008-W52-7'
    assert week_number('2008-12-28') == '2009-W01-1'
    assert week_number('2008-12-31') == '2009-W01-4'
    assert week_number('2009-01-01') == '2009-W01-5'
    assert week_number('2009-01-04') == '2009-W02-1'
    assert week_number('2009-12-26') == '2009-W52-7'
    assert week_number('2009-12-27') == '2010-W01-1'
    assert week_number('2009-12-31') == '2010-W01-5'
    assert week_number('2010-01-01') == '2010-W01-6'
    assert week_number('2010-01-03') == '2010-W02-1'

    assert week_of_month('2003-01-01') == '1'  # Wed
    assert week_of_month('2003-02-01') == '1'  # Sat
    assert week_of_month('2003-02-28') == '5'
    assert week_of_month('2003-12-31') == '5'
    assert week_of_month('2004-01-01') == '1'  # Thu
    assert week_of_month('2004-02-29') == '5'


def test_week_numbering_sunday_mindays_4():
    locale = Locale.parse('pt_PT')
    assert locale.first_week_day == 6
    assert locale.min_week_days == 4

    def week_number(value):
        value = date.fromisoformat(value)
        return dates.format_date(value, format="YYYY-'W'ww-e", locale=locale)

    def week_of_month(value):
        value = date.fromisoformat(value)
        return dates.format_date(value, format='W', locale=locale)

    assert week_number('2003-01-01') == '2003-W01-4'
    assert week_number('2003-01-05') == '2003-W02-1'
    assert week_number('2003-12-27') == '2003-W52-7'
    assert week_number('2003-12-28') == '2003-W53-1'
    assert week_number('2003-12-31') == '2003-W53-4'
    assert week_number('2004-01-01') == '2003-W53-5'
    assert week_number('2004-01-04') == '2004-W01-1'
    assert week_number('2004-12-25') == '2004-W51-7'
    assert week_number('2004-12-26') == '2004-W52-1'
    assert week_number('2004-12-31') == '2004-W52-6'
    assert week_number('2005-01-01') == '2004-W52-7'
    assert week_number('2005-01-02') == '2005-W01-1'
    assert week_number('2005-12-24') == '2005-W51-7'
    assert week_number('2005-12-25') == '2005-W52-1'
    assert week_number('2005-12-31') == '2005-W52-7'
    assert week_number('2006-01-01') == '2006-W01-1'
    assert week_number('2006-01-08') == '2006-W02-1'
    assert week_number('2006-12-30') == '2006-W52-7'
    assert week_number('2006-12-31') == '2007-W01-1'
    assert week_number('2007-01-01') == '2007-W01-2'
    assert week_number('2007-01-07') == '2007-W02-1'
    assert week_number('2007-12-29') == '2007-W52-7'
    assert week_number('2007-12-30') == '2008-W01-1'
    assert week_number('2007-12-31') == '2008-W01-2'
    assert week_number('2008-01-01') == '2008-W01-3'
    assert week_number('2008-01-06') == '2008-W02-1'
    assert week_number('2008-12-27') == '2008-W52-7'
    assert week_number('2008-12-28') == '2008-W53-1'
    assert week_number('2008-12-31') == '2008-W53-4'
    assert week_number('2009-01-01') == '2008-W53-5'
    assert week_number('2009-01-04') == '2009-W01-1'
    assert week_number('2009-12-26') == '2009-W51-7'
    assert week_number('2009-12-27') == '2009-W52-1'
    assert week_number('2009-12-31') == '2009-W52-5'
    assert week_number('2010-01-01') == '2009-W52-6'
    assert week_number('2010-01-03') == '2010-W01-1'

    assert week_of_month('2003-01-01') == '1'  # Wed
    assert week_of_month('2003-02-28') == '4'
    assert week_of_month('2003-05-01') == '0'  # Thu
    assert week_of_month('2003-12-31') == '5'
    assert week_of_month('2004-01-01') == '0'  # Thu
    assert week_of_month('2004-02-29') == '5'


def test_week_numbering_friday_mindays_1():
    locale = Locale.parse('dv_MV')
    assert locale.first_week_day == 4
    assert locale.min_week_days == 1

    def week_number(value):
        value = date.fromisoformat(value)
        return dates.format_date(value, format="YYYY-'W'ww-e", locale=locale)

    def week_of_month(value):
        value = date.fromisoformat(value)
        return dates.format_date(value, format='W', locale=locale)

    assert week_number('2003-01-01') == '2003-W01-6'
    assert week_number('2003-01-03') == '2003-W02-1'
    assert week_number('2003-12-25') == '2003-W52-7'
    assert week_number('2003-12-26') == '2004-W01-1'
    assert week_number('2003-12-31') == '2004-W01-6'
    assert week_number('2004-01-01') == '2004-W01-7'
    assert week_number('2004-01-02') == '2004-W02-1'
    assert week_number('2004-12-23') == '2004-W52-7'
    assert week_number('2004-12-24') == '2004-W53-1'
    assert week_number('2004-12-30') == '2004-W53-7'
    assert week_number('2004-12-31') == '2005-W01-1'
    assert week_number('2005-01-01') == '2005-W01-2'
    assert week_number('2005-01-07') == '2005-W02-1'
    assert week_number('2005-12-29') == '2005-W52-7'
    assert week_number('2005-12-30') == '2006-W01-1'
    assert week_number('2005-12-31') == '2006-W01-2'
    assert week_number('2006-01-01') == '2006-W01-3'
    assert week_number('2006-01-06') == '2006-W02-1'
    assert week_number('2006-12-28') == '2006-W52-7'
    assert week_number('2006-12-29') == '2007-W01-1'
    assert week_number('2006-12-31') == '2007-W01-3'
    assert week_number('2007-01-01') == '2007-W01-4'
    assert week_number('2007-01-05') == '2007-W02-1'
    assert week_number('2007-12-27') == '2007-W52-7'
    assert week_number('2007-12-28') == '2008-W01-1'
    assert week_number('2007-12-31') == '2008-W01-4'
    assert week_number('2008-01-01') == '2008-W01-5'
    assert week_number('2008-01-04') == '2008-W02-1'
    assert week_number('2008-12-25') == '2008-W52-7'
    assert week_number('2008-12-26') == '2009-W01-1'
    assert week_number('2008-12-31') == '2009-W01-6'
    assert week_number('2009-01-01') == '2009-W01-7'
    assert week_number('2009-01-02') == '2009-W02-1'
    assert week_number('2009-12-24') == '2009-W52-7'
    assert week_number('2009-12-25') == '2009-W53-1'
    assert week_number('2009-12-31') == '2009-W53-7'
    assert week_number('2010-01-01') == '2010-W01-1'
    assert week_number('2010-01-08') == '2010-W02-1'

    assert week_of_month('2003-01-01') == '1'  # Wed
    assert week_of_month('2003-02-28') == '5'
    assert week_of_month('2003-03-01') == '1'  # Sun
    assert week_of_month('2003-12-31') == '5'
    assert week_of_month('2004-01-01') == '1'  # Thu
    assert week_of_month('2004-02-29') == '5'


def test_week_numbering_saturday_mindays_1():
    locale = Locale.parse('fr_DZ')
    assert locale.first_week_day == 5
    assert locale.min_week_days == 1

    def week_number(value):
        value = date.fromisoformat(value)
        return dates.format_date(value, format="YYYY-'W'ww-e", locale=locale)

    def week_of_month(value):
        value = date.fromisoformat(value)
        return dates.format_date(value, format='W', locale=locale)

    assert week_number('2003-01-01') == '2003-W01-5'
    assert week_number('2003-01-04') == '2003-W02-1'
    assert week_number('2003-12-26') == '2003-W52-7'
    assert week_number('2003-12-27') == '2004-W01-1'
    assert week_number('2003-12-31') == '2004-W01-5'
    assert week_number('2004-01-01') == '2004-W01-6'
    assert week_number('2004-01-03') == '2004-W02-1'
    assert week_number('2004-12-24') == '2004-W52-7'
    assert week_number('2004-12-31') == '2004-W53-7'
    assert week_number('2005-01-01') == '2005-W01-1'
    assert week_number('2005-01-08') == '2005-W02-1'
    assert week_number('2005-12-30') == '2005-W52-7'
    assert week_number('2005-12-31') == '2006-W01-1'
    assert week_number('2006-01-01') == '2006-W01-2'
    assert week_number('2006-01-07') == '2006-W02-1'
    assert week_number('2006-12-29') == '2006-W52-7'
    assert week_number('2006-12-31') == '2007-W01-2'
    assert week_number('2007-01-01') == '2007-W01-3'
    assert week_number('2007-01-06') == '2007-W02-1'
    assert week_number('2007-12-28') == '2007-W52-7'
    assert week_number('2007-12-31') == '2008-W01-3'
    assert week_number('2008-01-01') == '2008-W01-4'
    assert week_number('2008-01-05') == '2008-W02-1'
    assert week_number('2008-12-26') == '2008-W52-7'
    assert week_number('2008-12-27') == '2009-W01-1'
    assert week_number('2008-12-31') == '2009-W01-5'
    assert week_number('2009-01-01') == '2009-W01-6'
    assert week_number('2009-01-03') == '2009-W02-1'
    assert week_number('2009-12-25') == '2009-W52-7'
    assert week_number('2009-12-26') == '2010-W01-1'
    assert week_number('2009-12-31') == '2010-W01-6'
    assert week_number('2010-01-01') == '2010-W01-7'
    assert week_number('2010-01-02') == '2010-W02-1'

    assert week_of_month('2003-01-01') == '1'  # Wed
    assert week_of_month('2003-02-28') == '4'
    assert week_of_month('2003-08-01') == '1'  # Fri
    assert week_of_month('2003-12-31') == '5'
    assert week_of_month('2004-01-01') == '1'  # Thu
    assert week_of_month('2004-02-29') == '5'


def test_en_gb_first_weekday():
    assert Locale.parse('en').first_week_day == 0  # Monday in general
    assert Locale.parse('en_US').first_week_day == 6  # Sunday in the US
    assert Locale.parse('en_GB').first_week_day == 0  # Monday in the UK


def test_issue_798():
    assert dates.format_timedelta(timedelta(), format='narrow', locale='es_US') == '0s'


def test_issue_892():
    assert dates.format_timedelta(timedelta(seconds=1), format='narrow', locale='pt_BR') == '1 s'
    assert dates.format_timedelta(timedelta(minutes=1), format='narrow', locale='pt_BR') == '1 min'
    assert dates.format_timedelta(timedelta(hours=1), format='narrow', locale='pt_BR') == '1 h'
    assert dates.format_timedelta(timedelta(days=1), format='narrow', locale='pt_BR') == '1 dia'
    assert dates.format_timedelta(timedelta(days=30), format='narrow', locale='pt_BR') == '1 mês'
    assert dates.format_timedelta(timedelta(days=365), format='narrow', locale='pt_BR') == '1 ano'


def test_issue_1089():
    assert dates.format_datetime(datetime.now(), locale="ja_JP@mod")
    assert dates.format_datetime(datetime.now(), locale=Locale.parse("ja_JP@mod"))


@pytest.mark.parametrize(('locale', 'format', 'negative', 'expected'), [
    ('en_US', 'long', False, 'in 3 hours'),
    ('en_US', 'long', True, '3 hours ago'),
    ('en_US', 'narrow', False, 'in 3h'),
    ('en_US', 'narrow', True, '3h ago'),
    ('en_US', 'short', False, 'in 3 hr.'),
    ('en_US', 'short', True, '3 hr. ago'),
    ('fi_FI', 'long', False, '3 tunnin päästä'),
    ('fi_FI', 'long', True, '3 tuntia sitten'),
    ('fi_FI', 'short', False, '3 t päästä'),
    ('fi_FI', 'short', True, '3 t sitten'),
    ('sv_SE', 'long', False, 'om 3 timmar'),
    ('sv_SE', 'long', True, 'för 3 timmar sedan'),
    ('sv_SE', 'short', False, 'om 3 tim'),
    ('sv_SE', 'short', True, 'för 3 tim sedan'),
])
def test_issue_1162(locale, format, negative, expected):
    delta = timedelta(seconds=10800) * (-1 if negative else +1)
    assert dates.format_timedelta(delta, add_direction=True, format=format, locale=locale) == expected


def test_issue_1192():
    # The actual returned value here is not actually strictly specified ("get_timezone_name"
    # is not an operation specified as such). Issue #1192 concerned this invocation returning
    # the invalid "no inheritance marker" value; _that_ should never be returned here.
    # IOW, if the below "Hawaii-Aleutian Time" changes with e.g. CLDR updates, that's fine.
    assert dates.get_timezone_name('Pacific/Honolulu', 'short', locale='en_GB') == "Hawaii-Aleutian Time"


@pytest.mark.xfail
def test_issue_1192_fmt(timezone_getter):
    """
    There is an issue in how we format the fallback for z/zz in the absence of data
    (esp. with the no inheritance marker present).
    This test is marked xfail until that's fixed.
    """
    # env TEST_TIMEZONES=Pacific/Honolulu TEST_LOCALES=en_US,en_GB TEST_TIME_FORMAT="YYYY-MM-dd H:mm z" bin/icu4c_date_format
    # Defaulting TEST_TIME to 2025-03-04T13:53:00Z
    # Pacific/Honolulu        en_US   2025-03-04 3:53 HST
    # Pacific/Honolulu        en_GB   2025-03-04 3:53 GMT-10
    # env TEST_TIMEZONES=Pacific/Honolulu TEST_LOCALES=en_US,en_GB TEST_TIME_FORMAT="YYYY-MM-dd H:mm zz" bin/icu4c_date_format
    # Pacific/Honolulu        en_US   2025-03-04 3:53 HST
    # Pacific/Honolulu        en_GB   2025-03-04 3:53 GMT-10
    tz = timezone_getter("Pacific/Honolulu")
    dt = _localize(tz, datetime(2025, 3, 4, 13, 53, tzinfo=UTC))
    assert dates.format_datetime(dt, "YYYY-MM-dd H:mm z", locale="en_US") == "2025-03-04 3:53 HST"
    assert dates.format_datetime(dt, "YYYY-MM-dd H:mm z", locale="en_GB") == "2025-03-04 3:53 GMT-10"
    assert dates.format_datetime(dt, "YYYY-MM-dd H:mm zz", locale="en_US") == "2025-03-04 3:53 HST"
    assert dates.format_datetime(dt, "YYYY-MM-dd H:mm zz", locale="en_GB") == "2025-03-04 3:53 GMT-10"
