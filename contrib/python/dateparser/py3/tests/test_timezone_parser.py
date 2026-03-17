import datetime as dt
from datetime import datetime, timedelta
from unittest import SkipTest
from unittest.mock import Mock, patch

from parameterized import param, parameterized
from pytz import timezone

import dateparser.timezone_parser
from dateparser import parse
from dateparser.timezone_parser import (
    StaticTzInfo,
    get_local_tz_offset,
    pop_tz_offset_from_string,
)
from tests import BaseTestCase


class TestTZPopping(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.initial_string = self.datetime_string = self.timezone_offset = (
            NotImplemented
        )

    @parameterized.expand(
        [
            param("Sep 03 2014 | 4:32 pm EDT", -4),
            param("17th October, 2034 @ 01:08 am PDT", -7),
            param("17th October, 2034 @ 01:08 am (PDT)", -7),
            param("October 17, 2014 at 7:30 am PST", -8),
            param("20 Oct 2014 13:08 CET", +1),
            param("20 Oct 2014 13:08cet", +1),
            param("Nov 25 2014 | 10:17 pm EST", -5),
            param("Nov 25 2014 | 10:17 pm +0600", +6),
            param("Nov 25 2014 | 10:17 pm -0930", -9.5),
            param("20 Oct 2014 | 05:17 am -1200", -12),
            param("20 Oct 2014 | 05:17 am +0000", 0),
            param("20 Oct 2014 | 05:17 am -0000", 0),
            param("15 May 2004", None),
            param("Wed Aug 05 12:00:00 EDTERR 2015", None),
            param("Wed Aug 05 12:00:00 EDT 2015", -4),
            param("April 10, 2016 at 12:00:00 UTC", 0),
            param("April 10, 2016 at 12:00:00 MEZ", 1),
            param("April 10, 2016 at 12:00:00 MESZ", 2),
            param("April 10, 2016 at 12:00:00 GMT+2", 2),
            param("April 10, 2016 at 12:00:00 UTC+2:00", 2),
            param("April 10, 2016 at 12:00:00 GMT+02:00", 2),
            param("April 10, 2016 at 12:00:00 UTC+5:30", 5.5),
            param("April 10, 2016 at 12:00:00 GMT+05:30", 5.5),
            param("April 10, 2016 at 12:00:00 UTC-2", -2),
            param("April 10, 2016 at 12:00:00 GMT-2:00", -2),
            param("April 10, 2016 at 12:00:00 UTC-02:00", -2),
            param("April 10, 2016 at 12:00:00 GMT-9:30", -9.5),
            param("April 10, 2016 at 12:00:00 UTC-09:30", -9.5),
            param("Thu, 24 Nov 2016 16:03:00 UT", 0),
            param("Fri Sep 23 2016 10:34:51 GMT+0800 (CST)", 8),
            param("Fri Sep 23 2016 10:34:51 GMT+12", 12),
            param("Fri Sep 23 2016 10:34:51 UTC+13", 13),
            param("Fri Sep 23 2016 10:34:51 GMT+1245 (CST)", 12.75),
            param("Fri Sep 23 2016 10:34:51 UTC+1245", 12.75),
            param("2019-07-17T12:30:00.000-03:30", -3.5),
            param("2019-07-17T12:30:00.000-02:30", -2.5),
            param("16. srpna 2021 9:59:44 SELČ", 2),
            param("16. srpna 2021 9:59:44 SEČ", 1),
            param("16. srpna 2021 9:59:44 ZEČ", 0),
            param("16. srpna 2021 9:59:44 VEČ", 2),
        ]
    )
    def test_extracting_valid_offset(self, initial_string, expected_offset):
        self.given_string(initial_string)
        self.when_offset_popped_from_string()
        self.then_offset_is(expected_offset)

    @parameterized.expand(
        [
            param("Sep 03 2014 | 4:32 pm EDT", "Sep 03 2014 | 4:32 pm "),
            param(
                "17th October, 2034 @ 01:08 am PDT", "17th October, 2034 @ 01:08 am "
            ),
            param("October 17, 2014 at 7:30 am PST", "October 17, 2014 at 7:30 am "),
            param("20 Oct 2014 13:08 CET", "20 Oct 2014 13:08 "),
            param("20 Oct 2014 13:08cet", "20 Oct 2014 13:08"),
            param("Nov 25 2014 | 10:17 pm EST", "Nov 25 2014 | 10:17 pm "),
            param(
                "17th October, 2034 @ 01:08 am +0700", "17th October, 2034 @ 01:08 am "
            ),
            param("Sep 03 2014 4:32 pm +0630", "Sep 03 2014 4:32 pm "),
        ]
    )
    def test_timezone_deleted_from_string(self, initial_string, result_string):
        self.given_string(initial_string)
        self.when_offset_popped_from_string()
        self.then_string_modified_to(result_string)

    def test_string_not_changed_if_no_timezone(self):
        self.given_string("15 May 2004")
        self.when_offset_popped_from_string()
        self.then_string_modified_to("15 May 2004")

    def given_string(self, string_):
        self.initial_string = string_

    def when_offset_popped_from_string(self):
        self.datetime_string, timezone_offset = pop_tz_offset_from_string(
            self.initial_string
        )
        if timezone_offset:
            self.timezone_offset = timezone_offset.utcoffset("")
        else:
            self.timezone_offset = timezone_offset

    def then_string_modified_to(self, expected_string):
        self.assertEqual(expected_string, self.datetime_string)

    def then_offset_is(self, expected_offset):
        delta = (
            timedelta(hours=expected_offset) if expected_offset is not None else None
        )
        self.assertEqual(delta, self.timezone_offset)


class TestLocalTZOffset(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.timezone_offset = NotImplemented

    @parameterized.expand(
        [
            param(utc="2014-08-20 4:32", local="2014-08-20 8:32", offset=+4),
            param(utc="2052-01-02 11:07", local="2052-01-02 10:07", offset=-1),
            param(utc="2013-12-31 23:59", local="2014-01-01 00:29", offset=+0.5),
            param(utc="2011-07-14 11:59", local="2011-07-13 23:59", offset=-12),
            param(utc="2014-10-18 17:41", local="2014-10-18 17:41", offset=0),
        ]
    )
    def test_timezone_offset_calculation(self, utc, local, offset):
        try:
            self.given_time(utc, local)
        except OverflowError:
            raise SkipTest("Unsupported with 32-bit time_t")
        self.when_offset_popped_from_string()
        self.then_offset_is(offset)

    def when_offset_popped_from_string(self):
        self.timezone_offset = get_local_tz_offset()

    def then_offset_is(self, expected_offset):
        delta = (
            timedelta(seconds=3600 * expected_offset)
            if expected_offset is not None
            else None
        )
        self.assertEqual(delta, self.timezone_offset)

    def given_time(self, utc_dt_string, local_dt_string):
        datetime_cls = dateparser.timezone_parser.datetime
        if not isinstance(datetime_cls, Mock):
            datetime_cls = Mock(wraps=datetime)
        utc_dt_obj = datetime.strptime(utc_dt_string, "%Y-%m-%d %H:%M").replace(
            tzinfo=dt.timezone.utc
        )
        local_dt_obj = datetime.strptime(local_dt_string, "%Y-%m-%d %H:%M")

        def _dt_now(tz=None):
            if tz == dt.timezone.utc:
                return utc_dt_obj
            return local_dt_obj

        datetime_cls.now = Mock(side_effect=_dt_now)
        self.add_patch(patch("dateparser.timezone_parser.datetime", new=datetime_cls))


class TestTimeZoneConversion(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.settings = {}
        self.parser = parse
        self.result = NotImplemented

    @parameterized.expand(
        [
            param(
                "2015-12-31 10:04 AM",
                "Asia/Karachi",
                "UTC",
                datetime(2015, 12, 31, 5, 4),
            ),
            param(
                "2015-12-30 10:04 AM",
                "Asia/Karachi",
                "+0200",
                datetime(2015, 12, 30, 7, 4),
            ),
        ]
    )
    def test_timezone_conversion(self, datestring, from_tz, to_tz, expected):
        self.given_from_timezone(from_tz)
        self.given_to_timezone(to_tz)
        self.when_date_is_parsed(datestring)
        self.then_date_is(expected)

    def given_from_timezone(self, timezone):
        self.settings["TIMEZONE"] = timezone

    def given_to_timezone(self, timezone):
        self.settings["TO_TIMEZONE"] = timezone

    def when_date_is_parsed(self, datestring):
        self.result = self.parser(datestring, settings=self.settings)

    def then_date_is(self, date):
        self.assertEqual(date, self.result)


class TestStaticTzInfo(BaseTestCase):
    def setUp(self):
        super().setUp()

    @parameterized.expand(
        [
            param(given_date=datetime(2007, 1, 18, tzinfo=timezone("UTC"))),
            param(given_date=datetime(2003, 3, 31, tzinfo=timezone("US/Arizona"))),
            param(given_date=datetime(2000, 2, 20, tzinfo=timezone("Pacific/Samoa"))),
        ]
    )
    def test_localize_raises_error_if_date_has_tzinfo(self, given_date):
        self.timezone_info = StaticTzInfo("UTC\\+00:00", timedelta(0))
        self.when_date_is_localized(given_date)
        self.then_error_was_raised(
            ValueError, ["Not naive datetime (tzinfo is already set)"]
        )

    def when_date_is_localized(self, given_date):
        try:
            self.localized_date = self.timezone_info.localize(given_date)
        except Exception as error:
            self.error = error

    def then_localized_date_is(self, expected_date, expected_tzname):
        self.assertEqual(self.localized_date.date(), expected_date.date())
        self.assertEqual(self.localized_date.tzname(), expected_tzname)
