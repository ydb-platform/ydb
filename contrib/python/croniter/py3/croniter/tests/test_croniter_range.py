#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from datetime import datetime, timedelta

import pytz

from croniter import CroniterBadCronError, CroniterBadDateError, CroniterBadTypeRangeError, croniter, croniter_range
from croniter.tests import base


class mydatetime(datetime):
    """."""


class CroniterRangeTest(base.TestCase):
    def test_1day_step(self):
        start = datetime(2016, 12, 2)
        stop = datetime(2016, 12, 10)
        fwd = list(croniter_range(start, stop, "0 0 * * *"))
        self.assertEqual(len(fwd), 9)
        self.assertEqual(fwd[0], start)
        self.assertEqual(fwd[-1], stop)
        # Test the same, but in reverse
        rev = list(croniter_range(stop, start, "0 0 * * *"))
        self.assertEqual(len(rev), 9)
        # Ensure forward/reverse are a mirror image
        rev.reverse()
        self.assertEqual(fwd, rev)

    def test_1day_step_no_ends(self):
        # Test without ends (exclusive)
        start = datetime(2016, 12, 2)
        stop = datetime(2016, 12, 10)
        fwd = list(croniter_range(start, stop, "0 0 * * *", exclude_ends=True))
        self.assertEqual(len(fwd), 7)
        self.assertNotEqual(fwd[0], start)
        self.assertNotEqual(fwd[-1], stop)
        # Test the same, but in reverse
        rev = list(croniter_range(stop, start, "0 0 * * *", exclude_ends=True))
        self.assertEqual(len(rev), 7)
        self.assertNotEqual(fwd[0], stop)
        self.assertNotEqual(fwd[-1], start)

    def test_1month_step(self):
        start = datetime(1982, 1, 1)
        stop = datetime(1983, 12, 31)
        res = list(croniter_range(start, stop, "0 0 1 * *"))
        self.assertEqual(len(res), 24)
        self.assertEqual(res[0], start)
        self.assertEqual(res[5].day, 1)
        self.assertEqual(res[-1], datetime(1983, 12, 1))

    def test_1minute_step_float(self):
        start = datetime(2000, 1, 1, 0, 0)
        stop = datetime(2000, 1, 1, 0, 1)
        res = list(croniter_range(start, stop, "* * * * *", ret_type=float))
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0], 946684800.0)
        self.assertEqual(res[-1] - res[0], 60)

    def test_auto_ret_type(self):
        data = [
            (datetime(2019, 1, 1), datetime(2020, 1, 1), datetime),
            (1552252218.0, 1591823311.0, float),
        ]
        for start, stop, rtype in data:
            ret = list(croniter_range(start, stop, "0 0 * * *"))
            self.assertIsInstance(ret[0], rtype)

    def test_input_type_exceptions(self):
        dt_start1 = datetime(2019, 1, 1)
        dt_stop1 = datetime(2020, 1, 1)
        f_start1 = 1552252218.0
        f_stop1 = 1591823311.0
        # Mix start/stop types
        with self.assertRaises(TypeError):
            list(croniter_range(dt_start1, f_stop1, "0 * * * *"), ret_type=datetime)
        with self.assertRaises(TypeError):
            list(croniter_range(f_start1, dt_stop1, "0 * * * *"))

    def test_timezone_dst(self):
        """Test across DST transition, which technically is a timzone change."""
        tz = pytz.timezone("US/Eastern")
        start = tz.localize(datetime(2020, 10, 30))
        stop = tz.localize(datetime(2020, 11, 10))
        res = list(croniter_range(start, stop, "0 0 * * *"))
        self.assertNotEqual(res[0].tzinfo, res[-1].tzinfo)
        self.assertEqual(len(res), 12)

    def test_extra_hour_day_prio(self):
        def datetime_tz(*args, **kw):
            """Defined this in another branch.  single-use-version"""
            tzinfo = kw.pop("tzinfo")
            return tzinfo.localize(datetime(*args))

        tz = pytz.timezone("US/Eastern")
        cron = "0 3 * * *"
        start = datetime_tz(2020, 3, 7, tzinfo=tz)
        end = datetime_tz(2020, 3, 11, tzinfo=tz)
        ret = [i.isoformat() for i in croniter_range(start, end, cron)]
        self.assertEqual(
            ret,
            [
                "2020-03-07T03:00:00-05:00",
                "2020-03-08T03:00:00-04:00",
                "2020-03-09T03:00:00-04:00",
                "2020-03-10T03:00:00-04:00",
            ],
        )

    def test_issue145_getnext(self):
        # Example of quarterly event cron schedule
        start = datetime(2020, 9, 24)
        cron = "0 13 8 1,4,7,10 wed"
        with self.assertRaises(CroniterBadDateError):
            it = croniter(cron, start, day_or=False, max_years_between_matches=1)
            it.get_next()
        # New functionality (0.3.35) allowing croniter to find spare matches of cron patterns across multiple years
        it = croniter(cron, start, day_or=False, max_years_between_matches=5)
        self.assertEqual(it.get_next(datetime), datetime(2025, 1, 8, 13))

    def test_issue145_range(self):
        cron = "0 13 8 1,4,7,10 wed"
        matches = list(croniter_range(datetime(2020, 1, 1), datetime(2020, 12, 31), cron, day_or=False))
        self.assertEqual(len(matches), 3)
        self.assertEqual(matches[0], datetime(2020, 1, 8, 13))
        self.assertEqual(matches[1], datetime(2020, 4, 8, 13))
        self.assertEqual(matches[2], datetime(2020, 7, 8, 13))

        # No matches within this range; therefore expect empty list
        matches = list(croniter_range(datetime(2020, 9, 30), datetime(2020, 10, 30), cron, day_or=False))
        self.assertEqual(len(matches), 0)

    def test_croniter_range_derived_class(self):
        # trivial example extending croniter

        class croniter_nosec(croniter):
            """Like croniter, but it forbids second-level cron expressions."""

            @classmethod
            def expand(cls, expr_format, *args, **kwargs):
                if len(expr_format.split()) == 6:
                    raise CroniterBadCronError("Expected 'min hour day mon dow'")
                return croniter.expand(expr_format, *args, **kwargs)

        cron = "0 13 8 1,4,7,10 wed"
        matches = list(
            croniter_range(
                datetime(2020, 1, 1),
                datetime(2020, 12, 31),
                cron,
                day_or=False,
                _croniter=croniter_nosec,
            )
        )
        self.assertEqual(len(matches), 3)

        cron = "0 1 8 1,15,L wed 15,45"
        with self.assertRaises(CroniterBadCronError):
            # Should fail using the custom class that forbids the seconds expression
            croniter_nosec(cron)

        with self.assertRaises(CroniterBadCronError):
            # Should similarly fail because the custom class rejects seconds expr
            i = croniter_range(
                datetime(2020, 1, 1),
                datetime(2020, 12, 31),
                cron,
                _croniter=croniter_nosec,
            )
            next(i)

    def test_dt_types(self):
        start = mydatetime(2020, 9, 24)
        stop = datetime(2020, 9, 28)
        try:
            list(croniter_range(start, stop, "0 0 * * *"))
        except CroniterBadTypeRangeError:
            self.fail("should not be triggered")

    def test_configure_second_location(self):
        start = datetime(2016, 12, 2, 0, 0, 0)
        stop = datetime(2016, 12, 2, 0, 1, 0)
        fwd = list(croniter_range(start, stop, "*/20 * * * * *", second_at_beginning=True))
        self.assertEqual(len(fwd), 4)
        self.assertEqual(fwd[0], start)
        self.assertEqual(fwd[-1], stop)

    def test_year_range(self):
        start = datetime(2010, 1, 1)
        stop = datetime(2030, 1, 1)
        fwd = list(croniter_range(start, stop, "0 0 1 1 ? 0 2020-2024,2028"))
        self.assertEqual(len(fwd), 6)
        self.assertEqual(fwd[0], datetime(2020, 1, 1))
        self.assertEqual(fwd[-1], datetime(2028, 1, 1))


if __name__ == "__main__":
    unittest.main()
