import random
import uuid

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from datetime import datetime, timedelta

from croniter import CroniterBadCronError, CroniterNotAlphaError, croniter
from croniter.tests import base


class CroniterHashBase(base.TestCase):
    epoch = datetime(2020, 1, 1, 0, 0)
    hash_id = "hello"

    def _test_iter(self, definition, expectations, delta, epoch=None, hash_id=None, next_type=None):
        if epoch is None:
            epoch = self.epoch
        if hash_id is None:
            hash_id = self.hash_id
        if next_type is None:
            next_type = datetime
        if not isinstance(expectations, (list, tuple)):
            expectations = (expectations,)
        obj = croniter(definition, epoch, hash_id=hash_id)
        testval = obj.get_next(next_type)
        self.assertIn(testval, expectations)
        if delta is not None:
            self.assertEqual(obj.get_next(next_type), testval + delta)


class CroniterHashTest(CroniterHashBase):
    def test_hash_hourly(self):
        """Test manually-defined hourly"""
        self._test_iter("H * * * *", datetime(2020, 1, 1, 0, 10), timedelta(hours=1))

    def test_hash_daily(self):
        """Test manually-defined daily"""
        self._test_iter("H H * * *", datetime(2020, 1, 1, 11, 10), timedelta(days=1))

    def test_hash_weekly(self):
        """Test manually-defined weekly"""
        # croniter 1.0.5 changes the defined weekly range from (0, 6)
        # to (0, 7), to match cron's behavior that Sunday is 0 or 7.
        # This changes the hash, so test for either.
        self._test_iter(
            "H H * * H",
            (datetime(2020, 1, 3, 11, 10), datetime(2020, 1, 5, 11, 10)),
            timedelta(weeks=1),
        )

    def test_hash_monthly(self):
        """Test manually-defined monthly"""
        self._test_iter("H H H * *", datetime(2020, 1, 1, 11, 10), timedelta(days=31))

    def test_hash_yearly(self):
        """Test manually-defined yearly"""
        self._test_iter("H H H H *", datetime(2020, 9, 1, 11, 10), timedelta(days=365))

    def test_hash_second(self):
        """Test seconds

        If a sixth field is provided, seconds are included in the datetime()
        """
        self._test_iter("H H * * * H", datetime(2020, 1, 1, 11, 10, 32), timedelta(days=1))

    def test_hash_year(self):
        """Test years

        provide a seventh field as year
        """
        self._test_iter("H H * * * H H", datetime(2066, 1, 1, 11, 10, 32), timedelta(days=1))

    def test_hash_id_change(self):
        """Test a different hash_id returns different results given same definition and epoch"""
        self._test_iter("H H * * *", datetime(2020, 1, 1, 11, 10), timedelta(days=1))
        self._test_iter(
            "H H * * *",
            datetime(2020, 1, 1, 0, 24),
            timedelta(days=1),
            hash_id="different id",
        )

    def test_hash_epoch_change(self):
        """Test a different epoch returns different results given same definition and hash_id"""
        self._test_iter("H H * * *", datetime(2020, 1, 1, 11, 10), timedelta(days=1))
        self._test_iter(
            "H H * * *",
            datetime(2011, 11, 12, 11, 10),
            timedelta(days=1),
            epoch=datetime(2011, 11, 11, 11, 11),
        )

    def test_hash_range(self):
        """Test a hashed range definition"""
        self._test_iter("H H H(3-5) * *", datetime(2020, 1, 5, 11, 10), timedelta(days=31))
        self._test_iter("H H * * * 0 H(2025-2030)", datetime(2029, 1, 1, 11, 10), timedelta(days=1))

    def test_hash_division(self):
        """Test a hashed division definition"""
        self._test_iter("H H/3 * * *", datetime(2020, 1, 1, 2, 10), timedelta(hours=3))
        self._test_iter("H H H H * H H/2", datetime(2020, 9, 1, 11, 10, 32), timedelta(days=365 * 2))

    def test_hash_range_division(self):
        """Test a hashed range + division definition"""
        self._test_iter("H(30-59)/10 H * * *", datetime(2020, 1, 1, 11, 30), timedelta(minutes=10))

    def test_hash_invalid_range(self):
        """Test validation logic for range_begin and range_end values"""
        try:
            self._test_iter("H(11-10) H * * *", datetime(2020, 1, 1, 11, 31), timedelta(minutes=10))
        except (CroniterBadCronError) as ex:
            self.assertEqual("{0}".format(ex), "Range end must be greater than range begin")

    def test_hash_id_bytes(self):
        """Test hash_id as a bytes object"""
        self._test_iter(
            "H H * * *",
            datetime(2020, 1, 1, 14, 53),
            timedelta(days=1),
            hash_id=b"\x01\x02\x03\x04",
        )

    def test_hash_float(self):
        """Test result as a float object"""
        self._test_iter("H H * * *", 1577877000.0, (60 * 60 * 24), next_type=float)

    def test_invalid_definition(self):
        """Test an invalid definition raises CroniterNotAlphaError"""
        with self.assertRaises(CroniterNotAlphaError):
            croniter("X X * * *", self.epoch, hash_id=self.hash_id)

    def test_invalid_hash_id_type(self):
        """Test an invalid hash_id type raises TypeError"""
        with self.assertRaises(TypeError):
            croniter("H H * * *", self.epoch, hash_id={1: 2})

    def test_invalid_divisor(self):
        """Test an invalid divisor type raises CroniterBadCronError"""
        with self.assertRaises(CroniterBadCronError):
            croniter("* * H/0 * *", self.epoch, hash_id=self.hash_id)


class CroniterWordAliasTest(CroniterHashBase):
    def test_hash_word_midnight(self):
        """Test built-in @midnight

        @midnight is actually up to 3 hours after midnight, not exactly midnight
        """
        self._test_iter("@midnight", datetime(2020, 1, 1, 2, 10, 32), timedelta(days=1))

    def test_hash_word_hourly(self):
        """Test built-in @hourly"""
        self._test_iter("@hourly", datetime(2020, 1, 1, 0, 10, 32), timedelta(hours=1))

    def test_hash_word_daily(self):
        """Test built-in @daily"""
        self._test_iter("@daily", datetime(2020, 1, 1, 11, 10, 32), timedelta(days=1))

    def test_hash_word_weekly(self):
        """Test built-in @weekly"""
        # croniter 1.0.5 changes the defined weekly range from (0, 6)
        # to (0, 7), to match cron's behavior that Sunday is 0 or 7.
        # This changes the hash, so test for either.
        self._test_iter(
            "@weekly",
            (datetime(2020, 1, 3, 11, 10, 32), datetime(2020, 1, 5, 11, 10, 32)),
            timedelta(weeks=1),
        )

    def test_hash_word_monthly(self):
        """Test built-in @monthly"""
        self._test_iter("@monthly", datetime(2020, 1, 1, 11, 10, 32), timedelta(days=31))

    def test_hash_word_yearly(self):
        """Test built-in @yearly"""
        self._test_iter("@yearly", datetime(2020, 9, 1, 11, 10, 32), timedelta(days=365))

    def test_hash_word_annually(self):
        """Test built-in @annually

        @annually is the same as @yearly
        """
        obj_annually = croniter("@annually", self.epoch, hash_id=self.hash_id)
        obj_yearly = croniter("@yearly", self.epoch, hash_id=self.hash_id)
        self.assertEqual(obj_annually.get_next(datetime), obj_yearly.get_next(datetime))
        self.assertEqual(obj_annually.get_next(datetime), obj_yearly.get_next(datetime))


class CroniterHashExpanderBase(base.TestCase):
    def setUp(self):
        _rd = random.Random()
        _rd.seed(100)
        self.HASH_IDS = [uuid.UUID(int=_rd.getrandbits(128)).bytes for _ in range(350)]


class CroniterHashExpanderExpandMinutesTest(CroniterHashExpanderBase):
    MIN_VALUE = 0
    MAX_VALUE = 59
    TOTAL = 60

    def test_expand_minutes(self):
        minutes = set()
        expression = "H * * * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            minutes.add(expanded[0][0][0])
        assert len(minutes) == self.TOTAL
        assert min(minutes) == self.MIN_VALUE
        assert max(minutes) == self.MAX_VALUE

    def test_expand_minutes_range_2_minutes(self):
        minutes = set()
        expression = "H/2 * * * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _minutes = expanded[0][0]
            assert len(_minutes) == 30
            minutes.update(_minutes)
        assert len(minutes) == self.TOTAL
        assert min(minutes) == self.MIN_VALUE
        assert max(minutes) == self.MAX_VALUE

    def test_expand_minutes_range_3_minutes(self):
        minutes = set()
        expression = "H/3 * * * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _minutes = expanded[0][0]
            assert len(_minutes) == 20
            minutes.update(_minutes)
        assert len(minutes) == self.TOTAL
        assert min(minutes) == self.MIN_VALUE
        assert max(minutes) == self.MAX_VALUE

    def test_expand_minutes_range_15_minutes(self):
        minutes = set()
        expression = "H/15 * * * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _minutes = expanded[0][0]
            assert len(_minutes) == 4
            minutes.update(_minutes)
        assert len(minutes) == self.TOTAL
        assert min(minutes) == self.MIN_VALUE
        assert max(minutes) == self.MAX_VALUE

    def test_expand_minutes_with_full_range(self):
        minutes = set()
        expression = "H(0-59) * * * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            minutes.add(expanded[0][0][0])
        assert len(minutes) == self.TOTAL
        assert min(minutes) == self.MIN_VALUE
        assert max(minutes) == self.MAX_VALUE


class CroniterHashExpanderExpandHoursTest(CroniterHashExpanderBase):
    MIN_VALUE = 0
    MAX_VALUE = 23
    TOTAL = 24

    def test_expand_hours(self):
        hours = set()
        expression = "H H * * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            hours.add(expanded[0][1][0])
        assert len(hours) == self.TOTAL
        assert min(hours) == self.MIN_VALUE
        assert max(hours) == self.MAX_VALUE

    def test_expand_hours_range_every_2_hours(self):
        hours = set()
        expression = "H H/2 * * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _hours = expanded[0][1]
            assert len(_hours) == 12
            hours.update(_hours)
        assert len(hours) == self.TOTAL
        assert min(hours) == self.MIN_VALUE
        assert max(hours) == self.MAX_VALUE

    def test_expand_hours_range_4_hours(self):
        hours = set()
        expression = "H H/4 * * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _hours = expanded[0][1]
            assert len(_hours) == 6
            hours.update(_hours)
        assert len(hours) == self.TOTAL
        assert min(hours) == self.MIN_VALUE
        assert max(hours) == self.MAX_VALUE

    def test_expand_hours_range_8_hours(self):
        hours = set()
        expression = "H H/8 * * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _hours = expanded[0][1]
            assert len(_hours) == 3
            hours.update(_hours)
        assert len(hours) == self.TOTAL
        assert min(hours) == self.MIN_VALUE
        assert max(hours) == self.MAX_VALUE

    def test_expand_hours_range_10_hours(self):
        hours = set()
        expression = "H H/10 * * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _hours = expanded[0][1]
            assert len(_hours) in {2, 3}
            hours.update(_hours)
        assert len(hours) == self.TOTAL
        assert min(hours) == self.MIN_VALUE
        assert max(hours) == self.MAX_VALUE

    def test_expand_hours_range_12_hours(self):
        hours = set()
        expression = "H H/12 * * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _hours = expanded[0][1]
            assert len(_hours) == 2
            hours.update(_hours)
        assert len(hours) == self.TOTAL
        assert min(hours) == self.MIN_VALUE
        assert max(hours) == self.MAX_VALUE

    def test_expand_hours_with_full_range(self):
        minutes = set()
        expression = "* H(0-23) * * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            minutes.add(expanded[0][1][0])
        assert len(minutes) == self.TOTAL
        assert min(minutes) == self.MIN_VALUE
        assert max(minutes) == self.MAX_VALUE


class CroniterHashExpanderExpandMonthDaysTest(CroniterHashExpanderBase):
    MIN_VALUE = 1
    MAX_VALUE = 31
    TOTAL = 31

    def test_expand_month_days(self):
        month_days = set()
        expression = "H H H * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            month_days.add(expanded[0][2][0])
        assert len(month_days) == self.TOTAL
        assert min(month_days) == self.MIN_VALUE
        assert max(month_days) == self.MAX_VALUE

    def test_expand_month_days_range_2_days(self):
        month_days = set()
        expression = "0 0 H/2 * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _days = expanded[0][2]
            assert len(_days) in {15, 16}
            month_days.update(_days)
        assert len(month_days) == self.TOTAL
        assert min(month_days) == self.MIN_VALUE
        assert max(month_days) == self.MAX_VALUE

    def test_expand_month_days_range_5_days(self):
        month_days = set()
        expression = "H H H/5 * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _days = expanded[0][2]
            assert len(_days) in {6, 7}
            month_days.update(_days)
        assert len(month_days) == self.TOTAL
        assert min(month_days) == self.MIN_VALUE
        assert max(month_days) == self.MAX_VALUE

    def test_expand_month_days_range_12_days(self):
        month_days = set()
        expression = "H H H/12 * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _days = expanded[0][2]
            assert len(_days) in {2, 3}
            month_days.update(_days)
        assert len(month_days) == self.TOTAL
        assert min(month_days) == self.MIN_VALUE
        assert max(month_days) == self.MAX_VALUE

    def test_expand_month_days_with_full_range(self):
        month_days = set()
        expression = "* * H(1-31) * *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            month_days.add(expanded[0][2][0])
        assert len(month_days) == self.TOTAL
        assert min(month_days) == self.MIN_VALUE
        assert max(month_days) == self.MAX_VALUE


class CroniterHashExpanderExpandMonthTest(CroniterHashExpanderBase):
    MIN_VALUE = 1
    MAX_VALUE = 12
    TOTAL = 12

    def test_expand_month_days(self):
        month_days = set()
        expression = "H H * H *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            month_days.add(expanded[0][3][0])
        assert len(month_days) == self.TOTAL
        assert min(month_days) == self.MIN_VALUE
        assert max(month_days) == self.MAX_VALUE

    def test_expand_month_days_range_2_months(self):
        months = set()
        expression = "H H * H/2 *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _months = expanded[0][3]
            assert len(_months) == 6
            months.update(_months)
        assert len(months) == self.TOTAL
        assert min(months) == self.MIN_VALUE
        assert max(months) == self.MAX_VALUE

    def test_expand_month_days_range_3_months(self):
        months = set()
        expression = "H H * H/3 *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _months = expanded[0][3]
            assert len(_months) == 4
            months.update(_months)
        assert len(months) == self.TOTAL
        assert min(months) == self.MIN_VALUE
        assert max(months) == self.MAX_VALUE

    def test_expand_month_days_range_5_months(self):
        months = set()
        expression = "H H * H/5 *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _months = expanded[0][3]
            assert len(_months) in {2, 3}
            months.update(_months)
        assert len(months) == self.TOTAL
        assert min(months) == self.MIN_VALUE
        assert max(months) == self.MAX_VALUE

    def test_expand_months_with_full_range(self):
        months = set()
        expression = "* * * H(1-12) *"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            months.add(expanded[0][3][0])
        assert len(months) == self.TOTAL
        assert min(months) == self.MIN_VALUE
        assert max(months) == self.MAX_VALUE


class CroniterHashExpanderExpandWeekDays(CroniterHashExpanderBase):
    MIN_VALUE = 0
    MAX_VALUE = 6
    TOTAL = 7

    def test_expand_week_days(self):
        week_days = set()
        expression = "H H * * H"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            week_days.add(expanded[0][4][0])
        assert len(week_days) == self.TOTAL
        assert min(week_days) == self.MIN_VALUE
        assert max(week_days) == self.MAX_VALUE

    def test_expand_week_days_range_2_days(self):
        days = set()
        expression = "H H * * H/2"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _days = expanded[0][4]
            assert len(_days) in {3, 4}
            days.update(_days)
        assert len(days) == self.TOTAL
        assert min(days) == self.MIN_VALUE
        assert max(days) == self.MAX_VALUE

    def test_expand_week_days_range_4_days(self):
        days = set()
        expression = "H H * * H/4"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            _days = expanded[0][4]
            assert len(_days) in {1, 2}
            days.update(_days)
        assert len(days) == self.TOTAL
        assert min(days) == self.MIN_VALUE
        assert max(days) == self.MAX_VALUE

    def test_expand_week_days_with_full_range(self):
        days = set()
        expression = "* * * * H(0-6)"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            days.add(expanded[0][4][0])
        assert len(days) == self.TOTAL
        assert min(days) == self.MIN_VALUE
        assert max(days) == self.MAX_VALUE


class CroniterHashExpanderExpandYearsTest(CroniterHashExpanderBase):
    def test_expand_years_by_division(self):
        years = set()
        year_min, year_max = croniter.RANGES[6]
        expression = "* * * * * * H/10"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            assert len(expanded[0][6]) == 13
            years.update(expanded[0][6])
        assert len(years) == year_max - year_min + 1
        assert min(years) == year_min
        assert max(years) == year_max

    def test_expand_years_by_range(self):
        years = set()
        expression = "* * * * * * H(2020-2030)"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            years.add(expanded[0][6][0])
        assert len(years) == 11
        assert min(years) == 2020
        assert max(years) == 2030

    def test_expand_years_by_range_and_division(self):
        years = set()
        expression = "* * * * * * H(2020-2050)/10"
        for hash_id in self.HASH_IDS:
            expanded = croniter.expand(expression, hash_id=hash_id)
            years.update(expanded[0][6])
        assert len(years) == 31
        assert min(years) == 2020
        assert max(years) == 2050


if __name__ == "__main__":
    unittest.main()
