try:
    import unittest2 as unittest
except ImportError:
    import unittest

from datetime import datetime, timedelta

from croniter import croniter
from croniter.tests import base


class CroniterRandomTest(base.TestCase):
    epoch = datetime(2020, 1, 1, 0, 0)

    def test_random(self):
        """Test random definition"""
        obj = croniter("R R * * *", self.epoch)
        result_1 = obj.get_next(datetime)
        self.assertGreaterEqual(result_1, datetime(2020, 1, 1, 0, 0))
        self.assertLessEqual(result_1, datetime(2020, 1, 1, 0, 0) + timedelta(days=1))
        result_2 = obj.get_next(datetime)
        self.assertGreaterEqual(result_2, datetime(2020, 1, 2, 0, 0))
        self.assertLessEqual(result_2, datetime(2020, 1, 2, 0, 0) + timedelta(days=1))

    def test_random_range(self):
        """Test random definition within a range"""
        obj = croniter("R R R(10-20) * *", self.epoch)
        result_1 = obj.get_next(datetime)
        self.assertGreaterEqual(result_1, datetime(2020, 1, 10, 0, 0))
        self.assertLessEqual(result_1, datetime(2020, 1, 10, 0, 0) + timedelta(days=11))
        result_2 = obj.get_next(datetime)
        self.assertGreaterEqual(result_2, datetime(2020, 2, 10, 0, 0))
        self.assertLessEqual(result_2, datetime(2020, 2, 10, 0, 0) + timedelta(days=11))

    def test_random_float(self):
        """Test random definition, float result"""
        obj = croniter("R R * * *", self.epoch)
        result_1 = obj.get_next(float)
        self.assertGreaterEqual(result_1, 1577836800.0)
        self.assertLessEqual(result_1, 1577836800.0 + (60 * 60 * 24))
        result_2 = obj.get_next(float)
        self.assertGreaterEqual(result_2, 1577923200.0)
        self.assertLessEqual(result_2, 1577923200.0 + (60 * 60 * 24))

    def test_random_with_year(self):
        obj = croniter("* * * * * * R(2025-2030)", self.epoch)
        result = obj.get_next(datetime)
        self.assertGreaterEqual(result.year, 2025)
        self.assertLessEqual(result.year, 2030)


if __name__ == "__main__":
    unittest.main()
