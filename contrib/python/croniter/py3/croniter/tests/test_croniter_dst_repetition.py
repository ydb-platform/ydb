#!/usr/bin/env python
"""
All related DST croniter tests are isolated here.
"""
# -*- coding: utf-8 -*-

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import os
import time
from collections import OrderedDict
from datetime import datetime, timedelta

from croniter import (
    HOUR_FIELD,
    CroniterBadCronError,
    CroniterBadDateError,
    CroniterBadTypeRangeError,
    CroniterError,
    cron_m,
    croniter,
    croniter_range,
)
from croniter.tests import base

ORIG_OVERFLOW32B_MODE = cron_m.OVERFLOW32B_MODE


class CroniterDST138Test(base.TestCase):
    """
    See https://github.com/kiorky/croniter/issues/138.
    """

    _tz = "UTC"

    def setUp(self):
        self._time = os.environ.setdefault("TZ", "")
        self.base = datetime(2024, 1, 25, 4, 46)
        self.iter = croniter("*/5 * * * *", self.base)
        self.results = [
            datetime(2024, 1, 25, 4, 50),
            datetime(2024, 1, 25, 4, 55),
            datetime(2024, 1, 25, 5, 0),
        ]
        self.tzname, self.timezone = time.tzname, time.timezone

    def tearDown(self):
        cron_m.OVERFLOW32B_MODE = ORIG_OVERFLOW32B_MODE
        if not self._time:
            del os.environ["TZ"]
        else:
            os.environ["TZ"] = self._time
        time.tzset()

    def test_issue_138_dt_to_ts_32b(self):
        """
        test local tz, forcing 32b mode.
        """
        self._test(m32b=True)

    def test_issue_138_dt_to_ts_n(self):
        """
        test local tz, forcing non 32b mode.
        """
        self._test(m32b=False)

    def _test(self, tz="UTC", m32b=True):
        cron_m.OVERFLOW32B_MODE = m32b
        os.environ["TZ"] = tz
        time.tzset()
        res = [self.iter.get_next(datetime) for i in range(3)]
        self.assertEqual(res, self.results)


class CroniterDST138TestLocal(CroniterDST138Test):
    _tz = "UTC-8"


if __name__ == "__main__":
    unittest.main()
