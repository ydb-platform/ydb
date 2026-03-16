"""Parser regression tests."""

import unittest
import datetime as dt

from trollsift.parser import parse


class TestParser(unittest.TestCase):
    def test_002(self):
        res = parse(
            "hrpt16_{satellite:7s}_{start_time:%d-%b-%Y_%H:%M:%S.000}_{orbit_number:5d}",
            "hrpt16_NOAA-19_26-NOV-2014_10:12:00.000_29889",
        )
        self.assertEqual(
            res,
            {
                "orbit_number": 29889,
                "satellite": "NOAA-19",
                "start_time": dt.datetime(2014, 11, 26, 10, 12),
            },
        )
