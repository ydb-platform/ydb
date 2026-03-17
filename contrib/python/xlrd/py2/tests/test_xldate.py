#!/usr/bin/env python
# Author:  mozman <mozman@gmx.at>
# Purpose: test xldate.py
# Created: 04.12.2010
# Copyright (C) 2010, Manfred Moitzi
# License: BSD licence

import unittest

from xlrd import xldate

DATEMODE = 0 # 1900-based

class TestXLDate(unittest.TestCase):
    def test_date_as_tuple(self):
        date = xldate.xldate_as_tuple(2741., DATEMODE)
        self.assertEqual(date, (1907, 7, 3, 0, 0, 0))
        date = xldate.xldate_as_tuple(38406., DATEMODE)
        self.assertEqual(date, (2005, 2, 23, 0, 0, 0))
        date = xldate.xldate_as_tuple(32266., DATEMODE)
        self.assertEqual(date, (1988, 5, 3, 0, 0, 0))

    def test_time_as_tuple(self):
        time = xldate.xldate_as_tuple(.273611, DATEMODE)
        self.assertEqual(time, (0, 0, 0, 6, 34, 0))
        time = xldate.xldate_as_tuple(.538889, DATEMODE)
        self.assertEqual(time, (0, 0, 0, 12, 56, 0))
        time = xldate.xldate_as_tuple(.741123, DATEMODE)
        self.assertEqual(time, (0, 0, 0, 17, 47, 13))

    def test_xldate_from_date_tuple(self):
        date = xldate.xldate_from_date_tuple( (1907, 7, 3), DATEMODE )
        self.assertAlmostEqual(date, 2741.)
        date = xldate.xldate_from_date_tuple( (2005, 2, 23), DATEMODE )
        self.assertAlmostEqual(date, 38406.)
        date = xldate.xldate_from_date_tuple( (1988, 5, 3), DATEMODE )
        self.assertAlmostEqual(date, 32266.)

    def test_xldate_from_time_tuple(self):
        time = xldate.xldate_from_time_tuple( (6, 34, 0) )
        self.assertAlmostEqual(time, .273611, places=6)
        time = xldate.xldate_from_time_tuple( (12, 56, 0) )
        self.assertAlmostEqual(time, .538889, places=6)
        time = xldate.xldate_from_time_tuple( (17, 47, 13) )
        self.assertAlmostEqual(time, .741123, places=6)

    def test_xldate_from_datetime_tuple(self):
        date = xldate.xldate_from_datetime_tuple( (1907, 7, 3, 6, 34, 0), DATEMODE)
        self.assertAlmostEqual(date, 2741.273611, places=6)
        date = xldate.xldate_from_datetime_tuple( (2005, 2, 23, 12, 56, 0), DATEMODE)
        self.assertAlmostEqual(date, 38406.538889, places=6)
        date = xldate.xldate_from_datetime_tuple( (1988, 5, 3, 17, 47, 13), DATEMODE)
        self.assertAlmostEqual(date, 32266.741123, places=6)

if __name__=='__main__':
    unittest.main()
