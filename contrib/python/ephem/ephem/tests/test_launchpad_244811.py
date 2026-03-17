#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import ephem

# Make sure that a series of next-risings does not keep returning the
# same time over and over again.

class Launchpad244811Tests(unittest.TestCase):
    def runTest(self):
        boston = ephem.city("Boston")
        boston.pressure = 1010.0 # undo pressure auto-adjustment
        mars = ephem.Mars()
        cur_date = ephem.Date("2009/6/29 07:00:00")

        cur_date = boston.next_rising(mars, start=cur_date)
        self.assertEqual(str(cur_date), '2009/6/30 06:17:37')

        cur_date = boston.next_rising(mars, start=cur_date)
        self.assertEqual(str(cur_date), '2009/7/1 06:15:45')

        cur_date = boston.next_rising(mars, start=cur_date)
        self.assertEqual(str(cur_date), '2009/7/2 06:13:53')
