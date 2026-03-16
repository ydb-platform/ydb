#!/usr/bin/env python
#
# Copyright (C) 2013 Martin Owens
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3.0 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library.
#
"""
Test croniter extention to find out when items will next happen.
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, '../')

import unittest
import crontab

INITAL_TAB = """
# Basic Comment
20 * * * * execute # comment
"""

class CroniterTestCase(unittest.TestCase):
    """Test basic functionality of crontab."""
    def setUp(self):
        self.crontab = crontab.CronTab(tab=INITAL_TAB)
        self.job = list(self.crontab.find_command('execute'))[0]
        try:
            import croniter
        except ImportError:
            self.skipTest("Croniter not installed")

    def test_00_nocroniter(self):
        """No Croniter"""
        # Remove croniter if imported before.
        for i in range(1, 4):
            name = '.'.join((['croniter'] * i))
            if name in sys.modules:
                del sys.modules[name]

        old, sys.path = sys.path, []
        with self.assertRaises(ImportError):
            self.job.schedule(datetime(2001, 10, 11, 1, 12, 10))
        sys.path = old

    def test_01_schedule(self):
        """Get Scheduler"""
        ct = self.job.schedule(datetime(2009, 10, 11, 5, 12, 10))
        self.assertTrue(ct)

    def test_02_next(self):
        """Get Next Scheduled Items"""
        ct = self.job.schedule(datetime(2000, 10, 11, 5, 12, 10))
        self.assertEqual(ct.get_next(), datetime(2000, 10, 11, 5, 20, 0))
        self.assertEqual(ct.get_next(), datetime(2000, 10, 11, 6, 20, 0))

    def test_03_prev(self):
        """Get Prev Scheduled Items"""
        ct = self.job.schedule(datetime(2001, 10, 11, 1, 12, 10))
        self.assertEqual(ct.get_prev(), datetime(2001, 10, 11, 0, 20, 0))
        self.assertEqual(ct.get_prev(), datetime(2001, 10, 10, 23, 20, 0))

    def test_04_current(self):
        """Get Current Item"""
        ct = self.job.schedule(datetime(2001, 10, 11, 1, 12, 10))
        self.assertEqual(ct.get_current(), datetime(2001, 10, 11, 1, 12, 10))
        self.assertEqual(ct.get_current(), datetime(2001, 10, 11, 1, 12, 10))

if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(
       CroniterTestCase,
    )
