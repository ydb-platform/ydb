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
Test simple 'every' api.
"""

import os
import sys

sys.path.insert(0, '../')

import unittest
from crontab import CronTab

import yatest.common
TEST_DIR = yatest.common.test_source_path()

class EveryTestCase(unittest.TestCase):
    """Test basic functionality of crontab."""
    def setUp(self):
        self.crontab = CronTab(tabfile=os.path.join(TEST_DIR, 'data', 'test.tab'))

    def test_00_minutes(self):
        """Every Minutes"""
        for job in self.crontab:
            job.every(3).minutes()
            self.assertEqual(job.slices.clean_render(), '*/3 * * * *')
            job.minutes.every(5)
            self.assertEqual(job.slices.clean_render(), '*/5 * * * *')

    def test_01_hours(self):
        """Every Hours"""
        for job in self.crontab:
            job.every(3).hours()
            self.assertEqual(job.slices.clean_render(), '0 */3 * * *')

    def test_02_dom(self):
        """Every Day of the Month"""
        for job in self.crontab:
            job.every(3).dom()
            self.assertEqual(job.slices.clean_render(), '0 0 */3 * *')

    def test_03_single(self):
        """Every Single Hour"""
        for job in self.crontab:
            job.every().hour()
            self.assertEqual(job.slices.clean_render(), '0 * * * *')

    def test_04_month(self):
        """Every Month"""
        for job in self.crontab:
            job.every(3).months()
            self.assertEqual(job.slices.clean_render(), '0 0 1 */3 *')

    def test_05_dow(self):
        """Every Day of the Week"""
        for job in self.crontab:
            job.every(3).dow()
            self.assertEqual(job.slices.clean_render(), '0 0 * * */3')

    def test_06_year(self):
        """Every Year"""
        for job in self.crontab:
            job.every().year()
            self.assertEqual(job.slices.render(), '@yearly')
            self.assertEqual(job.slices.clean_render(), '0 0 1 1 *')
            self.assertRaises(ValueError, job.every(2).year)

    def test_07_reboot(self):
        """Every Reboot"""
        for job in self.crontab:
            job.every_reboot()
            self.assertEqual(job.slices.render(), '@reboot')
            self.assertEqual(job.slices.clean_render(), '* * * * *')

    def test_08_newitem(self):
        """Every on New Item"""
        job = self.crontab.new(command='hourly')
        job.every().hour()
        self.assertEqual(job.slices.render(), '@hourly')
        job = self.crontab.new(command='firstly')
        job.hours.every(2)
        self.assertEqual(job.slices.render(), '* */2 * * *')

if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(
       EveryTestCase,
    )
