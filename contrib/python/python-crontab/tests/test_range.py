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
Test crontab ranges.
"""
import unittest
from crontab import CronTab, CronSlice

from .utils import LoggingMixin

class RangeTestCase(LoggingMixin, unittest.TestCase):
    """Test basic functionality of crontab."""
    log_name = 'crontab'

    def setUp(self):
        super(RangeTestCase, self).setUp()
        self.crontab = CronTab(tab="")

    def test_01_atevery(self):
        """At Every"""
        tab = CronTab(tab="""
*  *  *  *  * command
61 *  *  *  * command
*  25 *  *  * command
*  *  32 *  * command
*  *  *  13 * command
*  *  *  *  8 command
        """)
        self.assertEqual(len(tab), 1)
        self.assertLog("error", "'61', not in 0-59 for Minutes")
        self.assertLog("error", "'25', not in 0-23 for Hours")
        self.assertLog("error", "'32', not in 1-31 for Day of Month")
        self.assertLog("error", "'13', not in 1-12 for Month")
        self.assertLog("error", "'8', not in 0-6 for Day of Week")

    def test_02_withinevery(self):
        """Within Every"""
        tab = CronTab(tab="""
*    *    *    *    * command
1-61 *    *    *    * command
*    1-25 *    *    * command
*    *    1-32 *    * command
*    *    *    1-13 * command
*    *    *    *    1-8 command
        """)
        self.assertEqual(len(tab), 1)
        self.assertLog("error", "'61', not in 0-59 for Minutes")
        self.assertLog("error", "'25', not in 0-23 for Hours")
        self.assertLog("error", "'32', not in 1-31 for Day of Month")
        self.assertLog("error", "'13', not in 1-12 for Month")
        self.assertLog("error", "'8', not in 0-6 for Day of Week")

    def test_03_outevery(self):
        """Out of Every"""
        tab = CronTab(tab="""
*    *    *    *    *   command
*/61 *    *    *    *   command
*    */25 *    *    *   command
*    *    */32 *    *   command
*    *    *    */13 *   command
*    *    *    *    */8 command
        """)
        self.assertEqual(len(tab), 1)
        self.assertLog("error", "'61', not in 0-59 for Minutes")
        self.assertLog("error", "'25', not in 0-23 for Hours")
        self.assertLog("error", "'32', not in 1-31 for Day of Month")
        self.assertLog("error", "'13', not in 1-12 for Month")
        self.assertLog("error", "'8', not in 0-6 for Day of Week")

    def test_03_inevery(self):
        """Inside of Every"""
        tab = CronTab(tab="""
*    *    *    *    *   command
*/59 *    *    *    *   command
*    */23 *    *    *   command
*    *    */30 *    *   command
*    *    *    */11 *   command
*    *    *    *    */7 command
        """)
        self.assertEqual(len(tab), 6, str(tab))

    def test_04_zero_seq(self):
        """Zero divisor in range"""
        tab = CronTab(tab="""
*/0 * * * * command
        """)
        self.assertEqual(len(tab), 0)
        self.assertLog("error", "Sequence can not be divided by zero or max")

    def test_14_invalid_range(self):
        """No numerator in range"""
        tab = CronTab(tab="/10 * * * * command")
        self.assertEqual(len(tab), 0)
        with self.assertRaises(ValueError):
            tab.render(errors=True)
        self.assertEqual(str(tab), "# DISABLED LINE\n# /10 * * * * command\n")
        self.assertLog('error', u"No enumeration for Minutes: '/10'")

    def test_05_sunday(self):
        """Test all possible day of week combinations"""
        for (a, b) in (\
              ("7", "0"), ("5-7", "0,5-6"), ("1-7", "*"), ("*/7", "0"),\
              ("0-6", "*"), ("2-7", "0,2-6"), ("1-5", "1-5"), ("0-5", "0-5")):
            v = str(CronSlice(4, a))
            self.assertEqual(v, b, "%s != %s, from %s" % (v, b, a))

    def test_06_backwards(self):
        """Test backwards ranges for error"""
        tab = CronTab(tab="* * * * 3-1 command")
        self.assertEqual(str(tab), "# DISABLED LINE\n# * * * * 3-1 command\n")
        self.assertLog("error", "Bad range '3-1'")

if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(RangeTestCase)
