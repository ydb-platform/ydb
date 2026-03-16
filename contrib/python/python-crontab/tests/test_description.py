#!/usr/bin/env python
#
# Copyright (C) 2016 Martin Owens
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
Test optional crontab description intergration.
"""

import sys
sys.path.insert(0, '../')

import unittest
import crontab

INITAL_TAB = """
# Basic Comment
20 10-20 */2 * * execute # At 20 minutes past the hour, between 10:00 AM and 08:59 PM, every 2 days
"""

class DescriptorTestCase(unittest.TestCase):
    """Test basic functionality of crontab."""
    def setUp(self):
        self.crontab = crontab.CronTab(tab=INITAL_TAB)
        self.job = list(self.crontab.find_command('execute'))[0]
        try:
            import cron_descriptor
        except ImportError:
            self.skipTest("Cron-descriptor module not installed")

    def test_00_no_module(self):
        """No module found"""
        # Remove module if imported already
        for i in range(1, 4):
            name = '.'.join((['cron_descriptor'] * i))
            if name in sys.modules:
                del sys.modules[name]

        old, sys.path = sys.path, []
        with self.assertRaises(ImportError):
            self.job.description()
        sys.path = old

    def test_01_description(self):
        """Get Job Description"""
        self.assertEqual(self.job.description(), self.job.comment)

if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(DescriptorTestCase)

