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
Test any internal scheduling
"""

import os
import sys

import datetime
import unittest
import crontab
import logging
import string
import random

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

logger = logging.getLogger()
logger.level = logging.WARNING

import yatest.common
TEST_DIR = yatest.common.test_source_path()
COMMAND = os.path.join(TEST_DIR, 'data', 'crontest ')

class SchedulerTestCase(unittest.TestCase):
    """Test scheduling functions of CronTab."""
    def setUp(self):
        self.tab = crontab.CronTab()
        try:
            import croniter
        except ImportError:
            self.skipTest("Croniter not installed")

        self.handlers = []
        self.log = crontab.LOG
        self.log.setLevel(logging.DEBUG)
        self.log.propagate = False
        for handler in self.log.handlers:
            self.handlers.append(handler)
            self.log.removeHandler(handler)

        self.err = StringIO()
        self.handler = logging.StreamHandler(self.err)
        self.log.addHandler(self.handler)

    def tearDown(self):
        logger.removeHandler(self.handler)
        for handler in self.handlers:
            logger.addHandler(handler)
        #self.err.close()

    def assertLog(self, text):
        self.handler.flush()
        self.assertEqual(self.err.getvalue().strip(), text)

    def assertSchedule(self, slices, count, result):
        uid = random.choice(string.ascii_letters)
        self.tab.new(command=COMMAND + '-h ' + uid).setall(slices)
        ret = list(self.tab.run_scheduler(count, cadence=0.01, warp=True))
        self.assertEqual(len(ret), result)
        if count > 0:
            self.assertEqual(ret[0], '-h|' + uid)

    def test_01_run(self):
        """Run the command"""
        self.tab.env['SHELL'] = crontab.SHELL
        ret = self.tab.new(command=COMMAND+'-h A').run()
        self.assertEqual(ret, '-h|A')

    def test_02_run_error(self):
        """Run with errors"""
        ret = self.tab.new(command=COMMAND+'-e B').run()
        self.assertEqual(ret, '')
        self.assertLog('-e|B')

    def test_03_schedule(self):
        """Simple Schedule"""
        self.assertSchedule("* * * * *", 5, 4)

    def test_04_schedule_ten(self):
        """Every Ten Minutes"""
        # If on the 10 minute mark, two runs are expected
        exact = (datetime.datetime.now().minute % 10) == 0
        self.assertSchedule("*/10 * * * *", 10, 1 + exact)

    def test_05_env_passed(self):
        """Environment is passed in"""
        self.tab.env['CR_VAR'] = 'BABARIAN'
        ret = self.tab.new(command=COMMAND+' -ev').run()
        self.assertEqual(ret, 'BABARIAN')

if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(SchedulerTestCase)
