#!/usr/bin/env python
#
# Copyright (C) 2015 Martin Owens
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
System cron is prefixed with the username the process should run under.
"""

import os
import sys

sys.path.insert(0, '../')

import unittest
from crontab import CronTab

import yatest.common
TEST_DIR = yatest.common.test_source_path()
TEST_FILE = os.path.join(TEST_DIR, 'data', 'crontab')
INITIAL_TAB = """
*/30 * * * * palin one_cross_each
*/30 * * * * matias\ttab_user_error
"""

class SystemCronTestCase(unittest.TestCase):
    """Test vixie cron user addition."""
    def setUp(self):
        self.crontab = CronTab(tab=INITIAL_TAB, user=False)

    def test_00_repr(self):
        """System crontab repr"""
        self.assertEqual(repr(self.crontab), "<Unattached System CronTab>")

    def test_01_read(self):
        """Read existing command"""
        job = self.crontab[0]
        self.assertEqual(job.user, 'palin')
        self.assertEqual(job.command, 'one_cross_each')
        job = self.crontab[1]
        self.assertEqual(job.user, 'matias')
        self.assertEqual(job.command, 'tab_user_error')
        self.assertEqual(len(self.crontab), 2)

    def test_02_new(self):
        """Create a new job"""
        job = self.crontab.new(command='release_brian', user='pontus')
        self.assertEqual(job.user, 'pontus')
        self.assertEqual(job.command, 'release_brian')
        self.assertEqual(str(self.crontab), INITIAL_TAB.replace('\t', ' ') + """
* * * * * pontus release_brian
""")

    def test_03_new_tab(self):
        """Create a new system crontab"""
        tab = CronTab(user=False)
        tab.env['SHELL'] = '/usr/bin/roman'
        job = tab.new(command='release_bwian', user='pontus')
        self.assertEqual(str(tab), """SHELL=/usr/bin/roman
* * * * * pontus release_bwian
""")

    def test_04_failure(self):
        """Fail when no user"""
        with self.assertRaises(ValueError):
            self.crontab.new(command='im_brian')
        cron = self.crontab.new(user='user', command='no_im_brian')
        cron.user = None
        with self.assertRaises(ValueError):
            cron.render()

    def test_05_remove(self):
        """Remove the user flag"""
        self.crontab._user = None
        self.assertEqual(str(self.crontab), """
*/30 * * * * one_cross_each
*/30 * * * * tab_user_error
""")
        self.crontab.new(command='now_go_away')


    def test_06_comments(self):
        """Comment with six parts parses successfully"""
        crontab = CronTab(user=False, tab="""
#a system_comment that has six parts_will_fail_to_parse
        """)

    def test_07_recreation(self):
        """Input doesn't change on save"""
        crontab = CronTab(user=False, tab="* * * * * user command")
        self.assertEqual(str(crontab), "* * * * * user command\n")
        crontab = CronTab(user=False, tab="* * * * * user command\n")
        self.assertEqual(str(crontab), "* * * * * user command\n")

    def test_09_resaving(self):
        """Cycle rendering to show no changes"""
        for i in range(10):
            self.crontab = CronTab(tab=str(self.crontab))

        self.assertEqual(INITIAL_TAB.replace('\t', ' '), str(self.crontab))

    def test_10_system_file(self):
        """Load system crontab from a file"""
        crontab = CronTab(user=False, tabfile=TEST_FILE)
        self.assertEqual(repr(crontab), "<System CronTab '%s'>" % TEST_FILE)

if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(SystemCronTestCase)
