#!/usr/bin/env python
#
# Copyright (C) 2012 Martin Owens
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
Test crontab interaction.
"""

import os
import sys

import unittest
import crontab

import yatest.common
TEST_DIR = yatest.common.test_source_path()

INITAL_TAB = """
# First Comment
0,30 * * * * firstcommand
"""

class UserTestCase(unittest.TestCase):
    def test_01_self_user(self):
        tab = crontab.CronTab(user='foo', tab=INITAL_TAB)
        self.assertEqual(tab.user_opt, {'u': 'foo'})
        tab = crontab.CronTab(user=crontab.current_user(), tab=INITAL_TAB)
        self.assertEqual(tab.user_opt, {})


class CompatTestCase(unittest.TestCase):
    """Test basic functionality of crontab."""
    @classmethod
    def setUpClass(cls):
        crontab.SYSTEMV = True

    @classmethod
    def tearDownClass(cls):
        crontab.SYSTEMV = False

    def setUp(self):
        self.crontab = crontab.CronTab(tab=INITAL_TAB)

    def test_00_enabled(self):
        """Test Compatability Mode"""
        self.assertTrue(crontab.SYSTEMV)

    def test_01_addition(self):
        """New Job Rendering"""
        job = self.crontab.new('addition1')
        job.minute.during(0, 3)
        job.hour.during(21, 23).every(1)
        job.dom.every(1)

        self.assertEqual(job.render(), '0,1,2,3 21,22,23 * * * addition1')

    def test_02_addition(self):
        """New Job Rendering"""
        job = self.crontab.new(command='addition2')

        job.minute.during(4, 9)
        job.hour.during(2, 10).every(2)
        job.dom.every(10)

        self.assertNotEqual(job.render(), '4-9 2-10/2 */3 * * addition2')
        self.assertEqual(job.render(), '4,5,6,7,8,9 2,4,6,8,10 1,11,21,31 * * addition2')

    def test_03_specials(self):
        """Ignore Special Symbols"""
        tab = crontab.CronTab(tabfile=os.path.join(TEST_DIR, 'data', 'specials.tab'))
        self.assertEqual(tab.render(), """0 * * * * hourly
0 0 * * * daily
0 0 * * 0 weekly
""")

    def test_04_comments(self):
        """Comments should be on their own lines"""
        self.assertEqual(self.crontab[0].comment, 'First Comment')
        self.assertEqual(self.crontab.render(), INITAL_TAB)
        job = self.crontab.new('command', comment="Test comment")
        self.assertEqual(job.render(), "# Test comment\n* * * * * command")

    def test_05_ansible(self):
        """Crontab shouldn't break ansible cronjobs"""
        cron = crontab.CronTab(tab="""
#Ansible: {job_name}
* * * * * {command}
""")
        self.assertEqual(cron[0].comment, '{job_name}')
        self.assertEqual(cron[0].command, '{command}')
        self.assertEqual(str(cron[0]), '#Ansible: {job_name}\n* * * * * {command}')

    def test_06_escaped_chars(self):
        """Do escaped chars parse correctly when read in"""
        cron = crontab.CronTab(tab="""
* * * * * cmd arg_with_\\#_character # comment
""")
        self.assertEqual(cron[0].command, 'cmd arg_with_\\#_character')
        self.assertEqual(cron[0].comment, 'comment')

    def test_07_non_posix_shell(self):
        """Shell in windows environments is split correctly"""
        from crontab import Process, POSIX
        if POSIX:
            return
        winfile = os.path.join(TEST_DIR, 'data', "bash\\win.exe")
        pipe = Process("{sys.executable} {winfile}".format(winfile=winfile, sys=sys), 'SLASHED', posix=False)._run()
        self.assertEqual(pipe.wait(), 0, 'Windows shell command not found!')
        (out, err) = pipe.communicate()
        self.assertEqual(out, b'Double Glazing Installed:SLASHED\n')
        self.assertEqual(err, b'')


if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(
       CompatTestCase,
    )
