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
Test cron item removal
"""

import os
import sys

sys.path.insert(0, '../')

import unittest
from crontab import CronTab

START_TAB = """
3 * * * * command1 # CommentID C
2 * * * * command2 # CommentID AAB
1 * * * * command3 # CommentID B3
"""

class RemovalTestCase(unittest.TestCase):
    """Test basic functionality of crontab."""
    def setUp(self):
        self.filenames = []
        self.crontab = CronTab(tab=START_TAB.strip())

    def test_01_remove(self):
        """Remove Item"""
        self.assertEqual(len(self.crontab), 3)
        self.crontab.remove(self.crontab.crons[0])
        self.assertEqual(len(self.crontab), 2)
        self.assertEqual(len(self.crontab.render()), 69)

    def test_02_remove_all(self):
        """Remove All"""
        self.crontab.remove_all()
        self.assertEqual(len(self.crontab), 0)
        self.assertEqual(str(self.crontab), '')

    def test_03_remove_cmd(self):
        """Remove all with Command"""
        self.crontab.remove_all(command='command2')
        self.assertEqual(len(self.crontab), 2)
        self.assertEqual(len(self.crontab.render()), 67)
        self.crontab.remove_all(command='command3')
        self.assertEqual(len(self.crontab), 1)
        self.assertEqual(len(self.crontab.render()), 33)

    def test_04_remove_id(self):
        """Remove all with Comment/ID"""
        self.crontab.remove_all(comment='CommentID B3')
        self.assertEqual(len(self.crontab), 2)
        self.assertEqual(len(self.crontab.render()), 68)

    def test_05_remove_date(self):
        """Remove all with Time Code"""
        self.crontab.remove_all(time='2 * * * *')
        self.assertEqual(len(self.crontab), 2)
        self.assertEqual(len(self.crontab.render()), 67)

    def test_05_remove_all_error(self):
        """Remove all with old arg"""
        with self.assertRaises(AttributeError):
            self.crontab.remove_all('command')

    def test_06_removal_of_none(self):
        """Remove all respects None as a possible value"""
        self.crontab[1].set_comment(None)
        self.crontab.remove_all(comment=None)
        self.assertEqual(len(self.crontab), 2)
        self.assertEqual(len(self.crontab.render()), 67)

    def test_07_removal_in_loop(self):
        """Remove items in a loop"""
        for job in self.crontab:
            self.crontab.remove(job)
        self.assertEqual(len(self.crontab), 0)

    def test_09_removal_during_iter(self):
        crontab = CronTab()
        for x in range(0, 5, 1):
            job = crontab.new(command="cmd", comment="SAME_ID")
            job.setall("%d * * * *" % (x + 1))
        for item in crontab.find_comment("SAME_ID"):
            crontab.remove(item)
        self.assertEqual(len(crontab), 0)

    def test_11_remove_generator(self):
        """Remove jobs from the find generator"""
        tabs = self.crontab.find_command('command2')
        self.crontab.remove(tabs)
        self.assertEqual(len(self.crontab), 2)

    def test_12_remove_nonsense(self):
        """Fail to remove bad type"""
        self.assertRaises(TypeError, self.crontab.remove, 5)
        self.assertRaises(TypeError, self.crontab.remove, "foo")

    def get_new_file(self, name):
        """Gets a filename and records it for deletion"""
        import yatest.common
        TEST_DIR = yatest.common.test_source_path()
        filename = os.path.join(TEST_DIR, 'data', 'spool', name)
        self.filenames.append(filename)
        return filename

    def tearDown(self):
        for filename in self.filenames:
            if os.path.isfile(filename):
                os.unlink(filename)


if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(
       RemovalTestCase,
    )
