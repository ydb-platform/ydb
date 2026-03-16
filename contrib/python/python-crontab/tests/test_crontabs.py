#!/usr/bin/env python
#
# Copyright (C) 2016 Martin Owens <doctormo@gmail.com>
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
Test crontabs listing all available crontabs.
"""

import os
import sys

import unittest
import crontab
import crontabs

import yatest.common
TEST_DIR = yatest.common.test_source_path()

class fake_getpwuid(object):
    def __init__(self, st):
        self.pw_name = st
        if st == 'error':
            raise KeyError("Expected Error")

class fake_stat(object):
    def __init__(self, path):
        self.st_uid = os.path.basename(path).split('_')[0]

crontabs.getpwuid = fake_getpwuid
crontabs.stat = fake_stat

class AnacronSourceTab(list):
    """This enables the anacron detection"""
    slices = "56 4 * * 4"
    def __init__(self, tabs=None):
        self.append(crontab.CronTab(user=False, tab="%s root anacron_cmd %s" \
            % (self.slices, os.path.join(TEST_DIR, 'data', 'anacron'))))

crontabs.KNOWN_LOCATIONS = [
    (AnacronSourceTab,),
    (crontabs.UserSpool, os.path.join(TEST_DIR, 'data', 'spool')),
    (crontabs.UserSpool, os.path.join(TEST_DIR, 'data', 'bad_spool')),
    (crontabs.SystemTab, os.path.join(TEST_DIR, 'data', 'crontab')),
    (crontabs.SystemTab, os.path.join(TEST_DIR, 'data', 'crontabs')),
    (crontabs.AnaCronTab, os.path.join(TEST_DIR, 'data', 'anacron')),
]

crontab.CRON_COMMAND = "%s %s" % (sys.executable, os.path.join(TEST_DIR, 'data', 'crontest'))

class CronTabsTestCase(unittest.TestCase):
    """Test use documentation in crontab."""
    def setUp(self):
        self.tabs = crontabs.CronTabs()

    def assertInTabs(self, command, *users):
        jobs = list(self.tabs.all.find_command(command))
        self.assertEqual(len(jobs), len(users))
        users = sorted([job.user for job in jobs])
        self.assertEqual(users, sorted(users))
        return jobs

    def assertNotInTabs(self, command):
        return self.assertInTabs(command)

    def test_05_spool(self):
        """Test a user spool"""
        self.assertInTabs('do_vlog_brothers', 'hgreen', 'jgreen')

    def test_06_bad_spool(self):
        """Test no access to spool (non-root)"""
        # IMPORTANT! This is testing the fact that the bad-spool will load
        # the user's own crontab, in this instance this is 'user' from the
        # crontest script. This tab is already loaded by the previous User
        # spool and so we expect to find two of them.
        self.assertInTabs('user_command', 'user', 'user')

    def test_10_crontab_dir(self):
        """Test crontabs loaded from system directory"""
        self.assertInTabs('baggins', 'bilbo', 'frodo', 'plastic')

    def test_11_crontab_file(self):
        """Test a single crontab file loaded from system tab"""
        self.assertInTabs('parker', 'driver', 'peter')

    def test_20_anacron(self):
        """Anacron digested"""
        self.assertNotInTabs('anacron_cmd')
        jobs = self.assertInTabs('an_command.sh', 'root')
        self.assertEqual(str(jobs[0].slices), AnacronSourceTab.slices)
        self.assertNotInTabs('not_command.txt')
        

if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(CronTabsTestCase)

