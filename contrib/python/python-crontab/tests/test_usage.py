#!/usr/bin/env python
#
# Copyright (C) 2012 Jay Sigbrandt <jsigbrandt@slb.com>
#                    Martin Owens <doctormo@gmail.com>
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
Test crontab usage.
"""

import os
import sys

import unittest
import crontab

from datetime import date, time, datetime, timedelta

crontab.LOG.setLevel(crontab.logging.ERROR)
import yatest.common
TEST_DIR = yatest.common.test_source_path()

class DummyStdout(object):
    def write(self, text):
        pass

BASIC = '@hourly firstcommand\n\n'
USER = '\n*/4 * * * * user_command # user_comment\n\n\n'
crontab.CRON_COMMAND = "%s %s" % (sys.executable, os.path.join(TEST_DIR, 'data', 'crontest'))

def flush():
    pass

class Attribute(object):
    def __init__(self, obj, attr, value):
        self.obj = obj
        self.attr = attr
        self.value = value

    def __enter__(self, *args, **kw):
        if hasattr(self.obj, self.attr):
            self.previous = getattr(self.obj, self.attr)
        setattr(self.obj, self.attr, self.value)

    def __exit__(self, *args, **kw):
        if hasattr(self, 'previous'):
            setattr(self.obj, self.attr, self.previous)
        else:
            delattr(self.obj, self.attr)


class UseTestCase(unittest.TestCase):
    """Test use documentation in crontab."""
    def setUp(self):
        self.filenames = []

    def test_01_empty(self):
        """Open system crontab"""
        cron = crontab.CronTab()
        self.assertEqual(cron.render(), "")
        self.assertEqual(cron.__str__(), "")
        self.assertEqual(repr(cron), "<Unattached CronTab>")

    def test_02_user(self):
        """Open a user's crontab"""
        cron = crontab.CronTab(user='basic')
        self.assertEqual(cron.render(), BASIC)
        self.assertEqual(repr(cron), "<User CronTab 'basic'>")

    def test_03_usage(self):
        """Dont modify crontab"""
        cron = crontab.CronTab(tab='')
        sys.stdout = DummyStdout()
        sys.stdout.flush = flush
        try:
            exec(crontab.__doc__)
        except ImportError:
            pass
        sys.stdout = sys.__stdout__
        self.assertEqual(cron.render(), '')

    def test_04_username(self):
        """Username is True"""
        cron = crontab.CronTab(user=True)
        self.assertNotEqual(cron.user, True)
        self.assertEqual(cron.render(), USER)
        self.assertEqual(repr(cron), "<My CronTab>")

    def test_05_nouser(self):
        """Username doesn't exist"""
        cron = crontab.CronTab(user='nouser')
        self.assertEqual(cron.render(), '')

    def test_06_touser(self):
        """Write to user API"""
        cron = crontab.CronTab(tab=USER)
        self.assertEqual(repr(cron), "<Unattached CronTab>")
        cron.write_to_user('bob')
        filename = os.path.join(TEST_DIR, 'data', 'spool', 'bob')
        self.filenames.append(filename)
        self.assertTrue(os.path.exists(filename))
        self.assertEqual(repr(cron), "<User CronTab 'bob'>")

    def test_07_ioerror_read(self):
        """No filename ioerror"""
        with self.assertRaises(IOError):
            cron = crontab.CronTab(user='error')
            cron.read()

    def test_07_ioerror_write(self):
        """User not specified, nowhere to write to"""
        cron = crontab.CronTab()
        with self.assertRaises(IOError):
            cron.write()

    def test_08_cronitem(self):
        """CronItem Standalone"""
        item = crontab.CronItem.from_line('noline')
        self.assertTrue(item.is_enabled())
        with self.assertRaises(UnboundLocalError):
            item.delete()
        item.set_command('nothing')
        self.assertEqual(item.render(), '* * * * * nothing')

    def test_10_time_object(self):
        """Set slices using time object"""
        item = crontab.CronItem(command='cmd')
        self.assertEqual(str(item.slices), '* * * * *')
        item.setall(time(1, 2))
        self.assertEqual(str(item.slices), '2 1 * * *')
        self.assertTrue(item.is_valid())
        item.setall(time(0, 30, 0, 0))
        self.assertEqual(str(item.slices), '30 0 * * *')
        self.assertTrue(item.is_valid())
        self.assertEqual(str(item), '30 0 * * * cmd')

    def test_11_date_object(self):
        """Set slices using date object"""
        item = crontab.CronItem(command='cmd')
        self.assertEqual(str(item.slices), '* * * * *')
        item.setall(date(2010, 6, 7))
        self.assertEqual(str(item.slices), '0 0 7 6 *')
        self.assertTrue(item.is_valid())

    def test_12_datetime_object(self):
        """Set slices using datetime object"""
        item = crontab.CronItem(command='cmd')
        self.assertEqual(str(item.slices), '* * * * *')
        item.setall(datetime(2009, 8, 9, 3, 4))
        self.assertTrue(item.is_valid())
        self.assertEqual(str(item.slices), '4 3 9 8 *')

    def test_20_slice_validation(self):
        """CronSlices class and objects can validate"""
        CronSlices = crontab.CronSlices
        self.assertTrue(CronSlices('* * * * *').is_valid())
        self.assertTrue(CronSlices.is_valid('* * * * *'))
        self.assertTrue(CronSlices.is_valid('*/2 * * * *'))
        self.assertTrue(CronSlices.is_valid('* 1,2 * * *'))
        self.assertTrue(CronSlices.is_valid('* * 1-5 * *'))
        self.assertTrue(CronSlices.is_valid('* * * * MON-WED'))
        self.assertTrue(CronSlices.is_valid('@reboot'))

        sliced = CronSlices('* * * * *')
        sliced[0].parts = [300]
        self.assertEqual(str(sliced), '300 * * * *')
        self.assertFalse(sliced.is_valid())
        self.assertFalse(CronSlices.is_valid('P'))
        self.assertFalse(CronSlices.is_valid('*/61 * * * *'))
        self.assertFalse(CronSlices.is_valid('* 1,300 * * *'))
        self.assertFalse(CronSlices.is_valid('* * 50-1 * *'))
        self.assertFalse(CronSlices.is_valid('* * * * FRO-TOO'))
        self.assertFalse(CronSlices.is_valid('@retool'))

        self.assertRaises(ValueError, CronSlices._parse_value, None)

    def test_21_slice_special(self):
        """Rendering can be done without specials"""
        cronitem = crontab.CronItem('true')
        cronitem.setall('0 0 * * *')
        self.assertEqual(cronitem.render(specials=True), '@daily true')
        self.assertEqual(cronitem.render(specials=None), '0 0 * * * true')
        cronitem.setall('@daily')
        self.assertEqual(cronitem.render(specials=None), '@daily true')
        self.assertEqual(cronitem.render(specials=False), '0 0 * * * true')

    def test_25_process(self):
        """Test opening pipes"""
        from crontab import Process, CRON_COMMAND
        process = Process(CRON_COMMAND, h=None, a='one', abc='two').run()
        self.assertEqual(int(process), 0)
        self.assertEqual(repr(process)[:8], "Process(")
        self.assertEqual(process.stderr, '')
        self.assertEqual(process.stdout, '--abc=two|-a|-h|one\n')

    def test_07_zero_padding(self):
        """Can we get zero padded output"""
        cron = crontab.CronTab(tab="02 3-5 2,4 */2 01 cmd")
        self.assertEqual(str(cron), '2 3-5 2,4 */2 1 cmd\n')
        with Attribute(crontab, 'ZERO_PAD', True):
            self.assertEqual(str(cron), '02 03-05 02,04 */2 01 cmd\n')

    def test_08_reset_after_daily(self):
        """Can jobs be rescheduled after midnight"""
        cron = crontab.CronTab(tab="@daily cmd")
        job = cron[0]
        self.assertEqual(str(job), '@daily cmd')
        job.minute.on(5)
        job.hour.on(1)
        self.assertEqual(str(job), '5 1 * * * cmd')

    def test_09_pre_comment(self):
        """Test use of pre_comments"""
        text = """
# Unattached comment

5 * * * 6 some_command
# Attached comment
*/5 * * * * another_command
*/5 * * * * my_command # Other comment
"""
        tab = crontab.CronTab(tab=text)
        self.assertEqual(tab[0].comment, "")
        self.assertEqual(tab[1].comment, "Attached comment")
        self.assertEqual(tab[2].comment, "Other comment")
        self.assertEqual(tab.render(), text)

    def tearDown(self):
        for filename in self.filenames:
            if os.path.exists(filename):
                os.unlink(filename)


if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(
       UseTestCase,
    )
