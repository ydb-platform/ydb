#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Martin Owens
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
Test crontab use of UTF-8 filenames and strings
"""

import os
import sys

import locale
import unittest
from crontab import CronTab

import yatest.common
TEST_DIR = yatest.common.test_source_path()

content = """
*/4 * * * * ůțƒ_command # ůțƒ_comment
"""
filename = os.path.join(TEST_DIR, 'data', 'output-ůțƒ-8.tab')

class Utf8TestCase(unittest.TestCase):
    """Test basic functionality of crontab."""
    def setUp(self):
        self.crontab = CronTab(tab=content)

    def test_01_input(self):
        """Read UTF-8 contents"""
        self.assertTrue(self.crontab)

    @unittest.skip("Distbuild skip")
    def test_02_write(self):
        """Write/Read UTF-8 Filename"""
        self.assertEqual(locale.getpreferredencoding(), 'UTF-8')
        self.crontab.write(filename)
        crontab = CronTab(tabfile=filename)
        self.assertTrue(crontab)
        with open(filename, "r") as fhl:
            self.assertEqual(content, fhl.read())
        os.unlink(filename)

    def test_04_command(self):
        """Read Command String"""
        self.assertEqual(self.crontab[0].command, u"ůțƒ_command")

    def test_05_comment(self):
        """Read Comment String"""
        self.assertEqual(self.crontab[0].comment, u'ůțƒ_comment')

    def test_06_unicode(self):
        """Write New via Unicode"""
        job = self.crontab.new(command=u"ůțƒ_command", comment=u'ůțƒ_comment')
        self.assertEqual(job.command, u"ůțƒ_command")
        self.assertEqual(job.comment, u"ůțƒ_comment")
        self.crontab.render()

    def test_07_utf8(self):
        """Write New via UTF-8"""
        job = self.crontab.new(command=b'\xc5\xaf\xc8\x9b\xc6\x92_command',
                               comment=b'\xc5\xaf\xc8\x9b\xc6\x92_comment')
        self.assertEqual(self.crontab.render(), u"""
*/4 * * * * ůțƒ_command # ůțƒ_comment

* * * * * ůțƒ_command # ůțƒ_comment
""")
        self.assertEqual(type(job.command), str)
        self.assertEqual(type(job.comment), str)

    def test_08_utf8_str(self):
        """Test UTF8 (non unicode) strings"""
        self.crontab[0].command = '￡１２'
        self.crontab[0].comment = '￼𝗔𝗕𝗖𝗗'
        self.assertEqual(self.crontab.render(), u"""
*/4 * * * * ￡１２ # ￼𝗔𝗕𝗖𝗗
""")

    @unittest.skip("Distbuild skip")
    def test_09_utf8_again(self):
        """Test Extra UTF8 input"""
        filename = os.path.join(TEST_DIR, 'data', 'utf_extra')
        with open(filename + '.in', 'w') as fhl:
            fhl.write('# 中文\n30 2 * * * source /etc/profile\n30 1 * * * source /etc/profile')

        try:
            cron = CronTab(tabfile=filename + '.in')
        finally:
            os.unlink(filename + '.in')

        cron.write(filename + '.out')
        if os.path.isfile(filename + '.out'):
            os.unlink(filename + '.out')

if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(
       Utf8TestCase,
    )
