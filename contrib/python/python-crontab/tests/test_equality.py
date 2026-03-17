#!/usr/bin/env python
#
# Copyright (C) 2019 Kyle Bakker <xelnaga.zealot@gmail.com>
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
Test equality between CronItems
"""

import unittest
from crontab import CronTab

CRONTAB1 = """
    3 * * * * command1 # CommentID C
    2 * * * * command2 # CommentID AAB
    1 * * * * command3 # CommentID B3
"""

CRONTAB2 = """
    3 * * * * command1 # CommentID C
    2 * * * * command2 # CommentID AAB
    1 * * * * command3 # CommentID B3
    4 * * * * command5 # CommentID K
"""

class EqualityTestCase(unittest.TestCase):
    """ Testing equality against CronItems"""
    def setUp(self):
        self.crontab1 = CronTab(tab=CRONTAB1)
        self.crontab2 = CronTab(tab=CRONTAB2)

    def test_equality(self):
        """Test equality"""
        self.assertEqual(self.crontab1[0], self.crontab2[0])

    def test_inequality(self):
        """Test inequality"""
        self.assertNotEqual(self.crontab1[0], self.crontab2[1])

    def test_listdifference(self):
        """Test diference between lists"""
        self.assertEqual(set(self.crontab2) - set(self.crontab1), {self.crontab2[3]})

    def test_hash(self):
        """Test object hashing"""
        self.assertEqual(self.crontab1[0].__hash__(), hash((self.crontab1[0])))

if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(EqualityTestCase)
