#!/usr/bin/env python
#
# Copyright (C) 2011 Martin Owens, 2019 Jordan Miller
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
Test crontab context.
"""

import os
import sys

sys.path.insert(0, '../')

import unittest
from crontab import CronTab

INITAL_TAB = """
* * * JAN     SAT          enums
* * * MAR-APR *            ranges
* * * *       MON,FRI,WED  multiples
* * * *       *            com1 # Comment One
* * * *       *            com2 # Comment One
"""

RESULT_TAB = """
* * * JAN SAT enums
* * * MAR-APR * ranges
* * * * MON,WED,FRI multiples
* * * * * com1 # Comment One
* * * * * com2 # Comment One
"""

class ContextTestCase(unittest.TestCase):
    """Test basic functionality of crontab."""
    def setUp(self):
        self.crontab = CronTab(tab=INITAL_TAB)

    def test_01_context(self):
        """context matches non-context"""
        self.crontab.write()
        results = RESULT_TAB.split('\n')
        for line_no, line in enumerate(self.crontab.intab.split('\n')):
            self.assertEqual(str(line), results[line_no])
        with CronTab(tab=INITAL_TAB) as crontab_context:
            self.crontab_context = crontab_context
        for line_no, line in enumerate(self.crontab_context.intab.split('\n')):
            self.assertEqual(str(line), results[line_no])

if __name__ == '__main__':
    try:
        from test import test_support
    except ImportError:
        from test import support as test_support

    test_support.run_unittest(
       ContextTestCase,
    )
