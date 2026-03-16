#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gc
import unittest

from dllist_test import testdllist
from sllist_test import testsllist


gc.set_debug(gc.DEBUG_UNCOLLECTABLE | gc.DEBUG_STATS)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(testsllist))
    suite.addTest(unittest.makeSuite(testdllist))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
