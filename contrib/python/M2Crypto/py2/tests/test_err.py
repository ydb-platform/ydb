#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Unit tests for M2Crypto.Err.

Copyright (C) 2019 Matěj Cepl
Released under the terms of MIT/X11 License,
see the file LICENCE for more.
"""
from M2Crypto import Err
from tests import unittest


class ErrTestCase(unittest.TestCase):

    def test_no_error(self):
        # Protection against gl#m2crypto/m2crypto#258
        self.assertEqual(Err.get_error_reason(0), '')



def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(ErrTestCase))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite())
