#!/usr/bin/env python

"""
Unit tests for M2Crypto.BN.

Copyright (c) 2005 Open Source Applications Foundation. All rights reserved.
"""

import re
import warnings

from M2Crypto import BN, Rand
from tests import unittest

loops = 16


class BNTestCase(unittest.TestCase):

    def test_rand(self):
        # defaults
        for _ in range(loops):
            r8 = BN.rand(8)

        # top
        for _ in range(loops):
            r8 = BN.rand(8, top=0)
            assert r8 & 128
        for _ in range(loops):
            r8 = BN.rand(8, top=1)
            assert r8 & 192

        # bottom
        for _ in range(loops):
            r8 = BN.rand(8, bottom=1)
            self.assertEqual(r8 % 2, 1)

        # make sure we can get big numbers and work with them
        for _ in range(loops):
            r8 = BN.rand(8, top=0)
            r16 = BN.rand(16, top=0)
            r32 = BN.rand(32, top=0)
            r64 = BN.rand(64, top=0)
            r128 = BN.rand(128, top=0)
            r256 = BN.rand(256, top=0)
            r512 = BN.rand(512, top=0)
            assert r8 < r16 < r32 < r64 < r128 < r256 < r512 < (r512 + 1)

    def test_rand_range(self):
        # small range
        for _ in range(loops):
            r = BN.rand_range(1)
            self.assertEqual(r, 0)

        for _ in range(loops):
            r = BN.rand_range(4)
            assert 0 <= r < 4

        # large range
        r512 = BN.rand(512, top=0)
        for _ in range(loops):
            r = BN.rand_range(r512)
            assert 0 <= r < r512

    def test_randfname(self):
        m = re.compile('^[a-zA-Z0-9]{8}$')
        for _ in range(loops):
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', DeprecationWarning)
                r = BN.randfname(8)
            assert m.match(r)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(BNTestCase)


if __name__ == '__main__':
    Rand.load_file('randpool.dat', -1)
    unittest.TextTestRunner().run(suite())
    Rand.save_file('randpool.dat')
