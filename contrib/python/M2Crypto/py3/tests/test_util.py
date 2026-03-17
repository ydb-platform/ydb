#!/usr/bin/env python

"""
Unit tests for M2Crypto.util.

Copyright (c) 2024 Matěj Cepl. All rights reserved.
"""

import os
import platform
from M2Crypto import m2, util
from tests import unittest


class UtilTestCase(unittest.TestCase):

    def test_is32bit(self):
        # defaults
        bit32 = m2.time_t_bits()
        # test for libc musl from unfinished upstream bug gh#python/cpython#87414
        if (
            util.is_32bit()
            and (platform.libc_ver() != ("", ""))
            and (
                os.environ.get('M2_TIMET_NO_ARCH', '').casefold()
                not in ['true', '1', 'yes']
            )
        ):
            self.assertEqual(bit32, 32)
        else:
            self.assertNotEqual(bit32, 32)
        self.assertIsInstance(bit32, int)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(UtilTestCase)


if __name__ == '__main__':
    Rand.load_file('randpool.dat', -1)
    unittest.TextTestRunner().run(suite())
    Rand.save_file('randpool.dat')
