#!/usr/bin/env python

"""Unit tests for M2Crypto.Rand.

Copyright (C) 2006 Open Source Applications Foundation (OSAF).
All Rights Reserved.
"""

import os
import ctypes
import warnings

from M2Crypto import Rand, m2
from tests import unittest


class RandTestCase(unittest.TestCase):
    def test_bytes(self):
        with self.assertRaises(MemoryError):
            Rand.rand_bytes(-1)
        self.assertEqual(Rand.rand_bytes(0), b'')
        self.assertEqual(len(Rand.rand_bytes(1)), 1)

    def test_pseudo_bytes(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            with self.assertRaises(MemoryError):
                Rand.rand_pseudo_bytes(-1)
            self.assertEqual(Rand.rand_pseudo_bytes(0), (b'', 1))
            a, b = Rand.rand_pseudo_bytes(1)
            self.assertEqual(len(a), 1)
            self.assertEqual(b, 1)

    def test_file_name(self):
        if os.name == 'nt':
            is_admin = ctypes.windll.shell32.IsUserAnAdmin() != 0
            rand_env = ('RANDFILE', 'HOME', 'USERPROFILE', 'SYSTEMROOT')
        else:
            rand_env = ('RANDFILE', 'HOME')
            is_admin = False
        key = next((k for k in rand_env if os.environ.get(k)), None)
        if is_admin and m2.OPENSSL_VERSION_NUMBER < 0x1010000F:
            path = 'C:\\'
        else:
            path = os.path.join(os.environ[key])
        self.assertIsNotNone(key, "Could not find required environment")
        rand_file = os.path.abspath(os.path.join(path, '.rnd'))
        self.assertEqual(os.path.abspath(Rand.rand_file_name()), rand_file)

    def test_load_save(self):
        try:
            os.remove('tests/randpool.dat')
        except OSError:
            pass
        self.assertIn(Rand.load_file('tests/randpool.dat', -1), [0, -1])
        self.assertEqual(Rand.save_file('tests/randpool.dat'), 1024)
        self.assertEqual(Rand.load_file('tests/randpool.dat', -1), 1024)

    def test_seed_add(self):
        self.assertIsNone(Rand.rand_seed(os.urandom(1024)))

        # XXX Should there be limits on the entropy parameter?
        self.assertIsNone(Rand.rand_add(os.urandom(2), 0.5))
        Rand.rand_add(os.urandom(2), -0.5)
        Rand.rand_add(os.urandom(2), 5000.0)

    def test_rand_status(self):
        # Although it is hard to believe we would ever get 0 (i.e., PRNG
        # hasn't enough entropy), it is a legitimate value.
        status = Rand.rand_status()
        self.assertIn(status, [0, 1],
                      'Illegal value of RAND.rand_status {0}!'.format(status))
        if status == 0:
            warnings.warn('RAND_status reports insufficient seeding of PRNG!')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(RandTestCase))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite())
