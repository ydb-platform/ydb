#!/usr/bin/env python

"""Unit tests for M2Crypto initialization."""
import M2Crypto
from tests import unittest


class InitTestCase(unittest.TestCase):

    def test_version_info(self):
        self.assertIs(type(()), type(M2Crypto.version_info))


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(InitTestCase)


if __name__ == '__main__':
    Rand.load_file('randpool.dat', -1)
    unittest.TextTestRunner().run(suite())
    Rand.save_file('randpool.dat')
