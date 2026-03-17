#!/usr/bin/env python
from __future__ import absolute_import, print_function
"""Unit tests for _aes.i.

Copyright (c) 2018 Matej Cepl. All rights reserved.
"""
import logging

from M2Crypto import m2
from tests import unittest

log = logging.getLogger('test_AES')

KEY_TEXT = (b'blabulka' * 3)[:16]  # 128/8 == 16


class AESTestCase(unittest.TestCase):
    """
    Functions in AES module are very low level, and you are HIGHLY
    encouraged NOT to use them. Use functions from EVP module instead.

    These tests are here only for checking regressions, not as an
    illustration for the real world use.
    """

    def test_existing_methods(self):
        missing = []
        exp_mtds = ('aes_128_cbc', 'aes_128_cfb', 'aes_128_ctr',
                    'aes_128_ecb', 'aes_128_ofb', 'aes_192_cbc',
                    'aes_192_cfb', 'aes_192_ctr', 'aes_192_ecb',
                    'aes_192_ofb', 'aes_256_cbc', 'aes_256_cfb',
                    'aes_256_ctr', 'aes_256_ecb', 'aes_256_ofb')
        for name in exp_mtds:
            if not hasattr(m2, name):
                missing.append(name)

        self.assertEqual(missing, [])

    def test_set_key(self):
        key = m2.aes_new()
        m2.AES_set_key(key, KEY_TEXT, 128, 1)
        m2.AES_free(key)

        key2 = m2.aes_new()
        m2.AES_set_key(key2, KEY_TEXT, 128, 0)
        m2.AES_free(key2)
        # Just to make sure nothing crashed

    def test_type_check(self):
        key = m2.aes_new()
        m2.AES_set_key(key, KEY_TEXT, 128, 1)
        res = m2.AES_type_check(key)
        m2.AES_free(key)
        self.assertEqual(res, 1)  # Just to make sure nothing crashed

    def test_crypt(self):
        padded = b'zezulicka       '  # len(padded) % 16 == 0
        key = m2.aes_new()
        # op == 0: encrypt
        # otherwise: decrypt (Python code will supply the value 1.)
        m2.AES_set_key(key, KEY_TEXT, 128, 0)
        enc = m2.AES_crypt(key, padded, len(padded), 0)
        m2.AES_free(key)

        key2 = m2.aes_new()
        m2.AES_set_key(key2, KEY_TEXT, 128, 1)
        observed = m2.AES_crypt(key2, enc, len(enc), 1)
        m2.AES_free(key2)

        self.assertEqual(padded, observed)


def suite():
    t_suite = unittest.TestSuite()
    t_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(AESTestCase))
    return t_suite


if __name__ == '__main__':
    unittest.main()
