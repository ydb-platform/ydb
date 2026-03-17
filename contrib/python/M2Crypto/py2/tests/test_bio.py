#!/usr/bin/env python
from __future__ import absolute_import
"""
Unit tests for M2Crypto.BIO.

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved.

Copyright (c) 2006 Open Source Applications Foundation
Author: Heikki Toivonen
"""
import logging

from M2Crypto import BIO, Rand, m2, six
from tests import unittest
from tests.fips import fips_mode

log = logging.getLogger('test_bio')

ciphers = [
    'des_ede_ecb', 'des_ede_cbc', 'des_ede_cfb', 'des_ede_ofb',
    'des_ede3_ecb', 'des_ede3_cbc', 'des_ede3_cfb', 'des_ede3_ofb',
    'aes_128_ecb', 'aes_128_cbc', 'aes_128_cfb', 'aes_128_ofb',
    'aes_192_ecb', 'aes_192_cbc', 'aes_192_cfb', 'aes_192_ofb',
    'aes_256_ecb', 'aes_256_cbc', 'aes_256_cfb', 'aes_256_ofb']
nonfips_ciphers = ['bf_ecb', 'bf_cbc', 'bf_cfb', 'bf_ofb',
                   # 'idea_ecb', 'idea_cbc', 'idea_cfb', 'idea_ofb',
                   'cast5_ecb', 'cast5_cbc', 'cast5_cfb', 'cast5_ofb',
                   # 'rc5_ecb', 'rc5_cbc', 'rc5_cfb', 'rc5_ofb',
                   'des_ecb', 'des_cbc', 'des_cfb', 'des_ofb',
                   'rc4', 'rc2_40_cbc']
if not fips_mode and m2.OPENSSL_VERSION_NUMBER < 0x30000000:  # Forbidden ciphers
    ciphers += nonfips_ciphers



class CipherStreamTestCase(unittest.TestCase):
    def try_algo(self, algo):
        data = b'123456789012345678901234'
        my_key = 3 * 15 * b"key"
        my_IV = 3 * 16 * b'IV'
        # Encrypt.
        mem = BIO.MemoryBuffer()
        cf = BIO.CipherStream(mem)
        cf.set_cipher(algo, my_key, my_IV, 1)
        cf.write(data)
        cf.flush()
        cf.write_close()
        cf.close()
        ciphertext = mem.read()

        # Decrypt.
        mem = BIO.MemoryBuffer(ciphertext)
        cf = BIO.CipherStream(mem)
        cf.set_cipher(algo, my_key, my_IV, 0)
        cf.write_close()
        data2 = cf.read()
        cf.close()
        self.assertFalse(cf.readable())

        with self.assertRaises(IOError):
            cf.read()
        with self.assertRaises(IOError):
            cf.readline()
        with self.assertRaises(IOError):
            cf.readlines()

        self.assertEqual(data, data2,
                         '%s algorithm cipher test failed' % algo)

    @unittest.skipUnless(six.PY34, "Doesn't support subTest")
    def test_algo(self):
        for algo in ciphers:
            with self.subTest(algo=algo):
                self.try_algo(algo)

    def test_nosuchalgo(self):
        with self.assertRaises(ValueError):
            self.try_algo('nosuchalgo4567')


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(CipherStreamTestCase)


if __name__ == '__main__':
    Rand.load_file('randpool.dat', -1)
    unittest.TextTestRunner().run(suite())
    Rand.save_file('randpool.dat')
