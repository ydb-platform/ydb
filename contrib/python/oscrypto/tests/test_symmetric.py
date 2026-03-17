# coding: utf-8
from __future__ import unicode_literals, division, absolute_import, print_function

import unittest
import sys

from oscrypto import symmetric, util, backend

from ._unittest_compat import patch
from .exception_context import assert_exception
from .unittest_data import data_decorator, data

patch()

if sys.version_info < (3,):
    byte_cls = str
else:
    byte_cls = bytes


_backend = backend()


if _backend == 'openssl':
    from oscrypto._openssl._libcrypto import libcrypto_legacy_support
    supports_legacy = libcrypto_legacy_support
else:
    supports_legacy = True


@data_decorator
class SymmetricTests(unittest.TestCase):

    @staticmethod
    def aes_key_lengths():
        return (
            ('128', 16),
            ('192', 24),
            ('256', 32),
        )

    @data('aes_key_lengths', True)
    def aes_cbc_no_padding_encrypt_decrypt(self, key_byte_len):
        key = util.rand_bytes(key_byte_len)
        data = b'This is the data'

        iv, ciphertext = symmetric.aes_cbc_no_padding_encrypt(key, data, None)
        self.assertNotEqual(data, ciphertext)
        self.assertEqual(byte_cls, type(ciphertext))

        plaintext = symmetric.aes_cbc_no_padding_decrypt(key, ciphertext, iv)
        self.assertEqual(data, plaintext)

    @data('aes_key_lengths', True)
    def aes_cbc_no_padding_encrypt_decrypt_two_block(self, key_byte_len):
        key = util.rand_bytes(key_byte_len)

        data = b'This is data to encrypt-32 bytes'

        iv, ciphertext = symmetric.aes_cbc_no_padding_encrypt(key, data, None)
        self.assertNotEqual(data, ciphertext)
        self.assertEqual(byte_cls, type(ciphertext))

        plaintext = symmetric.aes_cbc_no_padding_decrypt(key, ciphertext, iv)
        self.assertEqual(data, plaintext)

    @data('aes_key_lengths', True)
    def aes_cbc_no_padding_wrong_length(self, key_byte_len):
        key = util.rand_bytes(key_byte_len)

        with assert_exception(self, ValueError, r'data must be a multiple of 16 bytes long - is 31'):
            data = b'31 bytes of data to encrypt now'
            iv, ciphertext = symmetric.aes_cbc_no_padding_encrypt(key, data, None)

        with assert_exception(self, ValueError, r'data must be a multiple of 16 bytes long - is 33'):
            data = b'Thirty three bytes to encrypt now'
            iv, ciphertext = symmetric.aes_cbc_no_padding_encrypt(key, data, None)

        with assert_exception(self, ValueError, r'data must be a multiple of 16 bytes long - is 15'):
            data = b'Fifteen bytes!!'
            iv, ciphertext = symmetric.aes_cbc_no_padding_encrypt(key, data, None)

        with assert_exception(self, ValueError, r'data must be a multiple of 16 bytes long - is 24'):
            data = b'Twenty four bytes long!!'
            iv, ciphertext = symmetric.aes_cbc_no_padding_encrypt(key, data, None)

    @data('aes_key_lengths', True)
    def aes_encrypt_decrypt(self, key_byte_len):
        key = util.rand_bytes(key_byte_len)
        data = b'This is data to encrypt'

        iv, ciphertext = symmetric.aes_cbc_pkcs7_encrypt(key, data, None)
        self.assertNotEqual(data, ciphertext)
        self.assertEqual(byte_cls, type(ciphertext))

        plaintext = symmetric.aes_cbc_pkcs7_decrypt(key, ciphertext, iv)
        self.assertEqual(data, plaintext)

    def test_rc4_40_encrypt_decrypt(self):
        def do_run():
            key = util.rand_bytes(5)
            data = b'This is data to encrypt'

            ciphertext = symmetric.rc4_encrypt(key, data)
            self.assertNotEqual(data, ciphertext)
            self.assertEqual(byte_cls, type(ciphertext))

            plaintext = symmetric.rc4_decrypt(key, ciphertext)
            self.assertEqual(data, plaintext)

        if not supports_legacy:
            with self.assertRaises(EnvironmentError):
                do_run()
        else:
            do_run()

    def test_rc4_128_encrypt_decrypt(self):
        def do_run():
            key = util.rand_bytes(16)
            data = b'This is data to encrypt'

            ciphertext = symmetric.rc4_encrypt(key, data)
            self.assertNotEqual(data, ciphertext)
            self.assertEqual(byte_cls, type(ciphertext))

            plaintext = symmetric.rc4_decrypt(key, ciphertext)
            self.assertEqual(data, plaintext)

        if not supports_legacy:
            with self.assertRaises(EnvironmentError):
                do_run()
        else:
            do_run()

    def test_rc2_64_encrypt_decrypt(self):
        def do_run():
            key = util.rand_bytes(8)
            data = b'This is data to encrypt'

            iv, ciphertext = symmetric.rc2_cbc_pkcs5_encrypt(key, data, None)
            self.assertNotEqual(data, ciphertext)
            self.assertEqual(byte_cls, type(ciphertext))

            plaintext = symmetric.rc2_cbc_pkcs5_decrypt(key, ciphertext, iv)
            self.assertEqual(data, plaintext)

        if not supports_legacy:
            with self.assertRaises(EnvironmentError):
                do_run()
        else:
            do_run()

    def test_rc2_40_encrypt_decrypt(self):
        def do_run():
            key = util.rand_bytes(5)
            data = b'This is data to encrypt'

            iv, ciphertext = symmetric.rc2_cbc_pkcs5_encrypt(key, data, None)
            self.assertNotEqual(data, ciphertext)
            self.assertEqual(byte_cls, type(ciphertext))

            plaintext = symmetric.rc2_cbc_pkcs5_decrypt(key, ciphertext, iv)
            self.assertEqual(data, plaintext)

        if not supports_legacy:
            with self.assertRaises(EnvironmentError):
                do_run()
        else:
            do_run()

    def test_des_encrypt_decrypt(self):
        def do_run():
            key = util.rand_bytes(8)
            data = b'This is data to encrypt'

            iv, ciphertext = symmetric.des_cbc_pkcs5_encrypt(key, data, None)
            self.assertNotEqual(data, ciphertext)
            self.assertEqual(byte_cls, type(ciphertext))

            plaintext = symmetric.des_cbc_pkcs5_decrypt(key, ciphertext, iv)
            self.assertEqual(data, plaintext)

        if not supports_legacy:
            with self.assertRaises(EnvironmentError):
                do_run()
        else:
            do_run()

    def test_3des_2k_encrypt_decrypt(self):
        key = util.rand_bytes(16)
        data = b'This is data to encrypt'

        iv, ciphertext = symmetric.tripledes_cbc_pkcs5_encrypt(key, data, None)
        self.assertNotEqual(data, ciphertext)
        self.assertEqual(byte_cls, type(ciphertext))

        plaintext = symmetric.tripledes_cbc_pkcs5_decrypt(key, ciphertext, iv)
        self.assertEqual(data, plaintext)

    def test_3des_3k_encrypt_decrypt(self):
        key = util.rand_bytes(24)
        data = b'This is data to encrypt'

        iv, ciphertext = symmetric.tripledes_cbc_pkcs5_encrypt(key, data, None)
        self.assertNotEqual(data, ciphertext)
        self.assertEqual(byte_cls, type(ciphertext))

        plaintext = symmetric.tripledes_cbc_pkcs5_decrypt(key, ciphertext, iv)
        self.assertEqual(data, plaintext)
