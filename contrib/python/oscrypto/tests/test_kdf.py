# coding: utf-8
from __future__ import unicode_literals, division, absolute_import, print_function

import unittest
import sys

from oscrypto import kdf, _pkcs5

from ._unittest_compat import patch

patch()

if sys.version_info < (3,):
    byte_cls = str
else:
    byte_cls = bytes


class KDFTests(unittest.TestCase):

    def test_pbkdf1(self):
        key = kdf.pbkdf1('sha1', b'password', b'\x78\x57\x8E\x5A\x5D\x63\xCB\x06', 1000, 16)
        self.assertEqual(b'\xDC\x19\x84\x7E\x05\xC6\x4D\x2F\xAF\x10\xEB\xFB\x4A\x3D\x2A\x20', key)

    def test_pbkdf2(self):
        key = kdf.pbkdf2('sha1', b'password', b'\x78\x57\x8E\x5A\x5D\x63\xCB\x06', 2048, 24)
        self.assertEqual(
            b'\xBF\xDE\x6B\xE9\x4D\xF7\xE1\x1D\xD4\x09\xBC\xE2\x0A\x02\x55\xEC\x32\x7C\xB9\x36\xFF\xE9\x36\x43',
            key
        )

        key = kdf.pbkdf2('sha1', b'password', b'\x09\xb7\x8c\xf0\x0c\x7f\xf5\x4a', 2048, 24)
        self.assertEqual(
            b'\x00\x74\x9d\x47\x49\xa0\x88\x2b\xe9\x48\xb8\x65\x7b\xf2\x64\x97\xf1\x1e\xa2\xf9\xfc\xd4\x70\x46',
            key
        )

    def test_python_pbkdf2(self):
        key = _pkcs5.pbkdf2('sha1', b'password', b'\x78\x57\x8E\x5A\x5D\x63\xCB\x06', 2048, 24)
        self.assertEqual(
            b'\xBF\xDE\x6B\xE9\x4D\xF7\xE1\x1D\xD4\x09\xBC\xE2\x0A\x02\x55\xEC\x32\x7C\xB9\x36\xFF\xE9\x36\x43',
            key
        )

        key = _pkcs5.pbkdf2('sha1', b'password', b'\x09\xb7\x8c\xf0\x0c\x7f\xf5\x4a', 2048, 24)
        self.assertEqual(
            b'\x00\x74\x9d\x47\x49\xa0\x88\x2b\xe9\x48\xb8\x65\x7b\xf2\x64\x97\xf1\x1e\xa2\xf9\xfc\xd4\x70\x46',
            key
        )

    def test_pkcs12_kdf(self):
        key = kdf.pkcs12_kdf('sha1', b'sesame', b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF', 2048, 24, 1)
        self.assertEqual(
            b'\x7C\xD9\xFD\x3E\x2B\x3B\xE7\x69\x1A\x44\xE3\xBE\xF0\xF9\xEA\x0F\xB9\xB8\x97\xD4\xE3\x25\xD9\xD1',
            key
        )
