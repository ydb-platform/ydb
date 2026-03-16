# -*- coding: utf-8 -*-

import unittest
from collections import OrderedDict
from w3lib.http import (basic_auth_header,
                        headers_dict_to_raw, headers_raw_to_dict)

__doctests__ = ['w3lib.http'] # for trial support

class HttpTests(unittest.TestCase):

    def test_basic_auth_header(self):
        self.assertEqual(b'Basic c29tZXVzZXI6c29tZXBhc3M=',
                basic_auth_header('someuser', 'somepass'))
        # Check url unsafe encoded header
        self.assertEqual(b'Basic c29tZXVzZXI6QDx5dTk-Jm8_UQ==',
            basic_auth_header('someuser', '@<yu9>&o?Q'))

    def test_basic_auth_header_encoding(self):
        self.assertEqual(b'Basic c29tw6Z1c8Oocjpzw7htZXDDpHNz',
                basic_auth_header(u'somæusèr', u'sømepäss', encoding='utf8'))
        # default encoding (ISO-8859-1)
        self.assertEqual(b'Basic c29t5nVz6HI6c_htZXDkc3M=',
                basic_auth_header(u'somæusèr', u'sømepäss'))

    def test_headers_raw_dict_none(self):
        self.assertIsNone(headers_raw_to_dict(None))
        self.assertIsNone(headers_dict_to_raw(None))

    def test_headers_raw_to_dict(self):
        raw = b"Content-type: text/html\n\rAccept: gzip\n\r\
                Cache-Control: no-cache\n\rCache-Control: no-store\n\n"
        dct = {b'Content-type': [b'text/html'], b'Accept': [b'gzip'], 
               b'Cache-Control': [b'no-cache', b'no-store']}
        self.assertEqual(headers_raw_to_dict(raw), dct)

    def test_headers_dict_to_raw(self):
        dct = OrderedDict([
            (b'Content-type', b'text/html'),
            (b'Accept', b'gzip')
        ])
        self.assertEqual(
            headers_dict_to_raw(dct),
            b'Content-type: text/html\r\nAccept: gzip'
        )

    def test_headers_dict_to_raw_listtuple(self):
        dct = OrderedDict([
            (b'Content-type', [b'text/html']),
            (b'Accept', [b'gzip'])
        ])
        self.assertEqual(
            headers_dict_to_raw(dct),
            b'Content-type: text/html\r\nAccept: gzip'
        )

        dct = OrderedDict([
            (b'Content-type', (b'text/html',)),
            (b'Accept', (b'gzip',))
        ])
        self.assertEqual(
            headers_dict_to_raw(dct),
            b'Content-type: text/html\r\nAccept: gzip'
        )

        dct = OrderedDict([
            (b'Cookie', (b'val001', b'val002')),
            (b'Accept', b'gzip')
        ])
        self.assertEqual(
            headers_dict_to_raw(dct),
            b'Cookie: val001\r\nCookie: val002\r\nAccept: gzip'
        )

        dct = OrderedDict([
            (b'Cookie', [b'val001', b'val002']),
            (b'Accept', b'gzip')
        ])
        self.assertEqual(
            headers_dict_to_raw(dct),
            b'Cookie: val001\r\nCookie: val002\r\nAccept: gzip'
        )

    def test_headers_dict_to_raw_wrong_values(self):
        dct = OrderedDict([
            (b'Content-type', 0),
        ])
        self.assertEqual(
            headers_dict_to_raw(dct),
            b''
        )

        dct = OrderedDict([
            (b'Content-type', 1),
            (b'Accept', [b'gzip'])
        ])
        self.assertEqual(
            headers_dict_to_raw(dct),
            b'Accept: gzip'
        )
