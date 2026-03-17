#!/usr/bin/env python

"""Unit tests for M2Crypto.Rand.

Copyright (C) 2006 Open Source Applications Foundation (OSAF).
All Rights Reserved.
"""

from M2Crypto import six
from tests import unittest


class UtilTestCase(unittest.TestCase):
    def test_py3bytes(self):
        self.assertIsInstance(six.ensure_binary('test'), six.binary_type)

    def test_py3str(self):
        self.assertIsInstance(six.ensure_text('test'), six.text_type)

    def test_py3bytes_str(self):
        self.assertIsInstance(six.ensure_binary(u'test'), six.binary_type)

    def test_py3str_str(self):
        self.assertIsInstance(six.ensure_text(u'test'), six.string_types)

    def test_py3bytes_bytes(self):
        self.assertIsInstance(six.ensure_binary(b'test'), six.binary_type)

    def test_py3str_bytes(self):
        self.assertIsInstance(six.ensure_text(b'test'), six.text_type)

    def test_py3bytes_None(self):
        with self.assertRaises(TypeError):
            six.ensure_binary(None)

    def test_py3str_None(self):
        with self.assertRaises(TypeError):
            six.ensure_text(None)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(UtilTestCase))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite())
