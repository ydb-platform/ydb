#!/usr/bin/env python
"""Tests for tablib._vendor.dbfpy."""

import unittest

from tablib._vendor.dbfpy import fields


class DbfFieldDefTestCompareCase(unittest.TestCase):
    """dbfpy.fields.DbfFieldDef comparison test cases, via child classes."""

    def setUp(self) -> None:
        self.length = 10
        self.a = fields.DbfCharacterFieldDef("abc", self.length)
        self.z = fields.DbfCharacterFieldDef("xyz", self.length)
        self.a2 = fields.DbfCharacterFieldDef("abc", self.length)

    def test_compare__eq__(self):
        # Act / Assert
        self.assertEqual(self.a, self.a2)

    def test_compare__ne__(self):
        # Act / Assert
        self.assertNotEqual(self.a, self.z)

    def test_compare__lt__(self):
        # Act / Assert
        self.assertLess(self.a, self.z)

    def test_compare__le__(self):
        # Act / Assert
        self.assertLessEqual(self.a, self.a2)
        self.assertLessEqual(self.a, self.z)

    def test_compare__gt__(self):
        # Act / Assert
        self.assertGreater(self.z, self.a)

    def test_compare__ge__(self):
        # Act / Assert
        self.assertGreaterEqual(self.a2, self.a)
        self.assertGreaterEqual(self.z, self.a)
