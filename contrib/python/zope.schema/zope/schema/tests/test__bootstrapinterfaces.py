##############################################################################
#
# Copyright (c) 2012 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import unittest


try:
    compare = cmp
except NameError:
    def compare(a, b):
        return -1 if a < b else (0 if a == b else 1)


class ValidationErrorTests(unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._bootstrapinterfaces import ValidationError
        return ValidationError

    def _makeOne(self, *args, **kw):
        return self._getTargetClass()(*args, **kw)

    def test_doc(self):
        class Derived(self._getTargetClass()):
            """DERIVED"""
        inst = Derived()
        self.assertEqual(inst.doc(), 'DERIVED')

    def test___cmp___no_args(self):
        ve = self._makeOne()
        self.assertEqual(compare(ve, object()), -1)
        self.assertEqual(compare(object(), ve), 1)

    def test___cmp___hit(self):
        left = self._makeOne('abc')
        right = self._makeOne('def')
        self.assertEqual(compare(left, right), -1)
        self.assertEqual(compare(left, left), 0)
        self.assertEqual(compare(right, left), 1)

    def test___eq___no_args(self):
        ve = self._makeOne()
        self.assertNotEqual(ve, object())
        self.assertNotEqual(object(), ve)

    def test___eq___w_args(self):
        left = self._makeOne('abc')
        right = self._makeOne('def')
        self.assertNotEqual(left, right)
        self.assertNotEqual(right, left)
        self.assertEqual(left, left)
        self.assertEqual(right, right)


class TestOutOfBounds(unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._bootstrapinterfaces import OutOfBounds
        return OutOfBounds

    def test_TOO_LARGE_repr(self):
        self.assertIn('TOO_LARGE', repr(self._getTargetClass().TOO_LARGE))

    def test_TOO_SMALL_repr(self):
        self.assertIn('TOO_SMALL', repr(self._getTargetClass().TOO_SMALL))
