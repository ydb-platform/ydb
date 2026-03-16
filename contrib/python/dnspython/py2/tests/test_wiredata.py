# Copyright (C) 2016
# Author: Martin Basti <martin.basti@gmail.com>
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose with or without fee is hereby granted,
# provided that the above copyright notice and this permission notice
# appear in all copies.

import unittest

from dns.exception import FormError
from dns.wiredata import WireData


class WireDataSlicingTestCase(unittest.TestCase):

    def testSliceAll(self):
        """Get all data"""
        inst = WireData(b'0123456789')
        self.assertEqual(inst[:], WireData(b'0123456789'))

    def testSliceAllExplicitlyDefined(self):
        """Get all data"""
        inst = WireData(b'0123456789')
        self.assertEqual(inst[0:10], WireData(b'0123456789'))

    def testSliceLowerHalf(self):
        """Get lower half of data"""
        inst = WireData(b'0123456789')
        self.assertEqual(inst[:5], WireData(b'01234'))

    def testSliceLowerHalfWithNegativeIndex(self):
        """Get lower half of data"""
        inst = WireData(b'0123456789')
        self.assertEqual(inst[:-5], WireData(b'01234'))

    def testSliceUpperHalf(self):
        """Get upper half of data"""
        inst = WireData(b'0123456789')
        self.assertEqual(inst[5:], WireData(b'56789'))

    def testSliceMiddle(self):
        """Get data from middle"""
        inst = WireData(b'0123456789')
        self.assertEqual(inst[3:6], WireData(b'345'))

    def testSliceMiddleWithNegativeIndex(self):
        """Get data from middle"""
        inst = WireData(b'0123456789')
        self.assertEqual(inst[-6:-3], WireData(b'456'))

    def testSliceMiddleWithMixedIndex(self):
        """Get data from middle"""
        inst = WireData(b'0123456789')
        self.assertEqual(inst[-8:3], WireData(b'2'))
        self.assertEqual(inst[5:-3], WireData(b'56'))

    def testGetOne(self):
        """Get data one by one item"""
        data = b'0123456789'
        inst = WireData(data)
        for i, byte in enumerate(bytearray(data)):
            self.assertEqual(inst[i], byte)
        for i in range(-1, len(data) * -1, -1):
            self.assertEqual(inst[i], bytearray(data)[i])

    def testEmptySlice(self):
        """Test empty slice"""
        data = b'0123456789'
        inst = WireData(data)
        for i, byte in enumerate(data):
            self.assertEqual(inst[i:i], b'')
        for i in range(-1, len(data) * -1, -1):
            self.assertEqual(inst[i:i], b'')
        self.assertEqual(inst[-3:-6], b'')

    def testSliceStartOutOfLowerBorder(self):
        """Get data from out of lower border"""
        inst = WireData(b'0123456789')
        with self.assertRaises(FormError):
            inst[-11:]  # pylint: disable=pointless-statement

    def testSliceStopOutOfLowerBorder(self):
        """Get data from out of lower border"""
        inst = WireData(b'0123456789')
        with self.assertRaises(FormError):
            inst[:-11]  # pylint: disable=pointless-statement

    def testSliceBothOutOfLowerBorder(self):
        """Get data from out of lower border"""
        inst = WireData(b'0123456789')
        with self.assertRaises(FormError):
            inst[-12:-11]  # pylint: disable=pointless-statement

    def testSliceStartOutOfUpperBorder(self):
        """Get data from out of upper border"""
        inst = WireData(b'0123456789')
        with self.assertRaises(FormError):
            inst[11:]  # pylint: disable=pointless-statement

    def testSliceStopOutOfUpperBorder(self):
        """Get data from out of upper border"""
        inst = WireData(b'0123456789')
        with self.assertRaises(FormError):
            inst[:11]  # pylint: disable=pointless-statement

    def testSliceBothOutOfUpperBorder(self):
        """Get data from out of lower border"""
        inst = WireData(b'0123456789')
        with self.assertRaises(FormError):
            inst[10:20]  # pylint: disable=pointless-statement

    def testGetOneOutOfLowerBorder(self):
        """Get item outside of range"""
        inst = WireData(b'0123456789')
        with self.assertRaises(FormError):
            inst[-11]  # pylint: disable=pointless-statement

    def testGetOneOutOfUpperBorder(self):
        """Get item outside of range"""
        inst = WireData(b'0123456789')
        with self.assertRaises(FormError):
            inst[10]  # pylint: disable=pointless-statement
