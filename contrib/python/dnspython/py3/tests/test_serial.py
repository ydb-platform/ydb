# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import unittest

import dns.serial

def S2(v):
    return dns.serial.Serial(v, bits=2)

def S8(v):
    return dns.serial.Serial(v, bits=8)

class SerialTestCase(unittest.TestCase):
    def test_rfc_1982_2_bit_cases(self):
        self.assertEqual(S2(0) + S2(1), S2(1))
        self.assertEqual(S2(1) + S2(1), S2(2))
        self.assertEqual(S2(2) + S2(1), S2(3))
        self.assertEqual(S2(3) + S2(1), S2(0))
        self.assertTrue(S2(1) > S2(0))
        self.assertTrue(S2(2) > S2(1))
        self.assertTrue(S2(3) > S2(2))
        self.assertTrue(S2(0) > S2(3))
        self.assertFalse(S2(2) > S2(0))
        self.assertFalse(S2(0) > S2(2))
        self.assertFalse(S2(2) < S2(0))
        self.assertFalse(S2(0) < S2(2))

    def test_rfc_1982_8_bit_cases(self):
        self.assertEqual(S8(255) + S8(1), S8(0))
        self.assertEqual(S8(100) + S8(100), S8(200))
        self.assertEqual(S8(200) + S8(100), S8(44))
        self.assertTrue(S8(1) > S8(0))
        self.assertTrue(S8(44) > S8(0))
        self.assertTrue(S8(100) > S8(0))
        self.assertTrue(S8(100) > S8(44))
        self.assertTrue(S8(200) > S8(100))
        self.assertTrue(S8(255) > S8(200))
        self.assertTrue(S8(0) > S8(255))
        self.assertTrue(S8(255) < S8(0))
        self.assertTrue(S8(100) > S8(255))
        self.assertTrue(S8(0) > S8(200))
        self.assertTrue(S8(44) > S8(200))
        self.assertFalse(S8(0) > S8(128))
        self.assertFalse(S8(128) > S8(0))
        self.assertFalse(S8(0) < S8(128))
        self.assertFalse(S8(128) < S8(0))
        self.assertFalse(S8(1) > S8(129))
        self.assertFalse(S8(129) > S8(1))

    def test_incremental_ops(self):
        v = S8(255)
        v += 1
        self.assertEqual(v, 0)
        v = S8(255)
        v += S8(1)
        self.assertEqual(v, 0)
        v = S8(0)
        v -= 1
        self.assertEqual(v, 255)
        v = S8(0)
        v -= S8(1)
        self.assertEqual(v, 255)

    def test_sub(self):
        self.assertEqual(S8(0) - S8(1), S8(255))

    def test_addition_bounds(self):
        self.assertRaises(ValueError, lambda: S8(0) + 128)
        self.assertRaises(ValueError, lambda: S8(0) - 128)
        def bad1():
            v = S8(0)
            v += 128
        self.assertRaises(ValueError, bad1)
        def bad2():
            v = S8(0)
            v -= 128
        self.assertRaises(ValueError, bad2)

    def test_casting(self):
        self.assertTrue(S8(0) == 0)
        self.assertTrue(S8(0) != 1)
        self.assertTrue(S8(0) < 1)
        self.assertTrue(S8(0) <= 1)
        self.assertTrue(S8(0) > 255)
        self.assertTrue(S8(0) >= 255)

    def test_uncastable(self):
        self.assertRaises(ValueError, lambda: S8(0) + 'a')
        self.assertRaises(ValueError, lambda: S8(0) - 'a')
        def bad1():
            v = S8(0)
            v += 'a'
        self.assertRaises(ValueError, bad1)
        def bad2():
            v = S8(0)
            v -= 'a'
        self.assertRaises(ValueError, bad2)

    def test_uncomparable(self):
        self.assertFalse(S8(0) == S2(0))
        self.assertFalse(S8(0) == 'a')
        self.assertTrue(S8(0) != 'a')
        self.assertRaises(TypeError, lambda: S8(0) < 'a')
        self.assertRaises(TypeError, lambda: S8(0) <= 'a')
        self.assertRaises(TypeError, lambda: S8(0) > 'a')
        self.assertRaises(TypeError, lambda: S8(0) >= 'a')

    def test_modulo(self):
        self.assertEqual(S8(-1), 255)
        self.assertEqual(S8(257), 1)

    def test_repr(self):
        self.assertEqual(repr(S8(1)), 'dns.serial.Serial(1, 8)')

    def test_not_equal(self):
        self.assertNotEqual(S8(0), S8(1))
        self.assertNotEqual(S8(0), S2(0))
