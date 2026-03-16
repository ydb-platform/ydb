#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This module tests demjson.py using unittest.

NOTE ON PYTHON 3: If running in Python 3, you must transform this test
                  script with "2to3" first.

"""

import sys, os, time
import unittest
import string
import unicodedata
import codecs
import collections
import datetime

# Force PYTHONPATH to head of sys.path, as the easy_install (egg files) will
# have rudely forced itself ahead of PYTHONPATH.

for pos, name in enumerate(os.environ.get('PYTHONPATH','').split(os.pathsep)):
    if os.path.isdir(name):
        sys.path.insert(pos, name)

import demjson

# ------------------------------
# Python version-specific stuff...

is_python3 = False
is_python27_plus = False
try:
    is_python3 = (sys.version_info.major >= 3)
    is_python27_plus = (sys.version_info.major > 2 or (sys.version_info.major==2 and sys.version_info.minor >= 7))
except AttributeError:
    is_python3 = (sys.version_info[0] >= 3)
    is_python27_plus = (sys.version_info[0] > 2 or (sys.version_info[0]==2 and sys.version_info[1] >= 7))

is_wide_python = (sys.maxunicode > 0xFFFF)

try:
    import decimal
except ImportError:
    decimal = None

# ====================

if hasattr(unittest, 'skipUnless'):
    def skipUnlessPython3(method):
        return unittest.skipUnless(method, is_python3)
    def skipUnlessPython27(method):
        return unittest.skipUnless(method, is_python27_plus)
    def skipUnlessWidePython(method):
        return unittest.skipUnless(method, is_wide_python)
else:
    # Python <= 2.6 does not have skip* decorators, so
    # just make a dummy decorator that always passes the
    # test method.
    def skipUnlessPython3(method):
        def always_pass(self):
            print "\nSKIPPING TEST %s: Requires Python 3" % method.__name__
            return True
        return always_pass
    def skipUnlessPython27(method):
        def always_pass(self):
            print "\nSKIPPING TEST %s: Requires Python 2.7 or greater" % method.__name__
            return True
        return always_pass
    def skipUnlessWidePython(method):
        def always_pass(self):
            print "\nSKIPPING TEST %s: Requires Python with wide Unicode support (maxunicode > U+FFFF)" % method.__name__
            return True
        return always_pass

## ------------------------------

def is_negzero( n ):
    if isinstance(n,float):
        return n == -0.0 and repr(n).startswith('-')
    elif decimal and isinstance(n,decimal.Decimal):
        return n.is_zero() and n.is_signed()
    else:
        return False

def is_nan( n ):
    if isinstance(n,float):
        return n.hex() == 'nan'
    elif decimal and isinstance(n,decimal.Decimal):
        return n.is_nan()
    else:
        return False

def is_infinity( n ):
    if isinstance(n,float):
        return n.hex() in ('inf', '-inf')
    elif decimal and isinstance(n,decimal.Decimal):
        return n.is_infinite()
    else:
        return False

## ------------------------------

def rawbytes( byte_list ):
    if is_python3:
        b = bytes( byte_list )
    else:
        b = ''.join(chr(n) for n in byte_list)
    return b

## ------------------------------

try:
    import UserDict
    dict_mixin = UserDict.DictMixin
except ImportError:
    # Python 3 has no UserDict. MutableMapping is close, but must
    # supply own __iter__() and __len__() methods.
    dict_mixin = collections.MutableMapping

# A class that behaves like a dict, but is not a subclass of dict
class LetterOrdDict(dict_mixin):
    def __init__(self, letters):
        self._letters = letters
    def __getitem__(self,key):
        try:
            if key in self._letters:
                return ord(key)
        except TypeError:
            raise KeyError('Key of wrong type: %r' % key)
        raise KeyError('No such key', key)
    def __setitem__(self,key): raise RuntimeError('read only object')
    def __delitem__(self,key): raise RuntimeError('read only object')
    def keys(self):
        return list(self._letters)
    def __len__(self):
        return len(self._letters)
    def __iter__(self):
        for v in self._letters:
            yield v

## ------------------------------

class rot_one(codecs.CodecInfo):
    """Dummy codec for ROT-1.

    Rotate by 1 character.  A->B, B->C, ..., Z->A

    """
    @staticmethod
    def lookup( name ):
        if name.lower() in ('rot1','rot-1'):
            return codecs.CodecInfo( rot_one.encode, rot_one.decode, name='rot-1' )
        return None
    @staticmethod
    def encode( s ):
        byte_list = []
        for i, c in enumerate(s):
            if 'A' <= c <= 'Y':
                byte_list.append( ord(c)+1 )
            elif c == 'Z':
                byte_list.append( ord('A') )
            elif ord(c) <= 0x7f:
                byte_list.append( ord(c) )
            else:
                raise UnicodeEncodeError('rot-1',s,i,i,"Can not encode code point U+%04X"%ord(c))
        return (rawbytes(byte_list), i+1)
    @staticmethod
    def decode( byte_list ):
        if is_python3:
            byte_values = byte_list
        else:
            byte_values = [ord(n) for n in byte_list]
        chars = []
        for i, b in enumerate(byte_values):
            if ord('B') <= b <= ord('Z'):
                chars.append( unichr(b-1) )
            elif b == ord('A'):
                chars.append( u'Z' )
            elif b <= 0x7fL:
                chars.append( unichr(b) )
            else:
                raise UnicodeDecodeError('rot-1',byte_list,i,i,"Can not decode byte value 0x%02x"%b)
        return (u''.join(chars), i+1)

## ------------------------------
class no_curly_braces(codecs.CodecInfo):
    """Degenerate codec that does not have curly braces.
    """
    @staticmethod
    def lookup( name ):
        if name.lower() in ('degenerate','degenerate'):
            return codecs.CodecInfo( no_curly_braces.encode, no_curly_braces.decode, name='degenerate' )
        return None
    @staticmethod
    def encode( s ):
        byte_list = []
        for i, c in enumerate(s):
            if c=='{' or c=='}':
                raise UnicodeEncodeError('degenerate',s,i,i,"Can not encode curly braces")
            elif ord(c) <= 0x7f:
                byte_list.append( ord(c) )
            else:
                raise UnicodeEncodeError('degenerate',s,i,i,"Can not encode code point U+%04X"%ord(c))
        return (rawbytes(byte_list), i+1)
    @staticmethod
    def decode( byte_list ):
        if is_python3:
            byte_values = byte_list
        else:
            byte_values = [ord(n) for n in byte_list]
        chars = []
        for i, b in enumerate(byte_values):
            if b > 0x7f or b == ord('{') or b == ord('}'):
                raise UnicodeDecodeError('degenerate',byte_list,i,i,"Can not decode byte value 0x%02x"%b)
            else:
                chars.append( unichr(b) )
        return (u''.join(chars), i+1)

## ------------------------------

if is_python3:
    def hexencode_bytes( bytelist ):
        return ''.join( ['%02x' % n for n in bytelist] )

## ============================================================

class DemjsonTest(unittest.TestCase):
    """This class contains test cases for demjson.

    """
    def testConstants(self):
        self.assertFalse( not isinstance(demjson.nan, float), "Missing nan constant" )
        self.assertFalse( not isinstance(demjson.inf, float), "Missing inf constant" )
        self.assertFalse( not isinstance(demjson.neginf, float), "Missing neginf constant" )
        self.assertFalse( not hasattr(demjson, 'undefined'), "Missing undefined constant" )

    def testDecodeKeywords(self):
        self.assertEqual(demjson.decode('true'), True)
        self.assertEqual(demjson.decode('false'), False)
        self.assertEqual(demjson.decode('null'), None)
        self.assertEqual(demjson.decode('undefined'), demjson.undefined)

    def testEncodeKeywords(self):
        self.assertEqual(demjson.encode(None), 'null')
        self.assertEqual(demjson.encode(True), 'true')
        self.assertEqual(demjson.encode(False), 'false')
        self.assertEqual(demjson.encode(demjson.undefined), 'undefined')

    def testDecodeNumber(self):
        self.assertEqual(demjson.decode('0'), 0)
        self.assertEqual(demjson.decode('12345'), 12345)
        self.assertEqual(demjson.decode('-12345'), -12345)
        self.assertEqual(demjson.decode('1e6'), 1000000)
        self.assertEqual(demjson.decode('1.5'), 1.5)
        self.assertEqual(demjson.decode('-1.5'), -1.5)
        self.assertEqual(demjson.decode('3e10'), 30000000000)
        self.assertEqual(demjson.decode('3E10'), 30000000000)
        self.assertEqual(demjson.decode('3e+10'), 30000000000)
        self.assertEqual(demjson.decode('3E+10'), 30000000000)
        self.assertEqual(demjson.decode('3E+00010'), 30000000000)
        self.assertEqual(demjson.decode('1000e-2'), 10)
        self.assertEqual(demjson.decode('1.2E+3'), 1200)
        self.assertEqual(demjson.decode('3.5e+8'), 350000000)
        self.assertEqual(demjson.decode('-3.5e+8'), -350000000)
        self.assertAlmostEqual(demjson.decode('1.23456e+078'), 1.23456e78)
        self.assertAlmostEqual(demjson.decode('1.23456e-078'), 1.23456e-78)
        self.assertAlmostEqual(demjson.decode('-1.23456e+078'), -1.23456e78)
        self.assertAlmostEqual(demjson.decode('-1.23456e-078'), -1.23456e-78)

    def testDecodeStrictNumber(self):
        """Make sure that strict mode is picky about numbers."""
        for badnum in ['+1', '.5', '1.', '01', '0x1', '1e']:
            try:
                self.assertRaises(demjson.JSONDecodeError, demjson.decode, badnum, strict=True, allow_any_type_at_start=True)
            except demjson.JSONDecodeError:
                pass

    def testDecodeHexNumbers(self):
        self.assertEqual(demjson.decode('0x0', allow_hex_numbers=True), 0)
        self.assertEqual(demjson.decode('0X0', allow_hex_numbers=True), 0)
        self.assertEqual(demjson.decode('0x0000', allow_hex_numbers=True), 0)
        self.assertEqual(demjson.decode('0x8', allow_hex_numbers=True), 8)
        self.assertEqual(demjson.decode('0x1f', allow_hex_numbers=True), 31)
        self.assertEqual(demjson.decode('0x1F', allow_hex_numbers=True), 31)
        self.assertEqual(demjson.decode('0xff', allow_hex_numbers=True), 255)
        self.assertEqual(demjson.decode('0xffff', allow_hex_numbers=True), 65535)
        self.assertEqual(demjson.decode('0xffffffff', allow_hex_numbers=True), 4294967295)
        self.assertEqual(demjson.decode('0x0F1a7Cb', allow_hex_numbers=True), 0xF1A7CB)
        self.assertEqual(demjson.decode('0X0F1a7Cb', allow_hex_numbers=True), 0xF1A7CB)
        self.assertEqual(demjson.decode('0x0000000000000000000000000000000000123', allow_hex_numbers=True), 0x123)
        self.assertEqual(demjson.decode('0x000000000000000000000000000000000012300', allow_hex_numbers=True), 0x12300)
        self.assertTrue( is_negzero(demjson.decode('-0x0', allow_hex_numbers=True)),
                         "Decoding negative zero hex numbers should give -0.0" )
        self.assertEqual(demjson.decode('-0x1', allow_hex_numbers=True), -1)
        self.assertEqual(demjson.decode('-0x000Fc854ab', allow_hex_numbers=True), -0xFC854AB)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0x', allow_hex_numbers=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0xG', allow_hex_numbers=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0x-3', allow_hex_numbers=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0x3G', allow_hex_numbers=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0x0x3', allow_hex_numbers=True)

    def testDecodeLargeIntegers(self):
        self.assertEqual(demjson.decode('9876543210123456789'), 9876543210123456789)
        self.assertEqual(demjson.decode('-9876543210123456789'), -9876543210123456789)
        self.assertEqual(demjson.decode('0xfedcba9876543210ABCDEF', allow_hex_numbers=True), 308109520888805757320678895)
        self.assertEqual(demjson.decode('-0xfedcba9876543210ABCDEF', allow_hex_numbers=True), -308109520888805757320678895)
        self.assertEqual(demjson.decode('0177334565141662503102052746757', allow_leading_zeros=True, leading_zero_radix=8),
                         308109520888805757320678895)
        self.assertEqual(demjson.decode('-0177334565141662503102052746757', allow_leading_zeros=True, leading_zero_radix=8),
                         -308109520888805757320678895)

    def testDecodeOctalNumbers(self):
        self.assertEqual(demjson.decode('017', allow_leading_zeros=True, leading_zero_radix=8), 15)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '018', allow_leading_zeros=True, leading_zero_radix=8)
        self.assertEqual(demjson.decode('00017', allow_leading_zeros=True, leading_zero_radix=8), 15)
        self.assertEqual(demjson.decode('-017', allow_leading_zeros=True, leading_zero_radix=8), -15)
        self.assertEqual(demjson.decode('00', allow_leading_zeros=True, leading_zero_radix=8), 0)
        self.assertTrue( is_negzero(demjson.decode('-00', allow_leading_zeros=True, leading_zero_radix=8)),
                         "Decoding negative zero octal number should give -0.0")

    def testDecodeNewOctalNumbers(self):
        self.assertEqual(demjson.decode('0o0'), 0)
        self.assertEqual(demjson.decode('0O0'), 0)
        self.assertEqual(demjson.decode('0o000'), 0)
        self.assertEqual(demjson.decode('0o1'), 1)
        self.assertEqual(demjson.decode('0o7'), 7)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0o18')
        self.assertEqual(demjson.decode('0o17'), 15)
        self.assertEqual(demjson.decode('-0o17'), -15)
        self.assertEqual(demjson.decode('0o4036517'), 1064271)
        self.assertEqual(demjson.decode('0O4036517'), 1064271)
        self.assertEqual(demjson.decode('0o000000000000000000000000000000000000000017'), 15)
        self.assertEqual(demjson.decode('0o00000000000000000000000000000000000000001700'), 960)
        self.assertTrue( is_negzero(demjson.decode('-0o0')),
                         "Decoding negative zero octal number should give -0.0")
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0o')
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0oA')
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0o-3')
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0o3A')
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0o0o3')

    def testDecodeBinaryNumbers(self):
        self.assertEqual(demjson.decode('0b0'), 0)
        self.assertEqual(demjson.decode('0B0'), 0)
        self.assertEqual(demjson.decode('0b000'), 0)
        self.assertEqual(demjson.decode('0b1'), 1)
        self.assertEqual(demjson.decode('0b01'), 1)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0b2')
        self.assertEqual(demjson.decode('0b1101'), 13)
        self.assertEqual(demjson.decode('-0b1101'), -13)
        self.assertEqual(demjson.decode('0b11010001101111100010101101011'), 439862635),
        self.assertEqual(demjson.decode('0B11010001101111100010101101011'), 439862635),
        self.assertEqual(demjson.decode('0b00000000000000000000000000000000000000001101'), 13)
        self.assertEqual(demjson.decode('0b0000000000000000000000000000000000000000110100'), 52)
        self.assertTrue( is_negzero(demjson.decode('-0b0')),
                         "Decoding negative zero binary number should give -0.0")
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0b')
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0bA')
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0b-1')
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0b1A')
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '0b0b1')

    def testDecodeNegativeZero(self):
        """Makes sure 0 and -0 are distinct.

        This is not a JSON requirement, but is required by ECMAscript.

        """
        self.assertEqual(demjson.decode('-0.0'), -0.0)
        self.assertEqual(demjson.decode('0.0'), 0.0)
        self.assertTrue(demjson.decode('0.0') is not demjson.decode('-0.0'),
                        'Numbers 0.0 and -0.0 are not distinct')
        self.assertTrue(demjson.decode('0') is not demjson.decode('-0'),
                        'Numbers 0 and -0 are not distinct')

    def testDecodeNaN(self):
        """Checks parsing of JavaScript NaN.
        """
        # Have to use is_nan(), since by definition nan != nan
        self.assertTrue( isinstance(demjson.decode('NaN', allow_non_numbers=True), float) )
        self.assertTrue( is_nan(demjson.decode('NaN', allow_non_numbers=True)) )
        self.assertTrue( is_nan(demjson.decode('+NaN', allow_non_numbers=True)) )
        self.assertTrue( is_nan(demjson.decode('-NaN', allow_non_numbers=True)) )
        if decimal:
            self.assertTrue( isinstance(demjson.decode('NaN', allow_non_numbers=True, float_type=demjson.NUMBER_DECIMAL), decimal.Decimal) )
            self.assertTrue( is_nan(demjson.decode('NaN', allow_non_numbers=True, float_type=demjson.NUMBER_DECIMAL)) )
            self.assertTrue( is_nan(demjson.decode('+NaN', allow_non_numbers=True, float_type=demjson.NUMBER_DECIMAL)) )
            self.assertTrue( is_nan(demjson.decode('-NaN', allow_non_numbers=True, float_type=demjson.NUMBER_DECIMAL)) )
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, 'NaN', allow_non_numbers=False)

    def testDecodeInfinite(self):
        """Checks parsing of JavaScript Infinite.
        """
        # Have to use is_nan(), since by definition nan != nan
        self.assertTrue( isinstance(demjson.decode('Infinity', allow_non_numbers=True), float) )
        self.assertTrue( is_infinity(demjson.decode('Infinity', allow_non_numbers=True)) )
        self.assertTrue( is_infinity(demjson.decode('+Infinity', allow_non_numbers=True)) )
        self.assertTrue( is_infinity(demjson.decode('-Infinity', allow_non_numbers=True)) )
        self.assertTrue( demjson.decode('-Infinity', allow_non_numbers=True) < 0 )
        if decimal:
            self.assertTrue( isinstance(demjson.decode('Infinity', allow_non_numbers=True, float_type=demjson.NUMBER_DECIMAL), decimal.Decimal) )
            self.assertTrue( is_infinity(demjson.decode('Infinity', allow_non_numbers=True, float_type=demjson.NUMBER_DECIMAL)) )
            self.assertTrue( is_infinity(demjson.decode('+Infinity', allow_non_numbers=True, float_type=demjson.NUMBER_DECIMAL)) )
            self.assertTrue( is_infinity(demjson.decode('-Infinity', allow_non_numbers=True, float_type=demjson.NUMBER_DECIMAL)) )
            self.assertTrue( demjson.decode('-Infinity', allow_non_numbers=True, float_type=demjson.NUMBER_DECIMAL).is_signed() )
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, 'Infinity', allow_non_numbers=False)

    def assertMatchesRegex(self, value, pattern, msg=None):
        import re
        r = re.compile( '^' + pattern + '$' )
        try:
            m = r.match( value )
        except TypeError:
            raise self.failureException, \
                  "can't compare non-string to regex: %r" % value
        if m is None:
            raise self.failureException, \
                  (msg or '%r !~ /%s/' %(value,pattern))

    def testEncodeNumber(self):
        self.assertEqual(demjson.encode(0), '0')
        self.assertEqual(demjson.encode(12345), '12345')
        self.assertEqual(demjson.encode(-12345), '-12345')
        # Floating point numbers must be "approximately" compared to
        # allow for slight changes due to rounding errors in the
        # least significant digits.
        self.assertMatchesRegex(demjson.encode(1.5),
                                r'1.(' \
                                r'(5(000+[0-9])?)' \
                                r'|' \
                                r'(4999(9+[0-9])?)' \
                                r')' )
        self.assertMatchesRegex(demjson.encode(-1.5),
                                r'-1.(' \
                                r'(5(000+[0-9])?)' \
                                r'|' \
                                r'(4999(9+[0-9])?)' \
                                r')' )
        self.assertMatchesRegex(demjson.encode(1.2300456e78),
                                r'1.230045(' \
                                r'(6(0+[0-9])?)' r'|' \
                                r'(59(9+[0-9])?)' \
                                r')[eE][+]0*78')
        self.assertMatchesRegex(demjson.encode(1.2300456e-78),
                                r'1.230045(' \
                                r'(6(0+[0-9])?)' r'|' \
                                r'(59(9+[0-9])?)' \
                                r')[eE][-]0*78')
        self.assertMatchesRegex(demjson.encode(-1.2300456e78),
                                r'-1.230045(' \
                                r'(6(0+[0-9])?)' r'|' \
                                r'(59(9+[0-9])?)' \
                                r')[eE][+]0*78')
        self.assertMatchesRegex(demjson.encode(-1.2300456e-78),
                                r'-1.230045(' \
                                r'(6(0+[0-9])?)' r'|' \
                                r'(59(9+[0-9])?)' \
                                r')[eE][-]0*78')
        self.assertMatchesRegex(demjson.encode(0.0000043), r'4.3[0[0-9]*]?[eE]-0*6$')
        self.assertMatchesRegex(demjson.encode(40000000000), r'(4[eE]+0*10)|(40000000000)$',
                     'Large integer not encoded properly')

    def testEncodeNegativeZero(self):
        self.assertTrue(demjson.encode(-0.0) in ['-0','-0.0'],
                        'Float -0.0 is not encoded as a negative zero')
        if decimal:
            self.assertTrue(demjson.encode( decimal.Decimal('-0') ) in ['-0','-0.0'],
                            'Decimal -0 is not encoded as a negative zero')

    def testJsonInt(self):
        self.assertTrue( isinstance( demjson.json_int(0), (int,long) ) )
        self.assertEqual(demjson.json_int(0), 0)
        self.assertEqual(demjson.json_int(555999), 555999)
        self.assertEqual(demjson.json_int(-555999), -555999)
        self.assertEqual(demjson.json_int(12131415161718191029282726), 12131415161718191029282726)
        self.assertEqual(demjson.json_int('123'), 123)
        self.assertEqual(demjson.json_int('+123'), 123)
        self.assertEqual(demjson.json_int('-123'), -123)
        self.assertEqual(demjson.json_int('123',8), 83)
        self.assertEqual(demjson.json_int('123',16), 291)
        self.assertEqual(demjson.json_int('110101',2), 53)
        self.assertEqual( 123, demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_DECIMAL))
        self.assertEqual( 123, demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_HEX))
        self.assertEqual( 123, demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_OCTAL))
        self.assertEqual( 123, demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_LEGACYOCTAL))
        self.assertEqual( 123, demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_BINARY))
        self.assertEqual(demjson.json_int(123), demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_DECIMAL))
        self.assertEqual(demjson.json_int(123), demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_HEX))
        self.assertEqual(demjson.json_int(123), demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_OCTAL))
        self.assertEqual(demjson.json_int(123), demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_LEGACYOCTAL))
        self.assertEqual(demjson.json_int(123), demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_BINARY))
        self.assertEqual(demjson.json_int(123).json_format(), '123' )
        self.assertEqual(demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_DECIMAL).json_format(), '123' )
        self.assertEqual(demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_HEX).json_format(), '0x7b' )
        self.assertEqual(demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_OCTAL).json_format(), '0o173' )
        self.assertEqual(demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_LEGACYOCTAL).json_format(), '0173' )
        self.assertEqual(demjson.json_int(0,number_format=demjson.NUMBER_FORMAT_LEGACYOCTAL).json_format(), '0' )
        self.assertEqual(demjson.json_int(123,number_format=demjson.NUMBER_FORMAT_BINARY).json_format(), '0b1111011' )

    def testEncodeDecimalIntegers(self):
        self.assertEqual(demjson.encode( demjson.json_int(0)), '0')
        self.assertEqual(demjson.encode( demjson.json_int(123)), '123')
        self.assertEqual(demjson.encode( demjson.json_int(-123)), '-123')
        self.assertEqual(demjson.encode( demjson.json_int(12345678901234567890888)), '12345678901234567890888')

    def testEncodeHexIntegers(self):
        self.assertEqual(demjson.encode( demjson.json_int(0x0,number_format=demjson.NUMBER_FORMAT_HEX)), '0x0')
        self.assertEqual(demjson.encode( demjson.json_int(0xff,number_format=demjson.NUMBER_FORMAT_HEX)), '0xff')
        self.assertEqual(demjson.encode( demjson.json_int(-0x7f,number_format=demjson.NUMBER_FORMAT_HEX)), '-0x7f')
        self.assertEqual(demjson.encode( demjson.json_int(0x123456789abcdef,number_format=demjson.NUMBER_FORMAT_HEX)), '0x123456789abcdef')

    def testEncodeOctalIntegers(self):
        self.assertEqual(demjson.encode( demjson.json_int(0,number_format=demjson.NUMBER_FORMAT_OCTAL)), '0o0')
        self.assertEqual(demjson.encode( demjson.json_int(359,number_format=demjson.NUMBER_FORMAT_OCTAL)), '0o547')
        self.assertEqual(demjson.encode( demjson.json_int(-359,number_format=demjson.NUMBER_FORMAT_OCTAL)), '-0o547')

    def testEncodeLegacyOctalIntegers(self):
        self.assertEqual(demjson.encode( demjson.json_int(0,number_format=demjson.NUMBER_FORMAT_LEGACYOCTAL)), '0')
        self.assertEqual(demjson.encode( demjson.json_int(1,number_format=demjson.NUMBER_FORMAT_LEGACYOCTAL)), '01')
        self.assertEqual(demjson.encode( demjson.json_int(359,number_format=demjson.NUMBER_FORMAT_LEGACYOCTAL)), '0547')
        self.assertEqual(demjson.encode( demjson.json_int(-359,number_format=demjson.NUMBER_FORMAT_LEGACYOCTAL)), '-0547')

    def testIntAsFloat(self):
        self.assertEqual(demjson.decode('[0,-5,600,0xFF]', int_as_float=True), [0.0,-5.0,600.0,255.0] )
        if decimal:
            self.assertEqual(demjson.decode('[0,-5,600,0xFF]', int_as_float=True, float_type=demjson.NUMBER_DECIMAL),
                             [decimal.Decimal('0.0'), decimal.Decimal('-5.0'), decimal.Decimal('600.0'), decimal.Decimal('255.0')] )
            self.assertEqual([type(x) for x in demjson.decode('[0,-5,600,0xFF]', int_as_float=True, float_type=demjson.NUMBER_DECIMAL)],
                             [decimal.Decimal, decimal.Decimal, decimal.Decimal, decimal.Decimal] )

    def testKeepFormat(self):
        self.assertEqual(demjson.encode(demjson.decode( '[3,03,0o3,0x3,0b11]', keep_format=True )), '[3,03,0o3,0x3,0b11]' )

    def testEncodeNaN(self):
        self.assertEqual(demjson.encode( demjson.nan ), 'NaN')
        self.assertEqual(demjson.encode( -demjson.nan ), 'NaN')
        if decimal:
            self.assertEqual(demjson.encode( decimal.Decimal('NaN') ), 'NaN')
            self.assertEqual(demjson.encode( decimal.Decimal('sNaN') ), 'NaN')

    def testEncodeInfinity(self):
        self.assertEqual(demjson.encode( demjson.inf ), 'Infinity')
        self.assertEqual(demjson.encode( -demjson.inf ), '-Infinity')
        self.assertEqual(demjson.encode( demjson.neginf ), '-Infinity')
        if decimal:
            self.assertEqual(demjson.encode( decimal.Decimal('Infinity') ), 'Infinity')
            self.assertEqual(demjson.encode( decimal.Decimal('-Infinity') ), '-Infinity')

    def testDecodeString(self):
        self.assertEqual(demjson.decode(r'""'), '')
        self.assertEqual(demjson.decode(r'"a"'), 'a')
        self.assertEqual(demjson.decode(r'"abc def"'), 'abc def')
        self.assertEqual(demjson.decode(r'"\n\t\\\"\b\r\f"'), '\n\t\\"\b\r\f')
        self.assertEqual(demjson.decode(r'"\abc def"'), 'abc def')

    def testEncodeString(self):
        self.assertEqual(demjson.encode(''), r'""')
        self.assertEqual(demjson.encode('a'), r'"a"')
        self.assertEqual(demjson.encode('abc def'), r'"abc def"')
        self.assertEqual(demjson.encode('\n'), r'"\n"')
        self.assertEqual(demjson.encode('\n\t\r\b\f'), r'"\n\t\r\b\f"')
        self.assertEqual(demjson.encode('\n'), r'"\n"')
        self.assertEqual(demjson.encode('"'), r'"\""')
        self.assertEqual(demjson.encode('\\'), '"\\\\"')

    def testDecodeStringWithNull(self):
        self.assertEqual(demjson.decode('"\x00"',warnings=False), '\0')
        self.assertEqual(demjson.decode('"a\x00b"',warnings=False), 'a\x00b')

    def testDecodeStringUnicodeEscape(self):
        self.assertEqual(demjson.decode(r'"\u0000"',warnings=False), '\0')
        self.assertEqual(demjson.decode(r'"\u0061"'), 'a')
        self.assertEqual(demjson.decode(r'"\u2012"'), u'\u2012')
        self.assertEqual(demjson.decode(r'"\u1eDc"'), u'\u1edc')
        self.assertEqual(demjson.decode(r'"\uffff"'), u'\uffff')
        self.assertEqual(demjson.decode(r'"\u00a012"'), u'\u00a0' + '12')
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, r'"\u041"', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, r'"\u041Z"', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, r'"\u"', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, r'"\uZ"', strict=True)

    def testEncodeStringUnicodeEscape(self):
        self.assertEqual(demjson.encode('\0', escape_unicode=True), r'"\u0000"')
        self.assertEqual(demjson.encode(u'\u00e0', escape_unicode=True), r'"\u00e0"')
        self.assertEqual(demjson.encode(u'\u2012', escape_unicode=True), r'"\u2012"')

    def testHtmlSafe(self):
        self.assertEqual(demjson.encode('<', html_safe=True), r'"\u003c"')
        self.assertEqual(demjson.encode('>', html_safe=True), r'"\u003e"')
        self.assertEqual(demjson.encode('&', html_safe=True), r'"\u0026"')
        self.assertEqual(demjson.encode('/', html_safe=True), r'"\/"')
        self.assertEqual(demjson.encode('a<b>c&d/e', html_safe=True), r'"a\u003cb\u003ec\u0026d\/e"')
        self.assertEqual(demjson.encode('a<b>c&d/e', html_safe=False), r'"a<b>c&d/e"')

    def testDecodeStringExtendedUnicodeEscape(self):
        self.assertEqual(demjson.decode(r'"\u{0041}"',allow_extended_unicode_escapes=True), u'A')
        self.assertEqual(demjson.decode(r'"\u{1aFe}"',allow_extended_unicode_escapes=True), u'\u1afe')
        self.assertEqual(demjson.decode(r'"\u{41}"',allow_extended_unicode_escapes=True), u'A')
        self.assertEqual(demjson.decode(r'"\u{1}"',allow_extended_unicode_escapes=True), u'\u0001')
        self.assertEqual(demjson.decode(r'"\u{00000000000041}"',allow_extended_unicode_escapes=True), u'A')
        self.assertEqual(demjson.decode(r'"\u{1000a}"',allow_extended_unicode_escapes=True), u'\U0001000a')
        self.assertEqual(demjson.decode(r'"\u{10ffff}"',allow_extended_unicode_escapes=True), u'\U0010FFFF')
        self.assertEqual(demjson.decode(r'"\u{0000010ffff}"',allow_extended_unicode_escapes=True), u'\U0010FFFF')
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, r'"\u{0041}"', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, r'"\u{110000}"', allow_extended_unicode_escapes=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, r'"\u{012g}"', allow_extended_unicode_escapes=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, r'"\u{ 0041}"', allow_extended_unicode_escapes=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, r'"\u{0041 }"', allow_extended_unicode_escapes=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, r'"\u{0041"', allow_extended_unicode_escapes=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, r'"\u{}"', allow_extended_unicode_escapes=True)

    def testAutoDetectEncodingWithCustomUTF32(self):
        old_use_custom = demjson.helpers.always_use_custom_codecs
        try:
            demjson.helpers.always_use_custom_codecs = True
            self.runTestAutoDetectEncoding()
        finally:
            demjson.helpers.always_use_custom_codecs = old_use_custom

    def testAutoDetectEncodingWithBuiltinUTF32(self):
        old_use_custom = demjson.helpers.always_use_custom_codecs
        try:
            demjson.helpers.always_use_custom_codecs = False
            self.runTestAutoDetectEncoding()
        finally:
            demjson.helpers.always_use_custom_codecs = old_use_custom

    def runTestAutoDetectEncoding(self):
        QT = ord('"')
        TAB = ord('\t')
        FOUR = ord('4')
        TWO  = ord('2')
        # Plain byte strings, without BOM
        self.assertEqual(demjson.decode( rawbytes([ 0, 0, 0, FOUR               ]) ), 4 )  # UTF-32BE
        self.assertEqual(demjson.decode( rawbytes([ 0, 0, 0, FOUR, 0, 0, 0, TWO ]) ), 42 )

        self.assertEqual(demjson.decode( rawbytes([ FOUR, 0, 0, 0               ]) ), 4 )  # UTF-32LE
        self.assertEqual(demjson.decode( rawbytes([ FOUR, 0, 0, 0, TWO, 0, 0, 0 ]) ), 42 )

        self.assertEqual(demjson.decode( rawbytes([ 0, FOUR, 0, TWO ]) ), 42 ) # UTF-16BE
        self.assertEqual(demjson.decode( rawbytes([ FOUR, 0, TWO, 0 ]) ), 42 ) # UTF-16LE

        self.assertEqual(demjson.decode( rawbytes([ 0, FOUR ]) ), 4 )  #UTF-16BE
        self.assertEqual(demjson.decode( rawbytes([ FOUR, 0 ]) ), 4 )  #UTF-16LE

        self.assertEqual(demjson.decode( rawbytes([ FOUR, TWO ]) ), 42 )  # UTF-8
        self.assertEqual(demjson.decode( rawbytes([ TAB, FOUR, TWO ]) ), 42 ) # UTF-8
        self.assertEqual(demjson.decode( rawbytes([ FOUR ]) ), 4 ) # UTF-8

        # With byte-order marks (BOM)
        #    UTF-32BE
        self.assertEqual(demjson.decode( rawbytes([ 0, 0, 0xFE, 0xFF, 0, 0, 0, FOUR ]) ), 4 )
        self.assertRaises(demjson.JSONDecodeError,
                          demjson.decode, rawbytes([ 0, 0, 0xFE, 0xFF, FOUR, 0, 0, 0 ]) )
        #    UTF-32LE
        self.assertEqual(demjson.decode( rawbytes([ 0xFF, 0xFE, 0, 0, FOUR, 0, 0, 0 ]) ), 4 )
        self.assertRaises(demjson.JSONDecodeError,
                          demjson.decode, rawbytes([ 0xFF, 0xFE, 0, 0, 0, 0, 0, FOUR ]) )
        #    UTF-16BE
        self.assertEqual(demjson.decode( rawbytes([ 0xFE, 0xFF, 0, FOUR ]) ), 4 )
        self.assertRaises(demjson.JSONDecodeError,
                          demjson.decode, rawbytes([ 0xFE, 0xFF, FOUR, 0 ]) )
        #    UTF-16LE
        self.assertEqual(demjson.decode( rawbytes([ 0xFF, 0xFE, FOUR, 0 ]) ), 4 )
        self.assertRaises(demjson.JSONDecodeError,
                          demjson.decode, rawbytes([ 0xFF, 0xFE, 0, FOUR ]) )
        
        # Invalid Unicode strings
        self.assertRaises(demjson.JSONDecodeError,
                          demjson.decode, rawbytes([ 0 ]) )
        self.assertRaises(demjson.JSONDecodeError,
                          demjson.decode, rawbytes([ TAB, FOUR, TWO, 0 ]) )
        self.assertRaises(demjson.JSONDecodeError,
                          demjson.decode, rawbytes([ FOUR, 0, 0 ]) )
        self.assertRaises(demjson.JSONDecodeError,
                          demjson.decode, rawbytes([ FOUR, 0, 0, TWO ]) )

    def testDecodeStringRawUnicode(self):
        QT = ord('"')
        self.assertEqual(demjson.decode(rawbytes([ QT,0xC3,0xA0,QT ]),
                                        encoding='utf-8'), u'\u00e0')
        self.assertEqual(demjson.decode(rawbytes([ QT,0,0,0, 0xE0,0,0,0, QT,0,0,0 ]),
                                        encoding='ucs4le'), u'\u00e0')
        self.assertEqual(demjson.decode(rawbytes([ 0,0,0,QT, 0,0,0,0xE0, 0,0,0,QT ]),
                                        encoding='ucs4be'), u'\u00e0')
        self.assertEqual(demjson.decode(rawbytes([ 0,0,0,QT, 0,0,0,0xE0, 0,0,0,QT ]),
                                        encoding='utf-32be'), u'\u00e0')
        self.assertEqual(demjson.decode(rawbytes([ 0,0,0xFE,0xFF, 0,0,0,QT, 0,0,0,0xE0, 0,0,0,QT ]),
                                        encoding='ucs4'), u'\u00e0')

    def testEncodeStringRawUnicode(self):
        QT = ord('"')
        self.assertEqual(demjson.encode(u'\u00e0', escape_unicode=False, encoding='utf-8'),
                         rawbytes([ QT, 0xC3, 0xA0, QT ]) )
        self.assertEqual(demjson.encode(u'\u00e0', escape_unicode=False, encoding='ucs4le'),
                         rawbytes([ QT,0,0,0, 0xE0,0,0,0, QT,0,0,0 ]) )
        self.assertEqual(demjson.encode(u'\u00e0', escape_unicode=False, encoding='ucs4be'),
                         rawbytes([ 0,0,0,QT, 0,0,0,0xE0, 0,0,0,QT ]) )
        self.assertEqual(demjson.encode(u'\u00e0', escape_unicode=False, encoding='utf-32be'),
                         rawbytes([ 0,0,0,QT, 0,0,0,0xE0, 0,0,0,QT ]) )
        self.assertTrue(demjson.encode(u'\u00e0', escape_unicode=False, encoding='ucs4')
                        in [rawbytes([ 0,0,0xFE,0xFF, 0,0,0,QT, 0,0,0,0xE0, 0,0,0,QT ]),
                            rawbytes([ 0xFF,0xFE,0,0, QT,0,0,0, 0xE0,0,0,0, QT,0,0,0 ]) ])

    def testEncodeStringWithSpecials(self):
        # Make sure that certain characters are always \u-encoded even if the
        # output encoding could have represented them in the raw.

        # Test U+001B escape - a control character
        self.assertEqual(demjson.encode(u'\u001B', escape_unicode=False, encoding='utf-8'),
                         rawbytes([ ord(c) for c in '"\\u001b"' ]) )
        # Test U+007F delete - a control character
        self.assertEqual(demjson.encode(u'\u007F', escape_unicode=False, encoding='utf-8'),
                         rawbytes([ ord(c) for c in '"\\u007f"' ]) )
        # Test U+00AD soft hyphen - a format control character
        self.assertEqual(demjson.encode(u'\u00AD', escape_unicode=False, encoding='utf-8'),
                         rawbytes([ ord(c) for c in '"\\u00ad"' ]) )
        # Test U+200F right-to-left mark
        self.assertEqual(demjson.encode(u'\u200F', escape_unicode=False, encoding='utf-8'),
                         rawbytes([ ord(c) for c in '"\\u200f"' ]) )
        # Test U+2028 line separator
        self.assertEqual(demjson.encode(u'\u2028', escape_unicode=False, encoding='utf-8'),
                         rawbytes([ ord(c) for c in '"\\u2028"' ]) )
        # Test U+2029 paragraph separator
        self.assertEqual(demjson.encode(u'\u2029', escape_unicode=False, encoding='utf-8'),
                         rawbytes([ ord(c) for c in '"\\u2029"' ]) )
        # Test U+E007F cancel tag
        self.assertEqual(demjson.encode(u'\U000E007F', escape_unicode=False, encoding='utf-8'),
                         rawbytes([ ord(c) for c in '"\\udb40\\udc7f"' ]) )

    def testDecodeSupplementalUnicode(self):
        import sys
        if sys.maxunicode > 65535:
            self.assertEqual(demjson.decode( rawbytes([ ord(c) for c in r'"\udbc8\udf45"' ]) ),
                             u'\U00102345')
            self.assertEqual(demjson.decode( rawbytes([ ord(c) for c in r'"\ud800\udc00"' ]) ),
                             u'\U00010000')
            self.assertEqual(demjson.decode( rawbytes([ ord(c) for c in r'"\udbff\udfff"' ]) ),
                             u'\U0010ffff')
        for bad_case in [r'"\ud801"', r'"\udc02"',
                         r'"\ud801\udbff"', r'"\ud801\ue000"',
                         r'"\ud801\u2345"']:
            try:
                self.assertRaises(demjson.JSONDecodeError,
                                  demjson.decode( rawbytes([ ord(c) for c in bad_case ]) ) )
            except demjson.JSONDecodeError:
                pass

    def testEncodeSupplementalUnicode(self):
        import sys
        if sys.maxunicode > 65535:
            self.assertEqual(demjson.encode(u'\U00010000',encoding='ascii'),
                             rawbytes([ ord(c) for c in r'"\ud800\udc00"' ]) )
            self.assertEqual(demjson.encode(u'\U00102345',encoding='ascii'),
                             rawbytes([ ord(c) for c in r'"\udbc8\udf45"' ]) )
            self.assertEqual(demjson.encode(u'\U0010ffff',encoding='ascii'),
                             rawbytes([ ord(c) for c in r'"\udbff\udfff"' ]) )

    def have_codec(self, name):
        import codecs
        try:
            i = codecs.lookup(name)
        except LookupError:
            return False
        else:
            return True

    def testDecodeWithWindows1252(self):
        have_cp1252 = self.have_codec('cp1252')
        if have_cp1252:
            # Use Windows-1252 code page. Note character 0x8c is U+0152, which
            # is different than ISO8859-1.
            d = rawbytes([ ord('"'), ord('a'), 0xe0, 0x8c, ord('"') ])
            self.assertEqual(demjson.decode( d, encoding='cp1252' ),
                             u"a\u00e0\u0152")

    def testDecodeWithEBCDIC(self):
        have_ebcdic = self.have_codec('ibm037')
        if have_ebcdic:
            # Try EBCDIC
            d = rawbytes([ 0x7f, 0xc1, 0xc0, 0x7c, 0xe0, 0xa4, 0xf0, 0xf1, 0xf5, 0xf2, 0x7f ])
            self.assertEqual(demjson.decode( d, encoding='ibm037' ),
                             u"A{@\u0152")

    def testDecodeWithISO8859_1(self):
        have_iso8859_1 = self.have_codec('iso8859-1')
        if have_iso8859_1:
            # Try ISO-8859-1
            d = rawbytes([ ord('"'), ord('a'), 0xe0, ord('\\'), ord('u'), ord('0'), ord('1'), ord('5'), ord('2'), ord('"') ])
            self.assertEqual(demjson.decode( d, encoding='iso8859-1' ),
                             u"a\u00e0\u0152")

    def testDecodeWithCustomCodec(self):
        # Try Rot-1
        ci = rot_one.lookup('rot-1')
        d = rawbytes([ ord('"'), ord('A'), ord('B'), ord('Y'), ord('Z'), ord(' '), ord('5'), ord('"') ])
        self.assertEqual(demjson.decode( d, encoding=ci ),
                         u"ZAXY 5")

    def testDecodeWithDegenerateCodec(self):
        ci = no_curly_braces.lookup('degenerate')

        d = rawbytes([ord(c) for c in '"abc"' ])
        self.assertEqual(demjson.decode( d, encoding=ci ),
                         u"abc")

        d = rawbytes([ord(c) for c in '{"abc":42}' ])
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, d, encoding=ci )

    def testEncodeWithWindows1252(self):
        have_cp1252 = self.have_codec('cp1252')
        if have_cp1252:
            s = u'a\u00e0\u0152'
            self.assertEqual(demjson.encode( s, encoding='cp1252' ),
                             rawbytes([ ord('"'), ord('a'), 0xe0, 0x8c, ord('"') ]) )

    def testEncodeWithEBCDIC(self):
        have_ebcdic = self.have_codec('ibm037')
        if have_ebcdic:
            s = u"A{@\u0152"
            self.assertEqual(demjson.encode( s, encoding='ibm037' ),
                             rawbytes([ 0x7f, 0xc1, 0xc0, 0x7c, 0xe0, 0xa4, 0xf0, 0xf1, 0xf5, 0xf2, 0x7f ]) )

    def testEncodeWithISO8859_1(self):
        have_iso8859_1 = self.have_codec('iso8859-1')
        if have_iso8859_1:
            s = u'a\u00e0\u0152'
            self.assertEqual(demjson.encode( s, encoding='iso8859-1' ),
                             rawbytes([ ord('"'), ord('a'), 0xe0, ord('\\'), ord('u'), ord('0'), ord('1'), ord('5'), ord('2'), ord('"') ]) )

    def testEncodeWithCustomCodec(self):
        # Try Rot-1
        ci = rot_one.lookup('rot-1')
        d = u"ABYZ 5"
        self.assertEqual(demjson.encode( d, encoding=ci ),
                         rawbytes([ ord('"'), ord('B'), ord('C'), ord('Z'), ord('A'), ord(' '), ord('5'), ord('"') ]) )

    def testEncodeWithDegenerateCodec(self):
        ci = no_curly_braces.lookup('degenerate')

        self.assertRaises(demjson.JSONEncodeError, demjson.encode, u'"abc"', encoding=ci )
        self.assertRaises(demjson.JSONEncodeError, demjson.encode, u'{"abc":42}', encoding=ci )


    def testDecodeArraySimple(self):
        self.assertEqual(demjson.decode('[]'), [])
        self.assertEqual(demjson.decode('[ ]'), [])
        self.assertEqual(demjson.decode('[ 42 ]'), [42])
        self.assertEqual(demjson.decode('[ 42 ,99 ]'), [42, 99])
        self.assertEqual(demjson.decode('[ 42, ,99 ]', strict=False), [42, demjson.undefined, 99])
        self.assertEqual(demjson.decode('[ "z" ]'), ['z'])
        self.assertEqual(demjson.decode('[ "z[a]" ]'), ['z[a]'])

    def testDecodeArrayBad(self):
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '[,]', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '[1,]', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '[,1]', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '[1,,2]', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '[1 2]', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '[[][]]', strict=True)

    def testDecodeArrayNested(self):
        self.assertEqual(demjson.decode('[[]]'), [[]])
        self.assertEqual(demjson.decode('[ [ ] ]'), [[]])
        self.assertEqual(demjson.decode('[[],[]]'), [[],[]])
        self.assertEqual(demjson.decode('[[42]]'), [[42]])
        self.assertEqual(demjson.decode('[[42,99]]'), [[42,99]])
        self.assertEqual(demjson.decode('[[42],33]'), [[42],33])
        self.assertEqual(demjson.decode('[[42,[],44],[77]]'), [[42,[],44],[77]])

    def testEncodeArraySimple(self):
        self.assertEqual(demjson.encode([]), '[]')
        self.assertEqual(demjson.encode([42]), '[42]')
        self.assertEqual(demjson.encode([42,99]), '[42,99]')
        self.assertEqual(demjson.encode([42,demjson.undefined,99],strict=False), '[42,undefined,99]')

    def testEncodeArrayNested(self):
        self.assertEqual(demjson.encode([[]]), '[[]]')
        self.assertEqual(demjson.encode([[42]]), '[[42]]')
        self.assertEqual(demjson.encode([[42, 99]]), '[[42,99]]')
        self.assertEqual(demjson.encode([[42], 33]), '[[42],33]')
        self.assertEqual(demjson.encode([[42, [], 44], [77]]), '[[42,[],44],[77]]')

    def testDecodeObjectSimple(self):
        self.assertEqual(demjson.decode('{}'), {})
        self.assertEqual(demjson.decode('{"":1}'), {'':1})
        self.assertEqual(demjson.decode('{"a":1}'), {'a':1})
        self.assertEqual(demjson.decode('{ "a" : 1}'), {'a':1})
        self.assertEqual(demjson.decode('{"a":1,"b":2}'), {'a':1,'b':2})
        self.assertEqual(demjson.decode(' { "a" : 1 , "b" : 2 } '), {'a':1,'b':2})

    def testDecodeObjectHarder(self):
        self.assertEqual(demjson.decode('{ "b" :\n2 , "a" : 1\t,"\\u0063"\n\t: 3 }'), {'a':1,'b':2,'c':3})
        self.assertEqual(demjson.decode('{"a":1,"b":2,"c{":3}'), {'a':1,'b':2,'c{':3})
        self.assertEqual(demjson.decode('{"a":1,"b":2,"d}":3}'), {'a':1,'b':2,'d}':3})
        self.assertEqual(demjson.decode('{"a:{":1,"b,":2,"d}":3}'), {'a:{':1,'b,':2,'d}':3})

    def testDecodeObjectWithDuplicates(self):
        self.assertEqual(demjson.decode('{"a":1,"a":2}'), {'a':2})
        self.assertEqual(demjson.decode('{"a":2,"a":1}'), {'a':1})
        self.assertEqual(demjson.decode('{"a":1,"b":99,"a":2,"b":42}'), {'a':2,'b':42})
        self.assertEqual(demjson.decode('{"a":1,"b":2}', prevent_duplicate_keys=True), {'a':1,'b':2})
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '{"a":1,"a":1}', prevent_duplicate_keys=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '{"a":1,"a":2}', prevent_duplicate_keys=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '{"b":9,"a":1,"c":42,"a":2}', prevent_duplicate_keys=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '{"a":1,"\u0061":1}', prevent_duplicate_keys=True)

    def testDecodeObjectBad(self):
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '{"a"}', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '{"a":}', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '{,"a":1}', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '{"a":1,}', strict=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '{["a","b"]:1}', strict=True)

    def testDecodeObjectNested(self):
        self.assertEqual(demjson.decode('{"a":{"b":2}}'), {'a':{'b':2}})
        self.assertEqual(demjson.decode('{"a":{"b":2,"c":"{}"}}'), {'a':{'b':2,'c':'{}'}})
        self.assertEqual(demjson.decode('{"a":{"b":2},"c":{"d":4}}'), \
                         {'a':{'b':2},'c':{'d':4}})

    def testEncodeObjectSimple(self):
        self.assertEqual(demjson.encode({}), '{}')
        self.assertEqual(demjson.encode({'':1}), '{"":1}')
        self.assertEqual(demjson.encode({'a':1}), '{"a":1}')
        self.assertEqual(demjson.encode({'a':1,'b':2}), '{"a":1,"b":2}')
        self.assertEqual(demjson.encode({'a':1,'c':3,'b':'xyz'}), '{"a":1,"b":"xyz","c":3}')

    def testEncodeObjectNested(self):
        self.assertEqual(demjson.encode({'a':{'b':{'c':99}}}), '{"a":{"b":{"c":99}}}')
        self.assertEqual(demjson.encode({'a':{'b':88},'c':99}), '{"a":{"b":88},"c":99}')

    def testEncodeBadObject(self):
        self.assertRaises(demjson.JSONEncodeError, demjson.encode, {1:True}, strict=True)
        self.assertRaises(demjson.JSONEncodeError, demjson.encode, {('a','b'):True}, strict=True)

    def testEncodeObjectDictLike(self):
        """Makes sure it can encode things which look like dictionarys but aren't.

        """
        letters = 'ABCDEFGHIJKL'
        mydict = LetterOrdDict( letters )
        self.assertEqual( demjson.encode(mydict),
                          '{' + ','.join(['"%s":%d'%(c,ord(c)) for c in letters]) + '}' )

    def testEncodeArrayLike(self):
        class LikeList(object):
            def __iter__(self):
                class i(object):
                    def __init__(self):
                        self.n = 0
                    def next(self):
                        self.n += 1
                        if self.n < 10:
                            return 2**self.n
                        raise StopIteration
                return i()
        mylist = LikeList()
        self.assertEqual(demjson.encode(mylist), \
                         '[2,4,8,16,32,64,128,256,512]' )

    def testEncodeStringLike(self):
        import UserString
        class LikeString(UserString.UserString):
            pass
        mystring = LikeString('hello')
        self.assertEqual(demjson.encode(mystring), '"hello"')
        mystring = LikeString(u'hi\u2012there')
        self.assertEqual(demjson.encode(mystring, escape_unicode=True, encoding='utf-8'),
                         rawbytes([ ord(c) for c in r'"hi\u2012there"' ]) )

    def testObjectNonstringKeys(self):
        self.assertEqual(demjson.decode('{55:55}',strict=False), {55:55})
        self.assertEqual(demjson.decode('{fiftyfive:55}',strict=False), {'fiftyfive':55})
        self.assertRaises(demjson.JSONDecodeError, demjson.decode,
                          '{fiftyfive:55}', strict=True)
        self.assertRaises(demjson.JSONEncodeError, demjson.encode,
                          {55:'fiftyfive'}, strict=True)
        self.assertEqual(demjson.encode({55:55}, strict=False), '{55:55}')

    def testDecodeWhitespace(self):
        self.assertEqual(demjson.decode(' []'), [])
        self.assertEqual(demjson.decode('[] '), [])
        self.assertEqual(demjson.decode(' [ ] '), [])
        self.assertEqual(demjson.decode('\n[]\n'), [])
        self.assertEqual(demjson.decode('\t\r \n[\n\t]\n'), [])
        # Form-feed is not a valid JSON whitespace char
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, '\x0c[]', strict=True)
        # No-break-space is not a valid JSON whitespace char
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, u'\u00a0[]', strict=True)

    def testDecodeInvalidStartingType(self):
        if False:
            # THESE TESTS NO LONGER APPLY WITH RFC 7158, WHICH SUPERSEDED RFC 4627
            self.assertRaises(demjson.JSONDecodeError, demjson.decode, '', strict=True)
            self.assertRaises(demjson.JSONDecodeError, demjson.decode, '1', strict=True)
            self.assertRaises(demjson.JSONDecodeError, demjson.decode, '1.5', strict=True)
            self.assertRaises(demjson.JSONDecodeError, demjson.decode, '"a"', strict=True)
            self.assertRaises(demjson.JSONDecodeError, demjson.decode, 'true', strict=True)
            self.assertRaises(demjson.JSONDecodeError, demjson.decode, 'null', strict=True)

    def testDecodeMixed(self):
        self.assertEqual(demjson.decode('[0.5,{"3e6":[true,"d{["]}]'), \
                         [0.5, {'3e6': [True, 'd{[']}] )

    def testEncodeMixed(self):
        self.assertEqual(demjson.encode([0.5, {'3e6': [True, 'd{[']}] ),
                         '[0.5,{"3e6":[true,"d{["]}]' )

    def testDecodeComments(self):
        self.assertEqual(demjson.decode('//hi\n42', allow_comments=True), 42)
        self.assertEqual(demjson.decode('/*hi*/42', allow_comments=True), 42)
        self.assertEqual(demjson.decode('/*hi//x\n*/42', allow_comments=True), 42)
        self.assertEqual(demjson.decode('"a/*xx*/z"', allow_comments=True), 'a/*xx*/z')
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, \
                          '4/*aa*/2', allow_comments=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, \
                          '//hi/*x\n*/42', allow_comments=True)
        self.assertRaises(demjson.JSONDecodeError, demjson.decode, \
                          '/*hi/*x*/42', allow_comments=True)

    def testNamedTuples(self):
        import collections
        point = collections.namedtuple('point',['x','y'])
        rgb = collections.namedtuple('RGB',['red','green','blue'])

        position = point( 7, 3 )
        orange = rgb( 255, 255, 0 )

        ispoint = lambda typ: isinstance(typ,point)
        iscolor = lambda typ: isinstance(typ,rgb)

        self.assertEqual(demjson.encode(position, encode_namedtuple_as_object=True, compactly=True),
                         '{"x":7,"y":3}' )
        self.assertEqual(demjson.encode(position, encode_namedtuple_as_object=False, compactly=True),
                         '[7,3]' )
        self.assertEqual(demjson.encode(orange, encode_namedtuple_as_object=ispoint, compactly=True),
                         '[255,255,0]' )
        self.assertEqual(demjson.encode(orange, encode_namedtuple_as_object=iscolor, compactly=True),
                         '{"blue":0,"green":255,"red":255}' )

    def testDecodeNumberHook(self):
        """Tests the 'decode_number' and 'decode_float' hooks."""
        def round_plus_one(s):
            return round(float(s)) + 1
        def negate(s):
            if s.startswith('-'):
                return float( s[1:] )
            else:
                return float( '-' + s )
        def xnonnum(s):
            if s=='NaN':
                return 'x-not-a-number'
            elif s=='Infinity' or s=='+Infinity':
                return 'x-infinity'
            elif s=='-Infinity':
                return 'x-neg-infinity'
            else:
                raise demjson.JSONSkipHook
        self.assertEqual(demjson.decode('[3.14,-2.7]'),
                         [3.14, -2.7] )
        self.assertEqual(demjson.decode('[3.14,-2.7]',decode_number=negate),
                         [-3.14, 2.7] )
        self.assertEqual(demjson.decode('[3.14,-2.7]',decode_number=round_plus_one),
                         [4.0, -2.0] )
        self.assertEqual(demjson.decode('[3.14,-2.7,8]',decode_float=negate),
                         [-3.14, 2.7, 8] )
        self.assertEqual(demjson.decode('[3.14,-2.7,8]',decode_float=negate,decode_number=round_plus_one),
                         [-3.14, 2.7, 9.0] )
        self.assertEqual(demjson.decode('[2,3.14,NaN,Infinity,+Infinity,-Infinity]',
                                        strict=False, decode_number=xnonnum),
                         [2, 3.14, 'x-not-a-number', 'x-infinity', 'x-infinity', 'x-neg-infinity'] )

    def testDecodeArrayHook(self):
        def reverse(arr):
            return list(reversed(arr))
        self.assertEqual(demjson.decode('[3, 8, 9, [1, 3, 5]]'),
                         [3, 8, 9, [1, 3, 5]] )
        self.assertEqual(demjson.decode('[3, 8, 9, [1, 3, 5]]', decode_array=reverse),
                         [[5, 3, 1], 9, 8, 3] )
        self.assertEqual(demjson.decode('[3, 8, 9, [1, 3, 5]]', decode_array=sum),
                         29 )

    def testDecodeObjectHook(self):
        def pairs(dct):
            return sorted(dct.items())
        self.assertEqual(demjson.decode('{"a":42, "b":{"c":99}}'),
                         {u'a': 42, u'b': {u'c': 99}} )
        self.assertEqual(demjson.decode('{"a":42, "b":{"c":99}}', decode_object=pairs),
                         [(u'a', 42), (u'b', [(u'c', 99)])] )

    def testDecodeStringHook(self):
        import string
        def s2num( s ):
            try:
                s = int(s)
            except ValueError:
                pass
            return s
        doc = '{"one":["two","three",{"four":"005"}]}'
        self.assertEqual(demjson.decode(doc),
                         {'one':['two','three',{'four':'005'}]} )
        self.assertEqual(demjson.decode(doc, decode_string=lambda s: s.capitalize()),
                         {'One':['Two','Three',{'Four':'005'}]} )
        self.assertEqual(demjson.decode(doc, decode_string=s2num),
                         {'one':['two','three',{'four':5}]} )


    def testEncodeDictKey(self):
        d1 = {42: "forty-two", "a":"Alpha"}
        d2 = {complex(0,42): "imaginary-forty-two", "a":"Alpha"}

        def make_key( k ):
            if isinstance(k,basestring):
                raise demjson.JSONSkipHook
            else:
                return repr(k)

        def make_key2( k ):
            if isinstance(k, (int,basestring)):
                raise demjson.JSONSkipHook
            else:
                return repr(k)

        self.assertRaises(demjson.JSONEncodeError, demjson.encode, \
                          d1, strict=True)
        self.assertEqual(demjson.encode(d1,strict=False,sort_keys=demjson.SORT_ALPHA),
                         '{42:"forty-two","a":"Alpha"}' )

        self.assertEqual(demjson.encode(d1, encode_dict_key=make_key),
                         '{"42":"forty-two","a":"Alpha"}' )
        self.assertEqual(demjson.encode(d1,strict=False, encode_dict_key=make_key2, sort_keys=demjson.SORT_ALPHA),
                         '{42:"forty-two","a":"Alpha"}' )

        self.assertRaises(demjson.JSONEncodeError, demjson.encode, \
                          d2, strict=True)
        self.assertEqual(demjson.encode(d2, encode_dict_key=make_key),
                         '{"%r":"imaginary-forty-two","a":"Alpha"}' % complex(0,42) )

    def testEncodeDict(self):
        def d2pairs( d ):
            return sorted( d.items() )
        def add_keys( d ):
            d['keys'] = list(sorted(d.keys()))
            return d

        d = {"a":42, "b":{"c":99,"d":7}}
        self.assertEqual(demjson.encode( d, encode_dict=d2pairs ),
                         '[["a",42],["b",[["c",99],["d",7]]]]' )
        self.assertEqual(demjson.encode( d, encode_dict=add_keys ),
                         '{"a":42,"b":{"c":99,"d":7,"keys":["c","d"]},"keys":["a","b"]}' )

    def testEncodeDictSorting(self):
        d = {'apple':1,'Ball':1,'cat':1,'dog1':1,'dog002':1,'dog10':1,'DOG03':1}
        self.assertEqual(demjson.encode( d, sort_keys=demjson.SORT_ALPHA ),
                         '{"Ball":1,"DOG03":1,"apple":1,"cat":1,"dog002":1,"dog1":1,"dog10":1}' )
        self.assertEqual(demjson.encode( d, sort_keys=demjson.SORT_ALPHA_CI ),
                         '{"apple":1,"Ball":1,"cat":1,"dog002":1,"DOG03":1,"dog1":1,"dog10":1}' )
        self.assertEqual(demjson.encode( d, sort_keys=demjson.SORT_SMART ),
                         '{"apple":1,"Ball":1,"cat":1,"dog1":1,"dog002":1,"DOG03":1,"dog10":1}' )

    @skipUnlessPython27
    def testEncodeDictPreserveSorting(self):
        import collections
        d = collections.OrderedDict()
        d['X'] = 42
        d['A'] = 99
        d['Z'] = 50
        self.assertEqual(demjson.encode( d, sort_keys=demjson.SORT_PRESERVE ),
                         '{"X":42,"A":99,"Z":50}')
        d['E'] = {'h':'H',"d":"D","b":"B"}
        d['C'] = 1
        self.assertEqual(demjson.encode( d, sort_keys=demjson.SORT_PRESERVE ),
                         '{"X":42,"A":99,"Z":50,"E":{"b":"B","d":"D","h":"H"},"C":1}')

    def testEncodeSequence(self):
        def list2hash( seq ):
            return dict([ (str(i),val) for i, val in enumerate(seq) ])

        d = [1,2,3,[4,5,6],7,8]
        self.assertEqual(demjson.encode( d, encode_sequence=reversed ),
                         '[8,7,[6,5,4],3,2,1]' )
        self.assertEqual(demjson.encode( d, encode_sequence=list2hash ),
                         '{"0":1,"1":2,"2":3,"3":{"0":4,"1":5,"2":6},"4":7,"5":8}' )

    @skipUnlessPython3
    def testEncodeBytes(self):
        no_bytes = bytes([])
        all_bytes = bytes( list(range(256)) )

        self.assertEqual(demjson.encode( no_bytes ),
                         '[]' )
        self.assertEqual(demjson.encode( all_bytes ),
                         '[' + ','.join([str(n) for n in all_bytes]) + ']' )

        self.assertEqual(demjson.encode( no_bytes, encode_bytes=hexencode_bytes ),
                         '""' )
        self.assertEqual(demjson.encode( all_bytes, encode_bytes=hexencode_bytes ),
                         '"' + hexencode_bytes(all_bytes) + '"' )


    def testEncodeValue(self):
        def enc_val( val ):
            if isinstance(val, complex):
                return {'real':val.real, 'imaginary':val.imag}
            elif isinstance(val, basestring):
                return val.upper()
            elif isinstance(val, datetime.date):
                return val.strftime("Year %Y Month %m Day %d")
            else:
                raise demjson.JSONSkipHook

        v = {'ten':10, 'number': complex(3, 7.25), 'asof': datetime.date(2014,1,17)}
        self.assertEqual(demjson.encode( v, encode_value=enc_val ),
                         u'{"ASOF":"YEAR 2014 MONTH 01 DAY 17","NUMBER":{"IMAGINARY":7.25,"REAL":3.0},"TEN":10}' )

    def testEncodeDefault(self):
        import datetime
        def dictkeys( d ):
            return "/".join( sorted([ str(k) for k in d.keys() ]) )
        def magic( d ):
            return complex( 1, len(d))
        class Anon(object):
            def __init__(self, val):
                self.v = val
            def __repr__(self):
                return "<ANON>"
        class Anon2(object):
            def __init__(self, val):
                self.v = val
        def encode_anon( obj ):
            if isinstance(obj,Anon):
                return obj.v
            raise demjson.JSONSkipHook

        vals = [ "abc", 123, Anon("Hello"), sys, {'a':42,'wow':True} ]

        self.assertEqual(demjson.encode( vals, encode_default=repr ),
                         u'["abc",123,"%s","%s",{"a":42,"wow":true}]' % ( repr(vals[2]), repr(vals[3])) )

        self.assertEqual(demjson.encode( vals, encode_default=repr, encode_dict=dictkeys ),
                         u'["abc",123,"%s","%s","a/wow"]' % ( repr(vals[2]), repr(vals[3])) )

        self.assertEqual(demjson.encode( vals, encode_default=repr, encode_dict=magic ),
                         u'["abc",123,"%s","%s","%s"]' % ( repr(vals[2]), repr(vals[3]), repr(magic(vals[4])) ) )


        self.assertRaises( demjson.JSONEncodeError, demjson.encode, Anon("Hello") )
        self.assertEqual( demjson.encode( Anon("Hello"), encode_default=encode_anon ), '"Hello"' )
        self.assertRaises( demjson.JSONEncodeError, demjson.encode, Anon2("Hello"), encode_default=encode_anon )

    def testEncodeDate(self):
        d = datetime.date(2014,01,04)
        self.assertEqual(demjson.encode( d ), '"2014-01-04"' )
        self.assertEqual(demjson.encode( d, date_format='%m/%d/%Y' ), '"01/04/2014"' )

    def testEncodeDatetime(self):
        d = datetime.datetime(2014,01,04,13,22,15)
        self.assertEqual(demjson.encode( d ), '"2014-01-04T13:22:15"' )
        self.assertEqual(demjson.encode( d, datetime_format='%m/%d/%Y %H hr %M min' ), '"01/04/2014 13 hr 22 min"' )

    def testEncodeTime(self):
        pass #!!!

    def testEncodeTimedelta(self):
        pass #!!!

    def testStopProcessing(self):
        def jack_in_the_box( obj ):
            if obj == 42 or obj == "42":
                raise demjson.JSONStopProcessing
            else:
                raise demjson.JSONSkipHook

        self.assertEqual(demjson.encode( [1,2,3], encode_value=jack_in_the_box), "[1,2,3]" )
        self.assertRaises( demjson.JSONEncodeError, demjson.encode, [1,2,42], encode_value=jack_in_the_box )

        self.assertEqual(demjson.decode( '[1,2,3]', decode_number=jack_in_the_box), [1,2,3] )


    def decode_stats(self, data, *args, **kwargs):
        """Runs demjson.decode() and returns the statistics object."""
        kwargs['return_stats'] = True
        res = demjson.decode( data, *args, **kwargs )
        if res:
            return res.stats
        return None

    def testStatsSimple(self):
        self.assertEqual( self.decode_stats( '1' ).num_ints, 1 )
        self.assertEqual( self.decode_stats( '3.14' ).num_floats, 1 )
        self.assertEqual( self.decode_stats( 'true' ).num_bools, 1 )
        self.assertEqual( self.decode_stats( 'false' ).num_bools, 1 )
        self.assertEqual( self.decode_stats( 'null' ).num_nulls, 1 )
        self.assertEqual( self.decode_stats( '"hello"' ).num_strings, 1 )
        self.assertEqual( self.decode_stats( '[]' ).num_arrays, 1 )
        self.assertEqual( self.decode_stats( '{}' ).num_objects, 1 )
        self.assertEqual( self.decode_stats( '1//HI' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '1/*HI*/' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( 'NaN' ).num_nans, 1 )
        self.assertEqual( self.decode_stats( 'Infinity' ).num_infinities, 1 )
        self.assertEqual( self.decode_stats( '-Infinity' ).num_infinities, 1 )
        self.assertEqual( self.decode_stats( 'undefined' ).num_undefineds, 1 )
        self.assertEqual( self.decode_stats( '[,1,2]' ).num_undefineds, 1 )
        self.assertEqual( self.decode_stats( '[1,,2]' ).num_undefineds, 1 )
        self.assertEqual( self.decode_stats( '[1,2,]' ).num_undefineds, 0 )
        self.assertEqual( self.decode_stats( '{hello:1}' ).num_identifiers, 1 )
        self.assertEqual( self.decode_stats( '[1,{"a":2},[{"b":{"c":[4,[[5]],6]}},7],8]' ).max_depth, 7 )

    def testStatsWhitespace(self):
        self.assertEqual( self.decode_stats( '1' ).num_excess_whitespace, 0 )
        self.assertEqual( self.decode_stats( ' 1' ).num_excess_whitespace, 1 )
        self.assertEqual( self.decode_stats( '1 ' ).num_excess_whitespace, 1 )
        self.assertEqual( self.decode_stats( ' 1 ' ).num_excess_whitespace, 2 )
        self.assertEqual( self.decode_stats( '[1, 2]' ).num_excess_whitespace, 1 )
        self.assertEqual( self.decode_stats( '[1  , 2]' ).num_excess_whitespace, 3 )
        self.assertEqual( self.decode_stats( '[  1, 2]' ).num_excess_whitespace, 3 )
        self.assertEqual( self.decode_stats( '[1, 2  ]' ).num_excess_whitespace, 3 )
        self.assertEqual( self.decode_stats( '{"a": 1}' ).num_excess_whitespace, 1 )
        self.assertEqual( self.decode_stats( '{"a"  : 1}' ).num_excess_whitespace, 3 )
        self.assertEqual( self.decode_stats( '{ "a": 1}' ).num_excess_whitespace, 2 )
        self.assertEqual( self.decode_stats( '{"a": 1  }' ).num_excess_whitespace, 3 )
        self.assertEqual( self.decode_stats( '{"a":1,"b":2}' ).num_excess_whitespace, 0 )
        self.assertEqual( self.decode_stats( '{"a":1,  "b":2}' ).num_excess_whitespace, 2 )
        self.assertEqual( self.decode_stats( '{"a":1  ,"b":2}' ).num_excess_whitespace, 2 )
        self.assertEqual( self.decode_stats( '{"a":1 ,  "b":2}' ).num_excess_whitespace, 3 )
        self.assertEqual( self.decode_stats( '\n[\t1 , \t2 ]\n' ).num_excess_whitespace, 7 )


    def testStatsArrays(self):
        self.assertEqual( self.decode_stats( '123' ).num_arrays, 0 )
        self.assertEqual( self.decode_stats( '[]' ).num_arrays, 1 )
        self.assertEqual( self.decode_stats( '[1,2]' ).num_arrays, 1 )
        self.assertEqual( self.decode_stats( '{"a":[1,2]}' ).num_arrays, 1 )
        self.assertEqual( self.decode_stats( '[1,[2],3]' ).num_arrays, 2 )
        self.assertEqual( self.decode_stats( '[[[1,[],[[]],4],[3]]]' ).num_arrays, 7 )
        self.assertEqual( self.decode_stats( '123' ).max_depth, 0 )
        self.assertEqual( self.decode_stats( '[123]' ).max_items_in_array, 1 )
        self.assertEqual( self.decode_stats( '[[[1,[],[[]],4],[3]]]' ).max_depth, 5 )
        self.assertEqual( self.decode_stats( '123' ).max_items_in_array, 0 )
        self.assertEqual( self.decode_stats( '[]' ).max_items_in_array, 0 )
        self.assertEqual( self.decode_stats( '[[[[[[[]]]]]]]' ).max_items_in_array, 1 )
        self.assertEqual( self.decode_stats( '[[[[],[[]]],[]]]' ).max_items_in_array, 2 )
        self.assertEqual( self.decode_stats( '[[[1,[],[[]],4],[3]]]' ).max_items_in_array, 4 )

    def testStatsObjects(self):
        self.assertEqual( self.decode_stats( '123' ).num_objects, 0 )
        self.assertEqual( self.decode_stats( '{}' ).num_objects, 1 )
        self.assertEqual( self.decode_stats( '{"a":1}' ).num_objects, 1 )
        self.assertEqual( self.decode_stats( '[{"a":1}]' ).num_objects, 1 )
        self.assertEqual( self.decode_stats( '{"a":1,"b":2}' ).num_objects, 1 )
        self.assertEqual( self.decode_stats( '{"a":{}}' ).num_objects, 2 )
        self.assertEqual( self.decode_stats( '{"a":{"b":null}}' ).num_objects, 2 )
        self.assertEqual( self.decode_stats( '{"a":{"b":{"c":false}},"d":{}}' ).num_objects, 4 )
        self.assertEqual( self.decode_stats( '123' ).max_depth, 0 )
        self.assertEqual( self.decode_stats( '{}' ).max_depth, 1 )
        self.assertEqual( self.decode_stats( '{"a":{"b":{"c":false}},"d":{}}' ).max_depth, 3 )
        self.assertEqual( self.decode_stats( '123' ).max_items_in_object, 0 )
        self.assertEqual( self.decode_stats( '{}' ).max_items_in_object, 0 )
        self.assertEqual( self.decode_stats( '{"a":1}' ).max_items_in_object, 1 )
        self.assertEqual( self.decode_stats( '{"a":1,"b":2}' ).max_items_in_object, 2 )
        self.assertEqual( self.decode_stats( '{"a":{"b":{"c":false}},"d":{}}' ).max_items_in_object, 2 )

    def testStatsIntegers(self):
        n8s = [0,1,127,-127,-128]  # -128..127
        n16s = [128,255,32767,-129,-32768]  # -32768..32767
        n32s = [32768,2147483647,-32769,-2147483648] # -2147483648..2147483647
        n64s = [2147483648,9223372036854775807,-2147483649,-9223372036854775808]# -9223372036854775808..9223372036854775807
        nxls = [9223372036854775808,-9223372036854775809,10**20,-10**20]
        allnums = []
        allnums.extend(n8s)
        allnums.extend(n16s)
        allnums.extend(n32s)
        allnums.extend(n64s)
        allnums.extend(nxls)
        alljson = '[' + ','.join([str(n) for n in allnums]) + ']'

        self.assertEqual( self.decode_stats( 'true' ).num_ints, 0 )
        self.assertEqual( self.decode_stats( '1' ).num_ints, 1 )
        self.assertEqual( self.decode_stats( '[1,2,"a",3]' ).num_ints, 3 )
        self.assertEqual( self.decode_stats( '[1,2,{"a":3}]' ).num_ints, 3 )
        self.assertEqual( self.decode_stats( alljson ).num_ints_8bit, len(n8s) )
        self.assertEqual( self.decode_stats( alljson ).num_ints_16bit, len(n16s) )
        self.assertEqual( self.decode_stats( alljson ).num_ints_32bit, len(n32s) )
        self.assertEqual( self.decode_stats( alljson ).num_ints_64bit, len(n64s) )
        self.assertEqual( self.decode_stats( alljson ).num_ints_long, len(nxls) )

        n53s = [-9007199254740992,-9007199254740991, 9007199254740991,9007199254740992]# -9007199254740991..9007199254740991
        self.assertEqual( self.decode_stats( repr(n53s).replace('L','') ).num_ints_53bit, 2 )

    def testStatsFloats(self):
        self.assertEqual( self.decode_stats( 'true' ).num_floats, 0 )
        self.assertEqual( self.decode_stats( '1' ).num_floats, 0 )
        self.assertEqual( self.decode_stats( '1.1' ).num_floats, 1 )
        self.assertEqual( self.decode_stats( '1e-8' ).num_floats, 1 )
        self.assertEqual( self.decode_stats( '[1.0,2.0,{"a":-3.0}]' ).num_floats, 3 )
        self.assertEqual( self.decode_stats( '0.0' ).num_negative_zero_floats, 0 )
        self.assertEqual( self.decode_stats( '-0.0' ).num_negative_zero_floats, 1 )
        self.assertEqual( self.decode_stats( '-0.0', float_type=demjson.NUMBER_DECIMAL ).num_negative_zero_floats, 1 )
        self.assertEqual( self.decode_stats( '-1.0e-500', float_type=demjson.NUMBER_FLOAT ).num_negative_zero_floats, 1 )
        self.assertEqual( self.decode_stats( '1.0e500', float_type=demjson.NUMBER_FLOAT ).num_infinities, 1 )
        self.assertEqual( self.decode_stats( '-1.0e500', float_type=demjson.NUMBER_FLOAT ).num_infinities, 1 )
        if decimal:
            self.assertEqual( self.decode_stats( '3.14e100' ).num_floats_decimal, 0 )
            self.assertEqual( self.decode_stats( '3.14e500' ).num_floats_decimal, 1 )
            self.assertEqual( self.decode_stats( '3.14e-500' ).num_floats_decimal, 1 )
            self.assertEqual( self.decode_stats( '3.14159265358979' ).num_floats_decimal, 0 )
            self.assertEqual( self.decode_stats( '3.141592653589793238462643383279502884197169399375105820974944592307816406286' ).num_floats_decimal, 1 )


    def testStatsStrings(self):
        self.assertEqual( self.decode_stats( 'true' ).num_strings, 0 )
        self.assertEqual( self.decode_stats( '""' ).num_strings, 1 )
        self.assertEqual( self.decode_stats( '"abc"' ).num_strings, 1 )
        self.assertEqual( self.decode_stats( '["a","b",null,{"c":"d","e":42}]' ).num_strings, 5 )
        self.assertEqual( self.decode_stats( '""' ).max_string_length, 0 )
        self.assertEqual( self.decode_stats( '""' ).total_string_length, 0 )
        self.assertEqual( self.decode_stats( '"abc"' ).max_string_length, 3 )
        self.assertEqual( self.decode_stats( '"abc"' ).total_string_length, 3 )
        self.assertEqual( self.decode_stats( r'"\u2020"' ).max_string_length, 1 )
        self.assertEqual( self.decode_stats( u'"\u2020"' ).max_string_length, 1 )
        self.assertEqual( self.decode_stats( u'"\U0010ffff"' ).max_string_length, (1 if is_wide_python else 2) )
        self.assertEqual( self.decode_stats( r'"\ud804\udc88"' ).max_string_length, (1 if is_wide_python else 2) )
        self.assertEqual( self.decode_stats( '["","abc","defghi"]' ).max_string_length, 6 )
        self.assertEqual( self.decode_stats( '["","abc","defghi"]' ).total_string_length, 9 )
        self.assertEqual( self.decode_stats( '""' ).min_codepoint, None )
        self.assertEqual( self.decode_stats( '""' ).max_codepoint, None )
        self.assertEqual( self.decode_stats( r'"\0"' ).min_codepoint, 0 )
        self.assertEqual( self.decode_stats( r'"\0"' ).max_codepoint, 0 )
        self.assertEqual( self.decode_stats( r'"\u0000"' ).min_codepoint, 0 )
        self.assertEqual( self.decode_stats( r'"\u0000"' ).max_codepoint, 0 )
        self.assertEqual( self.decode_stats( u'"\u0000"' ).min_codepoint, 0 )
        self.assertEqual( self.decode_stats( u'"\u0000"' ).max_codepoint, 0 )
        self.assertEqual( self.decode_stats( r'"\1"' ).min_codepoint, 1 )
        self.assertEqual( self.decode_stats( r'"\1"' ).max_codepoint, 1 )
        self.assertEqual( self.decode_stats( r'"\u0001"' ).min_codepoint, 1 )
        self.assertEqual( self.decode_stats( r'"\u0001"' ).max_codepoint, 1 )
        self.assertEqual( self.decode_stats( r'"\ud804\udc88"' ).min_codepoint, (69768 if is_wide_python else 0xd804) )
        self.assertEqual( self.decode_stats( r'"\ud804\udc88"' ).max_codepoint, (69768 if is_wide_python else 0xdc88) )
        self.assertEqual( self.decode_stats( r'"\u60ccABC\u0001"' ).min_codepoint, 1 )
        self.assertEqual( self.decode_stats( r'"\u60ccABC\u0001"' ).max_codepoint, 0x60cc )
        self.assertEqual( self.decode_stats( r'"\377"' ).min_codepoint, 255 )
        self.assertEqual( self.decode_stats( r'"\377"' ).max_codepoint, 255 )
        self.assertEqual( self.decode_stats( r'"\uffff"' ).min_codepoint, 0xffff )
        self.assertEqual( self.decode_stats( r'"\uffff"' ).max_codepoint, 0xffff )
        self.assertEqual( self.decode_stats( u'"\uffff"' ).min_codepoint, 0xffff )
        self.assertEqual( self.decode_stats( u'"\uffff"' ).max_codepoint, 0xffff )
        self.assertEqual( self.decode_stats( '["mnoapj","kzcde"]' ).min_codepoint, ord('a') )
        self.assertEqual( self.decode_stats( '["mnoapj","kzcde"]' ).max_codepoint, ord('z') )
        self.assertEqual( self.decode_stats( u'"\U0010ffff"' ).min_codepoint, (0x10ffff if is_wide_python else 0xdbff) )
        self.assertEqual( self.decode_stats( u'"\U0010ffff"' ).max_codepoint, (0x10ffff if is_wide_python else 0xdfff) )

    def testStatsComments(self):
        self.assertEqual( self.decode_stats( 'true' ).num_comments, 0 )
        self.assertEqual( self.decode_stats( '/**/true' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '/*hi*/true' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( 'true/*hi*/' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '/*hi*/true/*there*/' ).num_comments, 2 )
        self.assertEqual( self.decode_stats( 'true//' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( 'true//\n' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( 'true//hi' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( 'true//hi\n' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '//hi\ntrue' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '/**//**/true' ).num_comments, 2 )
        self.assertEqual( self.decode_stats( '/**/ /**/true' ).num_comments, 2 )
        self.assertEqual( self.decode_stats( 'true//hi/*there*/\n' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( 'true/*hi//there*/' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( 'true/*hi\nthere\nworld*/' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( 'true/*hi\n//there\nworld*/' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( 'true/*ab*cd*/' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '"abc/*HI*/xyz"' ).num_comments, 0 )
        self.assertEqual( self.decode_stats( '[1,2]' ).num_comments, 0 )
        self.assertEqual( self.decode_stats( '[/*hi*/1,2]' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '[1/*hi*/,2]' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '[1,/*hi*/2]' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '[1,2/*hi*/]' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '{"a":1,"b":2}' ).num_comments, 0 )
        self.assertEqual( self.decode_stats( '{/*hi*/"a":1,"b":2}' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '{"a"/*hi*/:1,"b":2}' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '{"a":/*hi*/1,"b":2}' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '{"a":1/*hi*/,"b":2}' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '{"a":1,/*hi*/"b":2}' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '{"a":1,"b"/*hi*/:2}' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '{"a":1,"b":/*hi*/2}' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '{"a":1,"b":2/*hi*/}' ).num_comments, 1 )
        self.assertEqual( self.decode_stats( '//\n[/*A*/1/**/,/*\n\n*/2/*C\n*///D\n]//' ).num_comments, 7 )

    def testStatsIdentifiers(self):
        self.assertEqual( self.decode_stats( 'true' ).num_identifiers, 0 )
        self.assertEqual( self.decode_stats( '{"a":2}' ).num_identifiers, 0 )
        self.assertEqual( self.decode_stats( '{a:2}' ).num_identifiers, 1 )
        self.assertEqual( self.decode_stats( '{a:2,xyz:4}' ).num_identifiers, 2 )


def run_all_tests():
    unicode_width = 'narrow' if sys.maxunicode<=0xFFFF else 'wide'
    print 'Running with demjson version %s, Python version %s with %s-Unicode' % (demjson.__version__, sys.version.split(' ',1)[0],unicode_width)
    if int( demjson.__version__.split('.',1)[0] ) < 2:
        print 'WARNING: TESTING AGAINST AN OLD VERSION!'
    unittest.main()
    
if __name__ == '__main__':
    run_all_tests()

# end file
