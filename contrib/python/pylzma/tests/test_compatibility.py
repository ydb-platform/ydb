#!/usr/bin/python -u
#
# Python Bindings for LZMA
#
# Copyright (c) 2004-2015 by Joachim Bauch, mail@joachim-bauch.de
# 7-Zip Copyright (C) 1999-2010 Igor Pavlov
# LZMA SDK Copyright (C) 1999-2010 Igor Pavlov
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
# $Id$
#
try:
    from hashlib import md5
except ImportError:
    from md5 import new as md5
import random
import sys
import pylzma
import unittest
from binascii import unhexlify

if not hasattr(pylzma, 'decompress_compat'):
    raise ImportError('no compatibility support available')

if sys.version_info[:2] < (3, 0):
    def bytes(s, encoding):
        return s

ALL_CHARS = [bytes(chr(x), 'ascii') for x in range(128)]

# cache random strings to speed up tests
_random_strings = {}
def generate_random(size, choice=random.choice, ALL_CHARS=ALL_CHARS):
    global _random_strings
    s = _random_strings.get(size, None)
    if s is None:
        s = _random_strings[size] = bytes('', 'ascii').join([choice(ALL_CHARS) for x in range(size)])
    return s

class TestPyLZMACompability(unittest.TestCase):
    
    def setUp(self):
        self.plain = bytes('hello, this is a test string', 'ascii')
        self.plain_with_eos = unhexlify('5d0000800000341949ee8def8c6b64909b1386e370bebeb1b656f5736d653c127731a214ff7031c000')
        self.plain_without_eos = unhexlify('5d0000800000341949ee8def8c6b64909b1386e370bebeb1b656f5736d653c115edbe9')
        
    def test_decompression_noeos(self):
        # test decompression without the end of stream marker
        decompressed = pylzma.decompress_compat(self.plain_without_eos)
        self.assertEqual(decompressed, self.plain)

    def test_compression_decompression_noeos(self):
        # call compression and decompression on random data of various sizes
        for i in range(18):
            size = 1 << i
            original = generate_random(size)
            result = pylzma.decompress_compat(pylzma.compress(original, eos=0))[:size]
            self.assertEqual(md5(original).hexdigest(), md5(result).hexdigest())

    def test_multi(self):
        # call compression and decompression multiple times to detect memory leaks...
        for x in range(4):
            self.test_compression_decompression_noeos()

def suite():
    suite = unittest.TestSuite()

    test_cases = [
        TestPyLZMACompability,
    ]

    for tc in test_cases:
        suite.addTest(unittest.makeSuite(tc))

    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
