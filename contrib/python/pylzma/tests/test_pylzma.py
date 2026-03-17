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
import sys, random
try:
    from hashlib import md5
except ImportError:
    from md5 import new as md5
import pylzma
import unittest
from binascii import unhexlify
try:
    from io import BytesIO
except ImportError:
    from cStringIO import StringIO as BytesIO

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

class TestPyLZMA(unittest.TestCase):
    
    def setUp(self):
        self.plain = bytes('hello, this is a test string', 'ascii')
        self.plain_with_eos = unhexlify('5d0000800000341949ee8def8c6b64909b1386e370bebeb1b656f5736d653c127731a214ff7031c000')
        self.plain_without_eos = unhexlify('5d0000800000341949ee8def8c6b64909b1386e370bebeb1b656f5736d653c115edbe9')

    def test_version(self):
        self.assertNotEqual(pylzma.__version__, '')

    def test_compression_eos(self):
        # test compression with end of stream marker
        compressed = pylzma.compress(self.plain, eos=1)
        self.assertEqual(compressed, self.plain_with_eos)
    
    def test_compression_no_eos(self):
        # test compression without end of stream marker
        compressed = pylzma.compress(self.plain, eos=0)
        self.assertEqual(compressed, self.plain_without_eos)
        
    def test_decompression_eos(self):
        # test decompression with the end of stream marker
        decompressed = pylzma.decompress(self.plain_with_eos)
        self.assertEqual(decompressed, self.plain)
        
    def test_decompression_noeos(self):
        # test decompression without the end of stream marker
        decompressed = pylzma.decompress(self.plain_without_eos, maxlength=28)
        self.assertEqual(decompressed, self.plain)

    def test_compression_decompression_eos(self):
        # call compression and decompression on random data of various sizes
        for i in range(18):
            size = 1 << i
            original = generate_random(size)
            result = pylzma.decompress(pylzma.compress(original, eos=1))
            self.assertEqual(len(result), size)
            self.assertEqual(md5(original).hexdigest(), md5(result).hexdigest())

    def test_compression_decompression_noeos(self):
        # call compression and decompression on random data of various sizes
        for i in range(18):
            size = 1 << i
            original = generate_random(size)
            result = pylzma.decompress(pylzma.compress(original, eos=0), maxlength=size)
            self.assertEqual(md5(original).hexdigest(), md5(result).hexdigest())

    def test_multi(self):
        # call compression and decompression multiple times to detect memory leaks...
        for x in range(4):
            self.test_compression_decompression_eos()
            self.test_compression_decompression_noeos()

    def test_decompression_stream(self):
        # test decompression object in one steps
        decompress = pylzma.decompressobj()
        data = decompress.decompress(self.plain_with_eos)
        data += decompress.flush()
        self.assertEqual(data, self.plain)
    
    def test_decompression_stream_two(self):
        # test decompression in two steps
        decompress = pylzma.decompressobj()
        data = decompress.decompress(self.plain_with_eos[:10])
        data += decompress.decompress(self.plain_with_eos[10:])
        data += decompress.flush()
        self.assertEqual(data, self.plain)

    def test_decompression_stream_props(self):
        # test decompression with properties in separate step
        decompress = pylzma.decompressobj()
        data = decompress.decompress(self.plain_with_eos[:5])
        data += decompress.decompress(self.plain_with_eos[5:])
        data += decompress.flush()
        self.assertEqual(data, self.plain)

    def test_decompression_stream_reset(self):
        # test reset
        decompress = pylzma.decompressobj()
        data = decompress.decompress(self.plain_with_eos[:10])
        decompress.reset()
        data = decompress.decompress(self.plain_with_eos[:15])
        data += decompress.decompress(self.plain_with_eos[15:])
        data += decompress.flush()
        self.assertEqual(data, self.plain)

    def test_decompression_streaming(self):
        # test decompressing with one byte at a time...
        decompress = pylzma.decompressobj()
        infile = BytesIO(self.plain_with_eos)
        outfile = BytesIO()
        while 1:
            data = infile.read(1)
            if not data: break
            outfile.write(decompress.decompress(data, 1))
        outfile.write(decompress.flush())
        self.assertEqual(outfile.getvalue(), self.plain)

    def test_decompression_streaming_noeos(self):
        # test decompressing with one byte at a time...
        decompress = pylzma.decompressobj(maxlength=len(self.plain))
        infile = BytesIO(self.plain_without_eos)
        outfile = BytesIO()
        while 1:
            data = infile.read(1)
            if not data: break
            outfile.write(decompress.decompress(data, 1))
        outfile.write(decompress.flush())
        self.assertEqual(outfile.getvalue(), self.plain)

    def _test_compression_streaming(self):
        # test compressing with one byte at a time...
        # XXX: disabled as LZMA doesn't support streaming compression yet
        compress = pylzma.compressobj(eos=1)
        infile = BytesIO(self.plain)
        outfile = BytesIO()
        while 1:
            data = infile.read(1)
            if not data: break
            outfile.write(compress.compress(data, 1))
        outfile.write(compress.flush())
        check = pylzma.decompress(outfile.getvalue())
        self.assertEqual(check, self.plain)

    def test_compression_file(self):
        # test compressing from file-like object (C class)
        infile = BytesIO(self.plain)
        outfile = BytesIO()
        compress = pylzma.compressfile(infile, eos=1)
        while 1:
            data = compress.read(1)
            if not data: break
            outfile.write(data)
        check = pylzma.decompress(outfile.getvalue())
        self.assertEqual(check, self.plain)

    if sys.version_info[:2] < (3, 0):
        
        def test_compression_file_python(self):
            # test compressing from file-like object (Python class)
            from StringIO import StringIO as PyStringIO
            infile = PyStringIO(self.plain)
            outfile = PyStringIO()
            compress = pylzma.compressfile(infile, eos=1)
            while 1:
                data = compress.read(1)
                if not data: break
                outfile.write(data)
            check = pylzma.decompress(outfile.getvalue())
            self.assertEqual(check, self.plain)

    def test_compress_large_string(self):
        # decompress large block of repeating data, string version (bug reported by Christopher Perkins)
        data = bytes("asdf", 'ascii')*123456
        compressed = pylzma.compress(data)
        self.assertTrue(data == pylzma.decompress(compressed))

    def test_compress_large_stream(self):
        # decompress large block of repeating data, stream version (bug reported by Christopher Perkins)
        data = bytes("asdf", 'ascii')*123456
        decompress = pylzma.decompressobj()
        infile = BytesIO(pylzma.compress(data))
        outfile = BytesIO()
        while 1:
            tmp = infile.read(1)
            if not tmp: break
            outfile.write(decompress.decompress(tmp))
        outfile.write(decompress.flush())
        self.assertTrue(data == outfile.getvalue())

    def test_compress_large_stream_bigchunks(self):
        # decompress large block of repeating data, stream version with big chunks
        data = bytes("asdf", 'ascii')*123456
        decompress = pylzma.decompressobj()
        infile = BytesIO(pylzma.compress(data))
        outfile = BytesIO()
        while 1:
            tmp = infile.read(1024)
            if not tmp: break
            outfile.write(decompress.decompress(tmp))
        outfile.write(decompress.flush())
        self.assertTrue(data == outfile.getvalue())

    def test_bugzilla_13(self):
        # prevent regression of bugzilla #13
        if sys.version_info[:2] < (3, 0):
            fp = pylzma.compressfile('/tmp/test')
            self.assertTrue(isinstance(fp, pylzma.compressfile))
        else:
            self.assertRaises(TypeError, pylzma.compressfile, '/tmp/test')

    def test_github_10(self):
        # prevent regression of github #10
        self.assertRaises(ValueError, pylzma.compress, bytes("foo", 'ascii'), dictionary=100)

def suite():
    suite = unittest.TestSuite()

    test_cases = [
        TestPyLZMA,
    ]

    for tc in test_cases:
        suite.addTest(unittest.makeSuite(tc))

    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
