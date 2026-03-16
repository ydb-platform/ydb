# -*- coding: utf-8
# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

# Copyright (C) 2003-2017 Nominum, Inc.
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose with or without fee is hereby granted,
# provided that the above copyright notice and this permission notice
# appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND NOMINUM DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL NOMINUM BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
# OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

from typing import Dict # pylint: disable=unused-import
import copy
import operator
import pickle
import unittest

from io import BytesIO

import dns.name
import dns.reversename
import dns.e164

# pylint: disable=line-too-long,unsupported-assignment-operation


class NameTestCase(unittest.TestCase):
    def setUp(self):
        self.origin = dns.name.from_text('example.')

    def testFromTextRel1(self):
        n = dns.name.from_text('foo.bar')
        self.assertEqual(n.labels, (b'foo', b'bar', b''))

    def testFromTextRel2(self):
        n = dns.name.from_text('foo.bar', origin=self.origin)
        self.assertEqual(n.labels, (b'foo', b'bar', b'example', b''))

    def testFromTextRel3(self):
        n = dns.name.from_text('foo.bar', origin=None)
        self.assertEqual(n.labels, (b'foo', b'bar'))

    def testFromTextRel4(self):
        n = dns.name.from_text('@', origin=None)
        self.assertEqual(n, dns.name.empty)

    def testFromTextRel5(self):
        n = dns.name.from_text('@', origin=self.origin)
        self.assertEqual(n, self.origin)

    def testFromTextAbs1(self):
        n = dns.name.from_text('foo.bar.')
        self.assertEqual(n.labels, (b'foo', b'bar', b''))

    def testTortureFromText(self):
        good = [
            br'.',
            br'a',
            br'a.',
            br'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            br'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            br'\000.\008.\010.\032.\046.\092.\099.\255',
            br'\\',
            br'\..\.',
            br'\\.\\',
            br'!"#%&/()=+-',
            br'\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255.\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255.\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255.\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255',
            ]
        bad = [
            br'..',
            br'.a',
            br'\\..',
            b'\\',		# yes, we don't want the 'r' prefix!
            br'\0',
            br'\00',
            br'\00Z',
            br'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            br'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            br'\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255.\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255.\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255.\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255',
            ]
        for t in good:
            try:
                dns.name.from_text(t)
            except Exception:
                self.fail("good test '%s' raised an exception" % t)
        for t in bad:
            caught = False
            try:
                dns.name.from_text(t)
            except Exception:
                caught = True
            if not caught:
                self.fail("bad test '%s' did not raise an exception" % t)

    def testImmutable1(self):
        def bad():
            self.origin.labels = ()
        self.assertRaises(TypeError, bad)

    def testImmutable2(self):
        def bad():
            self.origin.labels[0] = 'foo'
        self.assertRaises(TypeError, bad)

    def testAbs1(self):
        self.assertTrue(dns.name.root.is_absolute())

    def testAbs2(self):
        self.assertFalse(dns.name.empty.is_absolute())

    def testAbs3(self):
        self.assertTrue(self.origin.is_absolute())

    def testAbs4(self):
        n = dns.name.from_text('foo', origin=None)
        self.assertFalse(n.is_absolute())

    def testWild1(self):
        n = dns.name.from_text('*.foo', origin=None)
        self.assertTrue(n.is_wild())

    def testWild2(self):
        n = dns.name.from_text('*a.foo', origin=None)
        self.assertFalse(n.is_wild())

    def testWild3(self):
        n = dns.name.from_text('a.*.foo', origin=None)
        self.assertFalse(n.is_wild())

    def testWild4(self):
        self.assertFalse(dns.name.root.is_wild())

    def testWild5(self):
        self.assertFalse(dns.name.empty.is_wild())

    def testHash1(self):
        n1 = dns.name.from_text('fOo.COM')
        n2 = dns.name.from_text('foo.com')
        self.assertEqual(hash(n1), hash(n2))

    def testCompare1(self):
        n1 = dns.name.from_text('a')
        n2 = dns.name.from_text('b')
        self.assertLess(n1, n2)
        self.assertLessEqual(n1, n2)
        self.assertGreater(n2, n1)
        self.assertGreaterEqual(n2, n1)

    def testCompare2(self):
        n1 = dns.name.from_text('')
        n2 = dns.name.from_text('b')
        self.assertLess(n1, n2)
        self.assertLessEqual(n1, n2)
        self.assertGreater(n2, n1)
        self.assertGreaterEqual(n2, n1)

    def testCompare3(self):
        self.assertLess(dns.name.empty, dns.name.root)
        self.assertGreater(dns.name.root, dns.name.empty)

    def testCompare4(self):
        self.assertNotEqual(dns.name.root, 1)

    def testSubdomain1(self):
        self.assertFalse(dns.name.empty.is_subdomain(dns.name.root))

    def testSubdomain2(self):
        self.assertFalse(dns.name.root.is_subdomain(dns.name.empty))

    def testSubdomain3(self):
        n = dns.name.from_text('foo', origin=self.origin)
        self.assertTrue(n.is_subdomain(self.origin))

    def testSubdomain4(self):
        n = dns.name.from_text('foo', origin=self.origin)
        self.assertTrue(n.is_subdomain(dns.name.root))

    def testSubdomain5(self):
        n = dns.name.from_text('foo', origin=self.origin)
        self.assertTrue(n.is_subdomain(n))

    def testSuperdomain1(self):
        self.assertFalse(dns.name.empty.is_superdomain(dns.name.root))

    def testSuperdomain2(self):
        self.assertFalse(dns.name.root.is_superdomain(dns.name.empty))

    def testSuperdomain3(self):
        n = dns.name.from_text('foo', origin=self.origin)
        self.assertTrue(self.origin.is_superdomain(n))

    def testSuperdomain4(self):
        n = dns.name.from_text('foo', origin=self.origin)
        self.assertTrue(dns.name.root.is_superdomain(n))

    def testSuperdomain5(self):
        n = dns.name.from_text('foo', origin=self.origin)
        self.assertTrue(n.is_superdomain(n))

    def testCanonicalize1(self):
        n = dns.name.from_text('FOO.bar', origin=self.origin)
        c = n.canonicalize()
        self.assertEqual(c.labels, (b'foo', b'bar', b'example', b''))

    def testToText1(self):
        n = dns.name.from_text('FOO.bar', origin=self.origin)
        t = n.to_text()
        self.assertEqual(t, 'FOO.bar.example.')

    def testToText2(self):
        n = dns.name.from_text('FOO.bar', origin=self.origin)
        t = n.to_text(True)
        self.assertEqual(t, 'FOO.bar.example')

    def testToText3(self):
        n = dns.name.from_text('FOO.bar', origin=None)
        t = n.to_text()
        self.assertEqual(t, 'FOO.bar')

    def testToText4(self):
        t = dns.name.empty.to_text()
        self.assertEqual(t, '@')

    def testToText5(self):
        t = dns.name.root.to_text()
        self.assertEqual(t, '.')

    def testToText6(self):
        n = dns.name.from_text('FOO bar', origin=None)
        t = n.to_text()
        self.assertEqual(t, r'FOO\032bar')

    def testToText7(self):
        n = dns.name.from_text(r'FOO\.bar', origin=None)
        t = n.to_text()
        self.assertEqual(t, r'FOO\.bar')

    def testToText8(self):
        n = dns.name.from_text(r'\070OO\.bar', origin=None)
        t = n.to_text()
        self.assertEqual(t, r'FOO\.bar')

    def testToText9(self):
        n = dns.name.from_text('FOO bar', origin=None)
        t = n.to_unicode()
        self.assertEqual(t, 'FOO\\032bar')

    def testToText10(self):
        t = dns.name.empty.to_unicode()
        self.assertEqual(t, '@')

    def testToText11(self):
        t = dns.name.root.to_unicode()
        self.assertEqual(t, '.')

    def testToText12(self):
        n = dns.name.from_text(r'a\.b.c')
        t = n.to_unicode()
        self.assertEqual(t, r'a\.b.c.')

    def testToText13(self):
        n = dns.name.from_text(r'\150\151\152\153\154\155\156\157\158\159.')
        t = n.to_text()
        self.assertEqual(t, r'\150\151\152\153\154\155\156\157\158\159.')

    def testToText14(self):
        # Something that didn't start as unicode should go to escapes and not
        # raise due to interpreting arbitrary binary DNS labels as UTF-8.
        n = dns.name.from_text(r'\150\151\152\153\154\155\156\157\158\159.')
        t = n.to_unicode()
        self.assertEqual(t, r'\150\151\152\153\154\155\156\157\158\159.')

    def testSlice1(self):
        n = dns.name.from_text(r'a.b.c.', origin=None)
        s = n[:]
        self.assertEqual(s, (b'a', b'b', b'c', b''))

    def testSlice2(self):
        n = dns.name.from_text(r'a.b.c.', origin=None)
        s = n[:2]
        self.assertEqual(s, (b'a', b'b'))

    def testSlice3(self):
        n = dns.name.from_text(r'a.b.c.', origin=None)
        s = n[2:]
        self.assertEqual(s, (b'c', b''))

    def testEmptyLabel1(self):
        def bad():
            dns.name.Name(['a', '', 'b'])
        self.assertRaises(dns.name.EmptyLabel, bad)

    def testEmptyLabel2(self):
        def bad():
            dns.name.Name(['', 'b'])
        self.assertRaises(dns.name.EmptyLabel, bad)

    def testEmptyLabel3(self):
        n = dns.name.Name(['b', ''])
        self.assertTrue(n)

    def testLongLabel(self):
        n = dns.name.Name(['a' * 63])
        self.assertTrue(n)

    def testLabelTooLong(self):
        def bad():
            dns.name.Name(['a' * 64, 'b'])
        self.assertRaises(dns.name.LabelTooLong, bad)

    def testLongName(self):
        n = dns.name.Name(['a' * 63, 'a' * 63, 'a' * 63, 'a' * 62])
        self.assertTrue(n)

    def testNameTooLong(self):
        def bad():
            dns.name.Name(['a' * 63, 'a' * 63, 'a' * 63, 'a' * 63])
        self.assertRaises(dns.name.NameTooLong, bad)

    def testConcat1(self):
        n1 = dns.name.Name(['a', 'b'])
        n2 = dns.name.Name(['c', 'd'])
        e = dns.name.Name(['a', 'b', 'c', 'd'])
        r = n1 + n2
        self.assertEqual(r, e)

    def testConcat2(self):
        n1 = dns.name.Name(['a', 'b'])
        n2 = dns.name.Name([])
        e = dns.name.Name(['a', 'b'])
        r = n1 + n2
        self.assertEqual(r, e)

    def testConcat3(self):
        n1 = dns.name.Name([])
        n2 = dns.name.Name(['a', 'b'])
        e = dns.name.Name(['a', 'b'])
        r = n1 + n2
        self.assertEqual(r, e)

    def testConcat4(self):
        n1 = dns.name.Name(['a', 'b', ''])
        n2 = dns.name.Name([])
        e = dns.name.Name(['a', 'b', ''])
        r = n1 + n2
        self.assertEqual(r, e)

    def testConcat5(self):
        n1 = dns.name.Name(['a', 'b'])
        n2 = dns.name.Name(['c', ''])
        e = dns.name.Name(['a', 'b', 'c', ''])
        r = n1 + n2
        self.assertEqual(r, e)

    def testConcat6(self):
        def bad():
            n1 = dns.name.Name(['a', 'b', ''])
            n2 = dns.name.Name(['c'])
            return n1 + n2
        self.assertRaises(dns.name.AbsoluteConcatenation, bad)

    def testBadEscape(self):
        def bad():
            n = dns.name.from_text(r'a.b\0q1.c.')
        self.assertRaises(dns.name.BadEscape, bad)

    def testDigestable1(self):
        n = dns.name.from_text('FOO.bar')
        d = n.to_digestable()
        self.assertEqual(d, b'\x03foo\x03bar\x00')

    def testDigestable2(self):
        n1 = dns.name.from_text('FOO.bar')
        n2 = dns.name.from_text('foo.BAR.')
        d1 = n1.to_digestable()
        d2 = n2.to_digestable()
        self.assertEqual(d1, d2)

    def testDigestable3(self):
        d = dns.name.root.to_digestable()
        self.assertEqual(d, b'\x00')

    def testDigestable4(self):
        n = dns.name.from_text('FOO.bar', None)
        d = n.to_digestable(dns.name.root)
        self.assertEqual(d, b'\x03foo\x03bar\x00')

    def testBadDigestable(self):
        def bad():
            n = dns.name.from_text('FOO.bar', None)
            n.to_digestable()
        self.assertRaises(dns.name.NeedAbsoluteNameOrOrigin, bad)

    def testToWire1(self):
        n = dns.name.from_text('FOO.bar')
        f = BytesIO()
        compress = {} # type: Dict[dns.name.Name,int]
        n.to_wire(f, compress)
        self.assertEqual(f.getvalue(), b'\x03FOO\x03bar\x00')

    def testToWire2(self):
        n = dns.name.from_text('FOO.bar')
        f = BytesIO()
        compress = {} # type: Dict[dns.name.Name,int]
        n.to_wire(f, compress)
        n.to_wire(f, compress)
        self.assertEqual(f.getvalue(), b'\x03FOO\x03bar\x00\xc0\x00')

    def testToWire3(self):
        n1 = dns.name.from_text('FOO.bar')
        n2 = dns.name.from_text('foo.bar')
        f = BytesIO()
        compress = {} # type: Dict[dns.name.Name,int]
        n1.to_wire(f, compress)
        n2.to_wire(f, compress)
        self.assertEqual(f.getvalue(), b'\x03FOO\x03bar\x00\xc0\x00')

    def testToWire4(self):
        n1 = dns.name.from_text('FOO.bar')
        n2 = dns.name.from_text('a.foo.bar')
        f = BytesIO()
        compress = {} # type: Dict[dns.name.Name,int]
        n1.to_wire(f, compress)
        n2.to_wire(f, compress)
        self.assertEqual(f.getvalue(), b'\x03FOO\x03bar\x00\x01\x61\xc0\x00')

    def testToWire5(self):
        n1 = dns.name.from_text('FOO.bar')
        n2 = dns.name.from_text('a.foo.bar')
        f = BytesIO()
        compress = {} # type: Dict[dns.name.Name,int]
        n1.to_wire(f, compress)
        n2.to_wire(f, None)
        self.assertEqual(f.getvalue(),
                         b'\x03FOO\x03bar\x00\x01\x61\x03foo\x03bar\x00')

    def testToWire6(self):
        n = dns.name.from_text('FOO.bar')
        v = n.to_wire()
        self.assertEqual(v, b'\x03FOO\x03bar\x00')

    def testToWireRelativeNameWithOrigin(self):
        n = dns.name.from_text('FOO', None)
        o = dns.name.from_text('bar')
        v = n.to_wire(origin=o)
        self.assertEqual(v, b'\x03FOO\x03bar\x00')

    def testToWireRelativeNameWithoutOrigin(self):
        n = dns.name.from_text('FOO', None)
        def bad():
            v = n.to_wire()
        self.assertRaises(dns.name.NeedAbsoluteNameOrOrigin, bad)

    def testBadToWire(self):
        def bad():
            n = dns.name.from_text('FOO.bar', None)
            f = BytesIO()
            compress = {} # type: Dict[dns.name.Name,int]
            n.to_wire(f, compress)
        self.assertRaises(dns.name.NeedAbsoluteNameOrOrigin, bad)

    def testGiantCompressionTable(self):
        # Only the first 16KiB of a message can have compression pointers.
        f = BytesIO()
        compress = {}  # type: Dict[dns.name.Name,int]
        # exactly 16 bytes encoded
        n = dns.name.from_text('0000000000.com.')
        n.to_wire(f, compress)
        # There are now two entries in the compression table (for the full
        # name, and for the com. suffix.
        self.assertEqual(len(compress), 2)
        for i in range(1023):
            # exactly 16 bytes encoded with compression
            n = dns.name.from_text(f'{i:013d}.com')
            n.to_wire(f, compress)
        # There are now 1025 entries in the compression table with
        # the last entry at offset 16368.
        self.assertEqual(len(compress), 1025)
        self.assertEqual(compress[n], 16368)
        # Adding another name should not increase the size of the compression
        # table, as the pointer would be at offset 16384, which is too big.
        n = dns.name.from_text('toobig.com.')
        n.to_wire(f, compress)
        self.assertEqual(len(compress), 1025)

    def testSplit1(self):
        n = dns.name.from_text('foo.bar.')
        (prefix, suffix) = n.split(2)
        ep = dns.name.from_text('foo', None)
        es = dns.name.from_text('bar.', None)
        self.assertEqual(prefix, ep)
        self.assertEqual(suffix, es)

    def testSplit2(self):
        n = dns.name.from_text('foo.bar.')
        (prefix, suffix) = n.split(1)
        ep = dns.name.from_text('foo.bar', None)
        es = dns.name.from_text('.', None)
        self.assertEqual(prefix, ep)
        self.assertEqual(suffix, es)

    def testSplit3(self):
        n = dns.name.from_text('foo.bar.')
        (prefix, suffix) = n.split(0)
        ep = dns.name.from_text('foo.bar.', None)
        es = dns.name.from_text('', None)
        self.assertEqual(prefix, ep)
        self.assertEqual(suffix, es)

    def testSplit4(self):
        n = dns.name.from_text('foo.bar.')
        (prefix, suffix) = n.split(3)
        ep = dns.name.from_text('', None)
        es = dns.name.from_text('foo.bar.', None)
        self.assertEqual(prefix, ep)
        self.assertEqual(suffix, es)

    def testBadSplit1(self):
        def bad():
            n = dns.name.from_text('foo.bar.')
            n.split(-1)
        self.assertRaises(ValueError, bad)

    def testBadSplit2(self):
        def bad():
            n = dns.name.from_text('foo.bar.')
            n.split(4)
        self.assertRaises(ValueError, bad)

    def testRelativize1(self):
        n = dns.name.from_text('a.foo.bar.', None)
        o = dns.name.from_text('bar.', None)
        e = dns.name.from_text('a.foo', None)
        self.assertEqual(n.relativize(o), e)

    def testRelativize2(self):
        n = dns.name.from_text('a.foo.bar.', None)
        o = n
        e = dns.name.empty
        self.assertEqual(n.relativize(o), e)

    def testRelativize3(self):
        n = dns.name.from_text('a.foo.bar.', None)
        o = dns.name.from_text('blaz.', None)
        e = n
        self.assertEqual(n.relativize(o), e)

    def testRelativize4(self):
        n = dns.name.from_text('a.foo', None)
        o = dns.name.root
        e = n
        self.assertEqual(n.relativize(o), e)

    def testDerelativize1(self):
        n = dns.name.from_text('a.foo', None)
        o = dns.name.from_text('bar.', None)
        e = dns.name.from_text('a.foo.bar.', None)
        self.assertEqual(n.derelativize(o), e)

    def testDerelativize2(self):
        n = dns.name.empty
        o = dns.name.from_text('a.foo.bar.', None)
        e = o
        self.assertEqual(n.derelativize(o), e)

    def testDerelativize3(self):
        n = dns.name.from_text('a.foo.bar.', None)
        o = dns.name.from_text('blaz.', None)
        e = n
        self.assertEqual(n.derelativize(o), e)

    def testChooseRelativity1(self):
        n = dns.name.from_text('a.foo.bar.', None)
        o = dns.name.from_text('bar.', None)
        e = dns.name.from_text('a.foo', None)
        self.assertEqual(n.choose_relativity(o, True), e)

    def testChooseRelativity2(self):
        n = dns.name.from_text('a.foo.bar.', None)
        o = dns.name.from_text('bar.', None)
        e = n
        self.assertEqual(n.choose_relativity(o, False), e)

    def testChooseRelativity3(self):
        n = dns.name.from_text('a.foo', None)
        o = dns.name.from_text('bar.', None)
        e = dns.name.from_text('a.foo.bar.', None)
        self.assertEqual(n.choose_relativity(o, False), e)

    def testChooseRelativity4(self):
        n = dns.name.from_text('a.foo', None)
        o = None
        e = n
        self.assertEqual(n.choose_relativity(o, True), e)

    def testChooseRelativity5(self):
        n = dns.name.from_text('a.foo', None)
        o = None
        e = n
        self.assertEqual(n.choose_relativity(o, False), e)

    def testChooseRelativity6(self):
        n = dns.name.from_text('a.foo.', None)
        o = None
        e = n
        self.assertEqual(n.choose_relativity(o, True), e)

    def testChooseRelativity7(self):
        n = dns.name.from_text('a.foo.', None)
        o = None
        e = n
        self.assertEqual(n.choose_relativity(o, False), e)

    def testFromWire1(self):
        w = b'\x03foo\x00\xc0\x00'
        (n1, cused1) = dns.name.from_wire(w, 0)
        (n2, cused2) = dns.name.from_wire(w, cused1)
        en1 = dns.name.from_text('foo.')
        en2 = en1
        ecused1 = 5
        ecused2 = 2
        self.assertEqual(n1, en1)
        self.assertEqual(cused1, ecused1)
        self.assertEqual(n2, en2)
        self.assertEqual(cused2, ecused2)

    def testFromWire2(self):
        w = b'\x03foo\x00\x01a\xc0\x00\x01b\xc0\x05'
        current = 0
        (n1, cused1) = dns.name.from_wire(w, current)
        current += cused1
        (n2, cused2) = dns.name.from_wire(w, current)
        current += cused2
        (n3, cused3) = dns.name.from_wire(w, current)
        en1 = dns.name.from_text('foo.')
        en2 = dns.name.from_text('a.foo.')
        en3 = dns.name.from_text('b.a.foo.')
        ecused1 = 5
        ecused2 = 4
        ecused3 = 4
        self.assertEqual(n1, en1)
        self.assertEqual(cused1, ecused1)
        self.assertEqual(n2, en2)
        self.assertEqual(cused2, ecused2)
        self.assertEqual(n3, en3)
        self.assertEqual(cused3, ecused3)

    def testBadFromWire1(self):
        def bad():
            w = b'\x03foo\xc0\x04'
            dns.name.from_wire(w, 0)
        self.assertRaises(dns.name.BadPointer, bad)

    def testBadFromWire2(self):
        def bad():
            w = b'\x03foo\xc0\x05'
            dns.name.from_wire(w, 0)
        self.assertRaises(dns.name.BadPointer, bad)

    def testBadFromWire3(self):
        def bad():
            w = b'\xbffoo'
            dns.name.from_wire(w, 0)
        self.assertRaises(dns.name.BadLabelType, bad)

    def testBadFromWire4(self):
        def bad():
            w = b'\x41foo'
            dns.name.from_wire(w, 0)
        self.assertRaises(dns.name.BadLabelType, bad)

    def testParent1(self):
        n = dns.name.from_text('foo.bar.')
        self.assertEqual(n.parent(), dns.name.from_text('bar.'))
        self.assertEqual(n.parent().parent(), dns.name.root)

    def testParent2(self):
        n = dns.name.from_text('foo.bar', None)
        self.assertEqual(n.parent(), dns.name.from_text('bar', None))
        self.assertEqual(n.parent().parent(), dns.name.empty)

    def testParent3(self):
        def bad():
            n = dns.name.root
            n.parent()
        self.assertRaises(dns.name.NoParent, bad)

    def testParent4(self):
        def bad():
            n = dns.name.empty
            n.parent()
        self.assertRaises(dns.name.NoParent, bad)

    def testFromUnicode1(self):
        n = dns.name.from_text('foo.bar')
        self.assertEqual(n.labels, (b'foo', b'bar', b''))

    def testFromUnicode2(self):
        n = dns.name.from_text('foo\u1234bar.bar')
        self.assertEqual(n.labels, (b'xn--foobar-r5z', b'bar', b''))

    def testFromUnicodeAlternateDot1(self):
        n = dns.name.from_text('foo\u3002bar')
        self.assertEqual(n.labels, (b'foo', b'bar', b''))

    def testFromUnicodeAlternateDot2(self):
        n = dns.name.from_text('foo\uff0ebar')
        self.assertEqual(n.labels, (b'foo', b'bar', b''))

    def testFromUnicodeAlternateDot3(self):
        n = dns.name.from_text('foo\uff61bar')
        self.assertEqual(n.labels, (b'foo', b'bar', b''))

    def testFromUnicodeRoot(self):
        n = dns.name.from_text('.')
        self.assertEqual(n.labels, (b'',))

    def testFromUnicodeAlternateRoot1(self):
        n = dns.name.from_text('\u3002')
        self.assertEqual(n.labels, (b'',))

    def testFromUnicodeAlternateRoot2(self):
        n = dns.name.from_text('\uff0e')
        self.assertEqual(n.labels, (b'',))

    def testFromUnicodeAlternateRoot3(self):
        n = dns.name.from_text('\uff61')
        self.assertEqual(n.labels, (b'', ))

    def testFromUnicodeIDNA2003Explicit(self):
        t = 'Königsgäßchen'
        e = dns.name.from_unicode(t, idna_codec=dns.name.IDNA_2003)
        self.assertEqual(str(e), 'xn--knigsgsschen-lcb0w.')

    def testFromUnicodeIDNA2003Default(self):
        t = 'Königsgäßchen'
        e = dns.name.from_unicode(t)
        self.assertEqual(str(e), 'xn--knigsgsschen-lcb0w.')

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def testFromUnicodeIDNA2008(self):
        t = 'Königsgäßchen'
        def bad():
            codec = dns.name.IDNA_2008_Strict
            return dns.name.from_unicode(t, idna_codec=codec)
        self.assertRaises(dns.name.IDNAException, bad)
        e1 = dns.name.from_unicode(t, idna_codec=dns.name.IDNA_2008)
        self.assertEqual(str(e1), 'xn--knigsgchen-b4a3dun.')
        c2 = dns.name.IDNA_2008_Transitional
        e2 = dns.name.from_unicode(t, idna_codec=c2)
        self.assertEqual(str(e2), 'xn--knigsgsschen-lcb0w.')

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def testFromUnicodeIDNA2008Mixed(self):
        # the IDN rules for names are very restrictive, disallowing
        # practical names like '_sip._tcp.Königsgäßchen'.  Dnspython
        # has a "practical" mode which permits labels which are purely
        # ASCII to go straight through, and thus not invalid useful
        # things in the real world.
        t = '_sip._tcp.Königsgäßchen'
        def bad1():
            codec = dns.name.IDNA_2008_Strict
            return dns.name.from_unicode(t, idna_codec=codec)
        def bad2():
            codec = dns.name.IDNA_2008_UTS_46
            return dns.name.from_unicode(t, idna_codec=codec)
        def bad3():
            codec = dns.name.IDNA_2008_Transitional
            return dns.name.from_unicode(t, idna_codec=codec)
        self.assertRaises(dns.name.IDNAException, bad1)
        self.assertRaises(dns.name.IDNAException, bad2)
        self.assertRaises(dns.name.IDNAException, bad3)
        e = dns.name.from_unicode(t,
                                  idna_codec=dns.name.IDNA_2008_Practical)
        self.assertEqual(str(e), '_sip._tcp.xn--knigsgchen-b4a3dun.')

    def testFromUnicodeEscapes(self):
        n = dns.name.from_unicode(r'\097.\098.\099.')
        t = n.to_unicode()
        self.assertEqual(t, 'a.b.c.')

    def testToUnicode1(self):
        n = dns.name.from_text('foo.bar')
        s = n.to_unicode()
        self.assertEqual(s, 'foo.bar.')

    def testToUnicode2(self):
        n = dns.name.from_text('foo\u1234bar.bar')
        s = n.to_unicode()
        self.assertEqual(s, 'foo\u1234bar.bar.')

    def testToUnicode3(self):
        n = dns.name.from_text('foo.bar')
        s = n.to_unicode()
        self.assertEqual(s, 'foo.bar.')

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def testToUnicode4(self):
        n = dns.name.from_text('ドメイン.テスト',
                               idna_codec=dns.name.IDNA_2008)
        s = n.to_unicode()
        self.assertEqual(str(n), 'xn--eckwd4c7c.xn--zckzah.')
        self.assertEqual(s, 'ドメイン.テスト.')

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def testToUnicode5(self):
        # Exercise UTS 46 remapping in decode.  This doesn't normally happen
        # as you can see from us having to instantiate the codec as
        # transitional with strict decoding, not one of our usual choices.
        codec = dns.name.IDNA2008Codec(True, True, False, True)
        n = dns.name.from_text('xn--gro-7ka.com')
        self.assertEqual(n.to_unicode(idna_codec=codec),
                         'gross.com.')

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def testToUnicode6(self):
        # Test strict 2008 decoding without UTS 46
        n = dns.name.from_text('xn--gro-7ka.com')
        self.assertEqual(n.to_unicode(idna_codec=dns.name.IDNA_2008_Strict),
                         'groß.com.')

    def testDefaultDecodeIsJustPunycode(self):
        # groß.com. in IDNA2008 form, pre-encoded.
        n = dns.name.from_text('xn--gro-7ka.com')
        # output using default codec which just decodes the punycode and
        # doesn't test for IDNA2003 or IDNA2008.
        self.assertEqual(n.to_unicode(), 'groß.com.')

    def testStrictINDA2003Decode(self):
        # groß.com. in IDNA2008 form, pre-encoded.
        n = dns.name.from_text('xn--gro-7ka.com')
        def bad():
            # This throws in IDNA2003 because it doesn't "round trip".
            n.to_unicode(idna_codec=dns.name.IDNA_2003_Strict)
        self.assertRaises(dns.name.IDNAException, bad)

    def testINDA2008Decode(self):
        # groß.com. in IDNA2008 form, pre-encoded.
        n = dns.name.from_text('xn--gro-7ka.com')
        self.assertEqual(n.to_unicode(idna_codec=dns.name.IDNA_2008),
                         'groß.com.')

    def testToUnicodeOmitFinalDot(self):
        # groß.com. in IDNA2008 form, pre-encoded.
        n = dns.name.from_text('xn--gro-7ka.com')
        self.assertEqual(n.to_unicode(True, dns.name.IDNA_2008),
                         'groß.com')

    def testIDNA2003Misc(self):
        self.assertEqual(dns.name.IDNA_2003.encode(''), b'')
        self.assertRaises(dns.name.LabelTooLong,
                          lambda: dns.name.IDNA_2003.encode('x' * 64))

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def testIDNA2008Misc(self):
        self.assertEqual(dns.name.IDNA_2008.encode(''), b'')
        self.assertRaises(dns.name.LabelTooLong,
                          lambda: dns.name.IDNA_2008.encode('x' * 64))
        self.assertRaises(dns.name.LabelTooLong,
                          lambda: dns.name.IDNA_2008.encode('groß' + 'x' * 60))

    def testReverseIPv4(self):
        e = dns.name.from_text('1.0.0.127.in-addr.arpa.')
        n = dns.reversename.from_address('127.0.0.1')
        self.assertEqual(e, n)

    def testReverseIPv6(self):
        e = dns.name.from_text('1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.ip6.arpa.')
        n = dns.reversename.from_address(b'::1')
        self.assertEqual(e, n)

    def testReverseIPv6MappedIpv4(self):
        e = dns.name.from_text('1.0.0.127.in-addr.arpa.')
        n = dns.reversename.from_address('::ffff:127.0.0.1')
        self.assertEqual(e, n)

    def testBadReverseIPv4(self):
        def bad():
            dns.reversename.from_address('127.0.foo.1')
        self.assertRaises(dns.exception.SyntaxError, bad)

    def testBadReverseIPv6(self):
        def bad():
            dns.reversename.from_address('::1::1')
        self.assertRaises(dns.exception.SyntaxError, bad)

    def testReverseIPv4AlternateOrigin(self):
        e = dns.name.from_text('1.0.0.127.foo.bar.')
        origin = dns.name.from_text('foo.bar')
        n = dns.reversename.from_address('127.0.0.1', v4_origin=origin)
        self.assertEqual(e, n)

    def testReverseIPv6AlternateOrigin(self):
        e = dns.name.from_text('1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.foo.bar.')
        origin = dns.name.from_text('foo.bar')
        n = dns.reversename.from_address(b'::1', v6_origin=origin)
        self.assertEqual(e, n)

    def testForwardIPv4(self):
        n = dns.name.from_text('1.0.0.127.in-addr.arpa.')
        e = '127.0.0.1'
        text = dns.reversename.to_address(n)
        self.assertEqual(text, e)

    def testForwardIPv6(self):
        n = dns.name.from_text('1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.ip6.arpa.')
        e = '::1'
        text = dns.reversename.to_address(n)
        self.assertEqual(text, e)

    def testForwardIPv4AlternateOrigin(self):
        n = dns.name.from_text('1.0.0.127.foo.bar.')
        e = '127.0.0.1'
        origin = dns.name.from_text('foo.bar')
        text = dns.reversename.to_address(n, v4_origin=origin)
        self.assertEqual(text, e)

    def testForwardIPv6AlternateOrigin(self):
        n = dns.name.from_text('1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.foo.bar.')
        e = '::1'
        origin = dns.name.from_text('foo.bar')
        text = dns.reversename.to_address(n, v6_origin=origin)
        self.assertEqual(text, e)

    def testUnknownReverseOrigin(self):
        n = dns.name.from_text('1.2.3.4.unknown.')
        with self.assertRaises(dns.exception.SyntaxError):
            dns.reversename.to_address(n)

    def testE164ToEnum(self):
        text = '+1 650 555 1212'
        e = dns.name.from_text('2.1.2.1.5.5.5.0.5.6.1.e164.arpa.')
        n = dns.e164.from_e164(text)
        self.assertEqual(n, e)

    def testEnumToE164(self):
        n = dns.name.from_text('2.1.2.1.5.5.5.0.5.6.1.e164.arpa.')
        e = '+16505551212'
        text = dns.e164.to_e164(n)
        self.assertEqual(text, e)

    def testBadEnumToE164(self):
        n = dns.name.from_text('2.1.2.q.5.5.5.0.5.6.1.e164.arpa.')
        self.assertRaises(dns.exception.SyntaxError,
                          lambda: dns.e164.to_e164(n))

    def test_incompatible_relations(self):
        n1 = dns.name.from_text('example')
        n2 = 'abc'
        for oper in [operator.lt, operator.le, operator.ge, operator.gt]:
            self.assertRaises(TypeError, lambda: oper(n1, n2))
        self.assertFalse(n1 == n2)
        self.assertTrue(n1 != n2)

    def testFromUnicodeSimpleEscape(self):
        n = dns.name.from_unicode(r'a.\b')
        e = dns.name.from_unicode(r'a.b')
        self.assertEqual(n, e)

    def testFromUnicodeBadEscape(self):
        def bad1():
            n = dns.name.from_unicode(r'a.b\0q1.c.')
        self.assertRaises(dns.name.BadEscape, bad1)
        def bad2():
            n = dns.name.from_unicode(r'a.b\0')
        self.assertRaises(dns.name.BadEscape, bad2)

    def testFromUnicodeNotString(self):
        def bad():
            dns.name.from_unicode(b'123')
        self.assertRaises(ValueError, bad)

    def testFromUnicodeBadOrigin(self):
        def bad():
            dns.name.from_unicode('example', 123)
        self.assertRaises(ValueError, bad)

    def testFromUnicodeEmptyLabel(self):
        def bad():
            dns.name.from_unicode('a..b.example')
        self.assertRaises(dns.name.EmptyLabel, bad)

    def testFromUnicodeEmptyName(self):
        self.assertEqual(dns.name.from_unicode('@', None), dns.name.empty)

    def testFromTextNotString(self):
        def bad():
            dns.name.from_text(123)
        self.assertRaises(ValueError, bad)

    def testFromTextBadOrigin(self):
        def bad():
            dns.name.from_text('example', 123)
        self.assertRaises(ValueError, bad)

    def testFromWireNotBytes(self):
        def bad():
            dns.name.from_wire(123, 0)
        self.assertRaises(ValueError, bad)

    def testBadPunycode(self):
        c = dns.name.IDNACodec()
        with self.assertRaises(dns.name.IDNAException):
            c.decode(b'xn--0000h')

    def testRootLabel2003StrictDecode(self):
        c = dns.name.IDNA_2003_Strict
        self.assertEqual(c.decode(b''), '')

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def testRootLabel2008StrictDecode(self):
        c = dns.name.IDNA_2008_Strict
        self.assertEqual(c.decode(b''), '')

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def testCodecNotFoundRaises(self):
        dns.name.have_idna_2008 = False
        with self.assertRaises(dns.name.NoIDNA2008):
            c = dns.name.IDNA2008Codec()
            c.encode('Königsgäßchen')
        with self.assertRaises(dns.name.NoIDNA2008):
            c = dns.name.IDNA2008Codec(strict_decode=True)
            c.decode('xn--eckwd4c7c.xn--zckzah.')
        dns.name.have_idna_2008 = True

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def testBadPunycodeStrict2008(self):
        c = dns.name.IDNA2008Codec(strict_decode=True)
        with self.assertRaises(dns.name.IDNAException):
            c.decode(b'xn--0000h')

    def testRelativizeSubtractionSyntax(self):
        n = dns.name.from_text('foo.example.')
        o = dns.name.from_text('example.')
        e = dns.name.from_text('foo', None)
        self.assertEqual(n - o, e)

    def testCopy(self):
        n1 = dns.name.from_text('foo.example.')
        n2 = copy.copy(n1)
        self.assertTrue(n1 is not n2)
        # the Name constructor always copies labels, so there is no
        # difference between copy and deepcopy
        self.assertTrue(n1.labels is not n2.labels)
        self.assertEqual(len(n1.labels), len(n2.labels))
        for i, l in enumerate(n1.labels):
            self.assertTrue(l is n2[i])

    def testDeepCopy(self):
        n1 = dns.name.from_text('foo.example.')
        n2 = copy.deepcopy(n1)
        self.assertTrue(n1 is not n2)
        self.assertTrue(n1.labels is not n2.labels)
        self.assertEqual(len(n1.labels), len(n2.labels))
        for i, l in enumerate(n1.labels):
            self.assertTrue(l is n2[i])

    def testNoAttributeDeletion(self):
        n = dns.name.from_text('foo.example.')
        with self.assertRaises(TypeError):
            del n.labels

    def testUnicodeEscapify(self):
        n = dns.name.from_unicode('Königsgäßchen;\ttext')
        self.assertEqual(n.to_unicode(), 'königsgässchen\\;\\009text.')

    def test_pickle(self):
        n1 = dns.name.from_text('foo.example')
        p = pickle.dumps(n1)
        n2 = pickle.loads(p)
        self.assertEqual(n1, n2)

if __name__ == '__main__':
    unittest.main()
