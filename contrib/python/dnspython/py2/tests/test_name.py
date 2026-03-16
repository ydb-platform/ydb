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

from __future__ import print_function

from typing import Dict # pylint: disable=unused-import
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
        self.failUnless(n == dns.name.empty)

    def testFromTextRel5(self):
        n = dns.name.from_text('@', origin=self.origin)
        self.failUnless(n == self.origin)

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
        self.failUnlessRaises(TypeError, bad)

    def testImmutable2(self):
        def bad():
            self.origin.labels[0] = 'foo'
        self.failUnlessRaises(TypeError, bad)

    def testAbs1(self):
        self.failUnless(dns.name.root.is_absolute())

    def testAbs2(self):
        self.failUnless(not dns.name.empty.is_absolute())

    def testAbs3(self):
        self.failUnless(self.origin.is_absolute())

    def testAbs4(self):
        n = dns.name.from_text('foo', origin=None)
        self.failUnless(not n.is_absolute())

    def testWild1(self):
        n = dns.name.from_text('*.foo', origin=None)
        self.failUnless(n.is_wild())

    def testWild2(self):
        n = dns.name.from_text('*a.foo', origin=None)
        self.failUnless(not n.is_wild())

    def testWild3(self):
        n = dns.name.from_text('a.*.foo', origin=None)
        self.failUnless(not n.is_wild())

    def testWild4(self):
        self.failUnless(not dns.name.root.is_wild())

    def testWild5(self):
        self.failUnless(not dns.name.empty.is_wild())

    def testHash1(self):
        n1 = dns.name.from_text('fOo.COM')
        n2 = dns.name.from_text('foo.com')
        self.assertEqual(hash(n1), hash(n2))

    def testCompare1(self):
        n1 = dns.name.from_text('a')
        n2 = dns.name.from_text('b')
        self.failUnless(n1 < n2)
        self.failUnless(n2 > n1)

    def testCompare2(self):
        n1 = dns.name.from_text('')
        n2 = dns.name.from_text('b')
        self.failUnless(n1 < n2)
        self.failUnless(n2 > n1)

    def testCompare3(self):
        self.failUnless(dns.name.empty < dns.name.root)
        self.failUnless(dns.name.root > dns.name.empty)

    def testCompare4(self):
        self.failUnless(dns.name.root != 1)

    def testSubdomain1(self):
        self.failUnless(not dns.name.empty.is_subdomain(dns.name.root))

    def testSubdomain2(self):
        self.failUnless(not dns.name.root.is_subdomain(dns.name.empty))

    def testSubdomain3(self):
        n = dns.name.from_text('foo', origin=self.origin)
        self.failUnless(n.is_subdomain(self.origin))

    def testSubdomain4(self):
        n = dns.name.from_text('foo', origin=self.origin)
        self.failUnless(n.is_subdomain(dns.name.root))

    def testSubdomain5(self):
        n = dns.name.from_text('foo', origin=self.origin)
        self.failUnless(n.is_subdomain(n))

    def testSuperdomain1(self):
        self.failUnless(not dns.name.empty.is_superdomain(dns.name.root))

    def testSuperdomain2(self):
        self.failUnless(not dns.name.root.is_superdomain(dns.name.empty))

    def testSuperdomain3(self):
        n = dns.name.from_text('foo', origin=self.origin)
        self.failUnless(self.origin.is_superdomain(n))

    def testSuperdomain4(self):
        n = dns.name.from_text('foo', origin=self.origin)
        self.failUnless(dns.name.root.is_superdomain(n))

    def testSuperdomain5(self):
        n = dns.name.from_text('foo', origin=self.origin)
        self.failUnless(n.is_superdomain(n))

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
        self.assertEqual(t, 'FOO\.bar')

    def testToText8(self):
        n = dns.name.from_text(r'\070OO\.bar', origin=None)
        t = n.to_text()
        self.assertEqual(t, 'FOO\.bar')

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
        self.failUnlessRaises(dns.name.EmptyLabel, bad)

    def testEmptyLabel2(self):
        def bad():
            dns.name.Name(['', 'b'])
        self.failUnlessRaises(dns.name.EmptyLabel, bad)

    def testEmptyLabel3(self):
        n = dns.name.Name(['b', ''])
        self.failUnless(n)

    def testLongLabel(self):
        n = dns.name.Name(['a' * 63])
        self.failUnless(n)

    def testLabelTooLong(self):
        def bad():
            dns.name.Name(['a' * 64, 'b'])
        self.failUnlessRaises(dns.name.LabelTooLong, bad)

    def testLongName(self):
        n = dns.name.Name(['a' * 63, 'a' * 63, 'a' * 63, 'a' * 62])
        self.failUnless(n)

    def testNameTooLong(self):
        def bad():
            dns.name.Name(['a' * 63, 'a' * 63, 'a' * 63, 'a' * 63])
        self.failUnlessRaises(dns.name.NameTooLong, bad)

    def testConcat1(self):
        n1 = dns.name.Name(['a', 'b'])
        n2 = dns.name.Name(['c', 'd'])
        e = dns.name.Name(['a', 'b', 'c', 'd'])
        r = n1 + n2
        self.failUnless(r == e)

    def testConcat2(self):
        n1 = dns.name.Name(['a', 'b'])
        n2 = dns.name.Name([])
        e = dns.name.Name(['a', 'b'])
        r = n1 + n2
        self.failUnless(r == e)

    def testConcat3(self):
        n1 = dns.name.Name([])
        n2 = dns.name.Name(['a', 'b'])
        e = dns.name.Name(['a', 'b'])
        r = n1 + n2
        self.failUnless(r == e)

    def testConcat4(self):
        n1 = dns.name.Name(['a', 'b', ''])
        n2 = dns.name.Name([])
        e = dns.name.Name(['a', 'b', ''])
        r = n1 + n2
        self.failUnless(r == e)

    def testConcat5(self):
        n1 = dns.name.Name(['a', 'b'])
        n2 = dns.name.Name(['c', ''])
        e = dns.name.Name(['a', 'b', 'c', ''])
        r = n1 + n2
        self.failUnless(r == e)

    def testConcat6(self):
        def bad():
            n1 = dns.name.Name(['a', 'b', ''])
            n2 = dns.name.Name(['c'])
            return n1 + n2
        self.failUnlessRaises(dns.name.AbsoluteConcatenation, bad)

    def testBadEscape(self):
        def bad():
            n = dns.name.from_text(r'a.b\0q1.c.')
            print(n)
        self.failUnlessRaises(dns.name.BadEscape, bad)

    def testDigestable1(self):
        n = dns.name.from_text('FOO.bar')
        d = n.to_digestable()
        self.assertEqual(d, b'\x03foo\x03bar\x00')

    def testDigestable2(self):
        n1 = dns.name.from_text('FOO.bar')
        n2 = dns.name.from_text('foo.BAR.')
        d1 = n1.to_digestable()
        d2 = n2.to_digestable()
        self.failUnless(d1 == d2)

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
        self.failUnlessRaises(dns.name.NeedAbsoluteNameOrOrigin, bad)

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

    def testBadToWire(self):
        def bad():
            n = dns.name.from_text('FOO.bar', None)
            f = BytesIO()
            compress = {} # type: Dict[dns.name.Name,int]
            n.to_wire(f, compress)
        self.failUnlessRaises(dns.name.NeedAbsoluteNameOrOrigin, bad)

    def testSplit1(self):
        n = dns.name.from_text('foo.bar.')
        (prefix, suffix) = n.split(2)
        ep = dns.name.from_text('foo', None)
        es = dns.name.from_text('bar.', None)
        self.failUnless(prefix == ep and suffix == es)

    def testSplit2(self):
        n = dns.name.from_text('foo.bar.')
        (prefix, suffix) = n.split(1)
        ep = dns.name.from_text('foo.bar', None)
        es = dns.name.from_text('.', None)
        self.failUnless(prefix == ep and suffix == es)

    def testSplit3(self):
        n = dns.name.from_text('foo.bar.')
        (prefix, suffix) = n.split(0)
        ep = dns.name.from_text('foo.bar.', None)
        es = dns.name.from_text('', None)
        self.failUnless(prefix == ep and suffix == es)

    def testSplit4(self):
        n = dns.name.from_text('foo.bar.')
        (prefix, suffix) = n.split(3)
        ep = dns.name.from_text('', None)
        es = dns.name.from_text('foo.bar.', None)
        self.failUnless(prefix == ep and suffix == es)

    def testBadSplit1(self):
        def bad():
            n = dns.name.from_text('foo.bar.')
            n.split(-1)
        self.failUnlessRaises(ValueError, bad)

    def testBadSplit2(self):
        def bad():
            n = dns.name.from_text('foo.bar.')
            n.split(4)
        self.failUnlessRaises(ValueError, bad)

    def testRelativize1(self):
        n = dns.name.from_text('a.foo.bar.', None)
        o = dns.name.from_text('bar.', None)
        e = dns.name.from_text('a.foo', None)
        self.failUnless(n.relativize(o) == e)

    def testRelativize2(self):
        n = dns.name.from_text('a.foo.bar.', None)
        o = n
        e = dns.name.empty
        self.failUnless(n.relativize(o) == e)

    def testRelativize3(self):
        n = dns.name.from_text('a.foo.bar.', None)
        o = dns.name.from_text('blaz.', None)
        e = n
        self.failUnless(n.relativize(o) == e)

    def testRelativize4(self):
        n = dns.name.from_text('a.foo', None)
        o = dns.name.root
        e = n
        self.failUnless(n.relativize(o) == e)

    def testDerelativize1(self):
        n = dns.name.from_text('a.foo', None)
        o = dns.name.from_text('bar.', None)
        e = dns.name.from_text('a.foo.bar.', None)
        self.failUnless(n.derelativize(o) == e)

    def testDerelativize2(self):
        n = dns.name.empty
        o = dns.name.from_text('a.foo.bar.', None)
        e = o
        self.failUnless(n.derelativize(o) == e)

    def testDerelativize3(self):
        n = dns.name.from_text('a.foo.bar.', None)
        o = dns.name.from_text('blaz.', None)
        e = n
        self.failUnless(n.derelativize(o) == e)

    def testChooseRelativity1(self):
        n = dns.name.from_text('a.foo.bar.', None)
        o = dns.name.from_text('bar.', None)
        e = dns.name.from_text('a.foo', None)
        self.failUnless(n.choose_relativity(o, True) == e)

    def testChooseRelativity2(self):
        n = dns.name.from_text('a.foo.bar.', None)
        o = dns.name.from_text('bar.', None)
        e = n
        self.failUnless(n.choose_relativity(o, False) == e)

    def testChooseRelativity3(self):
        n = dns.name.from_text('a.foo', None)
        o = dns.name.from_text('bar.', None)
        e = dns.name.from_text('a.foo.bar.', None)
        self.failUnless(n.choose_relativity(o, False) == e)

    def testChooseRelativity4(self):
        n = dns.name.from_text('a.foo', None)
        o = None
        e = n
        self.failUnless(n.choose_relativity(o, True) == e)

    def testChooseRelativity5(self):
        n = dns.name.from_text('a.foo', None)
        o = None
        e = n
        self.failUnless(n.choose_relativity(o, False) == e)

    def testChooseRelativity6(self):
        n = dns.name.from_text('a.foo.', None)
        o = None
        e = n
        self.failUnless(n.choose_relativity(o, True) == e)

    def testChooseRelativity7(self):
        n = dns.name.from_text('a.foo.', None)
        o = None
        e = n
        self.failUnless(n.choose_relativity(o, False) == e)

    def testFromWire1(self):
        w = b'\x03foo\x00\xc0\x00'
        (n1, cused1) = dns.name.from_wire(w, 0)
        (n2, cused2) = dns.name.from_wire(w, cused1)
        en1 = dns.name.from_text('foo.')
        en2 = en1
        ecused1 = 5
        ecused2 = 2
        self.failUnless(n1 == en1 and cused1 == ecused1 and \
                        n2 == en2 and cused2 == ecused2)

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
        self.failUnless(n1 == en1 and cused1 == ecused1 and \
                        n2 == en2 and cused2 == ecused2 and \
                        n3 == en3 and cused3 == ecused3)

    def testBadFromWire1(self):
        def bad():
            w = b'\x03foo\xc0\x04'
            dns.name.from_wire(w, 0)
        self.failUnlessRaises(dns.name.BadPointer, bad)

    def testBadFromWire2(self):
        def bad():
            w = b'\x03foo\xc0\x05'
            dns.name.from_wire(w, 0)
        self.failUnlessRaises(dns.name.BadPointer, bad)

    def testBadFromWire3(self):
        def bad():
            w = b'\xbffoo'
            dns.name.from_wire(w, 0)
        self.failUnlessRaises(dns.name.BadLabelType, bad)

    def testBadFromWire4(self):
        def bad():
            w = b'\x41foo'
            dns.name.from_wire(w, 0)
        self.failUnlessRaises(dns.name.BadLabelType, bad)

    def testParent1(self):
        n = dns.name.from_text('foo.bar.')
        self.failUnless(n.parent() == dns.name.from_text('bar.'))
        self.failUnless(n.parent().parent() == dns.name.root)

    def testParent2(self):
        n = dns.name.from_text('foo.bar', None)
        self.failUnless(n.parent() == dns.name.from_text('bar', None))
        self.failUnless(n.parent().parent() == dns.name.empty)

    def testParent3(self):
        def bad():
            n = dns.name.root
            n.parent()
        self.failUnlessRaises(dns.name.NoParent, bad)

    def testParent4(self):
        def bad():
            n = dns.name.empty
            n.parent()
        self.failUnlessRaises(dns.name.NoParent, bad)

    def testFromUnicode1(self):
        n = dns.name.from_text(u'foo.bar')
        self.assertEqual(n.labels, (b'foo', b'bar', b''))

    def testFromUnicode2(self):
        n = dns.name.from_text(u'foo\u1234bar.bar')
        self.assertEqual(n.labels, (b'xn--foobar-r5z', b'bar', b''))

    def testFromUnicodeAlternateDot1(self):
        n = dns.name.from_text(u'foo\u3002bar')
        self.assertEqual(n.labels, (b'foo', b'bar', b''))

    def testFromUnicodeAlternateDot2(self):
        n = dns.name.from_text(u'foo\uff0ebar')
        self.assertEqual(n.labels, (b'foo', b'bar', b''))

    def testFromUnicodeAlternateDot3(self):
        n = dns.name.from_text(u'foo\uff61bar')
        self.assertEqual(n.labels, (b'foo', b'bar', b''))

    def testFromUnicodeIDNA2003Explicit(self):
        t = u'Königsgäßchen'
        e = dns.name.from_unicode(t, idna_codec=dns.name.IDNA_2003)
        self.assertEqual(str(e), 'xn--knigsgsschen-lcb0w.')

    def testFromUnicodeIDNA2003Default(self):
        t = u'Königsgäßchen'
        e = dns.name.from_unicode(t)
        self.assertEqual(str(e), 'xn--knigsgsschen-lcb0w.')

    def testFromUnicodeIDNA2008(self):
        if dns.name.have_idna_2008:
            t = u'Königsgäßchen'
            def bad():
                codec = dns.name.IDNA_2008_Strict
                return dns.name.from_unicode(t, idna_codec=codec)
            self.failUnlessRaises(dns.name.IDNAException, bad)
            e1 = dns.name.from_unicode(t, idna_codec=dns.name.IDNA_2008)
            self.assertEqual(str(e1), 'xn--knigsgchen-b4a3dun.')
            c2 = dns.name.IDNA_2008_Transitional
            e2 = dns.name.from_unicode(t, idna_codec=c2)
            self.assertEqual(str(e2), 'xn--knigsgsschen-lcb0w.')

    def testFromUnicodeIDNA2008Mixed(self):
        # the IDN rules for names are very restrictive, disallowing
        # practical names like u'_sip._tcp.Königsgäßchen'.  Dnspython
        # has a "practical" mode which permits labels which are purely
        # ASCII to go straight through, and thus not invalid useful
        # things in the real world.
        if dns.name.have_idna_2008:
            t = u'_sip._tcp.Königsgäßchen'
            def bad1():
                codec = dns.name.IDNA_2008_Strict
                return dns.name.from_unicode(t, idna_codec=codec)
            def bad2():
                codec = dns.name.IDNA_2008_UTS_46
                return dns.name.from_unicode(t, idna_codec=codec)
            def bad3():
                codec = dns.name.IDNA_2008_Transitional
                return dns.name.from_unicode(t, idna_codec=codec)
            self.failUnlessRaises(dns.name.IDNAException, bad1)
            self.failUnlessRaises(dns.name.IDNAException, bad2)
            self.failUnlessRaises(dns.name.IDNAException, bad3)
            e = dns.name.from_unicode(t,
                                      idna_codec=dns.name.IDNA_2008_Practical)
            self.assertEqual(str(e), '_sip._tcp.xn--knigsgchen-b4a3dun.')

    def testToUnicode1(self):
        n = dns.name.from_text(u'foo.bar')
        s = n.to_unicode()
        self.assertEqual(s, u'foo.bar.')

    def testToUnicode2(self):
        n = dns.name.from_text(u'foo\u1234bar.bar')
        s = n.to_unicode()
        self.assertEqual(s, u'foo\u1234bar.bar.')

    def testToUnicode3(self):
        n = dns.name.from_text('foo.bar')
        s = n.to_unicode()
        self.assertEqual(s, u'foo.bar.')

    def testToUnicode4(self):
        if dns.name.have_idna_2008:
            n = dns.name.from_text(u'ドメイン.テスト',
                                   idna_codec=dns.name.IDNA_2008)
            s = n.to_unicode()
            self.assertEqual(str(n), 'xn--eckwd4c7c.xn--zckzah.')
            self.assertEqual(s, u'ドメイン.テスト.')

    def testDefaultDecodeIsJustPunycode(self):
        # groß.com. in IDNA2008 form, pre-encoded.
        n = dns.name.from_text('xn--gro-7ka.com')
        # output using default codec which just decodes the punycode and
        # doesn't test for IDNA2003 or IDNA2008.
        self.assertEqual(n.to_unicode(), u'groß.com.')

    def testStrictINDA2003Decode(self):
        # groß.com. in IDNA2008 form, pre-encoded.
        n = dns.name.from_text('xn--gro-7ka.com')
        def bad():
            # This throws in IDNA2003 because it doesn't "round trip".
            n.to_unicode(idna_codec=dns.name.IDNA_2003_Strict)
        self.failUnlessRaises(dns.name.IDNAException, bad)

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
        self.failUnless(e == n)

    def testBadReverseIPv4(self):
        def bad():
            dns.reversename.from_address('127.0.foo.1')
        self.failUnlessRaises(dns.exception.SyntaxError, bad)

    def testBadReverseIPv6(self):
        def bad():
            dns.reversename.from_address('::1::1')
        self.failUnlessRaises(dns.exception.SyntaxError, bad)

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

    def testE164ToEnum(self):
        text = '+1 650 555 1212'
        e = dns.name.from_text('2.1.2.1.5.5.5.0.5.6.1.e164.arpa.')
        n = dns.e164.from_e164(text)
        self.failUnless(n == e)

    def testEnumToE164(self):
        n = dns.name.from_text('2.1.2.1.5.5.5.0.5.6.1.e164.arpa.')
        e = '+16505551212'
        text = dns.e164.to_e164(n)
        self.assertEqual(text, e)

if __name__ == '__main__':
    unittest.main()
