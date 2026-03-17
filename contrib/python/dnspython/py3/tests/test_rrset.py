# -*- coding: utf-8
# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

# Copyright (C) 2003-2007, 2009-2011 Nominum, Inc.
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

import unittest

import dns.name
import dns.rrset

class RRsetTestCase(unittest.TestCase):

    def testEqual1(self):
        r1 = dns.rrset.from_text('foo', 300, 'in', 'a', '10.0.0.1', '10.0.0.2')
        r2 = dns.rrset.from_text('FOO', 300, 'in', 'a', '10.0.0.2', '10.0.0.1')
        self.assertEqual(r1, r2)

    def testEqual2(self):
        r1 = dns.rrset.from_text('foo', 300, 'in', 'a', '10.0.0.1', '10.0.0.2')
        r2 = dns.rrset.from_text('FOO', 600, 'in', 'a', '10.0.0.2', '10.0.0.1')
        self.assertEqual(r1, r2)

    def testNotEqual1(self):
        r1 = dns.rrset.from_text('fooa', 30, 'in', 'a', '10.0.0.1', '10.0.0.2')
        r2 = dns.rrset.from_text('FOO', 30, 'in', 'a', '10.0.0.2', '10.0.0.1')
        self.assertNotEqual(r1, r2)

    def testNotEqual2(self):
        r1 = dns.rrset.from_text('foo', 30, 'in', 'a', '10.0.0.1', '10.0.0.3')
        r2 = dns.rrset.from_text('FOO', 30, 'in', 'a', '10.0.0.2', '10.0.0.1')
        self.assertNotEqual(r1, r2)

    def testNotEqual3(self):
        r1 = dns.rrset.from_text('foo', 30, 'in', 'a', '10.0.0.1', '10.0.0.2',
                                 '10.0.0.3')
        r2 = dns.rrset.from_text('FOO', 30, 'in', 'a', '10.0.0.2', '10.0.0.1')
        self.assertNotEqual(r1, r2)

    def testNotEqual4(self):
        r1 = dns.rrset.from_text('foo', 30, 'in', 'a', '10.0.0.1')
        r2 = dns.rrset.from_text('FOO', 30, 'in', 'a', '10.0.0.2', '10.0.0.1')
        self.assertNotEqual(r1, r2)

    def testCodec2003(self):
        r1 = dns.rrset.from_text_list('Königsgäßchen', 30, 'in', 'ns',
                                      ['Königsgäßchen'])
        r2 = dns.rrset.from_text_list('xn--knigsgsschen-lcb0w', 30, 'in', 'ns',
                                      ['xn--knigsgsschen-lcb0w'])
        self.assertEqual(r1, r2)

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def testCodec2008(self):
        r1 = dns.rrset.from_text_list('Königsgäßchen', 30, 'in', 'ns',
                                      ['Königsgäßchen'],
                                      idna_codec=dns.name.IDNA_2008)
        r2 = dns.rrset.from_text_list('xn--knigsgchen-b4a3dun', 30, 'in', 'ns',
                                      ['xn--knigsgchen-b4a3dun'],
                                      idna_codec=dns.name.IDNA_2008)
        self.assertEqual(r1, r2)

    def testCopy(self):
        r1 = dns.rrset.from_text_list('foo', 30, 'in', 'a',
                                      ['10.0.0.1', '10.0.0.2'])
        r2 = r1.copy()
        self.assertFalse(r1 is r2)
        self.assertTrue(r1 == r2)

    def testFullMatch1(self):
        r1 = dns.rrset.from_text_list('foo', 30, 'in', 'a',
                                      ['10.0.0.1', '10.0.0.2'])
        self.assertTrue(r1.full_match(r1.name, dns.rdataclass.IN,
                                      dns.rdatatype.A, dns.rdatatype.NONE))

    def testFullMatch2(self):
        r1 = dns.rrset.from_text_list('foo', 30, 'in', 'a',
                                      ['10.0.0.1', '10.0.0.2'])
        r1.deleting = dns.rdataclass.NONE
        self.assertTrue(r1.full_match(r1.name, dns.rdataclass.IN,
                                      dns.rdatatype.A, dns.rdatatype.NONE,
                                      dns.rdataclass.NONE))

    def testNoFullMatch1(self):
        n = dns.name.from_text('bar', None)
        r1 = dns.rrset.from_text_list('foo', 30, 'in', 'a',
                                      ['10.0.0.1', '10.0.0.2'])
        self.assertFalse(r1.full_match(n, dns.rdataclass.IN,
                                       dns.rdatatype.A, dns.rdatatype.NONE,
                                       dns.rdataclass.ANY))

    def testNoFullMatch2(self):
        r1 = dns.rrset.from_text_list('foo', 30, 'in', 'a',
                                      ['10.0.0.1', '10.0.0.2'])
        r1.deleting = dns.rdataclass.NONE
        self.assertFalse(r1.full_match(r1.name, dns.rdataclass.IN,
                                       dns.rdatatype.A, dns.rdatatype.NONE,
                                       dns.rdataclass.ANY))

    def testNoFullMatch3(self):
        r1 = dns.rrset.from_text_list('foo', 30, 'in', 'a',
                                      ['10.0.0.1', '10.0.0.2'])
        self.assertFalse(r1.full_match(r1.name, dns.rdataclass.IN,
                                       dns.rdatatype.MX, dns.rdatatype.NONE,
                                       dns.rdataclass.ANY))

    def testMatchCompatibilityWithFullMatch(self):
        r1 = dns.rrset.from_text_list('foo', 30, 'in', 'a',
                                      ['10.0.0.1', '10.0.0.2'])
        self.assertTrue(r1.match(r1.name, dns.rdataclass.IN,
                                 dns.rdatatype.A, dns.rdatatype.NONE))

    def testMatchCompatibilityWithRdatasetMatch(self):
        r1 = dns.rrset.from_text_list('foo', 30, 'in', 'a',
                                      ['10.0.0.1', '10.0.0.2'])
        self.assertTrue(r1.match(dns.rdataclass.IN, dns.rdatatype.A,
                                 dns.rdatatype.NONE))

    def testToRdataset(self):
        r1 = dns.rrset.from_text_list('foo', 30, 'in', 'a',
                                      ['10.0.0.1', '10.0.0.2'])
        r2 = dns.rdataset.from_text_list('in', 'a', 30,
                                         ['10.0.0.1', '10.0.0.2'])
        self.assertEqual(r1.to_rdataset(), r2)

    def testFromRdata(self):
        rdata1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                     '10.0.0.1')
        rdata2 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                     '10.0.0.2')
        expected_rrs = dns.rrset.from_text('foo', 300, 'in', 'a', '10.0.0.1',
                                           '10.0.0.2')
        rrs = dns.rrset.from_rdata('foo', 300, rdata1, rdata2)
        self.assertEqual(rrs, expected_rrs)

    def testEmptyList(self):
        def bad():
            rrs = dns.rrset.from_rdata_list('foo', 300, [])
        self.assertRaises(ValueError, bad)

    def testTTLMinimization(self):
        rrs = dns.rrset.RRset(dns.name.from_text('foo'),
                              dns.rdataclass.IN, dns.rdatatype.A)
        rdata1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                     '10.0.0.1')
        rdata2 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                     '10.0.0.2')
        rrs.add(rdata1, 300)
        self.assertEqual(rrs.ttl, 300)
        rrs.add(rdata2, 30)
        self.assertEqual(rrs.ttl, 30)
        # adding the same thing with a smaller TTL also minimizes
        rrs.add(rdata2, 3)
        self.assertEqual(rrs.ttl, 3)

    def testNotEqualOtherType(self):
        rrs = dns.rrset.RRset(dns.name.from_text('foo'),
                              dns.rdataclass.IN, dns.rdatatype.A)
        self.assertFalse(rrs == 123)

    def testRepr(self):
        rrset = dns.rrset.from_text('foo', 30, 'in', 'a', '10.0.0.1',
                                    '10.0.0.2')
        self.assertEqual(repr(rrset),
                         '<DNS foo IN A RRset: [<10.0.0.1>, <10.0.0.2>]>')
        rrset.deleting = dns.rdataclass.NONE
        self.assertEqual(repr(rrset),
                         '<DNS foo IN A delete=NONE RRset: ' +
                         '[<10.0.0.1>, <10.0.0.2>]>')
        rrset = dns.rrset.from_text(
            'foo', 30, 'in', 'rrsig',
            'A 1 3 3600 20200701000000 20200601000000 1 NAME Ym9ndXM=')
        self.assertEqual(repr(rrset),
                         '<DNS foo IN RRSIG(A) RRset: ' +
                         '[<A 1 3 3600 20200701000000 20200601000000 ' +
                         '1 NAME Ym9ndXM=>]>')

if __name__ == '__main__':
    unittest.main()
