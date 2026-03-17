# -*- coding: utf-8
# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import unittest

import dns.name
import dns.rdata
import dns.rdataclass
import dns.rdataset
import dns.rdatatype

class RdatasetTestCase(unittest.TestCase):

    def testCodec2003(self):
        r1 = dns.rdataset.from_text_list('in', 'ns', 30,
                                         ['Königsgäßchen'])
        r2 = dns.rdataset.from_text_list('in', 'ns', 30,
                                         ['xn--knigsgsschen-lcb0w'])
        self.assertEqual(r1, r2)

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def testCodec2008(self):
        r1 = dns.rdataset.from_text_list('in', 'ns', 30,
                                         ['Königsgäßchen'],
                                         idna_codec=dns.name.IDNA_2008)
        r2 = dns.rdataset.from_text_list('in', 'ns', 30,
                                         ['xn--knigsgchen-b4a3dun'],
                                         idna_codec=dns.name.IDNA_2008)
        self.assertEqual(r1, r2)

    def testCopy(self):
        r1 = dns.rdataset.from_text_list('in', 'a', 30,
                                         ['10.0.0.1', '10.0.0.2'])
        r2 = r1.copy()
        self.assertFalse(r1 is r2)
        self.assertTrue(r1 == r2)

    def testAddIncompatible(self):
        rds = dns.rdataset.Rdataset(dns.rdataclass.IN, dns.rdatatype.A)
        rd1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                  '10.0.0.1')
        rd2 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.AAAA,
                                  '::1')
        rds.add(rd1, 30)
        self.assertRaises(dns.rdataset.IncompatibleTypes,
                          lambda: rds.add(rd2, 30))

    def testDifferingCovers(self):
        rds = dns.rdataset.Rdataset(dns.rdataclass.IN, dns.rdatatype.RRSIG,
                                    dns.rdatatype.A)
        rd1 = dns.rdata.from_text(
            dns.rdataclass.IN, dns.rdatatype.RRSIG,
            'A 1 3 3600 20200101000000 20030101000000 2143 foo Ym9ndXM=')
        rd2 = dns.rdata.from_text(
            dns.rdataclass.IN, dns.rdatatype.RRSIG,
            'AAAA 1 3 3600 20200101000000 20030101000000 2143 foo Ym9ndXM=')
        rds.add(rd1, 30)
        self.assertRaises(dns.rdataset.DifferingCovers,
                          lambda: rds.add(rd2, 30))

    def testUnionUpdate(self):
        rds1 = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1')
        rds2 = dns.rdataset.from_text('in', 'a', 30, '10.0.0.2')
        rdse = dns.rdataset.from_text('in', 'a', 30, '10.0.0.1', '10.0.0.2')
        rds1.union_update(rds2)
        self.assertEqual(rds1, rdse)

    def testIntersectionUpdate(self):
        rds1 = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1', '10.0.0.2')
        rds2 = dns.rdataset.from_text('in', 'a', 30, '10.0.0.2')
        rdse = dns.rdataset.from_text('in', 'a', 30, '10.0.0.2')
        rds1.intersection_update(rds2)
        self.assertEqual(rds1, rdse)

    def testNoEqualToOther(self):
        rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1')
        self.assertFalse(rds == 123)

    def testEmptyRdataList(self):
        self.assertRaises(ValueError,
                          lambda: dns.rdataset.from_rdata_list(300, []))

    def testToTextNoName(self):
        rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1')
        text = rds.to_text()
        self.assertEqual(text, '300 IN A 10.0.0.1')

    def testToTextOverrideClass(self):
        rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1')
        text = rds.to_text(override_rdclass=dns.rdataclass.NONE)
        self.assertEqual(text, '300 NONE A 10.0.0.1')

    def testRepr(self):
        rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1')
        self.assertEqual(repr(rds), "<DNS IN A rdataset: [<10.0.0.1>]>")

    def testTruncatedRepr(self):
        rds = dns.rdataset.from_text('in', 'txt', 300,
                                     'a' * 200)
        # * 99 not * 100 below as the " counts as one of the 100 chars
        self.assertEqual(repr(rds),
                         '<DNS IN TXT rdataset: [<"' + 'a' * 99 + '...>]>')

    def testStr(self):
        rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1')
        self.assertEqual(str(rds), "300 IN A 10.0.0.1")

    def testMultilineToText(self):
        rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1', '10.0.0.2')
        self.assertEqual(rds.to_text(), "300 IN A 10.0.0.1\n300 IN A 10.0.0.2")

    def testCoveredRepr(self):
        rds = dns.rdataset.from_text('in', 'rrsig', 300,
                                     'NSEC 1 3 3600 ' +
                                     '20190101000000 20030101000000 ' +
                                     '2143 foo Ym9ndXM=')
        # Using startswith as I don't care about the repr of the rdata,
        # just the covers
        self.assertTrue(repr(rds).startswith(
            '<DNS IN RRSIG(NSEC) rdataset:'))


class ImmutableRdatasetTestCase(unittest.TestCase):

    def test_basic(self):
        rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1', '10.0.0.2')
        rd = dns.rdata.from_text('in', 'a', '10.0.0.3')
        irds = dns.rdataset.ImmutableRdataset(rds)
        with self.assertRaises(TypeError):
            irds.update_ttl(100)
        with self.assertRaises(TypeError):
            irds.add(rd, 300)
        with self.assertRaises(TypeError):
            irds.union_update(rds)
        with self.assertRaises(TypeError):
            irds.intersection_update(rds)
        with self.assertRaises(TypeError):
            irds.update(rds)
        with self.assertRaises(TypeError):
            irds += rds
        with self.assertRaises(TypeError):
            irds -= rds
        with self.assertRaises(TypeError):
            irds &= rds
        with self.assertRaises(TypeError):
            irds |= rds
        with self.assertRaises(TypeError):
            del irds[0]
        with self.assertRaises(TypeError):
            irds.clear()

    def test_cloning(self):
        rds1 = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1', '10.0.0.2')
        rds1 = dns.rdataset.ImmutableRdataset(rds1)
        rds2 = dns.rdataset.from_text('in', 'a', 300, '10.0.0.2', '10.0.0.3')
        rds2 = dns.rdataset.ImmutableRdataset(rds2)
        expected = dns.rdataset.from_text('in', 'a', 300, '10.0.0.2')
        intersection = rds1.intersection(rds2)
        self.assertEqual(intersection, expected)

if __name__ == '__main__':
    unittest.main()
