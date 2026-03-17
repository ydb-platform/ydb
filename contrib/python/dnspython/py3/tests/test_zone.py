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

from io import BytesIO, StringIO
import difflib
import os
import sys
import unittest
from typing import cast

import dns.exception
import dns.message
import dns.name
import dns.node
import dns.rdata
import dns.rdataset
import dns.rdataclass
import dns.rdatatype
import dns.rrset
import dns.versioned
import dns.zone
import dns.node

from tests.util import here, here_out

example_text = """$TTL 3600
$ORIGIN example.
@ soa foo bar 1 2 3 4 5
@ ns ns1
@ ns ns2
ns1 a 10.0.0.1
ns2 a 10.0.0.2
$TTL 300
$ORIGIN foo.example.
bar mx 0 blaz
"""

example_text_output = """@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
"""

something_quite_similar = """@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.3
"""

something_different = """@ 3600 IN SOA fooa bar 1 2 3 4 5
@ 3600 IN NS ns11
@ 3600 IN NS ns21
bar.fooa 300 IN MX 0 blaz.fooa
ns11 3600 IN A 10.0.0.11
ns21 3600 IN A 10.0.0.21
"""

ttl_example_text = """$TTL 1h
$ORIGIN example.
@ soa foo bar 1 2 3 4 5
@ ns ns1
@ ns ns2
ns1 1d1s a 10.0.0.1
ns2 1w1D1h1m1S a 10.0.0.2
"""

# No $TTL so default TTL for RRs should be inherited from SOA minimum TTL (
# not from the last explicit RR TTL).
ttl_from_soa_text = """$ORIGIN example.
@ 1h soa foo bar 1 2 3 4 5
@ 1h ns ns1
@ 1h ns ns2
ns1 1w1D1h1m1S a 10.0.0.2
ns2 a 10.0.0.1
"""

# No $TTL and no SOA, so default TTL for RRs should be inherited from last
# explicit RR TTL.
ttl_from_last_text = """$ORIGIN example.
@ 1h ns ns1
@ 1h ns ns2
ns1 a 10.0.0.1
ns2 1w1D1h1m1S a 10.0.0.2
"""

# No $TTL and no SOA should raise SyntaxError as no TTL can be determined.
no_ttl_text = """$ORIGIN example.
@ ns ns1
@ ns ns2
ns1 a 10.0.0.1
ns2 a 10.0.0.2
"""

no_soa_text = """$TTL 1h
$ORIGIN example.
@ ns ns1
@ ns ns2
ns1 1d1s a 10.0.0.1
ns2 1w1D1h1m1S a 10.0.0.2
"""

no_ns_text = """$TTL 1h
$ORIGIN example.
@ soa foo bar 1 2 3 4 5
"""

include_text = """$INCLUDE "%s"
""" % here("example")

bad_directive_text = """$FOO bar
$ORIGIN example.
@ soa foo bar 1 2 3 4 5
@ ns ns1
@ ns ns2
ns1 1d1s a 10.0.0.1
ns2 1w1D1h1m1S a 10.0.0.2
"""

codec_text = """
@ soa foo bar 1 2 3 4 5
@ ns ns1
@ ns ns2
Königsgäßchen 300 NS Königsgäßchen
"""

misc_cases_input = """
$ORIGIN example.
$TTL 300

@ soa foo bar 1 2 3 4 5
@ ns ns1
@ ns ns2
out-of-zone. in a 10.0.0.1
"""

misc_cases_expected = """
$ORIGIN example.
$TTL 300
@ soa foo bar 1 2 3 4 5
@ ns ns1
@ ns ns2
"""

last_ttl_input = """
$ORIGIN example.
@ 300 ns ns1
@ 300 ns ns2
foo a 10.0.0.1
@ soa foo bar 1 2 3 4 5
"""

origin_sets_input = """
$ORIGIN example.
@ soa foo bar 1 2 3 4 5
@ 300 ns ns1
@ 300 ns ns2
"""

example_comments_text = """$TTL 3600
$ORIGIN example.
@ soa foo bar (1 ; not kept
2 3 4 5) ; kept
@ ns ns1
@ ns ns2
ns1 a 10.0.0.1 ; comment1
ns2 a 10.0.0.2 ; comment2
"""

example_comments_text_output = """@ 3600 IN SOA foo bar 1 2 3 4 5 ; kept
@ 3600 IN NS ns1
@ 3600 IN NS ns2
ns1 3600 IN A 10.0.0.1 ; comment1
ns2 3600 IN A 10.0.0.2 ; comment2
"""


example_cname = """$TTL 3600
$ORIGIN example.
@ soa foo bar (1 2 3 4 5)
@ ns ns1
@ ns ns2
ns1 a 10.0.0.1
ns2 a 10.0.0.2
www a 10.0.0.3
web cname www
    nsec @ CNAME RRSIG
    rrsig NSEC 1 3 3600 20200101000000 20030101000000 2143 foo MxFcby9k/yvedMfQgKzhH5er0Mu/vILz 45IkskceFGgiWCn/GxHhai6VAuHAoNUz 4YoU1tVfSCSqQYn6//11U6Nld80jEeC8 aTrO+KKmCaY=
    rrsig CNAME 1 3 3600 20200101000000 20030101000000 2143 foo MxFcby9k/yvedMfQgKzhH5er0Mu/vILz 45IkskceFGgiWCn/GxHhai6VAuHAoNUz 4YoU1tVfSCSqQYn6//11U6Nld80jEeC8 aTrO+KKmCaY=
web2 cname www
    nsec3 1 1 12 aabbccdd 2t7b4g4vsa5smi47k61mv5bv1a22bojr CNAME RRSIG
    rrsig NSEC3 1 3 3600 20200101000000 20030101000000 2143 foo MxFcby9k/yvedMfQgKzhH5er0Mu/vILz 45IkskceFGgiWCn/GxHhai6VAuHAoNUz 4YoU1tVfSCSqQYn6//11U6Nld80jEeC8 aTrO+KKmCaY=
    rrsig CNAME 1 3 3600 20200101000000 20030101000000 2143 foo MxFcby9k/yvedMfQgKzhH5er0Mu/vILz 45IkskceFGgiWCn/GxHhai6VAuHAoNUz 4YoU1tVfSCSqQYn6//11U6Nld80jEeC8 aTrO+KKmCaY=
"""


example_other_data = """$TTL 3600
$ORIGIN example.
@ soa foo bar (1 2 3 4 5)
@ ns ns1
@ ns ns2
ns1 a 10.0.0.1
ns2 a 10.0.0.2
www a 10.0.0.3
web a 10.0.0.4
    nsec @ A RRSIG
    rrsig A 1 3 3600 20200101000000 20030101000000 2143 foo MxFcby9k/yvedMfQgKzhH5er0Mu/vILz 45IkskceFGgiWCn/GxHhai6VAuHAoNUz 4YoU1tVfSCSqQYn6//11U6Nld80jEeC8 aTrO+KKmCaY=
    rrsig NSEC 1 3 3600 20200101000000 20030101000000 2143 foo MxFcby9k/yvedMfQgKzhH5er0Mu/vILz 45IkskceFGgiWCn/GxHhai6VAuHAoNUz 4YoU1tVfSCSqQYn6//11U6Nld80jEeC8 aTrO+KKmCaY=
"""

example_cname_and_other_data = """$TTL 3600
$ORIGIN example.
@ soa foo bar (1 2 3 4 5)
@ ns ns1
@ ns ns2
ns1 a 10.0.0.1
ns2 a 10.0.0.2
www a 10.0.0.3
web a 10.0.0.4
    cname www
    nsec @ A RRSIG
    rrsig A 1 3 3600 20200101000000 20030101000000 2143 foo MxFcby9k/yvedMfQgKzhH5er0Mu/vILz 45IkskceFGgiWCn/GxHhai6VAuHAoNUz 4YoU1tVfSCSqQYn6//11U6Nld80jEeC8 aTrO+KKmCaY=
    rrsig NSEC 1 3 3600 20200101000000 20030101000000 2143 foo MxFcby9k/yvedMfQgKzhH5er0Mu/vILz 45IkskceFGgiWCn/GxHhai6VAuHAoNUz 4YoU1tVfSCSqQYn6//11U6Nld80jEeC8 aTrO+KKmCaY=
"""

_keep_output = True

def _rdata_sort(a):
    return (a[0], a[2].rdclass, a[2].to_text())

def add_rdataset(msg, name, rds):
    rrset = msg.get_rrset(msg.answer, name, rds.rdclass, rds.rdtype,
                          create=True, force_unique=True)
    for rd in rds:
        rrset.add(rd, ttl=rds.ttl)

def make_xfr(zone):
    q = dns.message.make_query(zone.origin, 'AXFR')
    msg = dns.message.make_response(q)
    if zone.relativize:
        msg.origin = zone.origin
        soa_name = dns.name.empty
    else:
        soa_name = zone.origin
    soa = zone.find_rdataset(soa_name, 'SOA')
    add_rdataset(msg, soa_name, soa)
    for (name, rds) in zone.iterate_rdatasets():
        if rds.rdtype == dns.rdatatype.SOA:
            continue
        add_rdataset(msg, name, rds)
    add_rdataset(msg, soa_name, soa)
    return [msg]

def compare_files(test_name, a_name, b_name):
    with open(a_name, 'r') as a:
        with open(b_name, 'r') as b:
            differences = list(difflib.unified_diff(a.readlines(),
                                                    b.readlines()))
            if len(differences) == 0:
                return True
            else:
                print(f'{test_name} differences:')
                sys.stdout.writelines(differences)
                return False

class ZoneTestCase(unittest.TestCase):

    def testFromFile1(self):
        z = dns.zone.from_file(here('example'), 'example')
        ok = False
        try:
            z.to_file(here_out('example1.out'), nl=b'\x0a')
            ok = compare_files('testFromFile1',
                               here_out('example1.out'),
                               here('example1.good'))
        finally:
            if not _keep_output:
                os.unlink(here_out('example1.out'))
        self.assertTrue(ok)

    def testFromFile2(self):
        z = dns.zone.from_file(here('example'), 'example', relativize=False)
        ok = False
        try:
            z.to_file(here_out('example2.out'), relativize=False, nl=b'\x0a')
            ok = compare_files('testFromFile2',
                               here_out('example2.out'),
                               here('example2.good'))
        finally:
            if not _keep_output:
                os.unlink(here_out('example2.out'))
        self.assertTrue(ok)

    def testToFileTextualStream(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        f = StringIO()
        z.to_file(f)
        out = f.getvalue()
        f.close()
        self.assertEqual(out, example_text_output)

    def testToFileBinaryStream(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        f = BytesIO()
        z.to_file(f, nl=b'\n')
        out = f.getvalue()
        f.close()
        self.assertEqual(out, example_text_output.encode())

    def testToFileTextual(self):
        z = dns.zone.from_file(here('example'), 'example')
        try:
            f = open(here_out('example3-textual.out'), 'w')
            z.to_file(f)
            f.close()
            ok = compare_files('testToFileTextual',
                               here_out('example3-textual.out'),
                               here('example3.good'))
        finally:
            if not _keep_output:
                os.unlink(here_out('example3-textual.out'))
        self.assertTrue(ok)

    def testToFileBinary(self):
        z = dns.zone.from_file(here('example'), 'example')
        try:
            f = open(here_out('example3-binary.out'), 'wb')
            z.to_file(f)
            f.close()
            ok = compare_files('testToFileBinary',
                               here_out('example3-binary.out'),
                               here('example3.good'))
        finally:
            if not _keep_output:
                os.unlink(here_out('example3-binary.out'))
        self.assertTrue(ok)

    def testToFileFilename(self):
        z = dns.zone.from_file(here('example'), 'example')
        try:
            z.to_file(here_out('example3-filename.out'))
            ok = compare_files('testToFileFilename',
                               here_out('example3-filename.out'),
                               here('example3.good'))
        finally:
            if not _keep_output:
                os.unlink(here_out('example3-filename.out'))
        self.assertTrue(ok)

    def testToText(self):
        z = dns.zone.from_file(here('example'), 'example')
        ok = False
        try:
            text_zone = z.to_text(nl='\x0a')
            f = open(here_out('example3.out'), 'w')
            f.write(text_zone)
            f.close()
            ok = compare_files('testToText',
                               here_out('example3.out'),
                               here('example3.good'))
        finally:
            if not _keep_output:
                os.unlink(here_out('example3.out'))
        self.assertTrue(ok)

    def testToFileTextualWithOrigin(self):
        z = dns.zone.from_file(here('example'), 'example')
        try:
            f = open(here_out('example4-textual.out'), 'w')
            z.to_file(f, want_origin=True)
            f.close()
            ok = compare_files('testToFileTextualWithOrigin',
                               here_out('example4-textual.out'),
                               here('example4.good'))
        finally:
            if not _keep_output:
                os.unlink(here_out('example4-textual.out'))
        self.assertTrue(ok)

    def testToFileBinaryWithOrigin(self):
        z = dns.zone.from_file(here('example'), 'example')
        try:
            f = open(here_out('example4-binary.out'), 'wb')
            z.to_file(f, want_origin=True)
            f.close()
            ok = compare_files('testToFileBinaryWithOrigin',
                               here_out('example4-binary.out'),
                               here('example4.good'))
        finally:
            if not _keep_output:
                os.unlink(here_out('example4-binary.out'))
        self.assertTrue(ok)

    def testFromText(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        f = StringIO()
        names = list(z.nodes.keys())
        names.sort()
        for n in names:
            f.write(z[n].to_text(n))
            f.write('\n')
        self.assertEqual(f.getvalue(), example_text_output)

    def testTorture1(self):
        #
        # Read a zone containing all our supported RR types, and
        # for each RR in the zone, convert the rdata into wire format
        # and then back out, and see if we get equal rdatas.
        #
        f = BytesIO()
        o = dns.name.from_text('example.')
        z = dns.zone.from_file(here('example'), o)
        for node in z.values():
            for rds in node:
                for rd in rds:
                    f.seek(0)
                    f.truncate()
                    rd.to_wire(f, origin=o)
                    wire = f.getvalue()
                    rd2 = dns.rdata.from_wire(rds.rdclass, rds.rdtype,
                                              wire, 0, len(wire),
                                              origin=o)
                    self.assertEqual(rd, rd2)

    def testEqual(self):
        z1 = dns.zone.from_text(example_text, 'example.', relativize=True)
        z2 = dns.zone.from_text(example_text_output, 'example.',
                                relativize=True)
        self.assertEqual(z1, z2)

    def testNotEqual1(self):
        z1 = dns.zone.from_text(example_text, 'example.', relativize=True)
        z2 = dns.zone.from_text(something_quite_similar, 'example.',
                                relativize=True)
        self.assertNotEqual(z1, z2)

    def testNotEqual2(self):
        z1 = dns.zone.from_text(example_text, 'example.', relativize=True)
        z2 = dns.zone.from_text(something_different, 'example.',
                                relativize=True)
        self.assertNotEqual(z1, z2)

    def testNotEqual3(self):
        z1 = dns.zone.from_text(example_text, 'example.', relativize=True)
        z2 = dns.zone.from_text(something_different, 'example2.',
                                relativize=True)
        self.assertNotEqual(z1, z2)

    def testFindRdataset1(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        rds = z.find_rdataset('@', 'soa')
        exrds = dns.rdataset.from_text('IN', 'SOA', 300, 'foo bar 1 2 3 4 5')
        self.assertEqual(rds, exrds)

    def testFindRdataset2(self):
        def bad():
            z = dns.zone.from_text(example_text, 'example.', relativize=True)
            z.find_rdataset('@', 'loc')
        self.assertRaises(KeyError, bad)

    def testFindRRset1(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        rrs = z.find_rrset('@', 'soa')
        exrrs = dns.rrset.from_text('@', 300, 'IN', 'SOA', 'foo bar 1 2 3 4 5')
        self.assertEqual(rrs, exrrs)

    def testFindRRset2(self):
        def bad():
            z = dns.zone.from_text(example_text, 'example.', relativize=True)
            z.find_rrset('@', 'loc')
        self.assertRaises(KeyError, bad)

    def testGetRdataset1(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        rds = z.get_rdataset('@', 'soa')
        exrds = dns.rdataset.from_text('IN', 'SOA', 300, 'foo bar 1 2 3 4 5')
        self.assertEqual(rds, exrds)

    def testGetRdataset2(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        rds = z.get_rdataset('@', 'loc')
        self.assertTrue(rds is None)

    def testGetRdatasetWithRelativeNameFromAbsoluteZone(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=False)
        rds = z.get_rdataset(dns.name.empty, 'soa')
        self.assertIsNotNone(rds)
        exrds = dns.rdataset.from_text('IN', 'SOA', 300, 'foo.example. bar.example. 1 2 3 4 5')
        self.assertEqual(rds, exrds)

    def testGetRRset1(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        rrs = z.get_rrset('@', 'soa')
        exrrs = dns.rrset.from_text('@', 300, 'IN', 'SOA', 'foo bar 1 2 3 4 5')
        self.assertEqual(rrs, exrrs)

    def testGetRRset2(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        rrs = z.get_rrset('@', 'loc')
        self.assertTrue(rrs is None)

    def testReplaceRdataset1(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        rdataset = dns.rdataset.from_text('in', 'ns', 300, 'ns3', 'ns4')
        z.replace_rdataset('@', rdataset)
        rds = z.get_rdataset('@', 'ns')
        self.assertTrue(rds is rdataset)

    def testReplaceRdataset2(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        rdataset = dns.rdataset.from_text('in', 'txt', 300, '"foo"')
        z.replace_rdataset('@', rdataset)
        rds = z.get_rdataset('@', 'txt')
        self.assertTrue(rds is rdataset)

    def testDeleteRdataset1(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        z.delete_rdataset('@', 'ns')
        rds = z.get_rdataset('@', 'ns')
        self.assertTrue(rds is None)

    def testDeleteRdataset2(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        z.delete_rdataset('ns1', 'a')
        node = z.get_node('ns1')
        self.assertTrue(node is None)

    def testNodeFindRdataset1(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        node = z['@']
        rds = node.find_rdataset(dns.rdataclass.IN, dns.rdatatype.SOA)
        exrds = dns.rdataset.from_text('IN', 'SOA', 300, 'foo bar 1 2 3 4 5')
        self.assertEqual(rds, exrds)

    def testNodeFindRdataset2(self):
        def bad():
            z = dns.zone.from_text(example_text, 'example.', relativize=True)
            node = z['@']
            node.find_rdataset(dns.rdataclass.IN, dns.rdatatype.LOC)
        self.assertRaises(KeyError, bad)

    def testNodeFindRdataset3(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        node = z['@']
        rds = node.find_rdataset(dns.rdataclass.IN, dns.rdatatype.RRSIG,
                                 dns.rdatatype.A, create=True)
        self.assertEqual(rds.rdclass, dns.rdataclass.IN)
        self.assertEqual(rds.rdtype, dns.rdatatype.RRSIG)
        self.assertEqual(rds.covers, dns.rdatatype.A)

    def testNodeGetRdataset1(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        node = z['@']
        rds = node.get_rdataset(dns.rdataclass.IN, dns.rdatatype.SOA)
        exrds = dns.rdataset.from_text('IN', 'SOA', 300, 'foo bar 1 2 3 4 5')
        self.assertEqual(rds, exrds)

    def testNodeGetRdataset2(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        node = z['@']
        rds = node.get_rdataset(dns.rdataclass.IN, dns.rdatatype.LOC)
        self.assertTrue(rds is None)

    def testNodeDeleteRdataset1(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        node = z['@']
        node.delete_rdataset(dns.rdataclass.IN, dns.rdatatype.SOA)
        rds = node.get_rdataset(dns.rdataclass.IN, dns.rdatatype.SOA)
        self.assertTrue(rds is None)

    def testNodeDeleteRdataset2(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        node = z['@']
        node.delete_rdataset(dns.rdataclass.IN, dns.rdatatype.LOC)
        rds = node.get_rdataset(dns.rdataclass.IN, dns.rdatatype.LOC)
        self.assertTrue(rds is None)

    def testIterateNodes(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        count = 0
        for n in z:
            count += 1
        self.assertEqual(count, 4)

    def testIterateRdatasets(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        ns = [n for n, r in z.iterate_rdatasets('A')]
        ns.sort()
        self.assertEqual(ns, [dns.name.from_text('ns1', None),
                              dns.name.from_text('ns2', None)])

    def testIterateAllRdatasets(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        ns = [n for n, r in z.iterate_rdatasets()]
        ns.sort()
        self.assertEqual(ns, [dns.name.from_text('@', None),
                              dns.name.from_text('@', None),
                              dns.name.from_text('bar.foo', None),
                              dns.name.from_text('ns1', None),
                              dns.name.from_text('ns2', None)])

    def testIterateRdatas(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        l = list(z.iterate_rdatas('A'))
        l.sort()
        exl = [(dns.name.from_text('ns1', None),
                3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.1')),
               (dns.name.from_text('ns2', None),
                3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.2'))]
        self.assertEqual(l, exl)

    def testIterateAllRdatas(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        l = list(z.iterate_rdatas())
        l.sort(key=_rdata_sort)
        exl = [(dns.name.from_text('@', None),
                3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns1')),
               (dns.name.from_text('@', None),
                3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns2')),
               (dns.name.from_text('@', None),
                3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SOA,
                                    'foo bar 1 2 3 4 5')),
               (dns.name.from_text('bar.foo', None),
                300,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.MX,
                                    '0 blaz.foo')),
               (dns.name.from_text('ns1', None),
                3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.1')),
               (dns.name.from_text('ns2', None),
                3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.2'))]
        exl.sort(key=_rdata_sort)
        self.assertEqual(l, exl)

    def testNodeGetSetDel(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        n = z.node_factory()
        rds = dns.rdataset.from_text('IN', 'A', 300, '10.0.0.1')
        n.replace_rdataset(rds)
        z['foo'] = n
        self.assertTrue(z.find_rdataset('foo', 'A') is rds)
        self.assertEqual(z['foo'], n)
        self.assertEqual(z.get('foo'), n)
        del z['foo']
        self.assertEqual(z.get('foo'), None)
        with self.assertRaises(KeyError):
            z[123] = n
        with self.assertRaises(KeyError):
            z['foo.'] = n
        with self.assertRaises(KeyError):
            bn = z.find_node('bar')
        bn = z.find_node('bar', True)
        self.assertTrue(isinstance(bn, dns.node.Node))
        # The next two tests pass by not raising KeyError
        z.delete_node('foo')
        z.delete_node('bar')

    def testBadReplacement(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        rds = dns.rdataset.from_text('CH', 'TXT', 300, 'hi')
        def bad():
            z.replace_rdataset('foo', rds)
        self.assertRaises(ValueError, bad)

    def testTTLs(self):
        z = dns.zone.from_text(ttl_example_text, 'example.', relativize=True)
        n = z['@'] # type: dns.node.Node
        rds = cast(dns.rdataset.Rdataset, n.get_rdataset(dns.rdataclass.IN, dns.rdatatype.SOA))
        self.assertEqual(rds.ttl, 3600)
        n = z['ns1']
        rds = cast(dns.rdataset.Rdataset, n.get_rdataset(dns.rdataclass.IN, dns.rdatatype.A))
        self.assertEqual(rds.ttl, 86401)
        n = z['ns2']
        rds = cast(dns.rdataset.Rdataset, n.get_rdataset(dns.rdataclass.IN, dns.rdatatype.A))
        self.assertEqual(rds.ttl, 694861)

    def testTTLFromSOA(self):
        z = dns.zone.from_text(ttl_from_soa_text, 'example.', relativize=True)
        n = z['@']
        rds = cast(dns.rdataset.Rdataset, n.get_rdataset(dns.rdataclass.IN, dns.rdatatype.SOA))
        self.assertEqual(rds.ttl, 3600)
        soa_rd = rds[0]
        n = z['ns1']
        rds = cast(dns.rdataset.Rdataset, n.get_rdataset(dns.rdataclass.IN, dns.rdatatype.A))
        self.assertEqual(rds.ttl, 694861)
        n = z['ns2']
        rds = cast(dns.rdataset.Rdataset, n.get_rdataset(dns.rdataclass.IN, dns.rdatatype.A))
        self.assertEqual(rds.ttl, soa_rd.minimum)

    def testTTLFromLast(self):
        z = dns.zone.from_text(ttl_from_last_text, 'example.', check_origin=False)
        n = z['@']
        rds = cast(dns.rdataset.Rdataset, n.get_rdataset(dns.rdataclass.IN, dns.rdatatype.NS))
        self.assertEqual(rds.ttl, 3600)
        n = z['ns1']
        rds = cast(dns.rdataset.Rdataset, n.get_rdataset(dns.rdataclass.IN, dns.rdatatype.A))
        self.assertEqual(rds.ttl, 3600)
        n = z['ns2']
        rds = cast(dns.rdataset.Rdataset, n.get_rdataset(dns.rdataclass.IN, dns.rdatatype.A))
        self.assertEqual(rds.ttl, 694861)

    def testNoTTL(self):
        def bad():
            dns.zone.from_text(no_ttl_text, 'example.', check_origin=False)
        self.assertRaises(dns.exception.SyntaxError, bad)

    def testNoSOA(self):
        def bad():
            dns.zone.from_text(no_soa_text, 'example.', relativize=True)
        self.assertRaises(dns.zone.NoSOA, bad)

    def testNoNS(self):
        def bad():
            dns.zone.from_text(no_ns_text, 'example.', relativize=True)
        self.assertRaises(dns.zone.NoNS, bad)

    def testInclude(self):
        z1 = dns.zone.from_text(include_text, 'example.', relativize=True,
                                allow_include=True)
        z2 = dns.zone.from_file(here('example'), 'example.', relativize=True)
        self.assertEqual(z1, z2)

    def testBadDirective(self):
        def bad():
            dns.zone.from_text(bad_directive_text, 'example.', relativize=True)
        self.assertRaises(dns.exception.SyntaxError, bad)

    def testFirstRRStartsWithWhitespace(self):
        # no name is specified, so default to the initial origin
        z = dns.zone.from_text(' 300 IN A 10.0.0.1', origin='example.',
                               check_origin=False)
        n = z['@']
        rds = cast(dns.rdataset.Rdataset, n.get_rdataset(dns.rdataclass.IN, dns.rdatatype.A))
        self.assertEqual(rds.ttl, 300)

    def testZoneOrigin(self):
        z = dns.zone.Zone('example.')
        self.assertEqual(z.origin, dns.name.from_text('example.'))
        def bad1():
            o = dns.name.from_text('example', None)
            dns.zone.Zone(o)
        self.assertRaises(ValueError, bad1)
        def bad2():
            dns.zone.Zone(cast(str, 1.0))
        self.assertRaises(ValueError, bad2)

    def testZoneOriginNone(self):
        dns.zone.Zone(cast(str, None))

    def testZoneFromXFR(self):
        z1_abs = dns.zone.from_text(example_text, 'example.', relativize=False)
        z2_abs = dns.zone.from_xfr(make_xfr(z1_abs), relativize=False)
        self.assertEqual(z1_abs, z2_abs)

        z1_rel = dns.zone.from_text(example_text, 'example.', relativize=True)
        z2_rel = dns.zone.from_xfr(make_xfr(z1_rel), relativize=True)
        self.assertEqual(z1_rel, z2_rel)

    def testCodec2003(self):
        z = dns.zone.from_text(codec_text, 'example.', relativize=True)
        n2003 = dns.name.from_text('xn--knigsgsschen-lcb0w', None)
        n2008 = dns.name.from_text('xn--knigsgchen-b4a3dun', None)
        self.assertTrue(n2003 in z)
        self.assertFalse(n2008 in z)
        rrs = z.find_rrset(n2003, 'NS')
        self.assertEqual(rrs[0].target, n2003)

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def testCodec2008(self):
        z = dns.zone.from_text(codec_text, 'example.', relativize=True,
                               idna_codec=dns.name.IDNA_2008)
        n2003 = dns.name.from_text('xn--knigsgsschen-lcb0w', None)
        n2008 = dns.name.from_text('xn--knigsgchen-b4a3dun', None)
        self.assertFalse(n2003 in z)
        self.assertTrue(n2008 in z)
        rrs = z.find_rrset(n2008, 'NS')
        self.assertEqual(rrs[0].target, n2008)

    def testZoneMiscCases(self):
        # test that leading whitespace followed by EOL is treated like
        # a blank line, and that out-of-zone names are dropped.
        z1 = dns.zone.from_text(misc_cases_input, 'example.')
        z2 = dns.zone.from_text(misc_cases_expected, 'example.')
        self.assertEqual(z1, z2)

    def testUnknownOrigin(self):
        def bad():
            dns.zone.from_text('foo 300 in a 10.0.0.1')
        self.assertRaises(dns.zone.UnknownOrigin, bad)

    def testBadClass(self):
        def bad():
            dns.zone.from_text('foo 300 ch txt hi', 'example.')
        self.assertRaises(dns.exception.SyntaxError, bad)

    def testUnknownRdatatype(self):
        def bad():
            dns.zone.from_text('foo 300 BOGUSTYPE hi', 'example.')
        self.assertRaises(dns.exception.SyntaxError, bad)

    def testDangling(self):
        def bad1():
            dns.zone.from_text('foo', 'example.')
        self.assertRaises(dns.exception.SyntaxError, bad1)
        def bad2():
            dns.zone.from_text('foo 300', 'example.')
        self.assertRaises(dns.exception.SyntaxError, bad2)
        def bad3():
            dns.zone.from_text('foo 300 in', 'example.')
        self.assertRaises(dns.exception.SyntaxError, bad3)
        def bad4():
            dns.zone.from_text('foo 300 in a', 'example.')
        self.assertRaises(dns.exception.SyntaxError, bad4)
        def bad5():
            dns.zone.from_text('$TTL', 'example.')
        self.assertRaises(dns.exception.SyntaxError, bad5)
        def bad6():
            dns.zone.from_text('$ORIGIN', 'example.')
        self.assertRaises(dns.exception.SyntaxError, bad6)

    def testUseLastTTL(self):
        z = dns.zone.from_text(last_ttl_input, 'example.')
        rds = z.find_rdataset('foo', 'A')
        self.assertEqual(rds.ttl, 300)

    def testDollarOriginSetsZoneOriginIfUnknown(self):
        z = dns.zone.from_text(origin_sets_input)
        self.assertEqual(z.origin, dns.name.from_text('example'))

    def testValidateNameRelativizesNameInZone(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        self.assertEqual(z._validate_name('foo.bar.example.'),
                         dns.name.from_text('foo.bar', None))

    def testComments(self):
        z = dns.zone.from_text(example_comments_text, 'example.',
                               relativize=True)
        f = StringIO()
        z.to_file(f, want_comments=True)
        out = f.getvalue()
        f.close()
        self.assertEqual(out, example_comments_text_output)

    def testUncomparable(self):
        z = dns.zone.from_text(example_comments_text, 'example.',
                               relativize=True)
        self.assertFalse(z == 'a')

    def testUnsorted(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True)
        f = StringIO()
        z.to_file(f, sorted=False)
        out = f.getvalue()
        f.close()
        z2 = dns.zone.from_text(out, 'example.', relativize=True)
        self.assertEqual(z, z2)

    def testNodeReplaceRdatasetConvertsRRsets(self):
        node = dns.node.Node()
        rrs = dns.rrset.from_text('foo', 300, 'in', 'a', '10.0.0.1')
        node.replace_rdataset(rrs)
        rds = node.find_rdataset(dns.rdataclass.IN, dns.rdatatype.A)
        self.assertEqual(rds, rrs)
        self.assertTrue(rds is not rrs)
        self.assertFalse(isinstance(rds, dns.rrset.RRset))

    def testCnameAndOtherDataAddOther(self):
        z = dns.zone.from_text(example_cname, 'example.', relativize=True)
        rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1')
        z.replace_rdataset('web', rds)
        z.replace_rdataset('web2', rds.copy())
        n = z.find_node('web')
        self.assertEqual(len(n.rdatasets), 3)
        self.assertEqual(n.find_rdataset(dns.rdataclass.IN, dns.rdatatype.A),
                         rds)
        self.assertIsNotNone(n.get_rdataset(dns.rdataclass.IN,
                                            dns.rdatatype.NSEC))
        self.assertIsNotNone(n.get_rdataset(dns.rdataclass.IN,
                                            dns.rdatatype.RRSIG,
                                            dns.rdatatype.NSEC))
        n = z.find_node('web2')
        self.assertEqual(len(n.rdatasets), 3)
        self.assertEqual(n.find_rdataset(dns.rdataclass.IN, dns.rdatatype.A),
                         rds)
        self.assertIsNotNone(n.get_rdataset(dns.rdataclass.IN,
                                            dns.rdatatype.NSEC3))
        self.assertIsNotNone(n.get_rdataset(dns.rdataclass.IN,
                                            dns.rdatatype.RRSIG,
                                            dns.rdatatype.NSEC3))

    def testCnameAndOtherDataAddCname(self):
        z = dns.zone.from_text(example_other_data, 'example.', relativize=True)
        rds = dns.rdataset.from_text('in', 'cname', 300, 'www')
        z.replace_rdataset('web', rds)
        n = z.find_node('web')
        self.assertEqual(len(n.rdatasets), 3)
        self.assertEqual(n.find_rdataset(dns.rdataclass.IN,
                                         dns.rdatatype.CNAME),
                         rds)
        self.assertIsNotNone(n.get_rdataset(dns.rdataclass.IN,
                                            dns.rdatatype.NSEC))
        self.assertIsNotNone(n.get_rdataset(dns.rdataclass.IN,
                                            dns.rdatatype.RRSIG,
                                            dns.rdatatype.NSEC))

    def testCnameAndOtherDataInZonefile(self):
        with self.assertRaises(dns.zonefile.CNAMEAndOtherData):
            dns.zone.from_text(example_cname_and_other_data, 'example.',
                               relativize=True)

    def testNameInZoneWithStr(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=False)
        self.assertTrue('ns1.example.' in z)
        self.assertTrue('bar.foo.example.' in z)

    def testNameInZoneWhereNameIsNotValid(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=False)
        with self.assertRaises(KeyError):
            self.assertTrue(1 in z)


class VersionedZoneTestCase(unittest.TestCase):
    def testUseTransaction(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True,
                               zone_factory=dns.versioned.Zone)
        with self.assertRaises(dns.versioned.UseTransaction):
            z.find_node('not_there', True)
        with self.assertRaises(dns.versioned.UseTransaction):
            z.delete_node('not_there')
        with self.assertRaises(dns.versioned.UseTransaction):
            z.find_rdataset('not_there', 'a', create=True)
        with self.assertRaises(dns.versioned.UseTransaction):
            z.get_rdataset('not_there', 'a', create=True)
        with self.assertRaises(dns.versioned.UseTransaction):
            z.delete_rdataset('not_there', 'a')
        with self.assertRaises(dns.versioned.UseTransaction):
            z.replace_rdataset('not_there', None)

    def testImmutableNodes(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True,
                               zone_factory=dns.versioned.Zone)
        node = z.find_node('@')
        with self.assertRaises(TypeError):
            node.find_rdataset(dns.rdataclass.IN, dns.rdatatype.RP,
                               create=True)
        with self.assertRaises(TypeError):
            node.get_rdataset(dns.rdataclass.IN, dns.rdatatype.RP,
                               create=True)
        with self.assertRaises(TypeError):
            node.delete_rdataset(dns.rdataclass.IN, dns.rdatatype.SOA)
        with self.assertRaises(TypeError):
            node.replace_rdataset(None)

    def testSelectDefaultPruningPolicy(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True,
                               zone_factory=dns.versioned.Zone)
        z.set_pruning_policy(None)
        self.assertEqual(z._pruning_policy, z._default_pruning_policy)

    def testSetAlternatePruningPolicyInConstructor(self):
        def never_prune(version):
            return False
        z = dns.versioned.Zone('example', pruning_policy=never_prune)
        self.assertEqual(z._pruning_policy, never_prune)

    def testCannotSpecifyBothSerialAndVersionIdToReader(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True,
                               zone_factory=dns.versioned.Zone)
        with self.assertRaises(ValueError):
            z.reader(1, 1)

    def testUnknownVersion(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True,
                               zone_factory=dns.versioned.Zone)
        with self.assertRaises(KeyError):
            z.reader(99999)

    def testUnknownSerial(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=True,
                               zone_factory=dns.versioned.Zone)
        with self.assertRaises(KeyError):
            z.reader(serial=99999)

    def testNoRelativizeReader(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=False,
                               zone_factory=dns.versioned.Zone)
        with z.reader(serial=1) as txn:
            rds = txn.get('example.', 'soa')
            self.assertEqual(rds[0].serial, 1)

    def testNoRelativizeReaderOriginInText(self):
        z = dns.zone.from_text(example_text, relativize=False,
                               zone_factory=dns.versioned.Zone)
        with z.reader(serial=1) as txn:
            rds = txn.get('example.', 'soa')
            self.assertEqual(rds[0].serial, 1)

    def testNoRelativizeReaderAbsoluteGet(self):
        z = dns.zone.from_text(example_text, 'example.', relativize=False,
                               zone_factory=dns.versioned.Zone)
        with z.reader(serial=1) as txn:
            rds = txn.get(dns.name.empty, 'soa')
            self.assertEqual(rds[0].serial, 1)

    def testCnameAndOtherDataAddOther(self):
        z = dns.zone.from_text(example_cname, 'example.', relativize=True,
                               zone_factory=dns.versioned.Zone)
        rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1')
        with z.writer() as txn:
            txn.replace('web', rds)
            txn.replace('web2', rds.copy())
        n = z.find_node('web')
        self.assertEqual(len(n.rdatasets), 3)
        self.assertEqual(n.find_rdataset(dns.rdataclass.IN, dns.rdatatype.A),
                         rds)
        self.assertIsNotNone(n.get_rdataset(dns.rdataclass.IN,
                                            dns.rdatatype.NSEC))
        self.assertIsNotNone(n.get_rdataset(dns.rdataclass.IN,
                                            dns.rdatatype.RRSIG,
                                            dns.rdatatype.NSEC))
        n = z.find_node('web2')
        self.assertEqual(len(n.rdatasets), 3)
        self.assertEqual(n.find_rdataset(dns.rdataclass.IN, dns.rdatatype.A),
                         rds)
        self.assertIsNotNone(n.get_rdataset(dns.rdataclass.IN,
                                            dns.rdatatype.NSEC3))
        self.assertIsNotNone(n.get_rdataset(dns.rdataclass.IN,
                                            dns.rdatatype.RRSIG,
                                            dns.rdatatype.NSEC3))

    def testCnameAndOtherDataAddCname(self):
        z = dns.zone.from_text(example_other_data, 'example.', relativize=True,
                               zone_factory=dns.versioned.Zone)
        rds = dns.rdataset.from_text('in', 'cname', 300, 'www')
        with z.writer() as txn:
            txn.replace('web', rds)
        n = z.find_node('web')
        self.assertEqual(len(n.rdatasets), 3)
        self.assertEqual(n.find_rdataset(dns.rdataclass.IN,
                                         dns.rdatatype.CNAME),
                         rds)
        self.assertIsNotNone(n.get_rdataset(dns.rdataclass.IN,
                                            dns.rdatatype.NSEC))
        self.assertIsNotNone(n.get_rdataset(dns.rdataclass.IN,
                                            dns.rdatatype.RRSIG,
                                            dns.rdatatype.NSEC))


if __name__ == '__main__':
    unittest.main()
