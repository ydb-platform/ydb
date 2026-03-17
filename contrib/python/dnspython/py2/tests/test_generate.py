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

import sys
sys.path.insert(0, '../')  # Force the local project to be *the* dns

import unittest

import dns.exception
import dns.rdata
import dns.rdataclass
import dns.rdatatype
import dns.rrset
import dns.zone
from dns._compat import long

import pprint


pp = pprint.PrettyPrinter(indent=2)

example_text = """$TTL 1h
$ORIGIN 0.0.192.IN-ADDR.ARPA.
$GENERATE 1-2 0 CNAME SERVER$.EXAMPLE.
"""

example_text1 = """$TTL 1h
$ORIGIN 0.0.192.IN-ADDR.ARPA.
$GENERATE 1-10 fooo$ CNAME $.0
"""

example_text2 = """$TTL 1h
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
$GENERATE 3-5 foo$ A 10.0.0.$
"""

example_text3 = """$TTL 1h
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
$GENERATE 4-8/2 foo$ A 10.0.0.$
"""

example_text4 = """$TTL 1h
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
$GENERATE 11-13 wp-db${-10,2,d}.services.mozilla.com 0 CNAME SERVER.FOOBAR.
"""

example_text5 = """$TTL 1h
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
$GENERATE 11-13 wp-db${10,2,d}.services.mozilla.com 0 CNAME SERVER.FOOBAR.
"""

example_text6 = """$TTL 1h
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
$GENERATE 11-13 wp-db${+10,2,d}.services.mozilla.com 0 CNAME SERVER.FOOBAR.
"""

example_text7 = """$TTL 1h
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
$GENERATE 11-13     sync${-10}.db   IN  A   10.10.16.0
"""

example_text8 = """$TTL 1h
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
$GENERATE 11-12 wp-db${-10,2,d} IN A 10.10.16.0
"""

example_text9 = """$TTL 1h
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
$GENERATE 11-12 wp-db${-10,2,d} IN A 10.10.16.0
$GENERATE 11-13     sync${-10}.db   IN  A   10.10.16.0
"""
example_text10 = """$TTL 1h
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
$GENERATE 27-28 $.2 PTR zlb${-26}.oob
"""

def _rdata_sort(a):
    return (a[0], a[2].rdclass, a[2].to_text())


class GenerateTestCase(unittest.TestCase):

    def testFromText(self): # type: () -> None
        def bad(): # type: () -> None
            dns.zone.from_text(example_text, 'example.', relativize=True)
        self.failUnlessRaises(dns.zone.NoSOA, bad)

    def testFromText1(self): # type: () -> None
        def bad(): # type: () -> None
            dns.zone.from_text(example_text1, 'example.', relativize=True)
        self.failUnlessRaises(dns.zone.NoSOA, bad)

    def testIterateAllRdatas2(self): # type: () -> None
        z = dns.zone.from_text(example_text2, 'example.', relativize=True)
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
                                    '10.0.0.2')),
                (dns.name.from_text('foo3', None),
                3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.3')),
                (dns.name.from_text('foo4', None),
                3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.4')),
                (dns.name.from_text('foo5', None),
                3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.5'))]

        exl.sort(key=_rdata_sort)
        self.failUnless(l == exl)

    def testIterateAllRdatas3(self): # type: () -> None
        z = dns.zone.from_text(example_text3, 'example.', relativize=True)
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
                                    '10.0.0.2')),
                (dns.name.from_text('foo4', None), 3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.4')),
                (dns.name.from_text('foo6', None), 3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.6')),
                (dns.name.from_text('foo8', None), 3600,
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.8'))]
        exl.sort(key=_rdata_sort)
        self.failUnless(l == exl)
    def testGenerate1(self): # type: () -> None
        z = dns.zone.from_text(example_text4, 'example.', relativize=True)
        l = list(z.iterate_rdatas())
        l.sort(key=_rdata_sort)
        exl = [(dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns1')),
               (dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns2')),
               (dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SOA,
                                    'foo bar 1 2 3 4 5')),
               (dns.name.from_text('bar.foo', None),
                long(300),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.MX,
                                    '0 blaz.foo')),
               (dns.name.from_text('ns1', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.1')),
               (dns.name.from_text('ns2', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.2')),

                (dns.name.from_text('wp-db01.services.mozilla.com', None),
                    long(0),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.CNAME,
                                    'SERVER.FOOBAR.')),

                (dns.name.from_text('wp-db02.services.mozilla.com', None),
                    long(0),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.CNAME,
                                    'SERVER.FOOBAR.')),

                (dns.name.from_text('wp-db03.services.mozilla.com', None),
                    long(0),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.CNAME,
                                    'SERVER.FOOBAR.'))]
        exl.sort(key=_rdata_sort)
        self.assertEqual(l, exl)

    def testGenerate2(self): # type: () -> None
        z = dns.zone.from_text(example_text5, 'example.', relativize=True)
        l = list(z.iterate_rdatas())
        l.sort(key=_rdata_sort)
        exl = [(dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns1')),
               (dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns2')),
               (dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SOA,
                                    'foo bar 1 2 3 4 5')),
               (dns.name.from_text('bar.foo', None),
                long(300),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.MX,
                                    '0 blaz.foo')),
               (dns.name.from_text('ns1', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.1')),
               (dns.name.from_text('ns2', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.2')),

                (dns.name.from_text('wp-db21.services.mozilla.com', None), long(0),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.CNAME,
                                    'SERVER.FOOBAR.')),

                (dns.name.from_text('wp-db22.services.mozilla.com', None), long(0),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.CNAME,
                                    'SERVER.FOOBAR.')),

                (dns.name.from_text('wp-db23.services.mozilla.com', None), long(0),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.CNAME,
                                    'SERVER.FOOBAR.'))]
        exl.sort(key=_rdata_sort)
        self.failUnless(l == exl)

    def testGenerate3(self): # type: () -> None
        z = dns.zone.from_text(example_text6, 'example.', relativize=True)
        l = list(z.iterate_rdatas())
        l.sort(key=_rdata_sort)

        exl = [(dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns1')),
               (dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns2')),
               (dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SOA,
                                    'foo bar 1 2 3 4 5')),
               (dns.name.from_text('bar.foo', None),
                long(300),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.MX,
                                    '0 blaz.foo')),
               (dns.name.from_text('ns1', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.1')),
               (dns.name.from_text('ns2', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.2')),
                (dns.name.from_text('wp-db21.services.mozilla.com', None), long(0),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.CNAME,
                                    'SERVER.FOOBAR.')),

                (dns.name.from_text('wp-db22.services.mozilla.com', None), long(0),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.CNAME,
                                    'SERVER.FOOBAR.')),

                (dns.name.from_text('wp-db23.services.mozilla.com', None), long(0),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.CNAME,
                                    'SERVER.FOOBAR.'))]
        exl.sort(key=_rdata_sort)
        self.failUnless(l == exl)

    def testGenerate4(self): # type: () -> None
        z = dns.zone.from_text(example_text7, 'example.', relativize=True)
        l = list(z.iterate_rdatas())
        l.sort(key=_rdata_sort)
        exl = [(dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns1')),
               (dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns2')),
               (dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SOA,
                                    'foo bar 1 2 3 4 5')),
               (dns.name.from_text('bar.foo', None),
                long(300),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.MX,
                                    '0 blaz.foo')),
               (dns.name.from_text('ns1', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.1')),
               (dns.name.from_text('ns2', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.2')),

                (dns.name.from_text('sync1.db', None), long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.10.16.0')),

                (dns.name.from_text('sync2.db', None), long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.10.16.0')),

                (dns.name.from_text('sync3.db', None), long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.10.16.0'))]
        exl.sort(key=_rdata_sort)
        self.failUnless(l == exl)

    def testGenerate6(self): # type: () -> None
        z = dns.zone.from_text(example_text9, 'example.', relativize=True)
        l = list(z.iterate_rdatas())
        l.sort(key=_rdata_sort)
        exl = [(dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns1')),
               (dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns2')),
               (dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SOA,
                                    'foo bar 1 2 3 4 5')),
               (dns.name.from_text('bar.foo', None),
                long(300),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.MX,
                                    '0 blaz.foo')),
               (dns.name.from_text('ns1', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.1')),
               (dns.name.from_text('ns2', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.2')),

                (dns.name.from_text('wp-db01', None), long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.10.16.0')),
                (dns.name.from_text('wp-db02', None), long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.10.16.0')),

                (dns.name.from_text('sync1.db', None), long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.10.16.0')),

                (dns.name.from_text('sync2.db', None), long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.10.16.0')),

                (dns.name.from_text('sync3.db', None), long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.10.16.0'))]
        exl.sort(key=_rdata_sort)
        self.failUnless(l == exl)

    def testGenerate7(self): # type: () -> None
        z = dns.zone.from_text(example_text10, 'example.', relativize=True)
        l = list(z.iterate_rdatas())
        l.sort(key=_rdata_sort)
        exl = [(dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns1')),
               (dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    'ns2')),
               (dns.name.from_text('@', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SOA,
                                    'foo bar 1 2 3 4 5')),
               (dns.name.from_text('bar.foo', None),
                long(300),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.MX,
                                    '0 blaz.foo')),
               (dns.name.from_text('ns1', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.1')),
               (dns.name.from_text('ns2', None),
                long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.2')),

                (dns.name.from_text('27.2', None), long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.PTR,
                                    'zlb1.oob')),

                (dns.name.from_text('28.2', None), long(3600),
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.PTR,
                                    'zlb2.oob'))]

        exl.sort(key=_rdata_sort)
        self.failUnless(l == exl)


if __name__ == '__main__':
    unittest.main()
