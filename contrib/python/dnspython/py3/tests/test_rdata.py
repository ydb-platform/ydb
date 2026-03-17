# -*- coding: utf-8
# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

# Copyright (C) 2006, 2007, 2009-2011 Nominum, Inc.
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

import io
import operator
import pickle
import struct
import unittest

import dns.wire
import dns.exception
import dns.name
import dns.rdata
import dns.rdataclass
import dns.rdataset
import dns.rdatatype
from dns.rdtypes.ANY.OPT import OPT
from dns.rdtypes.ANY.LOC import LOC
from dns.rdtypes.ANY.GPOS import GPOS
import dns.rdtypes.ANY.RRSIG
import dns.rdtypes.IN.APL
import dns.rdtypes.util
import dns.tokenizer
import dns.ttl
import dns.wire

import tests.stxt_module
import tests.ttxt_module
import tests.md_module
from tests.util import here

class RdataTestCase(unittest.TestCase):

    def test_str(self):
        rdata = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    "1.2.3.4")
        self.assertEqual(rdata.address, "1.2.3.4")

    def test_unicode(self):
        rdata = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    u"1.2.3.4")
        self.assertEqual(rdata.address, "1.2.3.4")

    def test_module_registration(self):
        TTXT = 64001
        dns.rdata.register_type(tests.ttxt_module, TTXT, 'TTXT')
        rdata = dns.rdata.from_text(dns.rdataclass.IN, TTXT, 'hello world')
        self.assertEqual(rdata.strings, (b'hello', b'world'))
        self.assertEqual(dns.rdatatype.to_text(TTXT), 'TTXT')
        self.assertEqual(dns.rdatatype.from_text('TTXT'), TTXT)

    def test_module_reregistration(self):
        def bad():
            TTXTTWO = dns.rdatatype.TXT
            dns.rdata.register_type(tests.ttxt_module, TTXTTWO, 'TTXTTWO')
        self.assertRaises(dns.rdata.RdatatypeExists, bad)

    def test_module_registration_singleton(self):
        STXT = 64002
        dns.rdata.register_type(tests.stxt_module, STXT, 'STXT',
                                is_singleton=True)
        rdata1 = dns.rdata.from_text(dns.rdataclass.IN, STXT, 'hello')
        rdata2 = dns.rdata.from_text(dns.rdataclass.IN, STXT, 'world')
        rdataset = dns.rdataset.from_rdata(3600, rdata1, rdata2)
        self.assertEqual(len(rdataset), 1)
        self.assertEqual(rdataset[0].strings, (b'world',))

    def test_replace(self):
        a1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A, "1.2.3.4")
        a2 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A, "2.3.4.5")
        self.assertEqual(a1.replace(address="2.3.4.5"), a2)

        mx = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.MX,
                                  "10 foo.example")
        name = dns.name.from_text("bar.example")
        self.assertEqual(mx.replace(preference=20).preference, 20)
        self.assertEqual(mx.replace(preference=20).exchange, mx.exchange)
        self.assertEqual(mx.replace(exchange=name).exchange, name)
        self.assertEqual(mx.replace(exchange=name).preference, mx.preference)

        for invalid_parameter in ("rdclass", "rdtype", "foo", "__class__"):
            with self.assertRaises(AttributeError):
                mx.replace(invalid_parameter=1)

    def test_invalid_replace(self):
        a1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A, "1.2.3.4")
        with self.assertRaises(dns.exception.SyntaxError):
            a1.replace(address="bogus")

    def test_replace_comment(self):
        a1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                 "1.2.3.4 ;foo")
        self.assertEqual(a1.rdcomment, "foo")
        a2 = a1.replace(rdcomment="bar")
        self.assertEqual(a1, a2)
        self.assertEqual(a1.rdcomment, "foo")
        self.assertEqual(a2.rdcomment, "bar")

    def test_no_replace_class_or_type(self):
        a1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A, "1.2.3.4")
        with self.assertRaises(AttributeError):
            a1.replace(rdclass=255)
        with self.assertRaises(AttributeError):
            a1.replace(rdtype=2)

    def test_to_generic(self):
        a = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A, "1.2.3.4")
        self.assertEqual(str(a.to_generic()), r'\# 4 01020304')

        mx = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.MX, "10 foo.")
        self.assertEqual(str(mx.to_generic()), r'\# 7 000a03666f6f00')

        origin = dns.name.from_text('example')
        ns = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                 "foo.example.", relativize_to=origin)
        self.assertEqual(str(ns.to_generic(origin=origin)),
                         r'\# 13 03666f6f076578616d706c6500')

    def test_txt_unicode(self):
        # TXT records are not defined for Unicode, but if we get
        # Unicode we should convert it to UTF-8 to preserve meaning as
        # best we can.  Note that it when the TXT record is sent
        # to_text(), it does NOT convert embedded UTF-8 back to
        # Unicode; it's just treated as binary TXT data.  Probably
        # there should be a TXT-like record with an encoding field.
        rdata = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.TXT,
                                    '"foo\u200bbar"')
        self.assertEqual(str(rdata), '"foo\\226\\128\\139bar"')
        # We used to encode UTF-8 in UTF-8 because we processed
        # escapes in quoted strings immediately.  This meant that the
        # \\226 below would be inserted as Unicode code point 226, and
        # then when we did to_text, we would UTF-8 encode that code
        # point, emitting \\195\\162 instead of \\226, and thus
        # from_text followed by to_text was not the equal to the
        # original input like it ought to be.
        rdata = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.TXT,
                                    '"foo\\226\\128\\139bar"')
        self.assertEqual(str(rdata), '"foo\\226\\128\\139bar"')
        # Our fix for TXT-like records uses a new tokenizer method,
        # unescape_to_bytes(), which converts Unicode to UTF-8 only
        # once.
        rdata = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.TXT,
                                    '"foo\u200b\\123bar"')
        self.assertEqual(str(rdata), '"foo\\226\\128\\139{bar"')

    def test_unicode_idna2003_in_rdata(self):
        rdata = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    "Königsgäßchen")
        self.assertEqual(str(rdata.target), 'xn--knigsgsschen-lcb0w')

    @unittest.skipUnless(dns.name.have_idna_2008,
                         'Python idna cannot be imported; no IDNA2008')
    def test_unicode_idna2008_in_rdata(self):
        rdata = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS,
                                    "Königsgäßchen",
                                    idna_codec=dns.name.IDNA_2008)
        self.assertEqual(str(rdata.target), 'xn--knigsgchen-b4a3dun')

    def test_digestable_downcasing(self):
        # Make sure all the types listed in RFC 4034 section 6.2 are
        # downcased properly, except for:
        #
        #   types we don't implement:  MD, MF, MB, MG, MR, MINFO, SIG,
        #                              NXT, A6
        #
        #   types that don't have names: HINFO
        #
        #   NSEC3, whose downcasing was removed by RFC 6840 section 5.1
        #
        cases = [
            ('SOA', 'NAME NAME 1 2 3 4 5'),
            ('AFSDB', '0 NAME'),
            ('CNAME', 'NAME'),
            ('DNAME', 'NAME'),
            ('KX', '10 NAME'),
            ('MX', '10 NAME'),
            ('NS', 'NAME'),
            ('NAPTR', '0 0 a B c NAME'),
            ('PTR', 'NAME'),
            ('PX', '65535 NAME NAME'),
            ('RP', 'NAME NAME'),
            ('RT', '0 NAME'),
            ('SRV', '0 0 0 NAME'),
            ('RRSIG',
             'A 1 3 3600 20200701000000 20200601000000 1 NAME Ym9ndXM=')
        ]
        for rdtype, text in cases:
            upper_origin = dns.name.from_text('EXAMPLE')
            lower_origin = dns.name.from_text('example')
            canonical_text = text.replace('NAME', 'name')
            rdata = dns.rdata.from_text(dns.rdataclass.IN, rdtype, text,
                                        origin=upper_origin, relativize=False)
            canonical_rdata = dns.rdata.from_text(dns.rdataclass.IN, rdtype,
                                                  canonical_text,
                                                  origin=lower_origin,
                                                  relativize=False)
            digestable_wire = rdata.to_digestable()
            f = io.BytesIO()
            canonical_rdata.to_wire(f)
            expected_wire = f.getvalue()
            self.assertEqual(digestable_wire, expected_wire)

    def test_digestable_no_downcasing(self):
        # Make sure that currently known types with domain names that
        # are NOT supposed to be downcased when canonicalized are
        # handled properly.
        #
        cases = [
            ('HIP', '2 200100107B1A74DF365639CC39F1D578 Ym9ndXM= NAME name'),
            ('IPSECKEY', '10 3 2 NAME Ym9ndXM='),
            ('NSEC', 'NAME A'),
        ]
        for rdtype, text in cases:
            origin = dns.name.from_text('example')
            rdata = dns.rdata.from_text(dns.rdataclass.IN, rdtype, text,
                                        origin=origin, relativize=False)
            digestable_wire = rdata.to_digestable(origin)
            expected_wire = rdata.to_wire(origin=origin)
            self.assertEqual(digestable_wire, expected_wire)

    def test_basic_relations(self):
        r1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                 '10.0.0.1')
        r2 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                 '10.0.0.2')
        self.assertTrue(r1 == r1)
        self.assertTrue(r1 != r2)
        self.assertTrue(r1 < r2)
        self.assertTrue(r1 <= r2)
        self.assertTrue(r2 > r1)
        self.assertTrue(r2 >= r1)

    def test_incompatible_relations(self):
        r1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                 '10.0.0.1')
        r2 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.AAAA,
                                 '::1')
        for oper in [operator.lt, operator.le, operator.ge, operator.gt]:
            self.assertRaises(TypeError, lambda: oper(r1, r2))
        self.assertFalse(r1 == r2)
        self.assertTrue(r1 != r2)

    def test_immutability(self):
        def bad1():
            r = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.1')
            r.address = '10.0.0.2'
        self.assertRaises(TypeError, bad1)
        def bad2():
            r = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                    '10.0.0.1')
            del r.address
        self.assertRaises(TypeError, bad2)

    def test_pickle(self):
        r1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A,
                                 '10.0.0.1')
        p = pickle.dumps(r1)
        r2 = pickle.loads(p)
        self.assertEqual(r1, r2)
        # Pickle something with a longer inheritance chain
        r3 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.MX,
                                 '10 mail.example.')
        p = pickle.dumps(r3)
        r4 = pickle.loads(p)
        self.assertEqual(r3, r4)

    def test_AFSDB_properties(self):
        rd = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.AFSDB,
                                 '0 afsdb.example.')
        self.assertEqual(rd.preference, rd.subtype)
        self.assertEqual(rd.exchange, rd.hostname)

    def equal_loc(self, a, b):
        rda = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.LOC, a)
        rdb = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.LOC, b)
        self.assertEqual(rda, rdb)

    def test_misc_good_LOC_text(self):
        # test just degrees
        self.equal_loc('60 N 24 39 0.000 E 10.00m 20m 2000m 20m',
                       '60 0 0 N 24 39 0.000 E 10.00m 20m 2000m 20m')
        self.equal_loc('60 0 0 N 24 E 10.00m 20m 2000m 20m',
                       '60 0 0 N 24 0 0 E 10.00m 20m 2000m 20m')
        # test variable length latitude
        self.equal_loc('60 9 0.510 N 24 39 0.000 E 10.00m 20m 2000m 20m',
                       '60 9 0.51 N 24 39 0.000 E 10.00m 20m 2000m 20m')
        self.equal_loc('60 9 0.500 N 24 39 0.000 E 10.00m 20m 2000m 20m',
                       '60 9 0.5 N 24 39 0.000 E 10.00m 20m 2000m 20m')
        self.equal_loc('60 9 1.000 N 24 39 0.000 E 10.00m 20m 2000m 20m',
                       '60 9 1 N 24 39 0.000 E 10.00m 20m 2000m 20m')
        # test variable length longtitude
        self.equal_loc('60 9 0.000 N 24 39 0.510 E 10.00m 20m 2000m 20m',
                       '60 9 0.000 N 24 39 0.51 E 10.00m 20m 2000m 20m')
        self.equal_loc('60 9 0.000 N 24 39 0.500 E 10.00m 20m 2000m 20m',
                       '60 9 0.000 N 24 39 0.5 E 10.00m 20m 2000m 20m')
        self.equal_loc('60 9 0.000 N 24 39 1.000 E 10.00m 20m 2000m 20m',
                       '60 9 0.000 N 24 39 1 E 10.00m 20m 2000m 20m')
        # test siz, hp, vp defaults
        self.equal_loc('60 9 0.510 N 24 39 0.000 E 10.00m',
                       '60 9 0.51 N 24 39 0.000 E 10.00m 1m 10000m 10m')
        self.equal_loc('60 9 0.510 N 24 39 0.000 E 10.00m 2m',
                       '60 9 0.51 N 24 39 0.000 E 10.00m 2m 10000m 10m')
        self.equal_loc('60 9 0.510 N 24 39 0.000 E 10.00m 2m 2000m',
                       '60 9 0.51 N 24 39 0.000 E 10.00m 2m 2000m 10m')
        # test siz, hp, vp optional units
        self.equal_loc('60 9 0.510 N 24 39 0.000 E 1m 20m 2000m 20m',
                       '60 9 0.51 N 24 39 0.000 E 1 20 2000 20')

    def test_LOC_to_text_SW_hemispheres(self):
        # As an extra, we test int->float conversion in the constructor
        loc = LOC(dns.rdataclass.IN, dns.rdatatype.LOC, -60, -24, 1)
        text = '60 0 0.000 S 24 0 0.000 W 0.01m'
        self.assertEqual(loc.to_text(), text)

    def test_zero_size(self):
        # This is to exercise the 0 path in _exponent_of.
        loc = dns.rdata.from_text('in', 'loc', '60 S 24 W 1 0')
        self.assertEqual(loc.size, 0.0)

    def test_bad_LOC_text(self):
        bad_locs = ['60 9 a.000 N 24 39 0.000 E 10.00m 20m 2000m 20m',
                    '60 9 60.000 N 24 39 0.000 E 10.00m 20m 2000m 20m',
                    '60 9 0.00a N 24 39 0.000 E 10.00m 20m 2000m 20m',
                    '60 9 0.0001 N 24 39 0.000 E 10.00m 20m 2000m 20m',
                    '60 9 0.000 Z 24 39 0.000 E 10.00m 20m 2000m 20m',
                    '91 9 0.000 N 24 39 0.000 E 10.00m 20m 2000m 20m',
                    '60 60 0.000 N 24 39 0.000 E 10.00m 20m 2000m 20m',

                    '60 9 0.000 N 24 39 a.000 E 10.00m 20m 2000m 20m',
                    '60 9 0.000 N 24 39 60.000 E 10.00m 20m 2000m 20m',
                    '60 9 0.000 N 24 39 0.00a E 10.00m 20m 2000m 20m',
                    '60 9 0.000 N 24 39 0.0001 E 10.00m 20m 2000m 20m',
                    '60 9 0.000 N 24 39 0.000 Z 10.00m 20m 2000m 20m',
                    '60 9 0.000 N 181 39 0.000 E 10.00m 20m 2000m 20m',
                    '60 9 0.000 N 24 60 0.000 E 10.00m 20m 2000m 20m',

                    '60 9 0.000 N 24 39 0.000 E 10.00m 100000000m 2000m 20m',
                    '60 9 0.000 N 24 39 0.000 E 10.00m 20m 100000000m 20m',
                    '60 9 0.000 N 24 39 0.000 E 10.00m 20m 20m 100000000m',
                    ]
        for loc in bad_locs:
            with self.assertRaises(dns.exception.SyntaxError):
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.LOC, loc)

    def test_bad_LOC_wire(self):
        bad_locs = [(0, 0, 0, 0x934fd901, 0x80000000, 100),
                    (0, 0, 0, 0x6cb026ff, 0x80000000, 100),
                    (0, 0, 0, 0x80000000, 0xa69fb201, 100),
                    (0, 0, 0, 0x80000000, 0x59604dff, 100),
                    (0xa0, 0, 0, 0x80000000, 0x80000000, 100),
                    (0x0a, 0, 0, 0x80000000, 0x80000000, 100),
                    (0, 0xa0, 0, 0x80000000, 0x80000000, 100),
                    (0, 0x0a, 0, 0x80000000, 0x80000000, 100),
                    (0, 0, 0xa0, 0x80000000, 0x80000000, 100),
                    (0, 0, 0x0a, 0x80000000, 0x80000000, 100),
                    ]
        for t in bad_locs:
            with self.assertRaises(dns.exception.FormError):
                wire = struct.pack('!BBBBIII', 0, t[0], t[1], t[2],
                                   t[3], t[4], t[5])
                dns.rdata.from_wire(dns.rdataclass.IN, dns.rdatatype.LOC,
                                    wire, 0, len(wire))
            with self.assertRaises(dns.exception.FormError):
                wire = struct.pack('!BBBBIII', 1, 0, 0, 0, 0, 0, 0)
                dns.rdata.from_wire(dns.rdataclass.IN, dns.rdatatype.LOC,
                                    wire, 0, len(wire))

    def equal_wks(self, a, b):
        rda = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.WKS, a)
        rdb = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.WKS, b)
        self.assertEqual(rda, rdb)

    def test_misc_good_WKS_text(self):
        self.equal_wks('10.0.0.1 tcp ( http )', '10.0.0.1 6 ( 80 )')
        self.equal_wks('10.0.0.1 udp ( domain )', '10.0.0.1 17 ( 53 )')

    def test_misc_bad_WKS_text(self):
        try:
            dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.WKS,
                                '10.0.0.1 132 ( domain )')
            self.assertTrue(False)  # should not happen
        except dns.exception.SyntaxError as e:
            self.assertIsInstance(e.__cause__, NotImplementedError)

    def test_GPOS_float_converters(self):
        rd = dns.rdata.from_text('in', 'gpos', '49 0 0')
        self.assertEqual(rd.float_latitude, 49.0)
        self.assertEqual(rd.float_longitude, 0.0)
        self.assertEqual(rd.float_altitude, 0.0)

    def test_GPOS_constructor_conversion(self):
        rd = GPOS(dns.rdataclass.IN, dns.rdatatype.GPOS, 49.0, 0.0, 0.0)
        self.assertEqual(rd.float_latitude, 49.0)
        self.assertEqual(rd.float_longitude, 0.0)
        self.assertEqual(rd.float_altitude, 0.0)
        rd = GPOS(dns.rdataclass.IN, dns.rdatatype.GPOS, 49, 0, 0)
        self.assertEqual(rd.float_latitude, 49.0)
        self.assertEqual(rd.float_longitude, 0.0)
        self.assertEqual(rd.float_altitude, 0.0)

    def test_bad_GPOS_text(self):
        bad_gpos = ['"-" "116.8652" "250"',
                    '"+" "116.8652" "250"',
                    '"" "116.8652" "250"',
                    '"." "116.8652" "250"',
                    '".a" "116.8652" "250"',
                    '"a." "116.8652" "250"',
                    '"a.a" "116.8652" "250"',
                    # We don't need to test all the bad permutations again
                    # but we do want to test that badness is detected
                    # in the other strings
                    '"0" "a" "250"',
                    '"0" "0" "a"',
                    # finally test bounds
                    '"90.1" "0" "0"',
                    '"-90.1" "0" "0"',
                    '"0" "180.1" "0"',
                    '"0" "-180.1" "0"',
                    ]
        for gpos in bad_gpos:
            with self.assertRaises(dns.exception.SyntaxError):
                dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.GPOS, gpos)

    def test_bad_GPOS_wire(self):
        bad_gpos = [b'\x01',
                    b'\x01\x31\x01',
                    b'\x01\x31\x01\x31\x01',
                    ]
        for wire in bad_gpos:
            self.assertRaises(dns.exception.FormError,
                              lambda: dns.rdata.from_wire(dns.rdataclass.IN,
                                                          dns.rdatatype.GPOS,
                                                          wire, 0, len(wire)))

    def test_chaos(self):
        # avoid red spot on our coverage :)
        r1 = dns.rdata.from_text(dns.rdataclass.CH, dns.rdatatype.A,
                                 'chaos. 12345')
        w = r1.to_wire()
        r2 = dns.rdata.from_wire(dns.rdataclass.CH, dns.rdatatype.A, w, 0,
                                 len(w))
        self.assertEqual(r1, r2)
        self.assertEqual(r1.domain, dns.name.from_text('chaos'))
        # the address input is octal
        self.assertEqual(r1.address, 0o12345)
        self.assertEqual(r1.to_text(), 'chaos. 12345')

    def test_opt_repr(self):
        opt = OPT(4096, dns.rdatatype.OPT, ())
        self.assertEqual(repr(opt), '<DNS CLASS4096 OPT rdata: >')

    def test_opt_short_lengths(self):
        with self.assertRaises(dns.exception.FormError):
            parser = dns.wire.Parser(bytes.fromhex('f00102'))
            OPT.from_wire_parser(4096, dns.rdatatype.OPT, parser)
        with self.assertRaises(dns.exception.FormError):
            parser = dns.wire.Parser(bytes.fromhex('f00100030000'))
            OPT.from_wire_parser(4096, dns.rdatatype.OPT, parser)

    def test_from_wire_parser(self):
        wire = bytes.fromhex('01020304')
        rdata = dns.rdata.from_wire('in', 'a', wire, 0, 4)
        self.assertEqual(rdata, dns.rdata.from_text('in', 'a', '1.2.3.4'))

    def _test_unpickle(self):
        expected_mx = dns.rdata.from_text('in', 'mx', '10 mx.example.')
        with open(here('mx-2-0.pickle'), 'rb') as f:
            mx = pickle.load(f)
        self.assertEqual(mx, expected_mx)
        self.assertIsNone(mx.rdcomment)

    def test_escaped_newline_in_quoted_string(self):
        rd = dns.rdata.from_text('in', 'txt', '"foo\\\nbar"')
        self.assertEqual(rd.strings, (b'foo\nbar',))
        self.assertEqual(rd.to_text(), '"foo\\010bar"')

    def test_escaped_newline_in_nonquoted_string(self):
        with self.assertRaises(dns.exception.UnexpectedEnd):
            dns.rdata.from_text('in', 'txt', 'foo\\\nbar')

    def test_wordbreak(self):
        text = b'abcdefgh'
        self.assertEqual(dns.rdata._wordbreak(text, 4), 'abcd efgh')
        self.assertEqual(dns.rdata._wordbreak(text, 0), 'abcdefgh')

    def test_escapify(self):
        self.assertEqual(dns.rdata._escapify('abc'), 'abc')
        self.assertEqual(dns.rdata._escapify(b'abc'), 'abc')
        self.assertEqual(dns.rdata._escapify(bytearray(b'abc')), 'abc')
        self.assertEqual(dns.rdata._escapify(b'ab"c'), 'ab\\"c')
        self.assertEqual(dns.rdata._escapify(b'ab\\c'), 'ab\\\\c')
        self.assertEqual(dns.rdata._escapify(b'ab\x01c'), 'ab\\001c')

    def test_truncate_bitmap(self):
        self.assertEqual(dns.rdata._truncate_bitmap(b'\x00\x01\x00\x00'),
                         b'\x00\x01')
        self.assertEqual(dns.rdata._truncate_bitmap(b'\x00\x01\x00\x01'),
                         b'\x00\x01\x00\x01')
        self.assertEqual(dns.rdata._truncate_bitmap(b'\x00\x00\x00\x00'),
                         b'\x00')

    def test_covers_and_extended_rdatatype(self):
        rd = dns.rdata.from_text('in', 'a', '10.0.0.1')
        self.assertEqual(rd.covers(), dns.rdatatype.NONE)
        self.assertEqual(rd.extended_rdatatype(), 0x00000001)
        rd = dns.rdata.from_text('in', 'rrsig',
                                 'NSEC 1 3 3600 ' +
                                 '20200101000000 20030101000000 ' +
                                 '2143 foo Ym9ndXM=')
        self.assertEqual(rd.covers(), dns.rdatatype.NSEC)
        self.assertEqual(rd.extended_rdatatype(), 0x002f002e)

    def test_uncomparable(self):
        rd = dns.rdata.from_text('in', 'a', '10.0.0.1')
        self.assertFalse(rd == 'a')
        self.assertTrue(rd != 'a')

    def test_bad_generic(self):
        # does not start with \#
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'type45678', '# 7 000a03666f6f00')
        # wrong length
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'type45678', '\\# 6 000a03666f6f00')

    def test_empty_generic(self):
        dns.rdata.from_text('in', 'type45678', r'\# 0')

    def test_covered_repr(self):
        text = 'NSEC 1 3 3600 20190101000000 20030101000000 ' + \
            '2143 foo Ym9ndXM='
        rd = dns.rdata.from_text('in', 'rrsig', text)
        self.assertEqual(repr(rd), '<DNS IN RRSIG(NSEC) rdata: ' + text + '>')

    def test_bad_registration_implementing_known_type_with_wrong_name(self):
        # Try to register an implementation at the MG codepoint that isn't
        # called "MG"
        with self.assertRaises(dns.rdata.RdatatypeExists):
            dns.rdata.register_type(None, dns.rdatatype.MG, 'NOTMG')

    def test_registration_implementing_known_type_with_right_name(self):
        # Try to register an implementation at the MD codepoint
        dns.rdata.register_type(tests.md_module, dns.rdatatype.MD, 'MD')
        rd = dns.rdata.from_text('in', 'md', 'foo.')
        self.assertEqual(rd.target, dns.name.from_text('foo.'))

    def test_CERT_with_string_type(self):
        rd = dns.rdata.from_text('in', 'cert', 'SPKI 1 PRIVATEOID Ym9ndXM=')
        self.assertEqual(rd.to_text(), 'SPKI 1 PRIVATEOID Ym9ndXM=')

    def test_CERT_algorithm(self):
        rd = dns.rdata.from_text('in', 'cert', 'SPKI 1 0 Ym9ndXM=')
        self.assertEqual(rd.algorithm, 0)
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'cert', 'SPKI 1 -1 Ym9ndXM=')
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'cert', 'SPKI 1 256 Ym9ndXM=')
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'cert', 'SPKI 1 BOGUS Ym9ndXM=')

    def test_bad_URI_text(self):
        # empty target
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'uri', '10 1 ""')
        # no target
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'uri', '10 1')

    def test_bad_URI_wire(self):
        wire = bytes.fromhex('000a0001')
        with self.assertRaises(dns.exception.FormError):
            dns.rdata.from_wire('in', 'uri', wire, 0, 4)

    def test_bad_NSAP_text(self):
        # does not start with 0x
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'nsap', '0y4700')
        # odd hex string length
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'nsap', '0x470')

    def test_bad_CAA_text(self):
        # tag too long
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'caa',
                                '0 ' + 'a' * 256 + ' "ca.example.net"')
        # tag not alphanumeric
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'caa',
                                '0 a-b "ca.example.net"')

    def test_bad_HIP_text(self):
        # hit too long
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'hip',
                                '2 ' +
                                '00' * 256 +
                                ' Ym9ndXM=')

    def test_bad_sigtime(self):
        try:
            dns.rdata.from_text('in', 'rrsig',
                                'NSEC 1 3 3600 ' +
                                '202001010000000 20030101000000 ' +
                                '2143 foo Ym9ndXM=')
            self.assertTrue(False)  # should not happen
        except dns.exception.SyntaxError as e:
            self.assertIsInstance(e.__cause__,
                                  dns.rdtypes.ANY.RRSIG.BadSigTime)
        try:
            dns.rdata.from_text('in', 'rrsig',
                                'NSEC 1 3 3600 ' +
                                '20200101000000 2003010100000 ' +
                                '2143 foo Ym9ndXM=')
            self.assertTrue(False)  # should not happen
        except dns.exception.SyntaxError as e:
            self.assertIsInstance(e.__cause__,
                                  dns.rdtypes.ANY.RRSIG.BadSigTime)

    def test_empty_TXT(self):
        # hit too long
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'txt', '')

    def test_too_long_TXT(self):
        # hit too long
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text('in', 'txt', 'a' * 256)

    def equal_smimea(self, a, b):
        a = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SMIMEA, a)
        b = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SMIMEA, b)
        self.assertEqual(a, b)

    def test_good_SMIMEA(self):
        self.equal_smimea('3 0 1 aabbccddeeff', '3 0 01 AABBCCDDEEFF')

    def test_bad_SMIMEA(self):
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SMIMEA, '1 1 1 aGVsbG8gd29ybGQh')

    def test_bad_APLItem_address_length(self):
        with self.assertRaises(ValueError):
            # 9999 is used in as an "unknown" address family.  In the unlikely
            # event it is ever defined, we should switch the test to another
            # value.
            dns.rdtypes.IN.APL.APLItem(9999, False, b'0xff' * 128, 255)

    def test_DNSKEY_chunking(self):
        inputs = (  # each with chunking as given by dig, unusual chunking, and no chunking
            # example 1
            (
                '257 3 13 aCoEWYBBVsP9Fek2oC8yqU8ocKmnS1iDSFZNORnQuHKtJ9Wpyz+kNryq uB78Pyk/NTEoai5bxoipVQQXzHlzyg==',
                '257 3 13 aCoEWYBBVsP9Fek2oC8yqU8ocK mnS1iDSFZNORnQuHKtJ9Wpyz+kNryquB78Pyk/   NTEoai5bxoipVQQXzHlzyg==',
                '257 3 13 aCoEWYBBVsP9Fek2oC8yqU8ocKmnS1iDSFZNORnQuHKtJ9Wpyz+kNryquB78Pyk/NTEoai5bxoipVQQXzHlzyg==',
            ),
            # example 2
            (
                '257 3 8 AwEAAcw5QLr0IjC0wKbGoBPQv4qmeqHy9mvL5qGQTuaG5TSrNqEAR6b/ qvxDx6my4JmEmjUPA1JeEI9YfTUieMr2UZflu7aIbZFLw0vqiYrywCGr CHXLalOrEOmrvAxLvq4vHtuTlH7JIszzYBSes8g1vle6KG7xXiP3U5Ll 96Qiu6bZ31rlMQSPB20xbqJJh6psNSrQs41QvdcXAej+K2Hl1Wd8kPri ec4AgiBEh8sk5Pp8W9ROLQ7PcbqqttFaW2m7N/Wy4qcFU13roWKDEAst bxH5CHPoBfZSbIwK4KM6BK/uDHpSPIbiOvOCW+lvu9TAiZPc0oysY6as lO7jXv16Gws=',
                '257 3 8 AwEAAcw5QLr0IjC0wKbGoBPQv4qmeq Hy9mvL5qGQTuaG5TSrNqEA R6b/qvxDx6my4JmEmjUPA1JeEI9Y  fTUieMr2UZflu7aIbZFLw0vqiYrywCGrC HXLalOrEOmrvAxLvq4vHtuTlH7JIszzYBSes8g1vle6KG7 xXiP3U5Ll 96Qiu6bZ31rlMQSPB20xbqJJh6psNSrQs41QvdcXAej+K2Hl1Wd8kPriec4AgiBEh8sk5Pp8W9ROLQ7PcbqqttFaW2m7N/Wy4qcFU13roWKDEAst bxH5CHPoBfZSbIwK4KM6BK/uDHpSPIbiOvOCW+lvu9TAiZPc0oysY6as lO7jXv16Gws=',
                '257 3 8 AwEAAcw5QLr0IjC0wKbGoBPQv4qmeqHy9mvL5qGQTuaG5TSrNqEAR6b/qvxDx6my4JmEmjUPA1JeEI9YfTUieMr2UZflu7aIbZFLw0vqiYrywCGrCHXLalOrEOmrvAxLvq4vHtuTlH7JIszzYBSes8g1vle6KG7xXiP3U5Ll96Qiu6bZ31rlMQSPB20xbqJJh6psNSrQs41QvdcXAej+K2Hl1Wd8kPriec4AgiBEh8sk5Pp8W9ROLQ7PcbqqttFaW2m7N/Wy4qcFU13roWKDEAstbxH5CHPoBfZSbIwK4KM6BK/uDHpSPIbiOvOCW+lvu9TAiZPc0oysY6aslO7jXv16Gws=',
            ),
            # example 3
            (
                '256 3 8 AwEAAday3UX323uVzQqtOMQ7EHQYfD5Ofv4akjQGN2zY5AgB/2jmdR/+ 1PvXFqzKCAGJv4wjABEBNWLLFm7ew1hHMDZEKVL17aml0EBKI6Dsz6Mx t6n7ScvLtHaFRKaxT4i2JxiuVhKdQR9XGMiWAPQKrRM5SLG0P+2F+TLK l3D0L/cD',
                '256 3 8 AwEAAday3UX323uVzQqtOMQ7EHQYfD5Ofv4akjQGN2zY5    AgB/2jmdR/+1PvXFqzKCAGJv4wjABEBNWLLFm7ew1hHMDZEKVL17aml0EBKI6Dsz6Mxt6n7ScvLtHaFRKaxT4i2JxiuVhKdQR9XGMiWAPQKrRM5SLG0P+2F+ TLKl3D0L/cD',
                '256 3 8 AwEAAday3UX323uVzQqtOMQ7EHQYfD5Ofv4akjQGN2zY5AgB/2jmdR/+1PvXFqzKCAGJv4wjABEBNWLLFm7ew1hHMDZEKVL17aml0EBKI6Dsz6Mxt6n7ScvLtHaFRKaxT4i2JxiuVhKdQR9XGMiWAPQKrRM5SLG0P+2F+TLKl3D0L/cD',
            ),
        )
        output_map = {
            32: (
                '257 3 13 aCoEWYBBVsP9Fek2oC8yqU8ocKmnS1iD SFZNORnQuHKtJ9Wpyz+kNryquB78Pyk/ NTEoai5bxoipVQQXzHlzyg==',
                '257 3 8 AwEAAcw5QLr0IjC0wKbGoBPQv4qmeqHy 9mvL5qGQTuaG5TSrNqEAR6b/qvxDx6my 4JmEmjUPA1JeEI9YfTUieMr2UZflu7aI bZFLw0vqiYrywCGrCHXLalOrEOmrvAxL vq4vHtuTlH7JIszzYBSes8g1vle6KG7x XiP3U5Ll96Qiu6bZ31rlMQSPB20xbqJJ h6psNSrQs41QvdcXAej+K2Hl1Wd8kPri ec4AgiBEh8sk5Pp8W9ROLQ7PcbqqttFa W2m7N/Wy4qcFU13roWKDEAstbxH5CHPo BfZSbIwK4KM6BK/uDHpSPIbiOvOCW+lv u9TAiZPc0oysY6aslO7jXv16Gws=',
                '256 3 8 AwEAAday3UX323uVzQqtOMQ7EHQYfD5O fv4akjQGN2zY5AgB/2jmdR/+1PvXFqzK CAGJv4wjABEBNWLLFm7ew1hHMDZEKVL1 7aml0EBKI6Dsz6Mxt6n7ScvLtHaFRKax T4i2JxiuVhKdQR9XGMiWAPQKrRM5SLG0 P+2F+TLKl3D0L/cD',
            ),
            56: (t[0] for t in inputs),
            0: (t[0][:12] + t[0][12:].replace(' ', '') for t in inputs)
        }

        for chunksize, outputs in output_map.items():
            for input, output in zip(inputs, outputs):
                for input_variation in input:
                    rr = dns.rdata.from_text('IN', 'DNSKEY', input_variation)
                    new_text = rr.to_text(chunksize=chunksize)
                    self.assertEqual(output, new_text)
                    
    def test_relative_vs_absolute_compare_unstrict(self):
        try:
            saved = dns.rdata._allow_relative_comparisons
            dns.rdata._allow_relative_comparisons = True
            r1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS, 'www.')
            r2 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS, 'www')
            self.assertFalse(r1 == r2)
            self.assertTrue(r1 != r2)
            self.assertFalse(r1 < r2)
            self.assertFalse(r1 <= r2)
            self.assertTrue(r1 > r2)
            self.assertTrue(r1 >= r2)
            self.assertTrue(r2 < r1)
            self.assertTrue(r2 <= r1)
            self.assertFalse(r2 > r1)
            self.assertFalse(r2 >= r1)
        finally:
            dns.rdata._allow_relative_comparisons = saved

    def test_relative_vs_absolute_compare_strict(self):
        try:
            saved = dns.rdata._allow_relative_comparisons
            dns.rdata._allow_relative_comparisons = False
            r1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS, 'www.')
            r2 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS, 'www')
            self.assertFalse(r1 == r2)
            self.assertTrue(r1 != r2)
            def bad1():
                r1 < r2
            def bad2():
                r1 <= r2
            def bad3():
                r1 > r2
            def bad4():
                r1 >= r2
            self.assertRaises(dns.rdata.NoRelativeRdataOrdering, bad1)
            self.assertRaises(dns.rdata.NoRelativeRdataOrdering, bad2)
            self.assertRaises(dns.rdata.NoRelativeRdataOrdering, bad3)
            self.assertRaises(dns.rdata.NoRelativeRdataOrdering, bad4)
        finally:
            dns.rdata._allow_relative_comparisons = saved

    def test_absolute_vs_absolute_compare(self):
        r1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS, 'www.')
        r2 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS, 'xxx.')
        self.assertFalse(r1 == r2)
        self.assertTrue(r1 != r2)
        self.assertTrue(r1 < r2)
        self.assertTrue(r1 <= r2)
        self.assertFalse(r1 > r2)
        self.assertFalse(r1 >= r2)

    def test_relative_vs_relative_compare_unstrict(self):
        try:
            saved = dns.rdata._allow_relative_comparisons
            dns.rdata._allow_relative_comparisons = True
            r1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS, 'www')
            r2 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS, 'xxx')
            self.assertFalse(r1 == r2)
            self.assertTrue(r1 != r2)
            self.assertTrue(r1 < r2)
            self.assertTrue(r1 <= r2)
            self.assertFalse(r1 > r2)
            self.assertFalse(r1 >= r2)
        finally:
            dns.rdata._allow_relative_comparisons = saved

    def test_relative_vs_relative_compare_strict(self):
        try:
            saved = dns.rdata._allow_relative_comparisons
            dns.rdata._allow_relative_comparisons = False
            r1 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS, 'www')
            r2 = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NS, 'xxx')
            self.assertFalse(r1 == r2)
            self.assertTrue(r1 != r2)
            def bad1():
                r1 < r2
            def bad2():
                r1 <= r2
            def bad3():
                r1 > r2
            def bad4():
                r1 >= r2
            self.assertRaises(dns.rdata.NoRelativeRdataOrdering, bad1)
            self.assertRaises(dns.rdata.NoRelativeRdataOrdering, bad2)
            self.assertRaises(dns.rdata.NoRelativeRdataOrdering, bad3)
            self.assertRaises(dns.rdata.NoRelativeRdataOrdering, bad4)
        finally:
            dns.rdata._allow_relative_comparisons = saved

class UtilTestCase(unittest.TestCase):

    def test_Gateway_bad_type0(self):
        with self.assertRaises(SyntaxError):
            dns.rdtypes.util.Gateway(0, 'bad.')

    def test_Gateway_bad_type3(self):
        with self.assertRaises(SyntaxError):
            dns.rdtypes.util.Gateway(3, 'bad.')

    def test_Gateway_type4(self):
        with self.assertRaises(SyntaxError):
            dns.rdtypes.util.Gateway(4)
        with self.assertRaises(dns.exception.FormError):
            dns.rdtypes.util.Gateway.from_wire_parser(4, None)

    def test_Bitmap(self):
        b = dns.rdtypes.util.Bitmap
        tok = dns.tokenizer.Tokenizer('A MX')
        windows = b.from_text(tok).windows
        ba = bytearray()
        ba.append(0x40)  # bit 1, for A
        ba.append(0x01)  # bit 15, for MX
        self.assertEqual(windows, [(0, bytes(ba))])

    def test_Bitmap_with_duplicate_types(self):
        b = dns.rdtypes.util.Bitmap
        tok = dns.tokenizer.Tokenizer('A MX A A MX')
        windows = b.from_text(tok).windows
        ba = bytearray()
        ba.append(0x40)  # bit 1, for A
        ba.append(0x01)  # bit 15, for MX
        self.assertEqual(windows, [(0, bytes(ba))])

    def test_Bitmap_with_out_of_order_types(self):
        b = dns.rdtypes.util.Bitmap
        tok = dns.tokenizer.Tokenizer('MX A')
        windows = b.from_text(tok).windows
        ba = bytearray()
        ba.append(0x40)  # bit 1, for A
        ba.append(0x01)  # bit 15, for MX
        self.assertEqual(windows, [(0, bytes(ba))])

    def test_Bitmap_zero_padding_works(self):
        b = dns.rdtypes.util.Bitmap
        tok = dns.tokenizer.Tokenizer('SRV')
        windows = b.from_text(tok).windows
        ba = bytearray()
        ba.append(0)
        ba.append(0)
        ba.append(0)
        ba.append(0)
        ba.append(0x40)  # bit 33, for SRV
        self.assertEqual(windows, [(0, bytes(ba))])

    def test_Bitmap_has_type_0_set(self):
        b = dns.rdtypes.util.Bitmap
        with self.assertRaises(dns.exception.SyntaxError):
            tok = dns.tokenizer.Tokenizer('NONE A MX')
            b.from_text(tok)

    def test_Bitmap_empty_window_not_written(self):
        b = dns.rdtypes.util.Bitmap
        tok = dns.tokenizer.Tokenizer('URI CAA')  # types 256 and 257
        windows = b.from_text(tok).windows
        ba = bytearray()
        ba.append(0xc0)  # bits 0 and 1 in window 1
        self.assertEqual(windows, [(1, bytes(ba))])

    def test_Bitmap_ok_parse(self):
        parser = dns.wire.Parser(b'\x00\x01\x40')
        b = dns.rdtypes.util.Bitmap([])
        windows = b.from_wire_parser(parser).windows
        self.assertEqual(windows, [(0, b'@')])

    def test_Bitmap_0_length_window_parse(self):
        parser = dns.wire.Parser(b'\x00\x00')
        with self.assertRaises(ValueError):
            b = dns.rdtypes.util.Bitmap([])
            b.from_wire_parser(parser)

    def test_Bitmap_too_long_parse(self):
        parser = dns.wire.Parser(b'\x00\x21' + b'\x01' * 33)
        with self.assertRaises(ValueError):
            b = dns.rdtypes.util.Bitmap([])
            b.from_wire_parser(parser)

    def test_compressed_in_generic_is_bad(self):
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.MX,
                                r'\# 4 000aC000')

    def test_rdataset_ttl_conversion(self):
        rds1 = dns.rdataset.from_text('in', 'a', 300, '10.0.0.1')
        self.assertEqual(rds1.ttl, 300)
        rds2 = dns.rdataset.from_text('in', 'a', '5m', '10.0.0.1')
        self.assertEqual(rds2.ttl, 300)
        with self.assertRaises(ValueError):
            dns.rdataset.from_text('in', 'a', 1.6, '10.0.0.1')
        with self.assertRaises(dns.ttl.BadTTL):
            dns.rdataset.from_text('in', 'a', '10.0.0.1', '10.0.0.2')


Rdata = dns.rdata.Rdata


class RdataConvertersTestCase(unittest.TestCase):
    def test_as_name(self):
        n = dns.name.from_text('hi')
        self.assertEqual(Rdata._as_name(n), n)
        self.assertEqual(Rdata._as_name('hi'), n)
        with self.assertRaises(ValueError):
            Rdata._as_name(100)

    def test_as_uint8(self):
        self.assertEqual(Rdata._as_uint8(0), 0)
        with self.assertRaises(ValueError):
            Rdata._as_uint8('hi')
        with self.assertRaises(ValueError):
            Rdata._as_uint8(-1)
        with self.assertRaises(ValueError):
            Rdata._as_uint8(256)

    def test_as_uint16(self):
        self.assertEqual(Rdata._as_uint16(0), 0)
        with self.assertRaises(ValueError):
            Rdata._as_uint16('hi')
        with self.assertRaises(ValueError):
            Rdata._as_uint16(-1)
        with self.assertRaises(ValueError):
            Rdata._as_uint16(65536)

    def test_as_uint32(self):
        self.assertEqual(Rdata._as_uint32(0), 0)
        with self.assertRaises(ValueError):
            Rdata._as_uint32('hi')
        with self.assertRaises(ValueError):
            Rdata._as_uint32(-1)
        with self.assertRaises(ValueError):
            Rdata._as_uint32(2 ** 32)

    def test_as_uint48(self):
        self.assertEqual(Rdata._as_uint48(0), 0)
        with self.assertRaises(ValueError):
            Rdata._as_uint48('hi')
        with self.assertRaises(ValueError):
            Rdata._as_uint48(-1)
        with self.assertRaises(ValueError):
            Rdata._as_uint48(2 ** 48)

    def test_as_int(self):
        self.assertEqual(Rdata._as_int(0, 0, 10), 0)
        with self.assertRaises(ValueError):
            Rdata._as_int('hi', 0, 10)
        with self.assertRaises(ValueError):
            Rdata._as_int(-1, 0, 10)
        with self.assertRaises(ValueError):
            Rdata._as_int(11, 0, 10)

    def test_as_bool(self):
        self.assertEqual(Rdata._as_bool(True), True)
        self.assertEqual(Rdata._as_bool(False), False)
        with self.assertRaises(ValueError):
            Rdata._as_bool('hi')

    def test_as_ttl(self):
        self.assertEqual(Rdata._as_ttl(300), 300)
        self.assertEqual(Rdata._as_ttl('5m'), 300)
        self.assertEqual(Rdata._as_ttl(dns.ttl.MAX_TTL), dns.ttl.MAX_TTL)
        with self.assertRaises(dns.ttl.BadTTL):
            Rdata._as_ttl('hi')
        with self.assertRaises(ValueError):
            Rdata._as_ttl(1.9)
        with self.assertRaises(ValueError):
            Rdata._as_ttl(dns.ttl.MAX_TTL + 1)

if __name__ == '__main__':
    unittest.main()
