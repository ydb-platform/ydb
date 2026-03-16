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
import base64

import dns.name
import dns.zone
import dns.rdtypes.ANY.TKEY
from dns.rdataclass import RdataClass
from dns.rdatatype import RdataType


class RdtypeAnyTKeyTestCase(unittest.TestCase):
    tkey_rdata_text = 'gss-tsig. 1594203795 1594206664 3 0 KEYKEYKEYKEYKEYKEYKEYKEYKEYKEYKEYKEY OTHEROTHEROTHEROTHEROTHEROTHEROT'
    tkey_rdata_text_no_other = 'gss-tsig. 1594203795 1594206664 3 0 KEYKEYKEYKEYKEYKEYKEYKEYKEYKEYKEYKEY'

    def testTextOptionalData(self):
        # construct the rdata from text and extract the TKEY
        tkey = dns.rdata.from_text(
            RdataClass.ANY, RdataType.TKEY,
            RdtypeAnyTKeyTestCase.tkey_rdata_text, origin='.')
        self.assertEqual(type(tkey), dns.rdtypes.ANY.TKEY.TKEY)

        # go to text and compare
        tkey_out_text = tkey.to_text(relativize=False)
        self.assertEqual(tkey_out_text,
                         RdtypeAnyTKeyTestCase.tkey_rdata_text)

    def testTextNoOptionalData(self):
        # construct the rdata from text and extract the TKEY
        tkey = dns.rdata.from_text(
            RdataClass.ANY, RdataType.TKEY,
            RdtypeAnyTKeyTestCase.tkey_rdata_text_no_other, origin='.')
        self.assertEqual(type(tkey), dns.rdtypes.ANY.TKEY.TKEY)

        # go to text and compare
        tkey_out_text = tkey.to_text(relativize=False)
        self.assertEqual(tkey_out_text,
                         RdtypeAnyTKeyTestCase.tkey_rdata_text_no_other)

    def testWireOptionalData(self):
        key = base64.b64decode('KEYKEYKEYKEYKEYKEYKEYKEYKEYKEYKEYKEY')
        other = base64.b64decode('OTHEROTHEROTHEROTHEROTHEROTHEROT')

        # construct the TKEY and compare the text output
        tkey = dns.rdtypes.ANY.TKEY.TKEY(dns.rdataclass.ANY,
                                         dns.rdatatype.TKEY,
                                         dns.name.from_text('gss-tsig.'),
                                         1594203795, 1594206664,
                                         3, 0, key, other)
        self.assertEqual(tkey.to_text(relativize=False),
                         RdtypeAnyTKeyTestCase.tkey_rdata_text)

        # go to/from wire and compare the text output
        wire = tkey.to_wire()
        tkey_out_wire = dns.rdata.from_wire(dns.rdataclass.ANY,
                                            dns.rdatatype.TKEY,
                                            wire, 0, len(wire))
        self.assertEqual(tkey_out_wire.to_text(relativize=False),
                         RdtypeAnyTKeyTestCase.tkey_rdata_text)

    def testWireNoOptionalData(self):
        key = base64.b64decode('KEYKEYKEYKEYKEYKEYKEYKEYKEYKEYKEYKEY')

        # construct the TKEY with no 'other' data and compare the text output
        tkey = dns.rdtypes.ANY.TKEY.TKEY(dns.rdataclass.ANY,
                                         dns.rdatatype.TKEY,
                                         dns.name.from_text('gss-tsig.'),
                                         1594203795, 1594206664,
                                         3, 0, key)
        self.assertEqual(tkey.to_text(relativize=False),
                         RdtypeAnyTKeyTestCase.tkey_rdata_text_no_other)

        # go to/from wire and compare the text output
        wire = tkey.to_wire()
        tkey_out_wire = dns.rdata.from_wire(dns.rdataclass.ANY,
                                            dns.rdatatype.TKEY,
                                            wire, 0, len(wire))
        self.assertEqual(tkey_out_wire.to_text(relativize=False),
                         RdtypeAnyTKeyTestCase.tkey_rdata_text_no_other)
