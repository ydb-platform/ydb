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

import dns.flags
import dns.rcode
import dns.opcode

class FlagsTestCase(unittest.TestCase):

    def test_rcode1(self):
        self.assertEqual(dns.rcode.from_text('FORMERR'), dns.rcode.FORMERR)

    def test_rcode2(self):
        self.assertEqual(dns.rcode.to_text(dns.rcode.FORMERR), "FORMERR")

    def test_rcode3(self):
        self.assertEqual(dns.rcode.to_flags(dns.rcode.FORMERR), (1, 0))

    def test_rcode4(self):
        self.assertEqual(dns.rcode.to_flags(dns.rcode.BADVERS),
                         (0, 0x01000000))

    def test_rcode6(self):
        self.assertEqual(dns.rcode.from_flags(0, 0x01000000),
                         dns.rcode.BADVERS)

    def test_rcode7(self):
        self.assertEqual(dns.rcode.from_flags(5, 0), dns.rcode.REFUSED)

    def test_rcode8(self):
        def bad():
            dns.rcode.to_flags(4096)
        self.assertRaises(ValueError, bad)

    def test_flags1(self):
        self.assertEqual(dns.flags.from_text("RA RD AA QR"),
                         dns.flags.QR|dns.flags.AA|dns.flags.RD|dns.flags.RA)

    def test_flags2(self):
        flags = dns.flags.QR|dns.flags.AA|dns.flags.RD|dns.flags.RA
        self.assertEqual(dns.flags.to_text(flags), "QR AA RD RA")

    def test_rcode_badvers(self):
        rcode = dns.rcode.BADVERS
        self.assertEqual(rcode.value, 16)
        self.assertEqual(rcode.name, 'BADVERS')
        self.assertEqual(dns.rcode.to_text(rcode), 'BADVERS')

    def test_rcode_badsig(self):
        rcode = dns.rcode.BADSIG
        self.assertEqual(rcode.value, 16)
        # Yes, we mean BADVERS on the next line.  BADSIG and BADVERS have
        # the same code.
        self.assertEqual(rcode.name, 'BADVERS')
        self.assertEqual(dns.rcode.to_text(rcode), 'BADVERS')
        # In TSIG text mode, it should be BADSIG
        self.assertEqual(dns.rcode.to_text(rcode, True), 'BADSIG')

    def test_unknown_rcode(self):
        with self.assertRaises(dns.rcode.UnknownRcode):
            dns.rcode.Rcode.make('BOGUS')


if __name__ == '__main__':
    unittest.main()
