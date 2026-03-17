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

import unittest

import dns.rdata
import dns.rdataclass
import dns.rdatatype

import tests.ttxt_module

class RdataTestCase(unittest.TestCase):

    def test_str(self):
        rdata = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A, "1.2.3.4")
        self.failUnless(rdata.address == "1.2.3.4")

    def test_unicode(self):
        rdata = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A, u"1.2.3.4")
        self.failUnless(rdata.address == "1.2.3.4")

    def test_module_registration(self):
        TTXT = 64001
        dns.rdata.register_type(tests.ttxt_module, TTXT, 'TTXT')
        rdata = dns.rdata.from_text(dns.rdataclass.IN, TTXT, 'hello world')
        self.failUnless(rdata.strings == [b'hello', b'world'])
        self.failUnless(dns.rdatatype.to_text(TTXT) == 'TTXT')
        self.failUnless(dns.rdatatype.from_text('TTXT') == TTXT)

    def test_module_reregistration(self):
        def bad():
            TTXTTWO = dns.rdatatype.TXT
            dns.rdata.register_type(tests.ttxt_module, TTXTTWO, 'TTXTTWO')
        self.failUnlessRaises(dns.rdata.RdatatypeExists, bad)

if __name__ == '__main__':
    unittest.main()
