# Copyright (C) 2014 Red Hat, Inc.
# Author: Petr Spacek <pspacek@redhat.com>
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose with or without fee is hereby granted,
# provided that the above copyright notice and this permission notice
# appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND RED HAT DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL NOMINUM BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
# OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import unittest

import dns.rrset
import dns.rdtypes.ANY.DNSKEY

from typing import Set # pylint: disable=unused-import

class RdtypeAnyDnskeyTestCase(unittest.TestCase):

    def testFlagsAll(self): # type: () -> None
        '''Test that all defined flags are recognized.'''
        good_s = {'SEP', 'REVOKE', 'ZONE'}
        good_f = 0x181
        self.assertEqual(dns.rdtypes.ANY.DNSKEY.SEP |
                         dns.rdtypes.ANY.DNSKEY.REVOKE |
                         dns.rdtypes.ANY.DNSKEY.ZONE, good_f)

    def testFlagsRRToText(self): # type: () -> None
        '''Test that RR method returns correct flags.'''
        rr = dns.rrset.from_text('foo', 300, 'IN', 'DNSKEY', '257 3 8 KEY=')[0]
        self.assertEqual(dns.rdtypes.ANY.DNSKEY.ZONE |
                         dns.rdtypes.ANY.DNSKEY.SEP,
                         rr.flags)


if __name__ == '__main__':
    unittest.main()
