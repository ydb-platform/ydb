# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

# Copyright (C) 2006-2017 Nominum, Inc.
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
import dns.rdtypes.ANY.TXT
import dns.ttl

class NSEC3TestCase(unittest.TestCase):
    def test_NSEC3_bitmap(self):
        rdata = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.NSEC3,
                u"1 0 100 ABCD SCBCQHKU35969L2A68P3AD59LHF30715 A CAA TYPE65534")
        bitmap = bytearray(b'\0' * 32)
        bitmap[31] = bitmap[31] | 2
        self.assertEqual(rdata.windows, [(0, bytearray(b'@')),
                                         (1, bytearray(b'@')), # CAA = 257
                                         (255, bitmap)
                                        ])

if __name__ == '__main__':
    unittest.main()
