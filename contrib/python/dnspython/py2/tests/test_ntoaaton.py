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

from __future__ import print_function

import unittest
import binascii

import dns.exception
import dns.ipv4
import dns.ipv6
import dns.inet

# for convenience
aton4 = dns.ipv4.inet_aton
ntoa4 = dns.ipv4.inet_ntoa
aton6 = dns.ipv6.inet_aton
ntoa6 = dns.ipv6.inet_ntoa

v4_bad_addrs = ['256.1.1.1', '1.1.1', '1.1.1.1.1',
                '+1.1.1.1', '1.1.1.1+', '1..2.3.4', '.1.2.3.4',
                '1.2.3.4.']

class NtoAAtoNTestCase(unittest.TestCase):

    def test_aton1(self):
        a = aton6('::')
        self.failUnless(a == b'\x00' * 16)

    def test_aton2(self):
        a = aton6('::1')
        self.failUnless(a == b'\x00' * 15 + b'\x01')

    def test_aton3(self):
        a = aton6('::10.0.0.1')
        self.failUnless(a == b'\x00' * 12 + b'\x0a\x00\x00\x01')

    def test_aton4(self):
        a = aton6('abcd::dcba')
        self.failUnless(a == b'\xab\xcd' + b'\x00' * 12 + b'\xdc\xba')

    def test_aton5(self):
        a = aton6('1:2:3:4:5:6:7:8')
        self.assertEqual(a,
                        binascii.unhexlify(b'00010002000300040005000600070008'))

    def test_bad_aton1(self):
        def bad():
            aton6('abcd:dcba')
        self.failUnlessRaises(dns.exception.SyntaxError, bad)

    def test_bad_aton2(self):
        def bad():
            aton6('abcd::dcba::1')
        self.failUnlessRaises(dns.exception.SyntaxError, bad)

    def test_bad_aton3(self):
        def bad():
            aton6('1:2:3:4:5:6:7:8:9')
        self.failUnlessRaises(dns.exception.SyntaxError, bad)

    def test_aton6(self):
        a = aton6('::')
        self.assertEqual(a, b'\x00' * 16)

    def test_aton7(self):
        a = aton6('::1')
        self.assertEqual(a, b'\x00' * 15 + b'\x01')

    def test_aton8(self):
        a = aton6('::10.0.0.1')
        self.assertEqual(a, b'\x00' * 12 + b'\x0a\x00\x00\x01')

    def test_aton9(self):
        a = aton6('abcd::dcba')
        self.assertEqual(a, b'\xab\xcd' + b'\x00' * 12 + b'\xdc\xba')

    def test_ntoa1(self):
        b = binascii.unhexlify(b'00010002000300040005000600070008')
        t = ntoa6(b)
        self.assertEqual(t, '1:2:3:4:5:6:7:8')

    def test_ntoa2(self):
        b = b'\x00' * 16
        t = ntoa6(b)
        self.assertEqual(t, '::')

    def test_ntoa3(self):
        b = b'\x00' * 15 + b'\x01'
        t = ntoa6(b)
        self.assertEqual(t, '::1')

    def test_ntoa4(self):
        b = b'\x80' + b'\x00' * 15
        t = ntoa6(b)
        self.assertEqual(t, '8000::')

    def test_ntoa5(self):
        b = b'\x01\xcd' + b'\x00' * 12 + b'\x03\xef'
        t = ntoa6(b)
        self.assertEqual(t, '1cd::3ef')

    def test_ntoa6(self):
        b = binascii.unhexlify(b'ffff00000000ffff000000000000ffff')
        t = ntoa6(b)
        self.assertEqual(t, 'ffff:0:0:ffff::ffff')

    def test_ntoa7(self):
        b = binascii.unhexlify(b'00000000ffff000000000000ffffffff')
        t = ntoa6(b)
        self.assertEqual(t, '0:0:ffff::ffff:ffff')

    def test_ntoa8(self):
        b = binascii.unhexlify(b'ffff0000ffff00000000ffff00000000')
        t = ntoa6(b)
        self.assertEqual(t, 'ffff:0:ffff::ffff:0:0')

    def test_ntoa9(self):
        b = binascii.unhexlify(b'0000000000000000000000000a000001')
        t = ntoa6(b)
        self.assertEqual(t, '::10.0.0.1')

    def test_ntoa10(self):
        b = binascii.unhexlify(b'0000000000000000000000010a000001')
        t = ntoa6(b)
        self.assertEqual(t, '::1:a00:1')

    def test_ntoa11(self):
        b = binascii.unhexlify(b'00000000000000000000ffff0a000001')
        t = ntoa6(b)
        self.assertEqual(t, '::ffff:10.0.0.1')

    def test_ntoa12(self):
        b = binascii.unhexlify(b'000000000000000000000000ffffffff')
        t = ntoa6(b)
        self.assertEqual(t, '::255.255.255.255')

    def test_ntoa13(self):
        b = binascii.unhexlify(b'00000000000000000000ffffffffffff')
        t = ntoa6(b)
        self.assertEqual(t, '::ffff:255.255.255.255')

    def test_ntoa14(self):
        b = binascii.unhexlify(b'0000000000000000000000000001ffff')
        t = ntoa6(b)
        self.assertEqual(t, '::0.1.255.255')

    def test_bad_ntoa1(self):
        def bad():
            ntoa6('')
        self.failUnlessRaises(ValueError, bad)

    def test_bad_ntoa2(self):
        def bad():
            ntoa6('\x00' * 17)
        self.failUnlessRaises(ValueError, bad)

    def test_good_v4_aton(self):
        pairs = [('1.2.3.4', b'\x01\x02\x03\x04'),
                 ('255.255.255.255', b'\xff\xff\xff\xff'),
                 ('0.0.0.0', b'\x00\x00\x00\x00')]
        for (t, b) in pairs:
            b1 = aton4(t)
            t1 = ntoa4(b1)
            self.assertEqual(b1, b)
            self.assertEqual(t1, t)

    def test_bad_v4_aton(self):
        def make_bad(a):
            def bad():
                return aton4(a)
            return bad
        for addr in v4_bad_addrs:
            print(addr)
            self.failUnlessRaises(dns.exception.SyntaxError, make_bad(addr))

    def test_bad_v6_aton(self):
        addrs = ['+::0', '0::0::', '::0::', '1:2:3:4:5:6:7:8:9',
                 ':::::::']
        embedded = ['::' + x for x in v4_bad_addrs]
        addrs.extend(embedded)
        def make_bad(a):
            def bad():
                x = aton6(a)
            return bad
        for addr in addrs:
            self.failUnlessRaises(dns.exception.SyntaxError, make_bad(addr))

    def test_rfc5952_section_4_2_2(self):
        addr = '2001:db8:0:1:1:1:1:1'
        b1 = aton6(addr)
        t1 = ntoa6(b1)
        self.assertEqual(t1, addr)

    def test_is_mapped(self):
        t1 = '2001:db8:0:1:1:1:1:1'
        t2 = '::ffff:127.0.0.1'
        t3 = '1::ffff:127.0.0.1'
        self.failIf(dns.ipv6.is_mapped(aton6(t1)))
        self.failUnless(dns.ipv6.is_mapped(aton6(t2)))
        self.failIf(dns.ipv6.is_mapped(aton6(t3)))

    def test_is_multicast(self):
        t1 = '223.0.0.1'
        t2 = '240.0.0.1'
        t3 = '224.0.0.1'
        t4 = '239.0.0.1'
        t5 = 'fe00::1'
        t6 = 'ff00::1'
        self.failIf(dns.inet.is_multicast(t1))
        self.failIf(dns.inet.is_multicast(t2))
        self.failUnless(dns.inet.is_multicast(t3))
        self.failUnless(dns.inet.is_multicast(t4))
        self.failIf(dns.inet.is_multicast(t5))
        self.failUnless(dns.inet.is_multicast(t6))

if __name__ == '__main__':
    unittest.main()
