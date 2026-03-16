# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import unittest

import dns.ttl

class TTLTestCase(unittest.TestCase):

    def test_bind_style_ok(self):
        ttl = dns.ttl.from_text('2w1d1h1m1s')
        self.assertEqual(ttl, 2 * 604800 + 86400 + 3600 + 60 + 1)

    def test_bind_style_ok2(self):
        # no one should do this, but it is legal! :)
        ttl = dns.ttl.from_text('1s2w1m1d1h')
        self.assertEqual(ttl, 2 * 604800 + 86400 + 3600 + 60 + 1)

    def test_bind_style_bad_unit(self):
        with self.assertRaises(dns.ttl.BadTTL):
            dns.ttl.from_text('5y')

    def test_bind_style_no_unit(self):
        with self.assertRaises(dns.ttl.BadTTL):
            dns.ttl.from_text('1d5')

    def test_bind_style_leading_unit(self):
        with self.assertRaises(dns.ttl.BadTTL):
            dns.ttl.from_text('s')

    def test_bind_style_unit_without_digits(self):
        with self.assertRaises(dns.ttl.BadTTL):
            dns.ttl.from_text('1mw')

    def test_empty(self):
        with self.assertRaises(dns.ttl.BadTTL):
            dns.ttl.from_text('')
