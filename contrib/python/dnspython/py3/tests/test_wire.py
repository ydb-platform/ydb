# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import unittest

import dns.exception
import dns.wire
import dns.name


class BinaryTestCase(unittest.TestCase):

    def test_basic(self):
        wire = bytes.fromhex('0102010203040102')
        p = dns.wire.Parser(wire)
        self.assertEqual(p.get_uint16(), 0x0102)
        with p.restrict_to(5):
            self.assertEqual(p.get_uint32(), 0x01020304)
            self.assertEqual(p.get_uint8(), 0x01)
            self.assertEqual(p.remaining(), 0)
            with self.assertRaises(dns.exception.FormError):
                p.get_uint16()
        self.assertEqual(p.remaining(), 1)
        self.assertEqual(p.get_uint8(), 0x02)
        with self.assertRaises(dns.exception.FormError):
            p.get_uint8()

    def test_name(self):
        # www.dnspython.org NS IN question
        wire = b'\x03www\x09dnspython\x03org\x00\x00\x02\x00\x01'
        expected = dns.name.from_text('www.dnspython.org')
        p = dns.wire.Parser(wire)
        self.assertEqual(p.get_name(), expected)
        self.assertEqual(p.get_uint16(), 2)
        self.assertEqual(p.get_uint16(), 1)
        self.assertEqual(p.remaining(), 0)

    def test_relativized_name(self):
        # www.dnspython.org NS IN question
        wire = b'\x03www\x09dnspython\x03org\x00\x00\x02\x00\x01'
        origin = dns.name.from_text('dnspython.org')
        expected = dns.name.from_text('www', None)
        p = dns.wire.Parser(wire)
        self.assertEqual(p.get_name(origin), expected)
        self.assertEqual(p.remaining(), 4)

    def test_compressed_name(self):
        # www.dnspython.org NS IN question
        wire = b'\x09dnspython\x03org\x00\x03www\xc0\x00'
        expected1 = dns.name.from_text('dnspython.org')
        expected2 = dns.name.from_text('www.dnspython.org')
        p = dns.wire.Parser(wire)
        self.assertEqual(p.get_name(), expected1)
        self.assertEqual(p.get_name(), expected2)
        self.assertEqual(p.remaining(), 0)
        # verify the restore_furthest()
        self.assertEqual(p.current, len(wire))

    def test_seek(self):
        wire = b'\x09dnspython\x03org\x00'
        p = dns.wire.Parser(wire)
        p.seek(10)
        self.assertEqual(p.get_uint8(), 3)
        # seeking to the end index is OK
        p.seek(len(wire))
        self.assertEqual(p.current, p.end)
        with self.assertRaises(dns.exception.FormError):
            # but reading there will not succeed
            p.get_uint8()
        with self.assertRaises(dns.exception.FormError):
            p.seek(-1)
        with self.assertRaises(dns.exception.FormError):
            p.seek(len(wire) + 1)

    def test_not_reading_everything_in_restriction(self):
        wire = bytes.fromhex('0102010203040102')
        p = dns.wire.Parser(wire)
        with self.assertRaises(dns.exception.FormError):
            with p.restrict_to(5):
                v = p.get_uint8()
                self.assertEqual(v, 1)
                # don't read the other 4 bytes

    def test_restriction_does_not_mask_exception(self):
        wire = bytes.fromhex('0102010203040102')
        p = dns.wire.Parser(wire)
        with self.assertRaises(NotImplementedError):
            with p.restrict_to(5):
                raise NotImplementedError
