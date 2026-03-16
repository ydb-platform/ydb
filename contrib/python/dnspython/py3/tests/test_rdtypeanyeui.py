# Copyright (C) 2015 Red Hat, Inc.
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
import dns.rdtypes.ANY.EUI48
import dns.rdtypes.ANY.EUI64
import dns.exception


class RdtypeAnyEUI48TestCase(unittest.TestCase):
    def testInstOk(self):
        '''Valid binary input.'''
        eui = b'\x01\x23\x45\x67\x89\xab'
        inst = dns.rdtypes.ANY.EUI48.EUI48(dns.rdataclass.IN,
                                           dns.rdatatype.EUI48,
                                           eui)
        self.assertEqual(inst.eui, eui)

    def testInstLength(self):
        '''Incorrect input length.'''
        eui = b'\x01\x23\x45\x67\x89\xab\xcd'
        with self.assertRaises(dns.exception.FormError):
            dns.rdtypes.ANY.EUI48.EUI48(dns.rdataclass.IN,
                                        dns.rdatatype.EUI48,
                                        eui)

    def testFromTextOk(self):
        '''Valid text input.'''
        r1 = dns.rrset.from_text('foo', 300, 'IN', 'EUI48',
                                 '01-23-45-67-89-ab')
        eui = b'\x01\x23\x45\x67\x89\xab'
        self.assertEqual(r1[0].eui, eui)

    def testFromTextLength(self):
        '''Invalid input length.'''
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rrset.from_text('foo', 300, 'IN', 'EUI48',
                                '00-01-23-45-67-89-ab')

    def testFromTextDelim(self):
        '''Invalid delimiter.'''
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rrset.from_text('foo', 300, 'IN', 'EUI48', '01_23-45-67-89-ab')

    def testFromTextExtraDash(self):
        '''Extra dash instead of hex digit.'''
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rrset.from_text('foo', 300, 'IN', 'EUI48', '0--23-45-67-89-ab')

    def testFromTextMultipleTokens(self):
        '''Invalid input divided to multiple tokens.'''
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rrset.from_text('foo', 300, 'IN', 'EUI48', '01 23-45-67-89-ab')

    def testFromTextInvalidHex(self):
        '''Invalid hexadecimal input.'''
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rrset.from_text('foo', 300, 'IN', 'EUI48', 'g0-23-45-67-89-ab')

    def testToTextOk(self):
        '''Valid text output.'''
        eui = b'\x01\x23\x45\x67\x89\xab'
        exp_text = '01-23-45-67-89-ab'
        inst = dns.rdtypes.ANY.EUI48.EUI48(dns.rdataclass.IN,
                                           dns.rdatatype.EUI48,
                                           eui)
        text = inst.to_text()
        self.assertEqual(exp_text, text)

    def testToWire(self):
        '''Valid wire format.'''
        eui = b'\x01\x23\x45\x67\x89\xab'
        inst = dns.rdtypes.ANY.EUI48.EUI48(dns.rdataclass.IN,
                                           dns.rdatatype.EUI48,
                                           eui)
        self.assertEqual(inst.to_wire(), eui)

    def testFromWireOk(self):
        '''Valid wire format.'''
        eui = b'\x01\x23\x45\x67\x89\xab'
        pad_len = 100
        wire = b'x' * pad_len + eui + b'y' * pad_len * 2
        inst = dns.rdata.from_wire(dns.rdataclass.IN, dns.rdatatype.EUI48,
                                   wire, pad_len, len(eui))
        self.assertEqual(inst.eui, eui)

    def testFromWireLength(self):
        '''Valid wire format.'''
        eui = b'\x01\x23\x45\x67\x89'
        pad_len = 100
        wire = b'x' * pad_len + eui + b'y' * pad_len * 2
        with self.assertRaises(dns.exception.FormError):
            dns.rdata.from_wire(dns.rdataclass.IN, dns.rdatatype.EUI48,
                                wire, pad_len, len(eui))


class RdtypeAnyEUI64TestCase(unittest.TestCase):
    def testInstOk(self):
        '''Valid binary input.'''
        eui = b'\x01\x23\x45\x67\x89\xab\xcd\xef'
        inst = dns.rdtypes.ANY.EUI64.EUI64(dns.rdataclass.IN,
                                           dns.rdatatype.EUI64,
                                           eui)
        self.assertEqual(inst.eui, eui)

    def testInstLength(self):
        '''Incorrect input length.'''
        eui = b'\x01\x23\x45\x67\x89\xab'
        with self.assertRaises(dns.exception.FormError):
            dns.rdtypes.ANY.EUI64.EUI64(dns.rdataclass.IN,
                                        dns.rdatatype.EUI64,
                                        eui)

    def testFromTextOk(self):
        '''Valid text input.'''
        r1 = dns.rrset.from_text('foo', 300, 'IN', 'EUI64',
                                 '01-23-45-67-89-ab-cd-ef')
        eui = b'\x01\x23\x45\x67\x89\xab\xcd\xef'
        self.assertEqual(r1[0].eui, eui)

    def testFromTextLength(self):
        '''Invalid input length.'''
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rrset.from_text('foo', 300, 'IN', 'EUI64',
                                '01-23-45-67-89-ab')

    def testFromTextDelim(self):
        '''Invalid delimiter.'''
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rrset.from_text('foo', 300, 'IN', 'EUI64',
                                '01_23-45-67-89-ab-cd-ef')

    def testFromTextExtraDash(self):
        '''Extra dash instead of hex digit.'''
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rrset.from_text('foo', 300, 'IN', 'EUI64',
                                '0--23-45-67-89-ab-cd-ef')

    def testFromTextMultipleTokens(self):
        '''Invalid input divided to multiple tokens.'''
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rrset.from_text('foo', 300, 'IN', 'EUI64',
                                '01 23-45-67-89-ab-cd-ef')

    def testFromTextInvalidHex(self):
        '''Invalid hexadecimal input.'''
        with self.assertRaises(dns.exception.SyntaxError):
            dns.rrset.from_text('foo', 300, 'IN', 'EUI64',
                                'g0-23-45-67-89-ab-cd-ef')

    def testToTextOk(self):
        '''Valid text output.'''
        eui = b'\x01\x23\x45\x67\x89\xab\xcd\xef'
        exp_text = '01-23-45-67-89-ab-cd-ef'
        inst = dns.rdtypes.ANY.EUI64.EUI64(dns.rdataclass.IN,
                                           dns.rdatatype.EUI64,
                                           eui)
        text = inst.to_text()
        self.assertEqual(exp_text, text)

    def testToWire(self):
        '''Valid wire format.'''
        eui = b'\x01\x23\x45\x67\x89\xab\xcd\xef'
        inst = dns.rdtypes.ANY.EUI64.EUI64(dns.rdataclass.IN,
                                           dns.rdatatype.EUI64,
                                           eui)
        self.assertEqual(inst.to_wire(), eui)

    def testFromWireOk(self):
        '''Valid wire format.'''
        eui = b'\x01\x23\x45\x67\x89\xab\xcd\xef'
        pad_len = 100
        wire = b'x' * pad_len + eui + b'y' * pad_len * 2
        inst = dns.rdata.from_wire(dns.rdataclass.IN, dns.rdatatype.EUI64,
                                   wire, pad_len, len(eui))
        self.assertEqual(inst.eui, eui)

    def testFromWireLength(self):
        '''Valid wire format.'''
        eui = b'\x01\x23\x45\x67\x89'
        pad_len = 100
        wire = b'x' * pad_len + eui + b'y' * pad_len * 2
        with self.assertRaises(dns.exception.FormError):
            dns.rdata.from_wire(dns.rdataclass.IN, dns.rdatatype.EUI64,
                                wire, pad_len, len(eui))


if __name__ == '__main__':
    unittest.main()
