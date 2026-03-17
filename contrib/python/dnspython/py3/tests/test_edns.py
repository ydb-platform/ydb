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

import operator
import struct
import unittest

from io import BytesIO

import dns.edns
import dns.wire

class OptionTestCase(unittest.TestCase):
    def testGenericOption(self):
        opt = dns.edns.GenericOption(3, b'data')
        io = BytesIO()
        opt.to_wire(io)
        data = io.getvalue()
        self.assertEqual(data, b'data')
        self.assertEqual(dns.edns.option_from_wire(3, data, 0, len(data)), opt)
        self.assertEqual(str(opt), 'Generic 3')

    def testECSOption_prefix_length(self):
        opt = dns.edns.ECSOption('1.2.255.33', 20)
        io = BytesIO()
        opt.to_wire(io)
        data = io.getvalue()
        self.assertEqual(data, b'\x00\x01\x14\x00\x01\x02\xf0')

    def testECSOption(self):
        opt = dns.edns.ECSOption('1.2.3.4', 24)
        io = BytesIO()
        opt.to_wire(io)
        data = io.getvalue()
        self.assertEqual(data, b'\x00\x01\x18\x00\x01\x02\x03')
        # default srclen
        opt = dns.edns.ECSOption('1.2.3.4')
        io = BytesIO()
        opt.to_wire(io)
        data = io.getvalue()
        self.assertEqual(data, b'\x00\x01\x18\x00\x01\x02\x03')
        self.assertEqual(opt.to_text(), 'ECS 1.2.3.4/24 scope/0')

    def testECSOption25(self):
        opt = dns.edns.ECSOption('1.2.3.255', 25)
        io = BytesIO()
        opt.to_wire(io)
        data = io.getvalue()
        self.assertEqual(data, b'\x00\x01\x19\x00\x01\x02\x03\x80')

        opt2 = dns.edns.option_from_wire(dns.edns.ECS, data, 0, len(data))
        self.assertEqual(opt2.otype, dns.edns.ECS)
        self.assertEqual(opt2.address, '1.2.3.128')
        self.assertEqual(opt2.srclen, 25)
        self.assertEqual(opt2.scopelen, 0)

    def testECSOption_v6(self):
        opt = dns.edns.ECSOption('2001:4b98::1')
        io = BytesIO()
        opt.to_wire(io)
        data = io.getvalue()
        self.assertEqual(data, b'\x00\x02\x38\x00\x20\x01\x4b\x98\x00\x00\x00')

        opt2 = dns.edns.option_from_wire(dns.edns.ECS, data, 0, len(data))
        self.assertEqual(opt2.otype, dns.edns.ECS)
        self.assertEqual(opt2.address, '2001:4b98::')
        self.assertEqual(opt2.srclen, 56)
        self.assertEqual(opt2.scopelen, 0)

    def testECSOption_from_text_valid(self):
        ecs1 = dns.edns.ECSOption.from_text('1.2.3.4/24/0')
        self.assertEqual(ecs1, dns.edns.ECSOption('1.2.3.4', 24, 0))

        ecs2 = dns.edns.ECSOption.from_text('1.2.3.4/24')
        self.assertEqual(ecs2, dns.edns.ECSOption('1.2.3.4', 24, 0))

        ecs3 = dns.edns.ECSOption.from_text('ECS 1.2.3.4/24')
        self.assertEqual(ecs3, dns.edns.ECSOption('1.2.3.4', 24, 0))

        ecs4 = dns.edns.ECSOption.from_text('ECS 1.2.3.4/24/32')
        self.assertEqual(ecs4, dns.edns.ECSOption('1.2.3.4', 24, 32))

        ecs5 = dns.edns.ECSOption.from_text('2001:4b98::1/64/56')
        self.assertEqual(ecs5, dns.edns.ECSOption('2001:4b98::1', 64, 56))

        ecs6 = dns.edns.ECSOption.from_text('2001:4b98::1/64')
        self.assertEqual(ecs6, dns.edns.ECSOption('2001:4b98::1', 64, 0))

        ecs7 = dns.edns.ECSOption.from_text('ECS 2001:4b98::1/0')
        self.assertEqual(ecs7, dns.edns.ECSOption('2001:4b98::1', 0, 0))

        ecs8 = dns.edns.ECSOption.from_text('ECS 2001:4b98::1/64/128')
        self.assertEqual(ecs8, dns.edns.ECSOption('2001:4b98::1', 64, 128))

    def testECSOption_from_text_invalid(self):
        with self.assertRaises(ValueError):
            dns.edns.ECSOption.from_text('some random text 1.2.3.4/24/0 24')

        with self.assertRaises(ValueError):
            dns.edns.ECSOption.from_text('1.2.3.4/twentyfour')

        with self.assertRaises(ValueError):
            dns.edns.ECSOption.from_text('BOGUS 1.2.3.4/5/6/7')

        with self.assertRaises(ValueError):
            dns.edns.ECSOption.from_text('1.2.3.4/5/6/7')

        with self.assertRaises(ValueError):
            dns.edns.ECSOption.from_text('1.2.3.4/24/O') # <-- that's not a zero

        with self.assertRaises(ValueError):
            dns.edns.ECSOption.from_text('')

        with self.assertRaises(ValueError):
            dns.edns.ECSOption.from_text('1.2.3.4/2001:4b98::1/24')

    def testECSOption_from_wire_invalid(self):
        with self.assertRaises(ValueError):
            opt = dns.edns.option_from_wire(dns.edns.ECS,
                                            b'\x00\xff\x18\x00\x01\x02\x03',
                                            0, 7)
    def testEDEOption(self):
        opt = dns.edns.EDEOption(3)
        io = BytesIO()
        opt.to_wire(io)
        data = io.getvalue()
        self.assertEqual(data, b'\x00\x03')
        self.assertEqual(str(opt), 'EDE 3')
        # with text
        opt = dns.edns.EDEOption(16, 'test')
        io = BytesIO()
        opt.to_wire(io)
        data = io.getvalue()
        self.assertEqual(data, b'\x00\x10test')

    def testEDEOption_invalid(self):
        with self.assertRaises(ValueError):
            opt = dns.edns.EDEOption(-1)
        with self.assertRaises(ValueError):
            opt = dns.edns.EDEOption(65536)
        with self.assertRaises(ValueError):
            opt = dns.edns.EDEOption(0, 0)

    def testEDEOption_from_wire(self):
        data = b'\x00\01'
        self.assertEqual(
            dns.edns.option_from_wire(dns.edns.EDE, data, 0, 2),
            dns.edns.EDEOption(1))
        data = b'\x00\01test'
        self.assertEqual(
            dns.edns.option_from_wire(dns.edns.EDE, data, 0, 6),
            dns.edns.EDEOption(1, 'test'))
        # utf-8 text MAY be null-terminated
        data = b'\x00\01test\x00'
        self.assertEqual(
            dns.edns.option_from_wire(dns.edns.EDE, data, 0, 7),
            dns.edns.EDEOption(1, 'test'))

    def test_basic_relations(self):
        o1 = dns.edns.ECSOption.from_text('1.2.3.0/24/0')
        o2 = dns.edns.ECSOption.from_text('1.2.4.0/24/0')
        self.assertTrue(o1 == o1)
        self.assertTrue(o1 != o2)
        self.assertTrue(o1 < o2)
        self.assertTrue(o1 <= o2)
        self.assertTrue(o2 > o1)
        self.assertTrue(o2 >= o1)
        o1 = dns.edns.ECSOption.from_text('1.2.4.0/23/0')
        o2 = dns.edns.ECSOption.from_text('1.2.4.0/24/0')
        self.assertTrue(o1 < o2)
        o1 = dns.edns.ECSOption.from_text('1.2.4.0/24/0')
        o2 = dns.edns.ECSOption.from_text('1.2.4.0/24/1')
        self.assertTrue(o1 < o2)

    def test_incompatible_relations(self):
        o1 = dns.edns.GenericOption(3, b'data')
        o2 = dns.edns.ECSOption.from_text('1.2.3.5/24/0')
        for oper in [operator.lt, operator.le, operator.ge, operator.gt]:
            self.assertRaises(TypeError, lambda: oper(o1, o2))
        self.assertFalse(o1 == o2)
        self.assertTrue(o1 != o2)
        self.assertFalse(o1 == 123)
        self.assertTrue(o1 != 123)

    def test_option_registration(self):
        U32OptionType = 9999

        class U32Option(dns.edns.Option):
            def __init__(self, value=None):
                super().__init__(U32OptionType)
                self.value = value

            def to_wire(self, file=None):
                data = struct.pack('!I', self.value)
                if file:
                    file.write(data)
                else:
                    return data

            @classmethod
            def from_wire_parser(cls, otype, parser):
                (value,) = parser.get_struct('!I')
                return cls(value)

        try:
            dns.edns.register_type(U32Option, U32OptionType)
            generic = dns.edns.GenericOption(U32OptionType, b'\x00\x00\x00\x01')
            wire1 = generic.to_wire()
            u32 = dns.edns.option_from_wire_parser(U32OptionType,
                                                   dns.wire.Parser(wire1))
            self.assertEqual(u32.value, 1)
            wire2 = u32.to_wire()
            self.assertEqual(wire1, wire2)
            self.assertEqual(u32, generic)
        finally:
            dns.edns._type_to_class.pop(U32OptionType, None)

        opt = dns.edns.option_from_wire_parser(9999, dns.wire.Parser(wire1))
        self.assertEqual(opt, generic)
