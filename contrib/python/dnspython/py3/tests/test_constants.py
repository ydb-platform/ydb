# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import unittest

import dns.dnssec
import dns.rdtypes.dnskeybase
import dns.flags
import dns.rcode
import dns.opcode
import dns.message
import dns.update
import dns.edns

import tests.util


class ConstantsTestCase(unittest.TestCase):

    def test_dnssec_constants(self):
        tests.util.check_enum_exports(dns.dnssec, self.assertEqual,
                                      only={dns.dnssec.Algorithm})
        tests.util.check_enum_exports(dns.rdtypes.dnskeybase, self.assertEqual)

    def test_flags_constants(self):
        tests.util.check_enum_exports(dns.flags, self.assertEqual)
        tests.util.check_enum_exports(dns.rcode, self.assertEqual)
        tests.util.check_enum_exports(dns.opcode, self.assertEqual)

    def test_message_constants(self):
        tests.util.check_enum_exports(dns.message, self.assertEqual)
        tests.util.check_enum_exports(dns.update, self.assertEqual)

    def test_rdata_constants(self):
        tests.util.check_enum_exports(dns.rdataclass, self.assertEqual)
        tests.util.check_enum_exports(dns.rdatatype, self.assertEqual)

    def test_edns_constants(self):
        tests.util.check_enum_exports(dns.edns, self.assertEqual,
                                      only={dns.edns.OptionType})
