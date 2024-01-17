#
# This file is part of pyasn1-modules software.
#
# Copyright (c) 2019, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder

from pyasn1_modules import pem
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc8398


class EAITestCase(unittest.TestCase):
    pem_text = "oCAGCCsGAQUFBwgJoBQMEuiAgeW4q0BleGFtcGxlLmNvbQ=="

    def setUp(self):
        self.asn1Spec = rfc5280.GeneralName()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertIn(asn1Object['otherName']['type-id'],
                      rfc5280.anotherNameMap)
        self.assertEqual(rfc8398.id_on_SmtpUTF8Mailbox,
                         asn1Object['otherName']['type-id'])

        eai, rest = der_decoder(
            asn1Object['otherName']['value'],
            asn1Spec=rfc5280.anotherNameMap[asn1Object['otherName']['type-id']])

        self.assertFalse(rest)
        self.assertTrue(eai.prettyPrint())
        self.assertEqual(asn1Object['otherName']['value'], der_encoder(eai))
        self.assertEqual(u'\u8001', eai[0])
        self.assertEqual(u'\u5E2B', eai[1])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(
            rfc8398.id_on_SmtpUTF8Mailbox, asn1Object['otherName']['type-id'])
        self.assertEqual(u'\u8001', asn1Object['otherName']['value'][0])

        self.assertEqual(u'\u5E2B', asn1Object['otherName']['value'][1])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
