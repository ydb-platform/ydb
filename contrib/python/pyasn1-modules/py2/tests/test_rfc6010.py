#
# This file is part of pyasn1-modules software.
#
# Created by Russ Housley
# Copyright (c) 2019, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder

from pyasn1_modules import pem
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc6010


class UnconstrainedCCCExtensionTestCase(unittest.TestCase):
    unconstrained_pem_text = "MB0GCCsGAQUFBwESBBEwDzANBgsqhkiG9w0BCRABAA=="

    def setUp(self):
        self.asn1Spec = rfc5280.Extension()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.unconstrained_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(
            rfc6010.id_pe_cmsContentConstraints, asn1Object['extnID'])

        evalue, rest = der_decoder(
            asn1Object['extnValue'],
            asn1Spec=rfc6010.CMSContentConstraints())

        self.assertFalse(rest)
        self.assertTrue(evalue.prettyPrint())
        self.assertEqual(asn1Object['extnValue'], der_encoder(evalue))
        self.assertEqual(
            rfc6010.id_ct_anyContentType, evalue[0]['contentType'])


class ConstrainedCCCExtensionTestCase(unittest.TestCase):
    constrained_pem_text = """\
MIG7BggrBgEFBQcBEgSBrjCBqzA0BgsqhkiG9w0BCRABEDAlMCMGCyqGSIb3DQEJ
EAwBMRQMElZpZ2lsIFNlY3VyaXR5IExMQzAwBgpghkgBZQIBAk4CMCIwIAYLKoZI
hvcNAQkQDAsxEQwPa3RhLmV4YW1wbGUuY29tMDEGCyqGSIb3DQEJEAEZMCIwIAYL
KoZIhvcNAQkQDAsxEQwPa3RhLmV4YW1wbGUuY29tMA4GCSqGSIb3DQEHAQoBAQ==
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Extension()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.constrained_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(
            rfc6010.id_pe_cmsContentConstraints, asn1Object['extnID'])

        evalue, rest = der_decoder(
            asn1Object['extnValue'],
            asn1Spec=rfc6010.CMSContentConstraints())

        self.assertFalse(rest)
        self.assertTrue(evalue.prettyPrint())
        self.assertEqual(asn1Object['extnValue'], der_encoder(evalue))

        constraint_count = 0
        attribute_count = 0
        cannot_count = 0

        for ccc in evalue:
            constraint_count += 1
            if ccc['canSource'] == 1:
                cannot_count += 1
            if ccc['attrConstraints'].hasValue():
                for attr in ccc['attrConstraints']:
                    attribute_count += 1

        self.assertEqual(4, constraint_count)
        self.assertEqual(3, attribute_count)
        self.assertEqual(1, cannot_count)

    def testExtensionsMap(self):
        substrate = pem.readBase64fromText(self.constrained_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertIn(asn1Object['extnID'], rfc5280.certificateExtensionsMap)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
