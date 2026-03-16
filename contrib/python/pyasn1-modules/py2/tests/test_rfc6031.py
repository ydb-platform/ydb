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
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc6031


class SymmetricKeyPkgTestCase(unittest.TestCase):
    key_pkg_pem_text = """\
MIG7BgsqhkiG9w0BCRABGaCBqzCBqKBEMCMGCyqGSIb3DQEJEAwBMRQMElZpZ2ls
IFNlY3VyaXR5IExMQzAdBgsqhkiG9w0BCRAMAzEODAxQcmV0ZW5kIDA0OEEwYDBe
MFYwGwYLKoZIhvcNAQkQDBsxDAwKZXhhbXBsZUlEMTAVBgsqhkiG9w0BCRAMCjEG
DARIT1RQMCAGCyqGSIb3DQEJEAwLMREMD2t0YS5leGFtcGxlLmNvbQQEMTIzNA==
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.key_pkg_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertIn(asn1Object['contentType'], rfc5652.cmsContentTypesMap)

        asn1Spec = rfc5652.cmsContentTypesMap[asn1Object['contentType']]
        skp, rest = der_decoder(asn1Object['content'], asn1Spec=asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(skp.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(skp))

        for attr in skp['sKeyPkgAttrs']:
            self.assertIn(attr['attrType'], rfc6031.sKeyPkgAttributesMap)

        for osk in skp['sKeys']:
            for attr in osk['sKeyAttrs']:
                self.assertIn(attr['attrType'], rfc6031.sKeyAttributesMap)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.key_pkg_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertIn(asn1Object['contentType'], rfc5652.cmsContentTypesMap)
        self.assertTrue(asn1Object['content'].hasValue())

        keypkg = asn1Object['content']

        self.assertEqual(
            rfc6031.KeyPkgVersion().subtype(value='v1'), keypkg['version'])

        for attr in keypkg['sKeyPkgAttrs']:
            self.assertIn(attr['attrType'], rfc6031.sKeyPkgAttributesMap)
            self.assertNotEqual('0x', attr['attrValues'][0].prettyPrint()[:2])

            # decodeOpenTypes=True did not decode if the value is shown in hex ...
            if attr['attrType'] == rfc6031.id_pskc_manufacturer:
                attr['attrValues'][0] == 'Vigil Security LLC'

        for osk in keypkg['sKeys']:
            for attr in osk['sKeyAttrs']:
                self.assertIn(attr['attrType'], rfc6031.sKeyAttributesMap)
                self.assertNotEqual(
                    '0x', attr['attrValues'][0].prettyPrint()[:2])

                # decodeOpenTypes=True did not decode if the value is shown in hex ...
                if attr['attrType'] == rfc6031.id_pskc_issuer:
                    attr['attrValues'][0] == 'kta.example.com'


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
