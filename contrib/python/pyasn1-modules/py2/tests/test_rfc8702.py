#
# This file is part of pyasn1-modules software.
#
# Copyright (c) 2020, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder

from pyasn1.type import univ

from pyasn1_modules import pem
from pyasn1_modules import rfc2985
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc6211
from pyasn1_modules import rfc8702


class AlgorithmIdentifierTestCase(unittest.TestCase):
    pem_text = """\
MEowCwYJYIZIAWUDBAILMAsGCWCGSAFlAwQCDDAKBggrBgEFBQcGHjAKBggrBgEF
BQcGHzAKBggrBgEFBQcGIDAKBggrBgEFBQcGIQ==
"""

    def setUp(self):
        self.asn1Spec = rfc2985.SMIMECapabilities()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        
        oid_list = (
            rfc8702.id_shake128,
            rfc8702.id_shake256,
            rfc8702.id_RSASSA_PSS_SHAKE128,
            rfc8702.id_RSASSA_PSS_SHAKE256,
            rfc8702.id_ecdsa_with_shake128,
            rfc8702.id_ecdsa_with_shake256,
        )

        for algid in asn1Object:
            self.assertIn(algid['algorithm'], oid_list)


class AuthenticatedDataTestCase(unittest.TestCase):
    auth_message_pem_text = """\
MIIDqgYLKoZIhvcNAQkQAQKgggOZMIIDlQIBADGCAk8wggJLAgEAMDMwJjEUMBIG
A1UECgwLZXhhbXBsZS5jb20xDjAMBgNVBAMMBUFsaWNlAgkAg/ULtwvVxA4wDQYJ
KoZIhvcNAQEBBQAEggIAdZphtN3x8a8kZoAFY15HYRD6JyPBueRUhLbTPoOH3pZ9
xeDK+zVXGlahl1y1UOe+McEx2oD7cxAkhFuruNZMrCYEBCTZMwVhyEOZlBXdZEs8
rZUHL3FFE5PJnygsSIO9DMxd1UuTFGTgCm5V5ZLFGmjeEGJRbsfTyo52S7iseJqI
N3dl743DbApu0+yuUoXKxqKdUFlEVxmhvc+Qbg/zfiwu8PTsYiUQDMBi4cdIlju8
iLjj389xQHNyndXHWD51is89GG8vpBe+IsN8mnbGtCcpqtJ/c65ErJhHTR7rSJSM
EqQD0LPOCKIY1q9FaSSJfMXJZk9t/rPxgUEVjfw7hAkKpgOAqoZRN+FpnFyBl0Fn
nXo8kLp55tfVyNibtUpmdCPkOwt9b3jAtKtnvDQ2YqY1/llfEUnFOVDKwuC6MYwi
fm92qNlAQA/T0+ocjs6gA9zOLx+wD1zqM13hMD/L+T2OHL/WgvGb62JLrNHXuPWA
8RShO4kIlPtARKXap2S3+MX/kpSUUrNa65Y5uK1jwFFclczG+CPCIBBn6iJiQT/v
OX1I97YUP4Qq6OGkjK064Bq6o8+e5+NmIOBcygYRv6wA7vGkmPLSWbnw99qD728b
Bh84fC3EjItdusqGIwjzL0eSUWXJ5eu0Z3mYhJGN1pe0R/TEB5ibiJsMLpWAr3gw
FQYJYIZIAWUDBAITMAgEBnB5YXNuMaELBglghkgBZQMEAgswNQYJKoZIhvcNAQcB
oCgEJldhdHNvbiwgY29tZSBoZXJlIC0gSSB3YW50IHRvIHNlZSB5b3UuooG/MBgG
CSqGSIb3DQEJAzELBgkqhkiG9w0BBwEwHAYJKoZIhvcNAQkFMQ8XDTE5MDkxOTEz
NDEwMFowHwYJKoZIhvcNAQkEMRIEENiFx45okcgTCVIBhhgF+ogwLwYLKoZIhvcN
AQkQAgQxIDAeDBFXYXRzb24sIGNvbWUgaGVyZQYJKoZIhvcNAQcBMDMGCSqGSIb3
DQEJNDEmMCQwCwYJYIZIAWUDBAILohUGCWCGSAFlAwQCEzAIBAZweWFzbjEEIBxm
7hx+iivDlWYp8iUmYYbc2xkpBAcTACkWH+KBRZuF
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.auth_message_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        
        self.assertEqual(rfc5652.id_ct_authData, asn1Object['contentType'])
        ad, rest = der_decoder(
            asn1Object['content'], asn1Spec=rfc5652.AuthenticatedData())

        self.assertFalse(rest)
        self.assertTrue(ad.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(ad))

        self.assertEqual(
            rfc8702.id_shake128, ad['digestAlgorithm']['algorithm'])

        ad_mac = ad['macAlgorithm']
        self.assertEqual(
            rfc8702.id_KMACWithSHAKE128, ad_mac['algorithm'])

        kmac128_p, rest = der_decoder(
            ad_mac['parameters'],
            asn1Spec=rfc5280.algorithmIdentifierMap[ad_mac['algorithm']])

        self.assertFalse(rest)
        self.assertTrue(kmac128_p.prettyPrint())
        self.assertEqual(ad_mac['parameters'], der_encoder(kmac128_p))
        
        self.assertEqual(
            univ.OctetString("pyasn1"), kmac128_p['customizationString'])

        found_kmac128_params = False
        for attr in ad['authAttrs']:
            if attr['attrType'] == rfc6211.id_aa_cmsAlgorithmProtect:
                av, rest = der_decoder(
                    attr['attrValues'][0],
                    asn1Spec=rfc6211.CMSAlgorithmProtection())

                self.assertFalse(rest)
                self.assertTrue(av.prettyPrint())
                self.assertEqual(attr['attrValues'][0], der_encoder(av))
        
                self.assertEqual(
                    rfc8702.id_shake128, av['digestAlgorithm']['algorithm'])

                self.assertEqual(
                    rfc8702.id_KMACWithSHAKE128, av['macAlgorithm']['algorithm'])

                found_kmac128_params = True

        self.assertTrue(found_kmac128_params)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    import sys

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
