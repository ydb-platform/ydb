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
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc6210


class AuthenticatedDataTestCase(unittest.TestCase):
    pem_text = """\
MIICRQYLKoZIhvcNAQkQAQKgggI0MIICMAIBADGBwDCBvQIBADAmMBIxEDAOBgNVBAMMB0
NhcmxSU0ECEEY0a8eAAFa8EdNuLs1dcdAwDQYJKoZIhvcNAQEBBQAEgYCH70EpEikY7deb
859YJRAWfFondQv1D4NFltw6C1ceheWnlAU0C2WEXr3LUBXZp1/PSte29FnJxu5bXCTn1g
elMm6zNlZNWNd0KadVBcaxi1n8L52tVM5sWFGJPO5cStOyAka2ucuZM6iAnCSkn1Ju7fgU
5j2g3bZ/IM8nHTcygjAKBggrBgEFBQgBAqFPBgsqhkiG9w0BCRADDQRAAQIDBAUGBwgJCg
sMDQ4PEBESEwQVFhcYGRobHB0eHyAhIiMEJSYnKCkqKywtLi8wMTIzBDU2Nzg5Ojs8PT4/
QDArBgkqhkiG9w0BBwGgHgQcVGhpcyBpcyBzb21lIHNhbXBsZSBjb250ZW50LqKBxzAYBg
kqhkiG9w0BCQMxCwYJKoZIhvcNAQcBMBwGCSqGSIb3DQEJBTEPFw0wOTEyMTAyMzI1MDBa
MB8GCSqGSIb3DQEJBDESBBCWaa5hG1eeg+oQK2tJ3cD5MGwGCSqGSIb3DQEJNDFfMF0wTw
YLKoZIhvcNAQkQAw0EQAECAwQFBgcICQoLDA0ODxAREhMEFRYXGBkaGxwdHh8gISIjBCUm
JygpKissLS4vMDEyMwQ1Njc4OTo7PD0+P0CiCgYIKwYBBQUIAQIEFLjUxQ9PJFzFnWraxb
EIbVbg2xql
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
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
        self.assertEqual(0, ad['version'])
        self.assertEqual(
            rfc6210.id_alg_MD5_XOR_EXPERIMENT, ad['digestAlgorithm']['algorithm'])

        mac_alg_p, rest = der_decoder(
            ad['digestAlgorithm']['parameters'],
            asn1Spec=rfc5280.algorithmIdentifierMap[ad['digestAlgorithm']['algorithm']])

        self.assertFalse(rest)
        self.assertTrue(mac_alg_p.prettyPrint())
        self.assertEqual(
            ad['digestAlgorithm']['parameters'], der_encoder(mac_alg_p))
        self.assertEqual("0x01020304", mac_alg_p.prettyPrint()[:10])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
