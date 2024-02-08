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
from pyasn1.type import univ

from pyasn1_modules import pem
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5958
from pyasn1_modules import rfc8410


class PrivateKeyTestCase(unittest.TestCase):
    priv_key_pem_text = """\
MHICAQEwBQYDK2VwBCIEINTuctv5E1hK1bbY8fdp+K06/nwoy/HU++CXqI9EdVhC
oB8wHQYKKoZIhvcNAQkJFDEPDA1DdXJkbGUgQ2hhaXJzgSEAGb9ECWmEzf6FQbrB
Z9w7lshQhqowtrbLDFw4rXAxZuE=
"""

    def setUp(self):
        self.asn1Spec = rfc5958.PrivateKeyInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.priv_key_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(
            rfc8410.id_Ed25519, asn1Object['privateKeyAlgorithm']['algorithm'])
        self.assertTrue(asn1Object['privateKey'].isValue)
        self.assertEqual(
            "0x0420d4ee", asn1Object['privateKey'].prettyPrint()[0:10])
        self.assertTrue(asn1Object['publicKey'].isValue)
        self.assertEqual(
            "1164575857", asn1Object['publicKey'].prettyPrint()[0:10])
        self.assertEqual(substrate, der_encoder(asn1Object))


class PrivateKeyOpenTypesTestCase(unittest.TestCase):
    asymmetric_key_pkg_pem_text = """\
MIGEBgpghkgBZQIBAk4FoHYwdDByAgEBMAUGAytlcAQiBCDU7nLb+RNYStW22PH3
afitOv58KMvx1Pvgl6iPRHVYQqAfMB0GCiqGSIb3DQEJCRQxDwwNQ3VyZGxlIENo
YWlyc4EhABm/RAlphM3+hUG6wWfcO5bIUIaqMLa2ywxcOK1wMWbh
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.asymmetric_key_pkg_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertIn(
            rfc5958.id_ct_KP_aKeyPackage, rfc5652.cmsContentTypesMap)

        oneKey = asn1Object['content'][0]

        self.assertEqual(
            rfc8410.id_Ed25519, oneKey['privateKeyAlgorithm']['algorithm'])

        pkcs_9_at_friendlyName = univ.ObjectIdentifier('1.2.840.113549.1.9.9.20')

        self.assertEqual(
            pkcs_9_at_friendlyName, oneKey['attributes'][0]['attrType'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
