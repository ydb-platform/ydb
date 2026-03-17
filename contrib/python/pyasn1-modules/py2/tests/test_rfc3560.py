#
# This file is part of pyasn1-modules software.
#
# Created by Russ Housley
# Copyright (c) 2019, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.codec.der import decoder as der_decoder
from pyasn1.codec.der import encoder as der_encoder

from pyasn1_modules import pem
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc3560


class OAEPDefautTestCase(unittest.TestCase):
    oaep_default_pem_text = "MAsGCSqGSIb3DQEBBw=="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.oaep_default_pem_text)
        asn1Object, rest = der_decoder.decode(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(rfc3560.id_RSAES_OAEP, asn1Object[0])
        self.assertEqual(substrate, der_encoder.encode(asn1Object))


class OAEPSHA256TestCase(unittest.TestCase):
    oaep_sha256_pem_text = "MDwGCSqGSIb3DQEBBzAvoA8wDQYJYIZIAWUDBAIBBQChHDAaBgkqhkiG9w0BAQgwDQYJYIZIAWUDBAIBBQA="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.oaep_sha256_pem_text)
        asn1Object, rest = der_decoder.decode(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(rfc3560.id_RSAES_OAEP, asn1Object[0])
        self.assertEqual(substrate, der_encoder.encode(asn1Object))


class OAEPFullTestCase(unittest.TestCase):
    oaep_full_pem_text = "MFMGCSqGSIb3DQEBBzBGoA8wDQYJYIZIAWUDBAICBQChHDAaBgkqhkiG9w0BAQgwDQYJYIZIAWUDBAICBQCiFTATBgkqhkiG9w0BAQkEBmZvb2Jhcg=="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.oaep_full_pem_text)
        asn1Object, rest = der_decoder.decode(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(rfc3560.id_RSAES_OAEP, asn1Object[0])
        self.assertEqual(substrate, der_encoder.encode(asn1Object))


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
