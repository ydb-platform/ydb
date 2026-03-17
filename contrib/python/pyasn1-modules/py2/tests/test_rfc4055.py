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
from pyasn1.type import univ

from pyasn1_modules import pem
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc4055


class PSSDefautTestCase(unittest.TestCase):
    pss_default_pem_text = "MAsGCSqGSIb3DQEBCg=="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pss_default_pem_text)
        asn1Object, rest = der_decoder.decode(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertTrue(rfc4055.id_RSASSA_PSS, asn1Object[0])
        self.assertEqual(substrate, der_encoder.encode(asn1Object))
        self.assertEqual(substrate, der_encoder.encode(asn1Object))

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pss_default_pem_text)
        asn1Object, rest = der_decoder.decode(substrate,
                                              asn1Spec=self.asn1Spec,
                                              decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder.encode(asn1Object))
        self.assertFalse(asn1Object['parameters'].hasValue())


class PSSSHA512TestCase(unittest.TestCase):
    pss_sha512_pem_text = "MDwGCSqGSIb3DQEBCjAvoA8wDQYJYIZIAWUDBAIDBQChHDAaBg" \
                          "kqhkiG9w0BAQgwDQYJYIZIAWUDBAIDBQA="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pss_sha512_pem_text)
        asn1Object, rest = der_decoder.decode(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertTrue(rfc4055.id_RSASSA_PSS, asn1Object[0])
        self.assertEqual(substrate, der_encoder.encode(asn1Object))
        self.assertEqual(substrate, der_encoder.encode(asn1Object))

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pss_sha512_pem_text)
        asn1Object, rest = der_decoder.decode(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder.encode(asn1Object))
        self.assertTrue(asn1Object['parameters'].hasValue())
        self.assertTrue(20, asn1Object['parameters']['saltLength'])


class OAEPDefautTestCase(unittest.TestCase):
    oaep_default_pem_text = "MAsGCSqGSIb3DQEBBw=="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.oaep_default_pem_text)
        asn1Object, rest = der_decoder.decode(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertTrue(rfc4055.id_RSAES_OAEP, asn1Object[0])
        self.assertEqual(substrate, der_encoder.encode(asn1Object))
        self.assertEqual(substrate, der_encoder.encode(asn1Object))

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.oaep_default_pem_text)
        asn1Object, rest = der_decoder.decode(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder.encode(asn1Object))
        self.assertFalse(asn1Object['parameters'].hasValue())


class OAEPSHA256TestCase(unittest.TestCase):
    oaep_sha256_pem_text = "MDwGCSqGSIb3DQEBBzAvoA8wDQYJYIZIAWUDBAIBBQChHDAaB" \
                           "gkqhkiG9w0BAQgwDQYJYIZIAWUDBAIBBQA="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.oaep_sha256_pem_text)
        asn1Object, rest = der_decoder.decode(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertTrue(rfc4055.id_RSAES_OAEP, asn1Object[0])
        self.assertEqual(substrate, der_encoder.encode(asn1Object))
        self.assertEqual(substrate, der_encoder.encode(asn1Object))

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.oaep_sha256_pem_text)
        asn1Object, rest = der_decoder.decode(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder.encode(asn1Object))
        self.assertTrue(asn1Object['parameters'].hasValue())

        oaep_p = asn1Object['parameters']

        self.assertEqual(univ.Null(""), oaep_p['hashFunc']['parameters'])
        self.assertEqual(univ.Null(""), oaep_p['maskGenFunc']['parameters']['parameters'])


class OAEPFullTestCase(unittest.TestCase):
    oaep_full_pem_text = "MFMGCSqGSIb3DQEBBzBGoA8wDQYJYIZIAWUDBAICBQChHDAaBgk" \
                         "qhkiG9w0BAQgwDQYJYIZIAWUDBAICBQCiFTATBgkqhkiG9w0BAQ" \
                         "kEBmZvb2Jhcg=="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.oaep_full_pem_text)
        asn1Object, rest = der_decoder.decode(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())

        self.assertTrue(rfc4055.id_RSAES_OAEP, asn1Object[0])

        self.assertEqual(substrate, der_encoder.encode(asn1Object))
        self.assertEqual(substrate, der_encoder.encode(asn1Object))

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.oaep_full_pem_text)
        asn1Object, rest = der_decoder.decode(substrate,
                                              asn1Spec=self.asn1Spec,
                                              decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder.encode(asn1Object))

        self.assertTrue(asn1Object['parameters'].hasValue())

        oaep_p = asn1Object['parameters']

        self.assertEqual(univ.Null(""), oaep_p['hashFunc']['parameters'])
        self.assertEqual(
            univ.Null(""), oaep_p['maskGenFunc']['parameters']['parameters'])
        self.assertEqual(
            univ.OctetString(value='foobar'),
            oaep_p['pSourceFunc']['parameters'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
