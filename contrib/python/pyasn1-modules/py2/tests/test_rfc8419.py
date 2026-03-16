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
from pyasn1_modules import rfc8419


class Ed25519TestCase(unittest.TestCase):
    alg_id_1_pem_text = "MAUGAytlcA=="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.alg_id_1_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(rfc8419.id_Ed25519, asn1Object['algorithm'])
        self.assertFalse(asn1Object['parameters'].isValue)
        self.assertEqual(substrate, der_encoder(asn1Object))


class Ed448TestCase(unittest.TestCase):
    alg_id_2_pem_text = "MAUGAytlcQ=="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.alg_id_2_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(rfc8419.id_Ed448, asn1Object['algorithm'])
        self.assertFalse(asn1Object['parameters'].isValue)
        self.assertEqual(substrate, der_encoder(asn1Object))


class SHA512TestCase(unittest.TestCase):
    alg_id_3_pem_text = "MAsGCWCGSAFlAwQCAw=="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.alg_id_3_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(rfc8419.id_sha512, asn1Object['algorithm'])
        self.assertFalse(asn1Object['parameters'].isValue)
        self.assertEqual(substrate, der_encoder(asn1Object))


class SHAKE256TestCase(unittest.TestCase):
    alg_id_4_pem_text = "MAsGCWCGSAFlAwQCDA=="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.alg_id_4_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(rfc8419.id_shake256, asn1Object['algorithm'])
        self.assertFalse(asn1Object['parameters'].isValue)
        self.assertEqual(substrate, der_encoder(asn1Object))


class SHAKE256LENTestCase(unittest.TestCase):
    alg_id_5_pem_text = "MA8GCWCGSAFlAwQCEgICAgA="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.alg_id_5_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(rfc8419.id_shake256_len, asn1Object['algorithm'])
        self.assertTrue(asn1Object['parameters'].isValue)
        self.assertEqual(substrate, der_encoder(asn1Object))

        param, rest = der_decoder(
            asn1Object['parameters'],
            asn1Spec=rfc5280.algorithmIdentifierMap[asn1Object['algorithm']])

        self.assertFalse(rest)
        self.assertTrue(param.prettyPrint())
        self.assertEqual(asn1Object['parameters'], der_encoder(param))
        self.assertEqual(512, param)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.alg_id_5_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(rfc8419.id_shake256_len, asn1Object['algorithm'])
        self.assertEqual(512, asn1Object['parameters'])
        self.assertEqual(substrate, der_encoder(asn1Object))


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
