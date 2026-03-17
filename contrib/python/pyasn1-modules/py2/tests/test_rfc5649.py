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
from pyasn1_modules import rfc5649


class AESKeyWrapTestCase(unittest.TestCase):
    kw_alg_id_pem_text = "MAsGCWCGSAFlAwQBLQ=="

    def setUp(self):
        self.asn1Spec = rfc5649.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.kw_alg_id_pem_text)
        asn1Object, rest = der_decoder.decode(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(rfc5649.id_aes256_wrap, asn1Object[0])
        self.assertEqual(substrate, der_encoder.encode(asn1Object))


class AESKeyWrapWithPadTestCase(unittest.TestCase):
    kw_pad_alg_id_pem_text = "MAsGCWCGSAFlAwQBMA=="

    def setUp(self):
        self.asn1Spec = rfc5649.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.kw_pad_alg_id_pem_text)
        asn1Object, rest = der_decoder.decode(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(rfc5649.id_aes256_wrap_pad, asn1Object[0])
        self.assertEqual(substrate, der_encoder.encode(asn1Object))


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
