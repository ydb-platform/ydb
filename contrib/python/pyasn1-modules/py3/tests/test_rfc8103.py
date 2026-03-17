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
from pyasn1_modules import rfc8103


class CAEADChaCha20Poly1305TestCase(unittest.TestCase):
    alg_id_pem_text = "MBsGCyqGSIb3DQEJEAMSBAzK/rq++s7brd7K+Ig="

    def setUp(self):
        self.asn1Spec = rfc5280.AlgorithmIdentifier()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.alg_id_pem_text)
        asn1Object, rest = der_decoder.decode(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(rfc8103.id_alg_AEADChaCha20Poly1305, asn1Object[0])

        param, rest = der_decoder.decode(
            asn1Object[1], rfc8103.AEADChaCha20Poly1305Nonce())

        self.assertFalse(rest)
        self.assertTrue(param.prettyPrint())
        self.assertEqual(
            rfc8103.AEADChaCha20Poly1305Nonce(value='\xca\xfe\xba\xbe\xfa'
                                                    '\xce\xdb\xad\xde\xca'
                                                    '\xf8\x88'),
            param)
        self.assertEqual(substrate, der_encoder.encode(asn1Object))


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())


