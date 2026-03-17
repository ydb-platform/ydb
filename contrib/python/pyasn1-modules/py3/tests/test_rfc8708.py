#
# This file is part of pyasn1-modules software.
#
# Created by Russ Housley
# Copyright (c) 2020, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#

import sys
import unittest

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder

from pyasn1_modules import pem
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc8708


class HashSigPublicKeyTestCase(unittest.TestCase):
    public_key_pem_text = """\
MFAwDQYLKoZIhvcNAQkQAxEDPwAEPAAAAAIAAAAGAAAAA9CPq9SiCR/wqMtO2DTn
RTQypYiFzZugQxI1Rmv/llHGySEkQE1F+lPPFhwo8a1ajg==
"""

    def setUp(self):
        self.asn1Spec = rfc5280.SubjectPublicKeyInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.public_key_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(
            asn1Object['algorithm']['algorithm'],
            rfc8708.id_alg_hss_lms_hashsig)


class HashSigSignedDataTestCase(unittest.TestCase):
    signed_data_pem_text = """\
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
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.signed_data_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(asn1Object['contentType'], rfc5652.id_signedData)
        sd, rest = der_decoder(
            asn1Object['content'], asn1Spec=rfc5652.SignedData())

        oid = sd['signerInfos'][0]['signatureAlgorithm']['algorithm']
        self.assertEqual(rfc8708.id_alg_hss_lms_hashsig, oid)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
