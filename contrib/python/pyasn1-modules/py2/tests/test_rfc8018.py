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
from pyasn1_modules import rfc8018


class PWRITestCase(unittest.TestCase):
    rfc3211_ex1_pem_text = """\
o1MCAQCgGgYJKoZIhvcNAQUMMA0ECBI0Vnh4VjQSAgEFMCAGCyqGSIb3DQEJEAMJMBEGBSsO
AwIHBAjv5ZjvIbM9bQQQuBslZe43PKbe3KJqF4sMEA==
"""

    def setUp(self):
        self.asn1Spec = rfc5652.RecipientInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.rfc3211_ex1_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        alg_oid = asn1Object['pwri']['keyDerivationAlgorithm']['algorithm']

        self.assertEqual(rfc8018.id_PBKDF2, alg_oid)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.rfc3211_ex1_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        icount = (asn1Object['pwri']['keyDerivationAlgorithm']
                            ['parameters']['iterationCount'])

        self.assertEqual(5, icount)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
