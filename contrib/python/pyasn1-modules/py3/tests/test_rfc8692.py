#
# This file is part of pyasn1-modules software.
#
# Copyright (c) 2019, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder

from pyasn1_modules import pem
from pyasn1_modules import rfc5751
from pyasn1_modules import rfc8692


class AlgorithmIdentifierTestCase(unittest.TestCase):
    pem_text = """\
MEowCwYJYIZIAWUDBAILMAsGCWCGSAFlAwQCDDAKBggrBgEFBQcGHjAKBggrBgEF
BQcGHzAKBggrBgEFBQcGIDAKBggrBgEFBQcGIQ==
"""

    def setUp(self):
        self.asn1Spec = rfc5751.SMIMECapabilities()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        
        oid_list = (
            rfc8692.id_shake128,
            rfc8692.id_shake256,
            rfc8692.id_RSASSA_PSS_SHAKE128,
            rfc8692.id_RSASSA_PSS_SHAKE256,
            rfc8692.id_ecdsa_with_shake128,
            rfc8692.id_ecdsa_with_shake256,
        )

        count = 0
        for algid in asn1Object:
            self.assertTrue(algid['capabilityID'] in oid_list)
            count += 1

        self.assertTrue(len(oid_list), count)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
