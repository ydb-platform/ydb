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
from pyasn1_modules import rfc4211


class CertificateReqTestCase(unittest.TestCase):
    pem_text = """\
MIIBozCCAZ8wggEFAgUAwTnj2jCByoABAqURMA8xDTALBgNVBAMTBHVzZXKmgZ8w
DQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBAJ6ZQ2cYbn/lFsmBOlRltbRbFQUvvE0Q
nbopOu1kC7Bmaaz7QTx8nxeiHi4m7uxCbGGxHNoGCt7EmdG8eZUBNAcHyGlXrJdm
0z3/uNEGiBHq+xB8FnFJCA5EIJ3RWFnlbu9otSITLxWK7c5+/NHmWM+yaeHD/f/h
rp01c/8qXZfZAgMBAAGpEDAOBgNVHQ8BAf8EBAMCBeAwLzASBgkrBgEFBQcFAQEM
BTExMTExMBkGCSsGAQUFBwUBAgwMc2VydmVyX21hZ2ljoYGTMA0GCSqGSIb3DQEB
BQUAA4GBAEI3KNEvTq/n1kNVhNhPkovk1AZxyJrN1u1+7Gkc4PLjWwjLOjcEVWt4
AajUk/gkIJ6bbeO+fZlMjHfPSDKcD6AV2hN+n72QZwfzcw3icNvBG1el9EU4XfIm
xfu5YVWi81/fw8QQ6X6YGHFQkomLd7jxakVyjxSng9BhO6GpjJNF
"""

    def setUp(self):
        self.asn1Spec = rfc4211.CertReqMessages()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        count = 0

        for crm in asn1Object:
            self.assertEqual(2, crm['certReq']['certTemplate']['version'])
            count += 1

        self.assertEqual(1, count)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
