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
from pyasn1_modules import rfc4387


class CertificateTestCase(unittest.TestCase):
    pem_text = """\
MIIDLzCCArWgAwIBAgIJAKWzVCgbsG5JMAoGCCqGSM49BAMDMD8xCzAJBgNVBAYT
AlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9n
dXMgQ0EwHhcNMTkxMTIyMDI1MzAzWhcNMjAxMTIxMDI1MzAzWjBZMQswCQYDVQQG
EwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xEDAOBgNVBAoTB0V4
YW1wbGUxGTAXBgNVBAMTEHJlcG8uZXhhbXBsZS5jb20wdjAQBgcqhkjOPQIBBgUr
gQQAIgNiAAS/J1NNkqicN432Uwlw+Gu4pLvYpSr2W8zJvCOy61ncEzKNIs4cxqSc
N0rl6K32tNCQGCsQFaBK4wZKXbHpUEPWrfYAWYebYDOhMlOE/agxH3nZRRnYv4O7
pGrk/YZamGijggFhMIIBXTALBgNVHQ8EBAMCB4AwQgYJYIZIAYb4QgENBDUWM1Ro
aXMgY2VydGlmaWNhdGUgY2Fubm90IGJlIHRydXN0ZWQgZm9yIGFueSBwdXJwb3Nl
LjAdBgNVHQ4EFgQUWDRoN3XtN1n8ZH+bQuSAsr42gQwwHwYDVR0jBBgwFoAU8jXb
NATapVXyvWkDmbBi7OIVCMEwgckGCCsGAQUFBwEBBIG8MIG5MCQGCCsGAQUFBzAB
hhhodHRwOi8vb2NzcC5leGFtcGxlLmNvbS8wMgYIKwYBBQUHMAKGJmh0dHA6Ly9y
ZXBvLmV4YW1wbGUuY29tL2NhaXNzdWVycy5odG1sMC4GCCsGAQUFBzAGhiJodHRw
Oi8vcmVwby5leGFtcGxlLmNvbS9jZXJ0cy5odG1sMC0GCCsGAQUFBzAHhiFodHRw
Oi8vcmVwby5leGFtcGxlLmNvbS9jcmxzLmh0bWwwCgYIKoZIzj0EAwMDaAAwZQIw
C9Y1McQ+hSEZLtzLw1xzk3QSQX6NxalySoIIoNXpcDrGZJcjLRunBg8G9B0hqG69
AjEAxtzj8BkMvhb5d9DTKDVg5pmjl9z7UtRK87/LJM+EW/9+PAzB2IT3T+BPHKb4
kjBJ
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        oid_list = [
            rfc4387.id_ad_http_certs,
            rfc4387.id_ad_http_crls,
        ]

        count = 0
        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5280.id_pe_authorityInfoAccess:
                extnValue, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.AuthorityInfoAccessSyntax())

                self.assertFalse(rest)
                self.assertTrue(extnValue.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(extnValue))

                for ad in extnValue:
                    if ad['accessMethod'] in oid_list:
                        uri = ad['accessLocation']['uniformResourceIdentifier']
                        self.assertIn('http://repo.example.com/c', uri)
                        count += 1

        self.assertEqual(len(oid_list), count)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    import sys

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
