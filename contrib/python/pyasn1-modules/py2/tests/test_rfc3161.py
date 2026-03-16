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
from pyasn1_modules import rfc3161


class TSPQueryTestCase(unittest.TestCase):
    tsp_query_pem_text = """\
MFYCAQEwUTANBglghkgBZQMEAgMFAARAGu1DauxDZZv8F7l4EKIbS00U40mUKfBW5C0giEz0
t1zOHCvK4A8i8zxwUXFHv4pAJZE+uFhZ+v53HTg9rLjO5Q==
"""

    def setUp(self):
        self.asn1Spec = rfc3161.TimeStampReq()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.tsp_query_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))


class TSPResponseTestCase(unittest.TestCase):
    tsp_response_pem_text = """\
MIIFMTADAgEAMIIFKAYJKoZIhvcNAQcCoIIFGTCCBRUCAQMxCzAJBgUrDgMCGgUAMIIBowYL
KoZIhvcNAQkQAQSgggGSBIIBjjCCAYoCAQEGBCoDBAEwUTANBglghkgBZQMEAgMFAARAGu1D
auxDZZv8F7l4EKIbS00U40mUKfBW5C0giEz0t1zOHCvK4A8i8zxwUXFHv4pAJZE+uFhZ+v53
HTg9rLjO5QIDDwJEGA8yMDE5MDUxMDE4MzQxOFoBAf+gggERpIIBDTCCAQkxETAPBgNVBAoT
CEZyZWUgVFNBMQwwCgYDVQQLEwNUU0ExdjB0BgNVBA0TbVRoaXMgY2VydGlmaWNhdGUgZGln
aXRhbGx5IHNpZ25zIGRvY3VtZW50cyBhbmQgdGltZSBzdGFtcCByZXF1ZXN0cyBtYWRlIHVz
aW5nIHRoZSBmcmVldHNhLm9yZyBvbmxpbmUgc2VydmljZXMxGDAWBgNVBAMTD3d3dy5mcmVl
dHNhLm9yZzEiMCAGCSqGSIb3DQEJARYTYnVzaWxlemFzQGdtYWlsLmNvbTESMBAGA1UEBxMJ
V3VlcnpidXJnMQswCQYDVQQGEwJERTEPMA0GA1UECBMGQmF5ZXJuMYIDWjCCA1YCAQEwgaMw
gZUxETAPBgNVBAoTCEZyZWUgVFNBMRAwDgYDVQQLEwdSb290IENBMRgwFgYDVQQDEw93d3cu
ZnJlZXRzYS5vcmcxIjAgBgkqhkiG9w0BCQEWE2J1c2lsZXphc0BnbWFpbC5jb20xEjAQBgNV
BAcTCVd1ZXJ6YnVyZzEPMA0GA1UECBMGQmF5ZXJuMQswCQYDVQQGEwJERQIJAMHphhYNqOmC
MAkGBSsOAwIaBQCggYwwGgYJKoZIhvcNAQkDMQ0GCyqGSIb3DQEJEAEEMBwGCSqGSIb3DQEJ
BTEPFw0xOTA1MTAxODM0MThaMCMGCSqGSIb3DQEJBDEWBBSuLICty7PQHx0Ynk0a3rGcCRrf
EjArBgsqhkiG9w0BCRACDDEcMBowGDAWBBSRbaPYYOzKguNLxZ0Xk+fpaIdfFDANBgkqhkiG
9w0BAQEFAASCAgBFDVbGQ3L5GcaUBMtBnMW7x3S57QowQhhrTewvncY+3Nc2i6tlM1UEdxIp
3m2iMqaH/N2xIm2sU/L/lIwaT1XIS4bJ2Nn8UPjZu/prJrVUFTMjJ5LWkG55x6c5A4pa2xxS
N/kOV2e+6RHYlGvcDOvu2fzuz08hE+NjaHIPg3idU1cBsl0gTWZCTrxdXTLuuvHahxUAdQKm
gTdGPjIiOR4GYpaVxEAgulaBQLZU5MhfBTASI1LkljhiFeDBQMhTUeZoA59/OxgnQR1Zpca4
ZuWuqnZImxziRQA1tX/6pjAo5eP1V+SLWYHeIO7ia/urGIK9AXd3jY3Ljq4h7R1E+RRKIseO
74mmtbJtCaiGL9H+6k164qC7U5fHBzKl3UboZtOUmNj10IJPUNyKQ5JPwCe6HEhbeXLRdh/8
bjdqy56hBHyG1NRBqiTXTvj9LOzsJGIF5GjwyCT0B2hpvzdTdzNtfQ27HUUYgnYg0fGEpNpi
vyaW5qCh9S704IKB0m/fXlqiIfNVdqDr/aAHNww8CouZP2oFO61WXCspbFNPLubeqxd5P4o4
dJzD4PKsurILdX7SL8pRI+O2UtJLwNB1t3LBLKfTZuOWoSBFvQwbqBsDEchrZIDZXSXMbXd6
uuvuO3ZsRWuej+gso+nWi3CRnRc9Wb0++cq4s8YSLaYSj2pHMA==
"""

    def setUp(self):
        self.asn1Spec = rfc3161.TimeStampResp()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.tsp_response_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
