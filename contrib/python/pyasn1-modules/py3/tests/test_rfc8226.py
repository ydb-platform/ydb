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
from pyasn1_modules import rfc8226


class JWTClaimConstraintsTestCase(unittest.TestCase):
    jwtcc_pem_text = ("MD2gBzAFFgNmb2+hMjAwMBkWA2ZvbzASDARmb28xDARmb28yDARmb2"
                      "8zMBMWA2JhcjAMDARiYXIxDARiYXIy")

    def setUp(self):
        self.asn1Spec = rfc8226.JWTClaimConstraints()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.jwtcc_pem_text)
        asn1Object, rest = der_decoder.decode(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder.encode(asn1Object))


class TNAuthorizationListTestCase(unittest.TestCase):
    tnal_pem_text = ("MCugBxYFYm9ndXOhEjAQFgo1NzE1NTUxMjEyAgIDFKIMFgo3MDM1NTU"
                     "xMjEy")

    def setUp(self):
        self.asn1Spec = rfc8226.TNAuthorizationList()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.tnal_pem_text)
        asn1Object, rest = der_decoder.decode(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder.encode(asn1Object))


class CertificateOpenTypesTestCase(unittest.TestCase):
    cert_pem_text = """\
MIICkTCCAhegAwIBAgIJAKWzVCgbsG4+MAoGCCqGSM49BAMDMD8xCzAJBgNVBAYT
AlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9n
dXMgQ0EwHhcNMTkwNzE4MTUwNzQ5WhcNMjAwNzE3MTUwNzQ5WjBxMQswCQYDVQQG
EwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xKDAmBgNVBAoTH0Zh
a2UgVGVsZXBob25lIFNlcnZpY2UgUHJvdmlkZXIxGTAXBgNVBAMTEGZha2UuZXhh
bXBsZS5jb20wdjAQBgcqhkjOPQIBBgUrgQQAIgNiAARLyLhnsvrS9WBY29tmN2LI
CF/wuX4ohhUy3sxO0ynCplHHojpDg+tghGzusf0aLtMDu1II915O8YK5XVL+KZJD
C82jybxWIKjjzX2qc5/O06joUttdEDzkTaD0kgbcXl6jgawwgakwCwYDVR0PBAQD
AgeAMEIGCWCGSAGG+EIBDQQ1FjNUaGlzIGNlcnRpZmljYXRlIGNhbm5vdCBiZSB0
cnVzdGVkIGZvciBhbnkgcHVycG9zZS4wHQYDVR0OBBYEFHOI3GpDt9dWsTAZxhcj
96uyL2aIMB8GA1UdIwQYMBaAFPI12zQE2qVV8r1pA5mwYuziFQjBMBYGCCsGAQUF
BwEaBAowCKAGFgRmYWtlMAoGCCqGSM49BAMDA2gAMGUCMQCy+qFhT7X1i18jcyIa
Jkgz/tumrPsaBA2RihkooTEr4GbqC650Z4Cwt7+x2xZq37sCMFSM6fRueLyV5StG
yEFWA6G95b/HbtPMTjLpPKtrOjhofc4LyVCDYhFhKzpvHh1qeA==
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder.decode(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder.encode(asn1Object))

        extn_list = []
        for extn in asn1Object['tbsCertificate']['extensions']:
            extn_list.append(extn['extnID'])
            if extn['extnID'] in rfc5280.certificateExtensionsMap.keys():
                extnValue, rest = der_decoder.decode(
                    extn['extnValue'],
                    asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']])

                self.assertEqual(
                    extn['extnValue'], der_encoder.encode(extnValue))

                if extn['extnID'] == rfc8226.id_pe_TNAuthList:
                    self.assertEqual('fake', extnValue[0]['spc'])

        self.assertIn(rfc8226.id_pe_TNAuthList, extn_list)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
