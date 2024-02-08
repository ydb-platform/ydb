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
from pyasn1.type import univ

from pyasn1_modules import pem
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc5480
from pyasn1_modules import rfc6402
from pyasn1_modules import rfc6955


class CertificationRequestTestCase(unittest.TestCase):
    pem_text = """\
MIIDPDCCArsCAQAwTjELMAkGA1UEBhMCVVMxETAPBgNVBAoTCFhFVEkgSW5jMRAw
DgYDVQQLEwdUZXN0aW5nMRowGAYDVQQDExFQS0lYIEV4YW1wbGUgVXNlcjCCAkEw
ggG2BgcqhkjOPgIBMIIBqQKBgQCUhOBFbH9pUWI+VoB8aOfFqZ6edHSU7ZCMHcTh
ShSC9dKUDBnjuRC7EbnlpfuOIVFjAoaqBrghNrZ/Nt/R1mhbeXwdWhR1H2qTdZPO
u5dyivAPI51H9tSzx/D05vYrwjLhiWe+fgau+NABa4sq9QLXtqhjlIOwGzF9Uhre
5QOFJwKBgCamMixaK9QzK1zcBodTP5AGYVA4PtK5fYEcEhDFDFPUZNGOMAcIjN0/
Ci8s1ht/V4bQ2rtuNioY6NO8cDF6SLZOGG7dHyIG6z/q1EFp2ZveR5V6cpHSCX9J
XDsDM1HI8Tma/wTVbn6UPQO49jEVJkiVqFzeR4i0aToAp4ae2tHNAiEA6HL6lvAR
QPXy3P07XXiUsYUB5Wk3IfclubpxSvxgMPsCYQCjkQHAqG6kTaBW/Gz+H6ewzQ+U
hwwlvpd2jevlpAldq4PNgAs1Z38MjqcxmDKFOUCdEZjY3rh/hpuvjWc9tna0YS8h
4UsOaP9TPofd2HFWaEfc9yBjSzxfeHGD5nCe4pIwGgMVABzVOg0Xgm0KgXWBRhCO
PtsJ5Jg0AgE3A4GEAAKBgBNjoYUEjEaoiOv0XqiTdK79rp6WJxJlxEwHBj4Y/pS4
qHlIvS40tkfKBDCh7DP9GgstnlDJeA+uauy1a2q+slzasp94LLl34nkrJb8uC1lK
k0v4s+yBNK6XR1LgqCmY7NGwyitveovbTo2lFX5+rzNiCZ4PEUSMwY2iEZ5T77Lo
oCEwHwYJKoZIhvcNAQkOMRIwEDAOBgNVHQ8BAf8EBAMCAwgwDAYIKwYBBQUHBgMF
AANtADBqMFIwSDELMAkGA1UEBhMCVVMxETAPBgNVBAoTCFhFVEkgSW5jMRAwDgYD
VQQLEwdUZXN0aW5nMRQwEgYDVQQDEwtSb290IERTQSBDQQIGANo5tuLLBBQtBXf+
Xo9l9a+tyVybAsCoiClhYw==
"""

    def setUp(self):
        self.asn1Spec = rfc6402.CertificationRequest()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki_a = asn1Object['certificationRequestInfo']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc5480.dhpublicnumber, spki_a['algorithm'])
        self.assertIn(spki_a['algorithm'], rfc5280.algorithmIdentifierMap)

        params, rest = der_decoder(
            spki_a['parameters'], asn1Spec=rfc6955.DomainParameters())

        self.assertFalse(rest)
        self.assertTrue(params.prettyPrint())
        self.assertEqual(spki_a['parameters'], der_encoder(params))
        self.assertEqual(55, params['validationParms']['pgenCounter'])

        sig_a = asn1Object['signatureAlgorithm']

        self.assertEqual(
            rfc6955.id_dhPop_static_sha1_hmac_sha1, sig_a['algorithm'])
        self.assertIn(sig_a['algorithm'], rfc5280.algorithmIdentifierMap)
        self.assertEqual(sig_a['parameters'], der_encoder(univ.Null("")))

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki_a = asn1Object['certificationRequestInfo']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc5480.dhpublicnumber, spki_a['algorithm'])
        self.assertEqual(
            55, spki_a['parameters']['validationParms']['pgenCounter'])

        sig_a = asn1Object['signatureAlgorithm']

        self.assertEqual(
            rfc6955.id_dhPop_static_sha1_hmac_sha1, sig_a['algorithm'])
        self.assertEqual(univ.Null(""), sig_a['parameters'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
