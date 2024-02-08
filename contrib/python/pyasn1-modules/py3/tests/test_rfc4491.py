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
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc4491
from pyasn1_modules import rfc4357


class GostR341094CertificateTestCase(unittest.TestCase):
    gostR3410_94_cert_pem_text = """\
MIICCzCCAboCECMO42BGlSTOxwvklBgufuswCAYGKoUDAgIEMGkxHTAbBgNVBAMM
FEdvc3RSMzQxMC05NCBleGFtcGxlMRIwEAYDVQQKDAlDcnlwdG9Qcm8xCzAJBgNV
BAYTAlJVMScwJQYJKoZIhvcNAQkBFhhHb3N0UjM0MTAtOTRAZXhhbXBsZS5jb20w
HhcNMDUwODE2MTIzMjUwWhcNMTUwODE2MTIzMjUwWjBpMR0wGwYDVQQDDBRHb3N0
UjM0MTAtOTQgZXhhbXBsZTESMBAGA1UECgwJQ3J5cHRvUHJvMQswCQYDVQQGEwJS
VTEnMCUGCSqGSIb3DQEJARYYR29zdFIzNDEwLTk0QGV4YW1wbGUuY29tMIGlMBwG
BiqFAwICFDASBgcqhQMCAiACBgcqhQMCAh4BA4GEAASBgLuEZuF5nls02CyAfxOo
GWZxV/6MVCUhR28wCyd3RpjG+0dVvrey85NsObVCNyaE4g0QiiQOHwxCTSs7ESuo
v2Y5MlyUi8Go/htjEvYJJYfMdRv05YmKCYJo01x3pg+2kBATjeM+fJyR1qwNCCw+
eMG1wra3Gqgqi0WBkzIydvp7MAgGBiqFAwICBANBABHHCH4S3ALxAiMpR3aPRyqB
g1DjB8zy5DEjiULIc+HeIveF81W9lOxGkZxnrFjXBSqnjLeFKgF1hffXOAP7zUM=
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.gostR3410_94_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        
        sa1 = asn1Object['signatureAlgorithm']['algorithm']
        self.assertEqual(rfc4491.id_GostR3411_94_with_GostR3410_94, sa1)

        sa2 = asn1Object['tbsCertificate']['signature']['algorithm']
        self.assertEqual(rfc4491.id_GostR3411_94_with_GostR3410_94, sa2)

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']
        self.assertEqual(rfc4491.id_GostR3410_94, spki_a['algorithm'])

        pk_p, rest = der_decoder(
            spki_a['parameters'],
            asn1Spec=rfc4491.GostR3410_94_PublicKeyParameters())

        self.assertFalse(rest)
        self.assertTrue(pk_p.prettyPrint())
        self.assertEqual(spki_a['parameters'], der_encoder(pk_p))
        self.assertEqual(rfc4357.id_GostR3411_94_CryptoProParamSet, pk_p['digestParamSet'])

    def testOpenTypes(self):
        openTypesMap = {
            rfc4491.id_GostR3410_94: rfc4491.GostR3410_94_PublicKeyParameters(),
        }

        substrate = pem.readBase64fromText(self.gostR3410_94_cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec,
            openTypes=openTypesMap, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        sa1 = asn1Object['signatureAlgorithm']['algorithm']
        self.assertEqual(rfc4491.id_GostR3411_94_with_GostR3410_94, sa1)

        sa2 = asn1Object['tbsCertificate']['signature']['algorithm']
        self.assertEqual(rfc4491.id_GostR3411_94_with_GostR3410_94, sa2)

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']
        self.assertEqual(rfc4491.id_GostR3410_94, spki_a['algorithm'])
        self.assertEqual(rfc4357.id_GostR3411_94_CryptoProParamSet, spki_a['parameters']['digestParamSet'])

class GostR34102001CertificateTestCase(unittest.TestCase):
    gostR3410_2001_cert_pem_text = """\
MIIB0DCCAX8CECv1xh7CEb0Xx9zUYma0LiEwCAYGKoUDAgIDMG0xHzAdBgNVBAMM
Fkdvc3RSMzQxMC0yMDAxIGV4YW1wbGUxEjAQBgNVBAoMCUNyeXB0b1BybzELMAkG
A1UEBhMCUlUxKTAnBgkqhkiG9w0BCQEWGkdvc3RSMzQxMC0yMDAxQGV4YW1wbGUu
Y29tMB4XDTA1MDgxNjE0MTgyMFoXDTE1MDgxNjE0MTgyMFowbTEfMB0GA1UEAwwW
R29zdFIzNDEwLTIwMDEgZXhhbXBsZTESMBAGA1UECgwJQ3J5cHRvUHJvMQswCQYD
VQQGEwJSVTEpMCcGCSqGSIb3DQEJARYaR29zdFIzNDEwLTIwMDFAZXhhbXBsZS5j
b20wYzAcBgYqhQMCAhMwEgYHKoUDAgIkAAYHKoUDAgIeAQNDAARAhJVodWACGkB1
CM0TjDGJLP3lBQN6Q1z0bSsP508yfleP68wWuZWIA9CafIWuD+SN6qa7flbHy7Df
D2a8yuoaYDAIBgYqhQMCAgMDQQA8L8kJRLcnqeyn1en7U23Sw6pkfEQu3u0xFkVP
vFQ/3cHeF26NG+xxtZPz3TaTVXdoiYkXYiD02rEx1bUcM97i
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.gostR3410_2001_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        sa1 = asn1Object['signatureAlgorithm']['algorithm']
        self.assertEqual(rfc4491.id_GostR3411_94_with_GostR3410_2001, sa1)

        sa2 = asn1Object['tbsCertificate']['signature']['algorithm']
        self.assertEqual(rfc4491.id_GostR3411_94_with_GostR3410_2001, sa2)

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']
        self.assertEqual(rfc4491.id_GostR3410_2001, spki_a['algorithm'])

        pk_p, rest = der_decoder(
            spki_a['parameters'], asn1Spec=rfc4491.GostR3410_2001_PublicKeyParameters())

        self.assertFalse(rest)
        self.assertTrue(pk_p.prettyPrint())
        self.assertEqual(spki_a['parameters'], der_encoder(pk_p))
        self.assertEqual(rfc4357.id_GostR3411_94_CryptoProParamSet, pk_p['digestParamSet'])

    def testOpenTypes(self):
        openTypeMap = {
            rfc4491.id_GostR3410_2001: rfc4491.GostR3410_2001_PublicKeyParameters(),
        }

        substrate = pem.readBase64fromText(self.gostR3410_2001_cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec,
            openTypes=openTypeMap, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        sa1 = asn1Object['signatureAlgorithm']['algorithm']
        self.assertEqual(rfc4491.id_GostR3411_94_with_GostR3410_2001, sa1)

        sa2 = asn1Object['tbsCertificate']['signature']['algorithm']
        self.assertEqual(rfc4491.id_GostR3411_94_with_GostR3410_2001, sa2)

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']
        self.assertEqual(rfc4491.id_GostR3410_2001, spki_a['algorithm'])
        self.assertEqual(rfc4357.id_GostR3411_94_CryptoProParamSet, spki_a['parameters']['digestParamSet'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
