#
# This file is part of pyasn1-modules software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder
from pyasn1.type import univ

from pyasn1_modules import pem
from pyasn1_modules import rfc5280


class CertificateTestCase(unittest.TestCase):
    pem_text = """\
MIIC5zCCAlACAQEwDQYJKoZIhvcNAQEFBQAwgbsxJDAiBgNVBAcTG1ZhbGlDZXJ0
IFZhbGlkYXRpb24gTmV0d29yazEXMBUGA1UEChMOVmFsaUNlcnQsIEluYy4xNTAz
BgNVBAsTLFZhbGlDZXJ0IENsYXNzIDMgUG9saWN5IFZhbGlkYXRpb24gQXV0aG9y
aXR5MSEwHwYDVQQDExhodHRwOi8vd3d3LnZhbGljZXJ0LmNvbS8xIDAeBgkqhkiG
9w0BCQEWEWluZm9AdmFsaWNlcnQuY29tMB4XDTk5MDYyNjAwMjIzM1oXDTE5MDYy
NjAwMjIzM1owgbsxJDAiBgNVBAcTG1ZhbGlDZXJ0IFZhbGlkYXRpb24gTmV0d29y
azEXMBUGA1UEChMOVmFsaUNlcnQsIEluYy4xNTAzBgNVBAsTLFZhbGlDZXJ0IENs
YXNzIDMgUG9saWN5IFZhbGlkYXRpb24gQXV0aG9yaXR5MSEwHwYDVQQDExhodHRw
Oi8vd3d3LnZhbGljZXJ0LmNvbS8xIDAeBgkqhkiG9w0BCQEWEWluZm9AdmFsaWNl
cnQuY29tMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDjmFGWHOjVsQaBalfD
cnWTq8+epvzzFlLWLU2fNUSoLgRNB0mKOCn1dzfnt6td3zZxFJmP3MKS8edgkpfs
2Ejcv8ECIMYkpChMMFp2bbFc893enhBxoYjHW5tBbcqwuI4V7q0zK89HBFx1cQqY
JJgpp0lZpd34t0NiYfPT4tBVPwIDAQABMA0GCSqGSIb3DQEBBQUAA4GBAFa7AliE
Zwgs3x/be0kz9dNnnfS0ChCzycUs4pJqcXgn8nCDQtM+z6lU9PHYkhaM0QTLS6vJ
n0WuPIqpsHEzXcjFV9+vqDWzf4mH6eglkrh/hXqu1rweN1gqZ8mRzyqBPu3GOd/A
PhmcGcwTTYJBtYze4D1gCCAPRX5ron+jjBXu
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):

        substrate = pem.readBase64fromText(self.pem_text)

        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))


class CertificateListTestCase(unittest.TestCase):
    pem_text = """\
MIIBVjCBwAIBATANBgkqhkiG9w0BAQUFADB+MQswCQYDVQQGEwJBVTETMBEGA1UE
CBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0cyBQdHkgTHRk
MRUwEwYDVQQDEwxzbm1wbGFicy5jb20xIDAeBgkqhkiG9w0BCQEWEWluZm9Ac25t
cGxhYnMuY29tFw0xMjA0MTExMzQwNTlaFw0xMjA1MTExMzQwNTlaoA4wDDAKBgNV
HRQEAwIBATANBgkqhkiG9w0BAQUFAAOBgQC1D/wwnrcY/uFBHGc6SyoYss2kn+nY
RTwzXmmldbNTCQ03x5vkWGGIaRJdN8QeCzbEi7gpgxgpxAx6Y5WkxkMQ1UPjNM5n
DGVDOtR0dskFrrbHuNpWqWrDaBN0/ryZiWKjr9JRbrpkHgVY29I1gLooQ6IHuKHY
vjnIhxTFoCb5vA==
"""

    def setUp(self):
        self.asn1Spec = rfc5280.CertificateList()

    def testDerCodec(self):

        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))


class CertificateOpenTypeTestCase(unittest.TestCase):
    pem_text = """\
MIIC5zCCAlACAQEwDQYJKoZIhvcNAQEFBQAwgbsxJDAiBgNVBAcTG1ZhbGlDZXJ0
IFZhbGlkYXRpb24gTmV0d29yazEXMBUGA1UEChMOVmFsaUNlcnQsIEluYy4xNTAz
BgNVBAsTLFZhbGlDZXJ0IENsYXNzIDMgUG9saWN5IFZhbGlkYXRpb24gQXV0aG9y
aXR5MSEwHwYDVQQDExhodHRwOi8vd3d3LnZhbGljZXJ0LmNvbS8xIDAeBgkqhkiG
9w0BCQEWEWluZm9AdmFsaWNlcnQuY29tMB4XDTk5MDYyNjAwMjIzM1oXDTE5MDYy
NjAwMjIzM1owgbsxJDAiBgNVBAcTG1ZhbGlDZXJ0IFZhbGlkYXRpb24gTmV0d29y
azEXMBUGA1UEChMOVmFsaUNlcnQsIEluYy4xNTAzBgNVBAsTLFZhbGlDZXJ0IENs
YXNzIDMgUG9saWN5IFZhbGlkYXRpb24gQXV0aG9yaXR5MSEwHwYDVQQDExhodHRw
Oi8vd3d3LnZhbGljZXJ0LmNvbS8xIDAeBgkqhkiG9w0BCQEWEWluZm9AdmFsaWNl
cnQuY29tMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDjmFGWHOjVsQaBalfD
cnWTq8+epvzzFlLWLU2fNUSoLgRNB0mKOCn1dzfnt6td3zZxFJmP3MKS8edgkpfs
2Ejcv8ECIMYkpChMMFp2bbFc893enhBxoYjHW5tBbcqwuI4V7q0zK89HBFx1cQqY
JJgpp0lZpd34t0NiYfPT4tBVPwIDAQABMA0GCSqGSIb3DQEBBQUAA4GBAFa7AliE
Zwgs3x/be0kz9dNnnfS0ChCzycUs4pJqcXgn8nCDQtM+z6lU9PHYkhaM0QTLS6vJ
n0WuPIqpsHEzXcjFV9+vqDWzf4mH6eglkrh/hXqu1rweN1gqZ8mRzyqBPu3GOd/A
PhmcGcwTTYJBtYze4D1gCCAPRX5ron+jjBXu
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):

        substrate = pem.readBase64fromText(self.pem_text)

        openTypesMap = {
            univ.ObjectIdentifier('1.2.840.113549.1.1.1'): univ.Null(""),
            univ.ObjectIdentifier('1.2.840.113549.1.1.5'): univ.Null(""),
            univ.ObjectIdentifier('1.2.840.113549.1.1.11'): univ.Null(""),
        }

        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, openTypes=openTypesMap,
            decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        sig_alg = asn1Object['tbsCertificate']['signature']

        self.assertEqual(univ.Null(""), sig_alg['parameters'])

        spki_alg = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(univ.Null(""), spki_alg['parameters'])

        for rdn in asn1Object['tbsCertificate']['subject']['rdnSequence']:
            for atv in rdn:
                if atv['type'] == rfc5280.id_emailAddress:
                    self.assertIn("valicert.com", atv['value'])
                else:
                    atv_ps = str(atv['value']['printableString'])
                    self.assertIn("valicert", atv_ps.lower())


class CertificateListOpenTypeTestCase(unittest.TestCase):
    pem_text = """\
MIIBVjCBwAIBATANBgkqhkiG9w0BAQUFADB+MQswCQYDVQQGEwJBVTETMBEGA1UE
CBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0cyBQdHkgTHRk
MRUwEwYDVQQDEwxzbm1wbGFicy5jb20xIDAeBgkqhkiG9w0BCQEWEWluZm9Ac25t
cGxhYnMuY29tFw0xMjA0MTExMzQwNTlaFw0xMjA1MTExMzQwNTlaoA4wDDAKBgNV
HRQEAwIBATANBgkqhkiG9w0BAQUFAAOBgQC1D/wwnrcY/uFBHGc6SyoYss2kn+nY
RTwzXmmldbNTCQ03x5vkWGGIaRJdN8QeCzbEi7gpgxgpxAx6Y5WkxkMQ1UPjNM5n
DGVDOtR0dskFrrbHuNpWqWrDaBN0/ryZiWKjr9JRbrpkHgVY29I1gLooQ6IHuKHY
vjnIhxTFoCb5vA==
"""

    def setUp(self):
        self.asn1Spec = rfc5280.CertificateList()

    def testDerCodec(self):

        substrate = pem.readBase64fromText(self.pem_text)

        openTypesMap = {
            univ.ObjectIdentifier('1.2.840.113549.1.1.1'): univ.Null(""),
            univ.ObjectIdentifier('1.2.840.113549.1.1.5'): univ.Null(""),
            univ.ObjectIdentifier('1.2.840.113549.1.1.11'): univ.Null(""),
        }

        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, openTypes=openTypesMap,
            decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        sig_alg = asn1Object['tbsCertList']['signature']

        self.assertEqual(univ.Null(""), sig_alg['parameters'])

        for rdn in asn1Object['tbsCertList']['issuer']['rdnSequence']:
            for atv in rdn:
                if atv['type'] == rfc5280.id_emailAddress:
                    self.assertIn("snmplabs.com", atv['value'])

                elif atv['type'] == rfc5280.id_at_countryName:
                    self.assertEqual('AU', atv['value'])

                else:
                    self.assertLess(9, len(atv['value']['printableString']))

        crl_extn_count = 0

        for extn in asn1Object['tbsCertList']['crlExtensions']:
            if extn['extnID'] in rfc5280.certificateExtensionsMap.keys():
                ev, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']])

                self.assertFalse(rest)
                self.assertTrue(ev.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(ev))

                crl_extn_count += 1

        self.assertEqual(1, crl_extn_count)

    def testExtensionsMap(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        cert_extn_count = 0

        for extn in asn1Object['tbsCertList']['crlExtensions']:
            if extn['extnID'] in rfc5280.certificateExtensionsMap.keys():
                extnValue, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']])

                self.assertEqual(extn['extnValue'], der_encoder(extnValue))

                cert_extn_count += 1

        self.assertEqual(1, cert_extn_count)


class ORAddressOpenTypeTestCase(unittest.TestCase):
    oraddress_pem_text = """\
MEMwK2EEEwJHQmIKEwhHT0xEIDQwMKIHEwVVSy5BQ4MHU2FsZm9yZKYFEwNSLUQx
FDASgAEBoQ0TC1N0ZXZlIEtpbGxl
"""

    def setUp(self):
        self.asn1Spec = rfc5280.ORAddress()

    def testDecodeOpenTypes(self):
        substrate = pem.readBase64fromText(self.oraddress_pem_text)

        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        ea0 = asn1Object['extension-attributes'][0]

        self.assertEqual(rfc5280.common_name, ea0['extension-attribute-type'])
        self.assertEqual("Steve Kille", ea0['extension-attribute-value'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
