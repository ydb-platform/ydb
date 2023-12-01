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
from pyasn1_modules import rfc8017
from pyasn1_modules import rfc2985


class SMIMECapabilitiesTestCase(unittest.TestCase):
    smime_capabilities_pem_text = """\
MIIBAzA8BgkqhkiG9w0BAQcwL6APMA0GCWCGSAFlAwQCAgUAoRwwGgYJKoZIhvcN
AQEIMA0GCWCGSAFlAwQCAgUAMDwGCSqGSIb3DQEBCjAvoA8wDQYJYIZIAWUDBAIC
BQChHDAaBgkqhkiG9w0BAQgwDQYJYIZIAWUDBAICBQAwDQYJKoZIhvcNAQECBQAw
DQYJKoZIhvcNAQEEBQAwDQYJKoZIhvcNAQEFBQAwDQYJKoZIhvcNAQEOBQAwDQYJ
KoZIhvcNAQELBQAwDQYJKoZIhvcNAQEMBQAwDQYJKoZIhvcNAQENBQAwDQYJKoZI
hvcNAQEPBQAwDQYJKoZIhvcNAQEQBQA=
"""

    def setUp(self):
        self.asn1Spec = rfc2985.SMIMECapabilities()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        for cap in asn1Object:
            self.assertIn(cap['algorithm'], rfc5280.algorithmIdentifierMap)

            if cap['parameters'].hasValue():
                p, rest = der_decoder(
                    cap['parameters'],
                    asn1Spec=rfc5280.algorithmIdentifierMap[cap['algorithm']])

                self.assertFalse(rest)
                if not p == univ.Null(""):
                    self.assertTrue(p.prettyPrint())
                self.assertEqual(cap['parameters'], der_encoder(p))

                if cap['algorithm'] == rfc8017.id_RSAES_OAEP:
                    self.assertEqual(
                        rfc8017.id_sha384, p['hashFunc']['algorithm'])
                    self.assertEqual(
                        rfc8017.id_mgf1, p['maskGenFunc']['algorithm'])

    def OpenTypesCodec(self):
        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        for cap in asn1Object:
            if cap['algorithm'] == rfc8017.id_RSAES_OAEP:
                p = cap['parameters']
                self.assertEqual(
                    rfc8017.id_sha384, p['hashFunc']['algorithm'])
                self.assertEqual(
                    rfc8017.id_mgf1, p['maskGenFunc']['algorithm'])


class MultiprimeRSAPrivateKeyTestCase(unittest.TestCase):
    pem_text = """\
MIIE2QIBAQKCAQEAn82EqwXasE2TFNSmZucB8LNza2mOWLHF3nxpxKXalPMDvezc
5Dq7Ytcv/k9jJL4j4jYfvR4yyZdU9iHLaD6hOINZ8E6hVpx/4c96ZUSOLzD2g+u+
jIuoNfG+zygSBGYCS6BLCAIsZ+2wUyxYpLJknHJld9/jy+aLmmyrilhH9dH5AUiV
3NeWht/68++dMXf4ZI/gV4bMSlWhggxkz2WJJpiQdCdJatGkwNDkHmLA9X0tC6OH
SPE7qYdxG38cYS5F445SgnhDpiK7BodSqYLwgehaDjoOYdEgHVnOcpBCDI5zCJSL
b1c/z8uhrB1xxlECR44wCLcKsIIYQxaEErRJ/wIDAQABAoIBAD+Ra5L0szeqxDVn
GgKZJkZvvBwgU0WpAgMtDo3xQ/A4c2ab0IrhaiU5YJgTUGcPVArqaNm8J4vVrTBz
5QxEzbFDXwWe4cMoYh6bgB7ElKLlIUr8/kGZUfgc7kI29luEjcAIEAC2/RQHesVn
DHkL5OzqZL+4fIwckAMh0tXdflsPgZ/jgIaKca4OqKu4KGnczm3UvqtlvwisAjkx
zMyfZXOLn0vEwP2bfbhQrCVrP7n6a+CV+Kqm8NBWnbiS6x2rWemVVssNTbfXQztq
wC6ZJZCLK7plciDBWvHcS6vxdcsS9DUxuqSV6o/stCGTl1D+9tDx8Od0Eunna2B2
wAoRHZECVgbNO1bqwfYpp5aFuySWoP+KZz8f/5ZkHjLwiNGpQcqVd4+7Ql2R4qgF
NgSoQQOZFhKtiOeLVU0HYfp6doI4waSINZdF/fJDHD6fY3AMOc/IIMDHHIzbAlYG
vKOocLXWj/2+gcyQ1XoAmrE70aIFUBLSvd7RCi8GI74zYWp5lCSvO850Z4GsWSZT
41iF13sTDDJPm3+BbzMvEu2GuACi/8/IpbUr24/FP9Cp1Rf7kwJWAgMxfoshbrNu
ebQB5laHNnT+DYhrOFVRNiNDaD2bUNSetrFidosWtD4ueHxMGENwa4BbFJ9+UrdP
fyxC6k7exM7khGjaNZczwTep1VpYtKjzP/bp9KcCVgYoj9s9HZ1FCAsNEPodjGfd
AcPTQS9mIa7wzy19B7uvFQJXPURi/p4KKBMVQ99Pp8/r9lJzxxiEf8FyPr8N7lZM
EUKkFkDrZQDhKpsrHWSNj6yRFlltAlYC7dYR8KLEWoOUATLosxQhwgypv+23r+d4
ZdPOdDv9n8Kmj+NFy/oISFfdXzlOU4RWQtMx3hEwAabwct7vjiJEej/kmiTqco02
17tt13VvvQ5ZXF73dDCCAQwwggEIAlYDfMpM1WNfxcLLOgkRZ+0S9OvIrEOi0ALV
SquTdi/thhCuCsK3lMD4miN9te8j16YtqEFVWXC3a6DWwIJ6m/xZ50bBwPqM8RsI
6FWhZw4Dr5VqjYXUvwJWAvapRk9SydDYri/cAtGIkUJVlspkE1emALAaSw30vmfd
hrgYLT6YGOmK3UmcNJ4NVeET275MXWF1ZOhkOGKTN6aj5wPhJaHBMnmUQrq7GwC6
/LfUkSsCVgMCDTV9gbFW8u6TcTVW85dBIeUGxZh1T2pbU3dkGO3IOxOhzJUplH4/
EeEs9dusHakg1ERXAg4Vo1YowPW8kuVbZ9faxeVrmuER5NcCuZzS5X/obGUw
"""

    def setUp(self):
        self.asn1Spec = rfc8017.RSAPrivateKey()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
