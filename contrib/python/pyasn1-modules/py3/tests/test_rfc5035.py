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
from pyasn1_modules import rfc5035


class SignedMessageTestCase(unittest.TestCase):
    signed_message_pem_text = """\
MIIFzAYJKoZIhvcNAQcCoIIFvTCCBbkCAQExDTALBglghkgBZQMEAgIwUQYJKoZI
hvcNAQcBoEQEQkNvbnRlbnQtVHlwZTogdGV4dC9wbGFpbg0KDQpXYXRzb24sIGNv
bWUgaGVyZSAtIEkgd2FudCB0byBzZWUgeW91LqCCAnwwggJ4MIIB/qADAgECAgkA
pbNUKBuwbjswCgYIKoZIzj0EAwMwPzELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAlZB
MRAwDgYDVQQHDAdIZXJuZG9uMREwDwYDVQQKDAhCb2d1cyBDQTAeFw0xOTA1Mjkx
NDQ1NDFaFw0yMDA1MjgxNDQ1NDFaMHAxCzAJBgNVBAYTAlVTMQswCQYDVQQIEwJW
QTEQMA4GA1UEBxMHSGVybmRvbjEQMA4GA1UEChMHRXhhbXBsZTEOMAwGA1UEAxMF
QWxpY2UxIDAeBgkqhkiG9w0BCQEWEWFsaWNlQGV4YW1wbGUuY29tMHYwEAYHKoZI
zj0CAQYFK4EEACIDYgAE+M2fBy/sRA6V1pKFqecRTE8+LuAHtZxes1wmJZrBBg+b
z7uYZfYQxI3dVB0YCSD6Mt3yXFlnmfBRwoqyArbjIBYrDbHBv2k8Csg2DhQ7qs/w
to8hMKoFgkcscqIbiV7Zo4GUMIGRMAsGA1UdDwQEAwIHgDBCBglghkgBhvhCAQ0E
NRYzVGhpcyBjZXJ0aWZpY2F0ZSBjYW5ub3QgYmUgdHJ1c3RlZCBmb3IgYW55IHB1
cnBvc2UuMB0GA1UdDgQWBBTEuloOPnrjPIGw9AKqaLsW4JYONTAfBgNVHSMEGDAW
gBTyNds0BNqlVfK9aQOZsGLs4hUIwTAKBggqhkjOPQQDAwNoADBlAjBjuR/RNbgL
3kRhmn+PJTeKaL9sh/oQgHOYTgLmSnv3+NDCkhfKuMNoo/tHrkmihYgCMQC94Mae
rDIrQpi0IDh+v0QSAv9rMife8tClafXWtDwwL8MS7oAh0ymT446Uizxx3PUxggLQ
MIICzAIBATBMMD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwH
SGVybmRvbjERMA8GA1UECgwIQm9ndXMgQ0ECCQCls1QoG7BuOzALBglghkgBZQME
AgKgggH1MBgGCSqGSIb3DQEJAzELBgkqhkiG9w0BBwEwHAYJKoZIhvcNAQkFMQ8X
DTE5MDUyOTE4MjMxOVowJQYLKoZIhvcNAQkQAgcxFgQUAbWZQYhLO5wtUgsOCGtT
4V3aNhUwLwYLKoZIhvcNAQkQAgQxIDAeDBFXYXRzb24sIGNvbWUgaGVyZQYJKoZI
hvcNAQcBMDUGCyqGSIb3DQEJEAICMSYxJAIBAQYKKwYBBAGBrGABARMTQm9hZ3Vz
IFByaXZhY3kgTWFyazA/BgkqhkiG9w0BCQQxMgQwtuQipP2CZx7U96rGbUT06LC5
jVFYccZW5/CaNvpcrOPiChDm2vI3m4k300z5mSZsME0GCyqGSIb3DQEJEAIBMT4w
PAQgx08hD2QnVwj1DoeRELNtdZ0PffW4BQIvcwwVc/goU6OAAQEwFTATgRFhbGlj
ZUBleGFtcGxlLmNvbTCBmwYLKoZIhvcNAQkQAi8xgYswgYgwdjB0BCACcp04gyM2
dTDg+0ydCwlucr6Mg8Wd3J3c9V+iLHsnZzBQMEOkQTA/MQswCQYDVQQGEwJVUzEL
MAkGA1UECAwCVkExEDAOBgNVBAcMB0hlcm5kb24xETAPBgNVBAoMCEJvZ3VzIENB
AgkApbNUKBuwbjswDjAMBgorBgEEAYGsYAEBMAoGCCqGSM49BAMDBGcwZQIxAO3K
D9YjFTKE3p383VVw/ol79WTVoMea4H1+7xn+3E1XO4oyb7qwQz0KmsGfdqWptgIw
T9yMtRLN5ZDU14y+Phzq9NKpSw/x5KyXoUKjCMc3Ru6dIW+CgcRQees+dhnvuD5U
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.signed_message_pem_text)
        asn1Object, rest = der_decoder (substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(rfc5652.id_signedData, asn1Object['contentType'])

        sd, rest = der_decoder(asn1Object['content'], asn1Spec=rfc5652.SignedData())

        self.assertFalse(rest)
        self.assertTrue(sd.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(sd))
       
        for sa in sd['signerInfos'][0]['signedAttrs']:
            sat = sa['attrType']
            sav0 = sa['attrValues'][0]

            if sat in rfc5652.cmsAttributesMap.keys():
                sav, rest = der_decoder(sav0, asn1Spec=rfc5652.cmsAttributesMap[sat])
                self.assertFalse(rest)
                self.assertTrue(sav.prettyPrint())
                self.assertEqual(sav0, der_encoder(sav))


class SignedReceiptTestCase(unittest.TestCase):
    signed_receipt_pem_text = """\
MIIE3gYJKoZIhvcNAQcCoIIEzzCCBMsCAQMxDTALBglghkgBZQMEAgEwga4GCyqGSIb3DQEJ
EAEBoIGeBIGbMIGYAgEBBgkqhkiG9w0BBwEEIMdPIQ9kJ1cI9Q6HkRCzbXWdD331uAUCL3MM
FXP4KFOjBGYwZAIwOLV5WCbYjy5HLHE69IqXQQHVDJQzmo18WwkFrEYH3EMsvpXEIGqsFTFN
6NV4VBe9AjA5fGOCP5IhI32YqmGfs+zDlqZyb2xSX6Gr/IfCIm0angfOI39g7lAZDyivjh5H
/oSgggJ3MIICczCCAfqgAwIBAgIJAKWzVCgbsG48MAoGCCqGSM49BAMDMD8xCzAJBgNVBAYT
AlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9ndXMgQ0Ew
HhcNMTkwNTI5MTkyMDEzWhcNMjAwNTI4MTkyMDEzWjBsMQswCQYDVQQGEwJVUzELMAkGA1UE
CBMCVkExEDAOBgNVBAcTB0hlcm5kb24xEDAOBgNVBAoTB0V4YW1wbGUxDDAKBgNVBAMTA0Jv
YjEeMBwGCSqGSIb3DQEJARYPYm9iQGV4YW1wbGUuY29tMHYwEAYHKoZIzj0CAQYFK4EEACID
YgAEMaRiVS8WvN8Ycmpfq75jBbOMUukNfXAg6AL0JJBXtIFAuIJcZVlkLn/xbywkcMLHK/O+
w9RWUQa2Cjw+h8b/1Cl+gIpqLtE558bD5PfM2aYpJ/YE6yZ9nBfTQs7z1TH5o4GUMIGRMAsG
A1UdDwQEAwIHgDBCBglghkgBhvhCAQ0ENRYzVGhpcyBjZXJ0aWZpY2F0ZSBjYW5ub3QgYmUg
dHJ1c3RlZCBmb3IgYW55IHB1cnBvc2UuMB0GA1UdDgQWBBTKa2Zy3iybV3+YjuLDKtNmjsIa
pTAfBgNVHSMEGDAWgBTyNds0BNqlVfK9aQOZsGLs4hUIwTAKBggqhkjOPQQDAwNnADBkAjAV
boS6OfEYQomLDi2RUkd71hzwwiQZztbxNbosahIzjR8ZQaHhjdjJlrP/T6aXBwsCMDfRweYz
3Ce4E4wPfoqQnvqpM7ZlfhstjQQGOsWAtIIfqW/l+TgCO8ux3XLV6fj36zGCAYkwggGFAgEB
MEwwPzELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAlZBMRAwDgYDVQQHDAdIZXJuZG9uMREwDwYD
VQQKDAhCb2d1cyBDQQIJAKWzVCgbsG48MAsGCWCGSAFlAwQCAaCBrjAaBgkqhkiG9w0BCQMx
DQYLKoZIhvcNAQkQAQEwHAYJKoZIhvcNAQkFMQ8XDTE5MDUyOTE5MzU1NVowLwYJKoZIhvcN
AQkEMSIEIGb9Hm2kCnM0CYNpZU4Uj7dN0AzOieIn9sDqZMcIcZrEMEEGCyqGSIb3DQEJEAIF
MTIEMBZzeHVja7fQ62ywyh8rtKzBP1WJooMdZ+8c6pRqfIESYIU5bQnH99OPA51QCwdOdjAK
BggqhkjOPQQDAgRoMGYCMQDZiT22xgab6RFMAPvN4fhWwzx017EzttD4VaYrpbolropBdPJ6
jIXiZQgCwxbGTCwCMQClaQ9K+L5LTeuW50ZKSIbmBZQ5dxjtnK3OlS7hYRi6U0JKZmWbbuS8
vFIgX7eIkd8=
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.signed_receipt_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(rfc5652.id_signedData, asn1Object['contentType'])

        sd, rest = der_decoder(
            asn1Object['content'], asn1Spec=rfc5652.SignedData())

        self.assertFalse(rest)
        self.assertTrue(sd.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(sd))
        self.assertEqual(
            rfc5035.id_ct_receipt, sd['encapContentInfo']['eContentType'])

        receipt, rest = der_decoder(
            sd['encapContentInfo']['eContent'], asn1Spec=rfc5035.Receipt())

        self.assertFalse(rest)
        self.assertTrue(receipt.prettyPrint())
        self.assertEqual(
            sd['encapContentInfo']['eContent'], der_encoder(receipt))

        for sa in sd['signerInfos'][0]['signedAttrs']:
            sat = sa['attrType']
            sav0 = sa['attrValues'][0]

            if sat in rfc5652.cmsAttributesMap.keys():
                sav, rest = der_decoder(
                    sav0, asn1Spec=rfc5652.cmsAttributesMap[sat])
                self.assertFalse(rest)
                self.assertTrue(sav.prettyPrint())
                self.assertEqual(sav0, der_encoder(sav))

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.signed_receipt_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertIn(asn1Object['contentType'], rfc5652.cmsContentTypesMap)
        self.assertEqual(rfc5652.id_signedData, asn1Object['contentType'])

        sd = asn1Object['content']

        self.assertEqual(
            rfc5652.CMSVersion().subtype(value='v3'), sd['version'])
        self.assertIn(
            sd['encapContentInfo']['eContentType'], rfc5652.cmsContentTypesMap)
        self.assertEqual(
            rfc5035.id_ct_receipt, sd['encapContentInfo']['eContentType'])

        for sa in sd['signerInfos'][0]['signedAttrs']:
            self.assertIn(sa['attrType'], rfc5652.cmsAttributesMap)
            if sa['attrType'] == rfc5035.id_aa_msgSigDigest:
                self.assertIn(
                    '0x167378', sa['attrValues'][0].prettyPrint()[:10])

        # Since receipt is inside an OCTET STRING, decodeOpenTypes=True cannot
        # automatically decode it 
        receipt, rest = der_decoder(
            sd['encapContentInfo']['eContent'],
            asn1Spec=rfc5652.cmsContentTypesMap[sd['encapContentInfo']['eContentType']])

        self.assertEqual(1, receipt['version'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
