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
from pyasn1_modules import rfc2985
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc7292


class PKCS9AttrsTestCase(unittest.TestCase):
    pem_text = """\
MYIQjzAOBgNVBEExBwwFQWxpY2UwDwYIKwYBBQUHCQMxAxMBTTAQBgNVBAUxCRMH
QjQ4LTAwNzAQBggrBgEFBQcJBDEEEwJVUzAQBggrBgEFBQcJBTEEEwJVUzARBgoq
hkiG9w0BCRkEMQMCATAwFAYJKoZIhvcNAQkCMQcWBUFsaWNlMBgGCiqGSIb3DQEJ
GQMxCgQIUTeqnHYky4AwHAYJKoZIhvcNAQkPMQ8wDTALBglghkgBZQMEAS0wHQYI
KwYBBQUHCQExERgPMjAxOTA4MDMxMjAwMDBaMB0GCCsGAQUFBwkCMREMD0hlcm5k
b24sIFZBLCBVUzApBgkqhkiG9w0BCRQxHB4aAEYAcgBpAGUAbgBkAGwAeQAgAE4A
YQBtAGUwLwYJKoZIhvcNAQkIMSITIDEyMyBVbmtub3duIFdheSwgTm93aGVyZSwg
VkEsIFVTMIGZBgoqhkiG9w0BCRkCMYGKMIGHMAsGCWCGSAFlAwQBLQR4VsJb7t4l
IqjJCT54rqkbCJsBPE17YQJeEYvyA4M1aDIUU5GnCgEhctgMiDPWGMvaSziixdIg
aU/0zvWvYCm8UwPvBBwMtm9X5NDvk9p4nXbGAT8E/OsV1SYWVvwRJwYak0yWWexM
HSixw1Ljh2nb0fIbqwLOeMmIMIIEsQYKKoZIhvcNAQkZBTGCBKEwggSdBgkqhkiG
9w0BBwKgggSOMIIEigIBATENMAsGCWCGSAFlAwQCAjBRBgkqhkiG9w0BBwGgRARC
Q29udGVudC1UeXBlOiB0ZXh0L3BsYWluDQoNCldhdHNvbiwgY29tZSBoZXJlIC0g
SSB3YW50IHRvIHNlZSB5b3UuoIICfDCCAngwggH+oAMCAQICCQCls1QoG7BuOzAK
BggqhkjOPQQDAzA/MQswCQYDVQQGEwJVUzELMAkGA1UECAwCVkExEDAOBgNVBAcM
B0hlcm5kb24xETAPBgNVBAoMCEJvZ3VzIENBMB4XDTE5MDUyOTE0NDU0MVoXDTIw
MDUyODE0NDU0MVowcDELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAlZBMRAwDgYDVQQH
EwdIZXJuZG9uMRAwDgYDVQQKEwdFeGFtcGxlMQ4wDAYDVQQDEwVBbGljZTEgMB4G
CSqGSIb3DQEJARYRYWxpY2VAZXhhbXBsZS5jb20wdjAQBgcqhkjOPQIBBgUrgQQA
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
9t33YdzJaFbz+8jb88xtf454Rvur66Cew/4GYX9u1Zef0DF9So1ay3IicpOf5emo
VWIwg4bh7bELi78i/MbdWtNZQcXimykfeTsYH8Q4u+1uxHS5pwEWWwKiUnLQVpZP
2ut255TdgSIhEILwsaLVelRrx/lp14EpY355FOusXiju6g14aWfBnt5udvuTXxDQ
ZHPPNNk+gwzgvvTey98T941hYUctjg0NApJiB66bfrlYB9mkc5ftg5zqhEasYH5C
4ajKKRNMM7zGlwSZvy8PPhnAeE3Q9LTnos0l4ygjQD/kMlvd7XSLW3GUzjyxtkG4
gQh6LGvnafAbgu7GpcapKEppN86sXEePHiQjj92n103+TxMYWwtaO4iAwkjqdEdt
avEHcXRcpdqC0st6nUwPAPAC4LKJbZgLQnNG+wlWIiCMMD56IdfQ7r/zGIr13MxC
kjNNUdISoWWE5GnQMYHbMBMGCSqGSIb3DQEJFTEGBAQBAAAAMFcGCSqGSIb3DQEJ
FDFKHkgAMwBmADcAMQBhAGYANgA1AC0AMQA2ADgANwAtADQANAA0AGEALQA5AGYA
NAA2AC0AYwA4AGIAZQAxADkANABjADMAZQA4AGUwawYJKwYBBAGCNxEBMV4eXABN
AGkAYwByAG8AcwBvAGYAdAAgAEUAbgBoAGEAbgBjAGUAZAAgAEMAcgB5AHAAdABv
AGcAcgBhAHAAaABpAGMAIABQAHIAbwB2AGkAZABlAHIAIAB2ADEALgAwMIIDaAYJ
KoZIhvcNAQcBoIIDWQSCA1UwggNRMIIDTQYLKoZIhvcNAQwKAQOgggMlMIIDIQYK
KoZIhvcNAQkWAaCCAxEEggMNMIIDCTCCAfGgAwIBAgIQNu32hzqhCKdHATXzboyI
ETANBgkqhkiG9w0BAQUFADAUMRIwEAYDVQQDEwlhbm9ueW1vdXMwIBcNMTYwNzE5
MjIwMDAxWhgPMjExNjA2MjUyMjAwMDFaMBQxEjAQBgNVBAMTCWFub255bW91czCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALy2sEJMGNdcDg6BI7mdFM5T
lPzo5sKBzvUnagK5SKBJ11xMPN5toPTBzICB/XTWEB3AwpD0O+srSca+bsUAyedS
5V4BNp8qCyEu5RNRR8qPHheJ/guhLT96/gGI4jlrUyUhFntPkLKODxu+7KanMy6K
dD+PVE8shXRUZTYe4PG64/c7z3wapnf4XoCXkJRzCY5f3MKz3Ul039kVnTlJcikd
C7I9I9RflXLwXVl4nxUbeeRt6Z8WVWS4pCq+14v2aVPvP3mtVmAYHedRkvS04Hrx
4xx98D3NSSw6Z5OLkzqOcFw15fYmH2NLdhh34gSWJmaaCBAbuQ+1rx/42p7MvvsC
AwEAAaNVMFMwFQYDVR0lBA4wDAYKKwYBBAGCNwoDBDAvBgNVHREEKDAmoCQGCisG
AQQBgjcUAgOgFgwUYW5vbnltb3VzQHdpbmRvd3MteAAwCQYDVR0TBAIwADANBgkq
hkiG9w0BAQUFAAOCAQEAuH7iqY0/MLozwFb39ILYAJDHE+HToZBQbHQP4YtienrU
Stk60rIp0WH65lam7m/JhgAcItc/tV1L8mEnLrvvKcA+NeIL8sDOtM28azvgcOi0
P3roeLLLRCuiykUaKmUcZEDm9cDYKIpJf7QetWQ3uuGTk9iRzpH79x2ix35BnyWQ
Rr3INZzmX/+9YRvPBXKYl/89F/w1ORYArpI9XtjfuPWaGQmM4f1WRHE2t3qRyKFF
ri7QiZdpcSx5zvsRHSyjfUMoKs+b6upk+P01lIhg/ewwYngGab+fZhF15pTNN2hx
8PdNGcrGzrkNKCmJKrWCa2xczuMA+z8SCuC1tYTKmDEVMBMGCSqGSIb3DQEJFTEG
BAQBAAAAMDswHzAHBgUrDgMCGgQUpWCP/fZR0TK5BwGuqvTd0+duiKcEFJTubF2k
HktMK+isIjxOTk4yJTOOAgIH0A==
"""

    def setUp(self):
        self.asn1Spec = rfc2985.AttributeSet()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(der_encoder(asn1Object), substrate)

        openTypesMap = {
            rfc2985.pkcs_9_at_smimeCapabilities: rfc2985.SMIMECapabilities(),
        }
        openTypesMap.update(rfc5280.certificateAttributesMap)
        openTypesMap.update(rfc5652.cmsAttributesMap)

        for attr in asn1Object:
            self.assertIn(attr['type'], openTypesMap)

            av, rest = der_decoder(
                attr['values'][0], asn1Spec=openTypesMap[attr['type']])

            self.assertFalse(rest)
            self.assertTrue(av.prettyPrint())
            self.assertEqual(attr['values'][0], der_encoder(av))

            if attr['type'] == rfc2985.pkcs_9_at_userPKCS12:

                self.assertEqual(univ.Integer(3), av['version'])
                self.assertEqual(rfc5652.id_data, av['authSafe']['contentType'])

                outdata, rest = der_decoder(
                    av['authSafe']['content'], asn1Spec=univ.OctetString())

                self.assertFalse(rest)

                authsafe, rest = der_decoder(
                    outdata, asn1Spec=rfc7292.AuthenticatedSafe())

                self.assertFalse(rest)

                for ci in authsafe:
                    self.assertEqual(rfc5652.id_data, ci['contentType'])

                    indata, rest = der_decoder(
                        ci['content'], asn1Spec=univ.OctetString())

                    self.assertFalse(rest)

                    sc, rest = der_decoder(
                        indata, asn1Spec=rfc7292.SafeContents())

                    self.assertFalse(rest)

                    for sb in sc:
                        if sb['bagId'] in rfc7292.pkcs12BagTypeMap:
                            bv, rest = der_decoder(
                                sb['bagValue'], asn1Spec=rfc7292.pkcs12BagTypeMap[sb['bagId']])

                            self.assertFalse(rest)

                            for bagattr in sb['bagAttributes']:
                                if bagattr['attrType'] in openTypesMap:
                                    inav, rest = der_decoder(
                                        bagattr['attrValues'][0], asn1Spec=openTypesMap[bagattr['attrType']])

                                    self.assertFalse(rest)

                                    if bagattr['attrType'] == rfc2985.pkcs_9_at_friendlyName:
                                        self.assertEqual( "3f71af65-1687-444a-9f46-c8be194c3e8e", inav)

                                    if bagattr['attrType'] == rfc2985.pkcs_9_at_localKeyId:
                                        self.assertEqual(univ.OctetString(hexValue='01000000'), inav)

            if attr['type'] == rfc2985.pkcs_9_at_pkcs7PDU:
                ci, rest = der_decoder(
                    attr['values'][0], asn1Spec=rfc5652.ContentInfo())

                self.assertFalse(rest)
                self.assertEqual(rfc5652.id_signedData, ci['contentType'])

                sd, rest = der_decoder(
                    ci['content'], asn1Spec=rfc5652.SignedData())

                self.assertFalse(rest)
                self.assertEqual(1, sd['version'])
        
                for si in sd['signerInfos']:
                    self.assertEqual(1, si['version'])

                    for siattr in si['signedAttrs']:
                        if siattr['attrType'] in openTypesMap:
                            siav, rest = der_decoder(
                                siattr['attrValues'][0], asn1Spec=openTypesMap[siattr['attrType']])

                            self.assertFalse(rest)

                            if siattr['attrType'] == rfc2985.pkcs_9_at_contentType:
                                self.assertEqual(rfc5652.id_data, siav)

                            if siattr['attrType'] == rfc2985.pkcs_9_at_messageDigest:
                                self.assertEqual('b6e422a4', siav.prettyPrint()[2:10])

                            if siattr['attrType'] == rfc2985.pkcs_9_at_signingTime:
                                self.assertEqual('190529182319Z', siav['utcTime'])

                for choices in sd['certificates']:
                    for rdn in choices[0]['tbsCertificate']['subject']['rdnSequence']:
                        if rdn[0]['type'] in openTypesMap:
                            nv, rest = der_decoder(
                                rdn[0]['value'], asn1Spec=openTypesMap[rdn[0]['type']])
                            self.assertFalse(rest)

                            if rdn[0]['type'] == rfc2985.pkcs_9_at_emailAddress:
                                self.assertEqual('alice@example.com', nv)

    def testOpenTypes(self):
        openTypesMap = {
            rfc2985.pkcs_9_at_smimeCapabilities: rfc2985.SMIMECapabilities(),
        }
        openTypesMap.update(rfc5280.certificateAttributesMap)
        openTypesMap.update(rfc5652.cmsAttributesMap)

        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec,
            openTypes=openTypesMap, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        for attr in asn1Object:
            self.assertTrue(attr['type'], openTypesMap)

            if attr['type'] == rfc2985.pkcs_9_at_userPKCS12:

                self.assertEqual(univ.Integer(3), attr['values'][0]['version'])
                self.assertEqual(rfc5652.id_data, attr['values'][0]['authSafe']['contentType'])

                authsafe, rest = der_decoder(
                    attr['values'][0]['authSafe']['content'],
                    asn1Spec=rfc7292.AuthenticatedSafe())

                self.assertFalse(rest)

                for ci in authsafe:
                    self.assertEqual(rfc5652.id_data, ci['contentType'])

                    indata, rest = der_decoder(
                        ci['content'], asn1Spec=univ.OctetString())

                    self.assertFalse(rest)

                    sc, rest = der_decoder(
                        indata, asn1Spec=rfc7292.SafeContents(), decodeOpenTypes=True)

                    self.assertFalse(rest)

                    for sb in sc:
                        if sb['bagId'] in rfc7292.pkcs12BagTypeMap:
                            for bagattr in sb['bagAttributes']:
                                if bagattr['attrType'] in openTypesMap:

                                    if bagattr['attrType'] == rfc2985.pkcs_9_at_friendlyName:
                                        self.assertEqual(
                                            "3f71af65-1687-444a-9f46-c8be194c3e8e",
                                            bagattr['attrValues'][0])

                                    if bagattr['attrType'] == rfc2985.pkcs_9_at_localKeyId:
                                        self.assertEqual(
                                            univ.OctetString(hexValue='01000000'),
                                            bagattr['attrValues'][0])

            if attr['type'] == rfc2985.pkcs_9_at_pkcs7PDU:
                self.assertEqual(rfc5652.id_signedData, attr['values'][0]['contentType'])
                self.assertEqual(1, attr['values'][0]['content']['version'])
        
                for si in attr['values'][0]['content']['signerInfos']:
                    self.assertEqual(1, si['version'])

                    for siattr in si['signedAttrs']:
                        if siattr['attrType'] in openTypesMap:
 
                            if siattr['attrType'] == rfc2985.pkcs_9_at_contentType:
                                self.assertEqual(rfc5652.id_data, siattr['attrValues'][0])

                            if siattr['attrType'] == rfc2985.pkcs_9_at_messageDigest:
                                self.assertEqual('b6e422a4', siattr['attrValues'][0].prettyPrint()[2:10])

                            if siattr['attrType'] == rfc2985.pkcs_9_at_signingTime:
                                self.assertEqual('190529182319Z', siattr['attrValues'][0]['utcTime'])

                for choices in attr['values'][0]['content']['certificates']:
                    for rdn in choices[0]['tbsCertificate']['subject']['rdnSequence']:
                        if rdn[0]['type'] in openTypesMap:
                            if rdn[0]['type'] == rfc2985.pkcs_9_at_emailAddress:
                                self.assertEqual('alice@example.com', rdn[0]['value'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
