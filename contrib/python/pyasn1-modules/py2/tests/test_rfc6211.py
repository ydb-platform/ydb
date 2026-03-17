#
# This file is part of pyasn1-modules software.
#
# Created by Russ Housley
# Copyright (c) 2019, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.type import univ

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder
from pyasn1_modules import pem
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc6211


class SignedMessageTestCase(unittest.TestCase):
    signed_message_pem_text = """\
MIIEyAYJKoZIhvcNAQcCoIIEuTCCBLUCAQExDTALBglghkgBZQMEAgIwUQYJKoZI
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
rDIrQpi0IDh+v0QSAv9rMife8tClafXWtDwwL8MS7oAh0ymT446Uizxx3PUxggHM
MIIByAIBATBMMD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwH
SGVybmRvbjERMA8GA1UECgwIQm9ndXMgQ0ECCQCls1QoG7BuOzALBglghkgBZQME
AgKggfIwGAYJKoZIhvcNAQkDMQsGCSqGSIb3DQEHATAcBgkqhkiG9w0BCQUxDxcN
MTkwNTI5MTgyMzE5WjAoBgkqhkiG9w0BCTQxGzAZMAsGCWCGSAFlAwQCAqEKBggq
hkjOPQQDAzA/BgkqhkiG9w0BCQQxMgQwtuQipP2CZx7U96rGbUT06LC5jVFYccZW
5/CaNvpcrOPiChDm2vI3m4k300z5mSZsME0GCyqGSIb3DQEJEAIBMT4wPAQgx08h
D2QnVwj1DoeRELNtdZ0PffW4BQIvcwwVc/goU6OAAQEwFTATgRFhbGljZUBleGFt
cGxlLmNvbTAKBggqhkjOPQQDAwRnMGUCMQChIMyN1nTN+LLQcYJuhWT297vSKMDK
fIUedSwWYrcSnSa1pq2s3Wue+pNBfecEjYECMGrUNu1UpWdafEJulP9Vz76qOPMa
5V/AnTEV5zkmzRle8sffN+nQ+SGkoos5zpI1kA==
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

        sd, rest = der_decoder(
            asn1Object['content'], asn1Spec=rfc5652.SignedData())

        self.assertFalse(rest)
        self.assertTrue(sd.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(sd))
       
        for sa in sd['signerInfos'][0]['signedAttrs']:
            sat = sa['attrType']
            sav0 = sa['attrValues'][0]

            if sat in rfc6211.id_aa_cmsAlgorithmProtect:
                sav, rest = der_decoder(
                    sav0, asn1Spec=rfc6211.CMSAlgorithmProtection())

                self.assertFalse(rest)
                self.assertTrue(sav.prettyPrint())
                self.assertEqual(sav0, der_encoder(sav))

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.signed_message_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertIn(asn1Object['contentType'], rfc5652.cmsContentTypesMap)
        self.assertEqual(rfc5652.id_signedData, asn1Object['contentType'])

        sd = asn1Object['content']

        self.assertEqual(
            rfc5652.CMSVersion().subtype(value='v1'), sd['version'])

        ect = sd['encapContentInfo']['eContentType']

        self.assertIn(ect, rfc5652.cmsContentTypesMap)
        self.assertEqual(rfc5652.id_data, ect)

        for sa in sd['signerInfos'][0]['signedAttrs']:
            if sa['attrType'] == rfc6211.id_aa_cmsAlgorithmProtect:
                self.assertIn(sa['attrType'], rfc5652.cmsAttributesMap)
                
                sav0 = sa['attrValues'][0]
                digest_oid = univ.ObjectIdentifier('2.16.840.1.101.3.4.2.2')
                sig_oid = univ.ObjectIdentifier('1.2.840.10045.4.3.3')

                self.assertEqual(
                    digest_oid, sav0['digestAlgorithm']['algorithm'])
                self.assertEqual(
                    sig_oid, sav0['signatureAlgorithm']['algorithm'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
