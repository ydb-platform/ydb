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
from pyasn1_modules import rfc7508


class SignedMessageTestCase(unittest.TestCase):
    signed_message_pem_text = """\
MIIE/AYJKoZIhvcNAQcCoIIE7TCCBOkCAQExDTALBglghkgBZQMEAgIwUQYJKoZI
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
rDIrQpi0IDh+v0QSAv9rMife8tClafXWtDwwL8MS7oAh0ymT446Uizxx3PUxggIA
MIIB/AIBATBMMD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwH
SGVybmRvbjERMA8GA1UECgwIQm9ndXMgQ0ECCQCls1QoG7BuOzALBglghkgBZQME
AgKgggElMBgGCSqGSIb3DQEJAzELBgkqhkiG9w0BBwEwHAYJKoZIhvcNAQkFMQ8X
DTE5MDUyOTE4MjMxOVowKAYJKoZIhvcNAQk0MRswGTALBglghkgBZQMEAgKhCgYI
KoZIzj0EAwMwMQYLKoZIhvcNAQkQAjcxIjEgCgEBMBswGRoERnJvbQwRYWxpY2VA
ZXhhbXBsZS5jb20wPwYJKoZIhvcNAQkEMTIEMLbkIqT9gmce1Peqxm1E9OiwuY1R
WHHGVufwmjb6XKzj4goQ5tryN5uJN9NM+ZkmbDBNBgsqhkiG9w0BCRACATE+MDwE
IMdPIQ9kJ1cI9Q6HkRCzbXWdD331uAUCL3MMFXP4KFOjgAEBMBUwE4ERYWxpY2VA
ZXhhbXBsZS5jb20wCgYIKoZIzj0EAwMEZzBlAjEAuZ8SebvwMRvLPn9+s3VHFUNU
bEtkkWCao1uNm5TOzphK0NbxzOsD854aC5ReKPSDAjAm1U0siLQw5p4qzGwyxDw9
5AI5J8Mvy+icNubmfsd4ofvxdaECdhr4rvsSMwbOsFk=
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.signed_message_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        secure_header_field_attr_found = False

        self.assertEqual(rfc5652.id_signedData, asn1Object['contentType'])

        sd, rest = der_decoder(
            asn1Object['content'], asn1Spec=rfc5652.SignedData())

        for sa in sd['signerInfos'][0]['signedAttrs']:
            sat = sa['attrType']
            sav0 = sa['attrValues'][0]

            if sat == rfc7508.id_aa_secureHeaderFieldsIdentifier:
                self.assertIn(sat, rfc5652.cmsAttributesMap)
                sav, rest = der_decoder(
                    sav0, asn1Spec=rfc5652.cmsAttributesMap[sat])

                self.assertFalse(rest)
                self.assertTrue(sav.prettyPrint())
                self.assertEqual(sav0, der_encoder(sav))

                from_field = rfc7508.HeaderFieldName('From')
                alice_email = rfc7508.HeaderFieldValue('alice@example.com')
                for shf in sav['secHeaderFields']:
                    if shf['field-Name'] == from_field:
                        self.assertEqual(alice_email, shf['field-Value'])
                        secure_header_field_attr_found = True

        self.assertTrue(secure_header_field_attr_found)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.signed_message_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertIn(asn1Object['contentType'], rfc5652.cmsContentTypesMap)
        self.assertEqual(asn1Object['contentType'], rfc5652.id_signedData)

        sd = asn1Object['content']

        self.assertEqual(
            rfc5652.CMSVersion().subtype(value='v1'), sd['version'])

        ect = sd['encapContentInfo']['eContentType']

        self.assertIn(ect, rfc5652.cmsContentTypesMap)
        self.assertEqual(rfc5652.id_data, ect)

        for sa in sd['signerInfos'][0]['signedAttrs']:
            if sa['attrType'] == rfc7508.id_aa_secureHeaderFieldsIdentifier:
                self.assertIn(sa['attrType'], rfc5652.cmsAttributesMap)

                secure_header_field_attr_found = False
                for sa in sd['signerInfos'][0]['signedAttrs']:
                    if sa['attrType'] == rfc7508.id_aa_secureHeaderFieldsIdentifier:
                        self.assertIn(sa['attrType'], rfc5652.cmsAttributesMap)
                        from_field = rfc7508.HeaderFieldName('From')
                        alice_email = rfc7508.HeaderFieldValue('alice@example.com')
                        for shf in sa['attrValues'][0]['secHeaderFields']:
                            if shf['field-Name'] == from_field:
                                self.assertEqual(alice_email, shf['field-Value'])
                                secure_header_field_attr_found = True

                self.assertTrue(secure_header_field_attr_found)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
