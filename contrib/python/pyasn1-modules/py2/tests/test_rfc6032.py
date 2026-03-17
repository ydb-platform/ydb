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
from pyasn1.compat.octets import str2octs

from pyasn1_modules import pem
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc6032


class EncryptedKeyPkgTestCase(unittest.TestCase):
    encrypted_key_pkg_pem_text = """\
MIIBBwYKYIZIAWUCAQJOAqCB+DCB9QIBAjCBzgYKYIZIAWUCAQJOAjAdBglghkgB
ZQMEASoEEN6HFteHMZ3DyeO35xIwWQOAgaCKTs0D0HguNzMhsLgiwG/Kw8OwX+GF
9/cZ1YVNesUTW/VsbXJcbTmFmWyfqZsM4DLBegIbrUEHQZnQRq6/NO4ricQdHApD
B/ip6RRqeN1yxMJLv1YN0zUOOIDBS2iMEjTLXZLWw3w22GN2JK7G+Lr4OH1NhMgU
ILJyh/RePmPseMwxvcJs7liEfkiSNMtDfEcpjtzA9bDe95GjhQRsiSByoR8wHQYJ
YIZIAWUCAQVCMRAEDnB0Zi1rZGMtODEyMzc0
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.encrypted_key_pkg_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(
            rfc6032.id_ct_KP_encryptedKeyPkg, asn1Object['contentType'])

        content, rest = der_decoder(
            asn1Object['content'], rfc6032.EncryptedKeyPackage())

        self.assertFalse(rest)
        self.assertTrue(content.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(content))
        self.assertEqual('encrypted', content.getName())

        eci = content['encrypted']['encryptedContentInfo']

        self.assertEqual(
            rfc6032.id_ct_KP_encryptedKeyPkg, eci['contentType'])

        attrType = content['encrypted']['unprotectedAttrs'][0]['attrType']

        self.assertEqual(rfc6032.id_aa_KP_contentDecryptKeyID, attrType)

        attrVal0 = content['encrypted']['unprotectedAttrs'][0]['attrValues'][0]
        keyid, rest = der_decoder(attrVal0, rfc6032.ContentDecryptKeyID())

        self.assertFalse(rest)
        self.assertTrue(keyid.prettyPrint())
        self.assertEqual(attrVal0, der_encoder(keyid))
        self.assertEqual(str2octs('ptf-kdc-812374'), keyid)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.encrypted_key_pkg_pem_text)
        asn1Object, rest = der_decoder(substrate,
                                       asn1Spec=self.asn1Spec,
                                       decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertIn(asn1Object['contentType'], rfc5652.cmsContentTypesMap)

        eci = asn1Object['content']['encrypted']['encryptedContentInfo']

        self.assertIn(eci['contentType'], rfc5652.cmsContentTypesMap)

        for attr in asn1Object['content']['encrypted']['unprotectedAttrs']:
            self.assertIn(attr['attrType'], rfc5652.cmsAttributesMap)
            self.assertNotEqual('0x', attr['attrValues'][0].prettyPrint()[:2])

            if attr['attrType'] == rfc6032.id_aa_KP_contentDecryptKeyID:
                self.assertEqual(str2octs(
                    'ptf-kdc-812374'), attr['attrValues'][0])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
