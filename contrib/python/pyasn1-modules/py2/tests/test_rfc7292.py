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
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc7292


class PKCS12TestCase(unittest.TestCase):
    pfx_pem_text = """\
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
"""

    def setUp(self):
        self.asn1Spec = rfc7292.PFX()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pfx_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(3, asn1Object['version'])

        oid = asn1Object['macData']['mac']['digestAlgorithm']['algorithm']

        self.assertEqual(univ.ObjectIdentifier('1.3.14.3.2.26'), oid)

        md_hex = asn1Object['macData']['mac']['digest'].prettyPrint()

        self.assertEqual('0xa5608ffdf651d132b90701aeaaf4ddd3e76e88a7', md_hex)
        self.assertEqual(
            rfc5652.id_data, asn1Object['authSafe']['contentType'])

        data, rest = der_decoder(
            asn1Object['authSafe']['content'], asn1Spec=univ.OctetString())

        self.assertFalse(rest)

        authsafe, rest = der_decoder(data, asn1Spec=rfc7292.AuthenticatedSafe())

        self.assertFalse(rest)
        self.assertTrue(authsafe.prettyPrint())
        self.assertEqual(data, der_encoder(authsafe))

        for ci in authsafe:
            self.assertEqual(rfc5652.id_data, ci['contentType'])

            data, rest = der_decoder(ci['content'], asn1Spec=univ.OctetString())

            self.assertFalse(rest)

            sc, rest = der_decoder(data, asn1Spec=rfc7292.SafeContents())

            self.assertFalse(rest)
            self.assertTrue(sc.prettyPrint())
            self.assertEqual(data, der_encoder(sc))

            for sb in sc:
                if sb['bagId'] in rfc7292.pkcs12BagTypeMap:
                    bv, rest = der_decoder(
                        sb['bagValue'],
                        asn1Spec=rfc7292.pkcs12BagTypeMap[sb['bagId']])

                    self.assertFalse(rest)
                    self.assertTrue(bv.prettyPrint())
                    self.assertEqual(sb['bagValue'], der_encoder(bv))

                    for attr in sb['bagAttributes']:
                        if attr['attrType'] in rfc5652.cmsAttributesMap:
                            av, rest = der_decoder(
                                attr['attrValues'][0],
                                asn1Spec=rfc5652.cmsAttributesMap[attr['attrType']])
                            self.assertFalse(rest)
                            self.assertTrue(av.prettyPrint())
                            self.assertEqual(
                                attr['attrValues'][0], der_encoder(av))

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pfx_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        digest_alg = asn1Object['macData']['mac']['digestAlgorithm']

        self.assertFalse(digest_alg['parameters'].hasValue())

        authsafe, rest = der_decoder(
            asn1Object['authSafe']['content'],
            asn1Spec=rfc7292.AuthenticatedSafe(),
            decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(authsafe.prettyPrint())
        self.assertEqual(
            asn1Object['authSafe']['content'], der_encoder(authsafe))

        for ci in authsafe:
            self.assertEqual(rfc5652.id_data, ci['contentType'])
            sc, rest = der_decoder(
                ci['content'], asn1Spec=rfc7292.SafeContents(),
                decodeOpenTypes=True)

            self.assertFalse(rest)
            self.assertTrue(sc.prettyPrint())
            self.assertEqual(ci['content'], der_encoder(sc))

            for sb in sc:
                if sb['bagId'] == rfc7292.id_pkcs8ShroudedKeyBag:
                    bv = sb['bagValue']
                    enc_alg = bv['encryptionAlgorithm']['algorithm']
                    self.assertEqual(
                        rfc7292.pbeWithSHAAnd3_KeyTripleDES_CBC, enc_alg)
                    enc_alg_param = bv['encryptionAlgorithm']['parameters']
                    self.assertEqual(2000, enc_alg_param['iterations'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
