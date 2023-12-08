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
from pyasn1_modules import rfc4055
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5752


class MultipleSignaturesTestCase(unittest.TestCase):
    pem_text = """\
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
"""

    def setUp(self):
        self.asn1Spec = rfc5652.SignedAttributes()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)

        layers = { }
        layers.update(rfc5652.cmsContentTypesMap)

        getNextLayer = {
            rfc5652.id_ct_contentInfo: lambda x: x['contentType'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContentType'],
            rfc5652.id_data: lambda x: None
        }

        getNextSubstrate = {
            rfc5652.id_ct_contentInfo: lambda x: x['content'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContent'],
            rfc5652.id_data: lambda x: None
        }

        next_layer = rfc5652.id_ct_contentInfo
        while not next_layer == rfc5652.id_data:
            asn1Object, rest = der_decoder(
                substrate, asn1Spec=layers[next_layer])

            self.assertFalse(rest)
            self.assertTrue(asn1Object.prettyPrint())
            self.assertEqual(substrate, der_encoder(asn1Object))

            if next_layer == rfc5652.id_signedData:
                signerInfos = asn1Object['signerInfos']

            substrate = getNextSubstrate[next_layer](asn1Object)
            next_layer = getNextLayer[next_layer](asn1Object)

        found_mult_sig1 = False
        for attr in signerInfos[0]['signedAttrs']:
            if attr['attrType'] in rfc5652.cmsAttributesMap:
                av, rest = der_decoder(
                    attr['attrValues'][0],
                    asn1Spec=rfc5652.cmsAttributesMap[attr['attrType']])

                self.assertFalse(rest)
                self.assertTrue(av.prettyPrint())
                self.assertEqual(attr['attrValues'][0], der_encoder(av))

                if attr['attrType'] == rfc5752.id_aa_multipleSignatures:
                    self.assertEqual(
                        av['bodyHashAlg']['algorithm'], rfc4055.id_sha384)

                    self.assertEqual(
                        'dfaf6c0a',
                        av['signAttrsHash']['hash'].prettyPrint()[2:10])

                    found_mult_sig1 = True

        found_mult_sig2 = False
        for attr in signerInfos[1]['signedAttrs']:
            if attr['attrType'] in rfc5652.cmsAttributesMap:
                av, rest = der_decoder(
                    attr['attrValues'][0],
                    asn1Spec=rfc5652.cmsAttributesMap[attr['attrType']])

                self.assertFalse(rest)
                self.assertTrue(av.prettyPrint())
                self.assertEqual(attr['attrValues'][0], der_encoder(av))

                if attr['attrType'] == rfc5752.id_aa_multipleSignatures:
                    self.assertEqual(
                        av['bodyHashAlg']['algorithm'], rfc4055.id_sha256)

                    self.assertEqual(
                        '7329527d',
                        av['signAttrsHash']['hash'].prettyPrint()[2:10])

                    found_mult_sig2 = True

        self.assertTrue(found_mult_sig1)
        self.assertTrue(found_mult_sig2)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=rfc5652.ContentInfo(), decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        found_mult_sig1 = False
        for attr in asn1Object['content']['signerInfos'][0]['signedAttrs']:
            if attr['attrType'] == rfc5752.id_aa_multipleSignatures:
                av = attr['attrValues'][0]

                self.assertEqual(
                    av['bodyHashAlg']['algorithm'], rfc4055.id_sha384)

                self.assertEqual(
                    'dfaf6c0a',
                    av['signAttrsHash']['hash'].prettyPrint()[2:10])

                found_mult_sig1 = True

        found_mult_sig2 = False
        for attr in asn1Object['content']['signerInfos'][1]['signedAttrs']:
            if attr['attrType'] == rfc5752.id_aa_multipleSignatures:
                av = attr['attrValues'][0]

                self.assertEqual(
                    av['bodyHashAlg']['algorithm'], rfc4055.id_sha256)

                self.assertEqual(
                    '7329527d',
                    av['signAttrsHash']['hash'].prettyPrint()[2:10])

                found_mult_sig2 = True

        self.assertTrue(found_mult_sig1)
        self.assertTrue(found_mult_sig2)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    import sys

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
