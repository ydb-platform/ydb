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
from pyasn1_modules import rfc5636


class TraceableAnonymousCertificateTestCase(unittest.TestCase):
    pem_text = """\
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
"""

    def testDerCodec(self):

        substrate = pem.readBase64fromText(self.pem_text)

        layers = { }
        layers.update(rfc5652.cmsContentTypesMap)

        getNextLayer = {
            rfc5652.id_ct_contentInfo: lambda x: x['contentType'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContentType'],
            rfc5636.id_kisa_tac_token: lambda x: None
        }

        getNextSubstrate = {
            rfc5652.id_ct_contentInfo: lambda x: x['content'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContent'],
            rfc5636.id_kisa_tac_token: lambda x: None
        }

        next_layer = rfc5652.id_ct_contentInfo
        while next_layer:
            asn1Object, rest = der_decoder(
                substrate, asn1Spec=layers[next_layer])

            self.assertFalse(rest)
            self.assertTrue(asn1Object.prettyPrint())
            self.assertEqual(substrate, der_encoder(asn1Object))

            substrate = getNextSubstrate[next_layer](asn1Object)
            next_layer = getNextLayer[next_layer](asn1Object)

        self.assertEqual('2019', asn1Object['timeout'][:4])
        self.assertEqual('5dcdf44e', asn1Object['userKey'].prettyPrint()[-8:])

    def testOpenTypes(self):
        asn1Spec=rfc5652.ContentInfo()
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        substrate = asn1Object['content']['encapContentInfo']['eContent']
        oid = asn1Object['content']['encapContentInfo']['eContentType']
        self.assertIn(oid, rfc5652.cmsContentTypesMap)

        tac_token, rest = der_decoder(
            substrate,
            asn1Spec=rfc5652.cmsContentTypesMap[oid],
            decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(tac_token.prettyPrint())
        self.assertEqual(substrate, der_encoder(tac_token))

        self.assertEqual('2019', tac_token['timeout'][:4])
        self.assertEqual('5dcdf44e', tac_token['userKey'].prettyPrint()[-8:])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
