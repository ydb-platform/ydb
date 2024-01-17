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
from pyasn1_modules import rfc3565
from pyasn1_modules import rfc5480
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5753


class EnvelopedDataTestCase(unittest.TestCase):
    pem_text = """\
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
ns9dHqSvkg==
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(rfc5652.id_envelopedData, asn1Object['contentType'])

        ed, rest = der_decoder(
            asn1Object['content'], asn1Spec=rfc5652.EnvelopedData())
        self.assertFalse(rest)
        self.assertTrue(ed.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(ed))

        opk_ai_p = rfc5480.ECParameters()
        opk_ai_p['namedCurve'] = rfc5480.secp384r1

        kwai = rfc5753.KeyWrapAlgorithm()
        kwai['algorithm'] = rfc3565.id_aes256_wrap

        ukm_found = False
        self.assertEqual(ed['version'], rfc5652.CMSVersion(value=2))
        for ri in ed['recipientInfos']:
            self.assertEqual(ri['kari']['version'], rfc5652.CMSVersion(value=3))
            opk_alg = ri['kari']['originator']['originatorKey']['algorithm']
            self.assertEqual(opk_alg['algorithm'], rfc5753.id_ecPublicKey)
            self.assertEqual(opk_alg['parameters'], der_encoder(opk_ai_p))
            kek_alg = ri['kari']['keyEncryptionAlgorithm']
            self.assertEqual(kek_alg['algorithm'], rfc5753.dhSinglePass_stdDH_sha384kdf_scheme)
            self.assertEqual(kek_alg['parameters'], der_encoder(kwai))
            ukm = ri['kari']['ukm']
            self.assertEqual(ukm, rfc5652.UserKeyingMaterial(hexValue='52464335373533'))
            ukm_found = True

        self.assertTrue(ukm_found)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        assert asn1Object['contentType'] == rfc5652.id_envelopedData
        ed = asn1Object['content']

        ukm_found = False
        self.assertEqual(ed['version'], rfc5652.CMSVersion(value=2))
        for ri in ed['recipientInfos']:
            self.assertEqual(ri['kari']['version'], rfc5652.CMSVersion(value=3))
            opk_alg = ri['kari']['originator']['originatorKey']['algorithm']
            self.assertEqual(opk_alg['algorithm'], rfc5753.id_ecPublicKey)
            self.assertEqual(opk_alg['parameters']['namedCurve'], rfc5480.secp384r1)
            kek_alg = ri['kari']['keyEncryptionAlgorithm']
            self.assertEqual(kek_alg['algorithm'], rfc5753.dhSinglePass_stdDH_sha384kdf_scheme)
            self.assertEqual(kek_alg['parameters']['algorithm'], rfc3565.id_aes256_wrap)
            ukm = ri['kari']['ukm']
            self.assertEqual(ukm, rfc5652.UserKeyingMaterial(hexValue='52464335373533'))
            ukm_found = True

        self.assertTrue(ukm_found)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
