#
# This file is part of pyasn1-modules software.
#
# Created by Russ Housley
# Copyright (c) 2019, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#

import sys

from pyasn1.type import univ

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder

from pyasn1_modules import pem
from pyasn1_modules import rfc2876
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5751

try:
    import unittest2 as unittest
except ImportError:
    import unittest


class EnvelopedDataTestCase(unittest.TestCase):
    env_data_pem_text = """\
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
z10epK+S
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.env_data_pem_text)
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

        kari_kea = ed['recipientInfos'][0]['kari']['keyEncryptionAlgorithm']
        self.assertEqual(rfc2876.id_kEAKeyEncryptionAlgorithm, kari_kea['algorithm'])
        kwa, rest = der_decoder(
            kari_kea['parameters'], asn1Spec=rfc5280.AlgorithmIdentifier())
        self.assertFalse(rest)
        self.assertTrue(kwa.prettyPrint())
        self.assertEqual(kari_kea['parameters'], der_encoder(kwa))
        self.assertEqual(rfc2876.id_fortezzaWrap80, kwa['algorithm'])

        cea = ed['encryptedContentInfo']['contentEncryptionAlgorithm']
        self.assertEqual(rfc2876.id_fortezzaConfidentialityAlgorithm, cea['algorithm'])
        param, rest = der_decoder(cea['parameters'], rfc2876.Skipjack_Parm())
        self.assertFalse(rest)
        self.assertTrue(param.prettyPrint())
        self.assertEqual(cea['parameters'], der_encoder(param))

        iv = univ.OctetString(hexValue='424f4755535f4956')
        self.assertEqual(iv, param['initialization-vector'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.env_data_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        
        self.assertIn(asn1Object['contentType'], rfc5652.cmsContentTypesMap.keys())
        kari_kea = asn1Object['content']['recipientInfos'][0]['kari']['keyEncryptionAlgorithm']
        self.assertEqual(rfc2876.id_kEAKeyEncryptionAlgorithm, kari_kea['algorithm'])
        self.assertEqual(rfc2876.id_fortezzaWrap80, kari_kea['parameters']['algorithm'])

        cea = asn1Object['content']['encryptedContentInfo']['contentEncryptionAlgorithm']
        self.assertEqual(rfc2876.id_fortezzaConfidentialityAlgorithm, cea['algorithm'])

        iv = univ.OctetString(hexValue='424f4755535f4956')
        self.assertEqual(iv, cea['parameters']['initialization-vector'])

class SMIMECapabilitiesTestCase(unittest.TestCase):
    smime_capabilities_pem_text = "\
MCcwGAYJYIZIAWUCAQEYMAsGCWCGSAFlAgEBFzALBglghkgBZQIBAQQ="

    def setUp(self):
        self.asn1Spec = rfc5751.SMIMECapabilities()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        found_wrap_alg = False
        for cap in asn1Object:
            if cap['capabilityID'] in rfc5751.smimeCapabilityMap.keys():
                if cap['parameters'].hasValue():
                    param, rest = der_decoder(
                        cap['parameters'],
                        asn1Spec=rfc5751.smimeCapabilityMap[cap['capabilityID']])
                    self.assertFalse(rest)
                    self.assertTrue(param.prettyPrint())
                    self.assertEqual(cap['parameters'], der_encoder(param))

                    if cap['capabilityID'] == rfc2876.id_kEAKeyEncryptionAlgorithm:
                        self.assertEqual(rfc2876.id_fortezzaWrap80, param['algorithm'])
                        found_wrap_alg = True

        self.assertTrue(found_wrap_alg)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        found_wrap_alg = False
        for cap in asn1Object:
            if cap['capabilityID'] == rfc2876.id_kEAKeyEncryptionAlgorithm:
                self.assertEqual(rfc2876.id_fortezzaWrap80, cap['parameters']['algorithm'])
                found_wrap_alg = True

        self.assertTrue(found_wrap_alg)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
