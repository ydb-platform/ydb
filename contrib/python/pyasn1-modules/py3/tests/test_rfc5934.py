#
# This file is part of pyasn1-modules software.
#
# Created by Russ Housley
# Acknowledgement to Carl Wallace for the test messages.
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
from pyasn1_modules import rfc5934


class TAMPStatusResponseTestCase(unittest.TestCase):
    tsr_pem_text = """\
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
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.tsr_pem_text)

        layers = {
            rfc5652.id_ct_contentInfo: rfc5652.ContentInfo(),
            rfc5652.id_signedData: rfc5652.SignedData(),
            rfc5934.id_ct_TAMP_statusResponse: rfc5934.TAMPStatusResponse()
        }

        getNextLayer = {
            rfc5652.id_ct_contentInfo: lambda x: x['contentType'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContentType'],
            rfc5934.id_ct_TAMP_statusResponse: lambda x: None
        }

        getNextSubstrate = {
            rfc5652.id_ct_contentInfo: lambda x: x['content'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContent'],
            rfc5934.id_ct_TAMP_statusResponse: lambda x: None
        }

        next_layer = rfc5652.id_ct_contentInfo

        while next_layer:
            asn1Object, rest = der_decoder(substrate, asn1Spec=layers[next_layer])
            self.assertFalse(rest)
            self.assertTrue(asn1Object.prettyPrint())
            self.assertEqual(substrate, der_encoder(asn1Object))

            substrate = getNextSubstrate[next_layer](asn1Object)
            next_layer = getNextLayer[next_layer](asn1Object)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.tsr_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=rfc5652.ContentInfo(), decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        eci = asn1Object['content']['encapContentInfo']

        self.assertIn(eci['eContentType'], rfc5652.cmsContentTypesMap)
        self.assertEqual(rfc5934.id_ct_TAMP_statusResponse, eci['eContentType'])

        tsr, rest = der_decoder(
            eci['eContent'],
            asn1Spec=rfc5652.cmsContentTypesMap[eci['eContentType']],
            decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(tsr.prettyPrint())
        self.assertEqual(eci['eContent'], der_encoder(tsr))
        self.assertEqual(2, tsr['version'])
        self.assertEqual(univ.Null(""), tsr['query']['target'])
        self.assertEqual(1568307071, tsr['query']['seqNum'])
        self.assertFalse(tsr['usesApex'])

        count = 0

        for tai in tsr['response']['verboseResponse']['taInfo']:
            count += 1
            self.assertEqual(1, tai['taInfo']['version'])

        self.assertEqual(3, count)


class TrustAnchorUpdateTestCase(unittest.TestCase):
    tau_pem_text = """\
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"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.tau_pem_text)

        layers = {
            rfc5652.id_ct_contentInfo: rfc5652.ContentInfo(),
            rfc5652.id_signedData: rfc5652.SignedData(),
            rfc5934.id_ct_TAMP_update: rfc5934.TAMPUpdate()
        }

        getNextLayer = {
            rfc5652.id_ct_contentInfo: lambda x: x['contentType'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContentType'],
            rfc5934.id_ct_TAMP_update: lambda x: None
        }

        getNextSubstrate = {
            rfc5652.id_ct_contentInfo: lambda x: x['content'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContent'],
            rfc5934.id_ct_TAMP_update: lambda x: None
        }

        next_layer = rfc5652.id_ct_contentInfo

        while next_layer:
            asn1Object, rest = der_decoder(substrate, asn1Spec=layers[next_layer])

            self.assertFalse(rest)
            self.assertTrue(asn1Object.prettyPrint())
            self.assertEqual(substrate, der_encoder(asn1Object))

            substrate = getNextSubstrate[next_layer](asn1Object)
            next_layer = getNextLayer[next_layer](asn1Object)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.tau_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=rfc5652.ContentInfo(),
            decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        eci = asn1Object['content']['encapContentInfo']
        self.assertIn(eci['eContentType'], rfc5652.cmsContentTypesMap)
        self.assertEqual(rfc5934.id_ct_TAMP_update, eci['eContentType'])

        tau, rest = der_decoder(
            eci['eContent'],
            asn1Spec=rfc5652.cmsContentTypesMap[eci['eContentType']],
            decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(tau.prettyPrint())
        self.assertEqual(eci['eContent'], der_encoder(tau))
        self.assertEqual(2, tau['version'])
        self.assertEqual(univ.Null(""), tau['msgRef']['target'])
        self.assertEqual(1568307088, tau['msgRef']['seqNum'])
        self.assertEqual(1, len(tau['updates']))


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
