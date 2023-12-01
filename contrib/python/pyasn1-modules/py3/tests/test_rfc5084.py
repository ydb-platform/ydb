#
# This file is part of pyasn1-modules software.
#
# Created by Russ Housley
# Copyright (c) 2018, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder

from pyasn1_modules import pem
from pyasn1_modules import rfc5083
from pyasn1_modules import rfc5084
from pyasn1_modules import rfc5652


class CCMParametersTestCase(unittest.TestCase):
    ccm_pem_text = "MBEEDE2HVyIurFKUEX8MEgIBBA=="

    def setUp(self):
        self.asn1Spec = rfc5084.CCMParameters()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.ccm_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))


class GCMParametersTestCase(unittest.TestCase):
    gcm_pem_text = "MBEEDE2HVyIurFKUEX8MEgIBEA=="

    def setUp(self):
        self.asn1Spec = rfc5084.GCMParameters()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.gcm_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))


class GCMOpenTypesTestCase(unittest.TestCase):
    rfc8591_pem_pext = """\
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
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.rfc8591_pem_pext)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(
            rfc5083.id_ct_authEnvelopedData, asn1Object['contentType'])

        aed, rest = der_decoder(
            asn1Object['content'], asn1Spec=rfc5083.AuthEnvelopedData(),
            decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(aed.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(aed))
        self.assertEqual(0, aed['version'])

        cea = aed['authEncryptedContentInfo']['contentEncryptionAlgorithm']

        self.assertEqual(rfc5084.id_aes128_GCM, cea['algorithm'])
        self.assertEqual(16, cea['parameters']['aes-ICVlen'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
