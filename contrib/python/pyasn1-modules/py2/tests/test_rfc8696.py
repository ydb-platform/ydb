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
from pyasn1_modules import rfc5083
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc8696


class KeyTransPSKTestCase(unittest.TestCase):
    key_trans_psk_pem_text = """\
MIICigYLKoZIhvcNAQkQARegggJ5MIICdQIBADGCAiekggIjBgsqhkiG9w0BCRANATCCAhIC
AQAEE3B0Zi1rbWM6MTM2MTQxMjIxMTIwDQYLKoZIhvcNAQkQAx0wCwYJYIZIAWUDBAEtMIIB
sDCCAawCAQKAFJ7rZ8m5WnTUTS8WOWaA6AG1y6ScMA0GCSqGSIb3DQEBAQUABIIBgKo/Hkhu
eoOdn1/cIEpt38NbEEdSC586IWcG+0l+ND9pcmQvvKvscpvFFVAjqLjvoXGatmSazr2Q4BVS
yWKm0JqlyVWEAhRsU7wNlD7zRAKI8+obWpU57gjEKs13D8gb1PI2YPZWajN1Ye+yHSF6h+fb
7YtaQepxTGHYF0LgHaAC8cqtgwIRW8N4Gnvl0Uuz+YEZXUX0I8fvJG6MKCEFzwHvfrfPb3rW
B8k7BHfekRpY+793JNrjSP2lY+W0fhqBN8dALDKGqlbUCyojMQkQiD/iXSBRbZWiJ1CE92iT
x7Ji9irq8rhYDNoDP2vghJUaepoZgIJwPWqhoTH+KRPqHTjLnnbi/TGzEdeO5h0C9Gc0DVzs
9OHvHknQ7mSxPT9xKMXGztVT+P3a9ct6TaMotpMqL9cuZxTYGpHMYNkLSUXFSadAGFrgP7QV
FGwC/Z/YomEzSLPgZi8HnVHsAGkJzXxmM/PJBu4dAXcKjEv/GgpmaS2B7gKHUpTyyAgdsBsy
2AQo6glHJQ+mbNUlWV5Sppqq3ojvzxsPEIq+KRBgORsc31kH82tAZ+RTQjA3BgkqhkiG9w0B
BwEwGwYJYIZIAWUDBAEuMA4EDMr+ur76ztut3sr4iIANmvLRbyFUf87+2bPvLQQMoOWSXMGE
4BckY8RM
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.key_trans_psk_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(
            rfc5083.id_ct_authEnvelopedData, asn1Object['contentType'])

        aed, rest = der_decoder(
            asn1Object['content'],
            asn1Spec=rfc5083.AuthEnvelopedData())

        self.assertFalse(rest)
        self.assertTrue(aed.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(aed))
        self.assertEqual(0, aed['version'])

        ri = aed['recipientInfos'][0]
        self.assertEqual(rfc8696.id_ori_keyTransPSK, ri['ori']['oriType'])

        ktpsk, rest = der_decoder(
            ri['ori']['oriValue'],
            asn1Spec=rfc8696.KeyTransPSKRecipientInfo())

        self.assertFalse(rest)
        self.assertTrue(ktpsk.prettyPrint())
        self.assertEqual(ri['ori']['oriValue'], der_encoder(ktpsk))
        self.assertEqual(0, ktpsk['version'])

        ktri = ktpsk['ktris'][0]
        self.assertEqual(2, ktri['version'])

    def testOtherRecipientInfoMap(self):
        substrate = pem.readBase64fromText(self.key_trans_psk_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(
            rfc5083.id_ct_authEnvelopedData, asn1Object['contentType'])

        aed, rest = der_decoder(
            asn1Object['content'],
            asn1Spec=rfc5083.AuthEnvelopedData())

        self.assertFalse(rest)
        self.assertTrue(aed.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(aed)) 
        self.assertEqual(0, aed['version'])

        ri = aed['recipientInfos'][0]
        self.assertIn(ri['ori']['oriType'], rfc5652.otherRecipientInfoMap)

        ori, rest = der_decoder(
            ri['ori']['oriValue'],
            asn1Spec=rfc5652.otherRecipientInfoMap[ri['ori']['oriType']])

        self.assertFalse(rest)
        self.assertTrue(ori.prettyPrint())
        self.assertEqual(ri['ori']['oriValue'], der_encoder(ori))

class KeyAgreePSKTestCase(unittest.TestCase):
    key_agree_psk_pem_text = """\
MIIBRwYLKoZIhvcNAQkQARegggE2MIIBMgIBADGB5aSB4gYLKoZIhvcNAQkQDQIwgdICAQAE
FHB0Zi1rbWM6MjE2ODQwMTEwMTIxoFWhUzATBgYrgQQBCwEGCWCGSAFlAwQBLQM8AAQ5G0Em
Jk/2ks8sXY1kzbuG3Uu3ttWwQRXALFDJICjvYfr+yTpOQVkchm88FAh9MEkw4NKctokKNgps
MA0GCyqGSIb3DQEJEAMdMAsGCWCGSAFlAwQBLTBEMEKgFgQU6CGLmLi32Gtenr3IrrjE7NwF
xSkEKCKf4LReQAA+fYJE7Bt+f/ssjcoWw29XNyIlU6cSY6kr3giGamAtY/QwNwYJKoZIhvcN
AQcBMBsGCWCGSAFlAwQBLjAOBAzbrd7K+IjK/rq++s6ADfxtb4I+PtLSCdDG/88EDFUCYMQu
WylxlCbB/w==
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.key_agree_psk_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(
            rfc5083.id_ct_authEnvelopedData, asn1Object['contentType'])

        aed, rest = der_decoder(
            asn1Object['content'],
            asn1Spec=rfc5083.AuthEnvelopedData())

        self.assertFalse(rest)
        self.assertTrue(aed.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(aed))
        self.assertEqual(0, aed['version'])

        ri = aed['recipientInfos'][0]
        self.assertEqual(rfc8696.id_ori_keyAgreePSK, ri['ori']['oriType'])

        kapsk, rest = der_decoder(
            ri['ori']['oriValue'],
            asn1Spec=rfc8696.KeyAgreePSKRecipientInfo())

        self.assertFalse(rest)
        self.assertTrue(kapsk.prettyPrint())
        self.assertEqual(ri['ori']['oriValue'], der_encoder(kapsk))
        self.assertEqual(0, kapsk['version'])

        rek = kapsk['recipientEncryptedKeys'][0]
        ski = rek['rid']['rKeyId']['subjectKeyIdentifier']
        expected_ski = univ.OctetString(
            hexValue='e8218b98b8b7d86b5e9ebdc8aeb8c4ecdc05c529')

        self.assertEqual(expected_ski, ski)

    def testOtherRecipientInfoMap(self):
        substrate = pem.readBase64fromText(self.key_agree_psk_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(
            rfc5083.id_ct_authEnvelopedData, asn1Object['contentType'])

        aed, rest = der_decoder(
            asn1Object['content'],
            asn1Spec=rfc5083.AuthEnvelopedData())

        self.assertFalse(rest)
        self.assertTrue(aed.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(aed))
        self.assertEqual(0, aed['version'])

        ri = aed['recipientInfos'][0]
        self.assertIn(ri['ori']['oriType'], rfc5652.otherRecipientInfoMap)

        ori, rest = der_decoder(
            ri['ori']['oriValue'],
            asn1Spec=rfc5652.otherRecipientInfoMap[ri['ori']['oriType']])

        self.assertFalse(rest)
        self.assertTrue(ori.prettyPrint())
        self.assertEqual(ri['ori']['oriValue'], der_encoder(ori))


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
