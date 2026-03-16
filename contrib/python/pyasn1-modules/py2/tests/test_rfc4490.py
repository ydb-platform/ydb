#
# This file is part of pyasn1-modules software.
#
# Copyright (c) 2019, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.type import univ

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder

from pyasn1_modules import pem
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc4357
from pyasn1_modules import rfc4490


class SignedTestCase(unittest.TestCase):
    signed_pem_text = """\
MIIBKAYJKoZIhvcNAQcCoIIBGTCCARUCAQExDDAKBgYqhQMCAgkFADAbBgkqhkiG
9w0BBwGgDgQMc2FtcGxlIHRleHQKMYHkMIHhAgEBMIGBMG0xHzAdBgNVBAMMFkdv
c3RSMzQxMC0yMDAxIGV4YW1wbGUxEjAQBgNVBAoMCUNyeXB0b1BybzELMAkGA1UE
BhMCUlUxKTAnBgkqhkiG9w0BCQEWGkdvc3RSMzQxMC0yMDAxQGV4YW1wbGUuY29t
AhAr9cYewhG9F8fc1GJmtC4hMAoGBiqFAwICCQUAMAoGBiqFAwICEwUABEDAw0LZ
P4/+JRERiHe/icPbg0IE1iD5aCqZ9v4wO+T0yPjVtNr74caRZzQfvKZ6DRJ7/RAl
xlHbjbL0jHF+7XKp
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.signed_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(rfc5652.id_signedData, asn1Object['contentType'])
        
        sd, rest = der_decoder(
            asn1Object['content'], asn1Spec=rfc5652.SignedData())
        self.assertFalse(rest)
        self.assertTrue(sd.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(sd))

        encoded_null = der_encoder(univ.Null(""))

        si = sd['signerInfos'][0]
        self.assertEqual(rfc4357.id_GostR3411_94, si['digestAlgorithm']['algorithm'])
        self.assertEqual(encoded_null, si['digestAlgorithm']['parameters'])

        self.assertEqual(rfc4357.id_GostR3410_2001, si['signatureAlgorithm']['algorithm'])
        self.assertEqual(encoded_null, si['signatureAlgorithm']['parameters'])

        sig = rfc4490.GostR3410_2001_Signature()
        sig = si['signature']
        self.assertEqual(64, len(sig))

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.signed_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(rfc5652.id_signedData, asn1Object['contentType'])

        si = asn1Object['content']['signerInfos'][0]
        self.assertEqual(rfc4357.id_GostR3411_94, si['digestAlgorithm']['algorithm'])
        self.assertEqual(univ.Null(""), si['digestAlgorithm']['parameters'])

        self.assertEqual(rfc4357.id_GostR3410_2001, si['signatureAlgorithm']['algorithm'])
        self.assertEqual(univ.Null(""), si['signatureAlgorithm']['parameters'])

        sig = rfc4490.GostR3410_2001_Signature()
        sig = si['signature']
        self.assertEqual(64, len(sig))

class KeyAgreeTestCase(unittest.TestCase):
    keyagree_pem_text = """\
MIIBpAYJKoZIhvcNAQcDoIIBlTCCAZECAQIxggFQoYIBTAIBA6BloWMwHAYGKoUD
AgITMBIGByqFAwICJAAGByqFAwICHgEDQwAEQLNVOfRngZcrpcTZhB8n+4HtCDLm
mtTyAHi4/4Nk6tIdsHg8ff4DwfQG5DvMFrnF9vYZNxwXuKCqx9GhlLOlNiChCgQI
L/D20YZLMoowHgYGKoUDAgJgMBQGByqFAwICDQAwCQYHKoUDAgIfATCBszCBsDCB
gTBtMR8wHQYDVQQDDBZHb3N0UjM0MTAtMjAwMSBleGFtcGxlMRIwEAYDVQQKDAlD
cnlwdG9Qcm8xCzAJBgNVBAYTAlJVMSkwJwYJKoZIhvcNAQkBFhpHb3N0UjM0MTAt
MjAwMUBleGFtcGxlLmNvbQIQK/XGHsIRvRfH3NRiZrQuIQQqMCgEIBajHOfOTukN
8ex0aQRoHsefOu24Ox8dSn75pdnLGdXoBAST/YZ+MDgGCSqGSIb3DQEHATAdBgYq
hQMCAhUwEwQItzXhegc1oh0GByqFAwICHwGADDmxivS/qeJlJbZVyQ==
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.keyagree_pem_text)
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

        ri = ed['recipientInfos'][0]
        alg1 = ri['kari']['originator']['originatorKey']['algorithm']
        self.assertEqual(rfc4357.id_GostR3410_2001, alg1['algorithm'])
        param1, rest = der_decoder(
            alg1['parameters'],
            asn1Spec=rfc4357.GostR3410_2001_PublicKeyParameters())
        self.assertFalse(rest)
        self.assertTrue(param1.prettyPrint())
        self.assertEqual(alg1['parameters'], der_encoder(param1))

        self.assertEqual(rfc4357.id_GostR3410_2001_CryptoPro_XchA_ParamSet, param1['publicKeyParamSet'])
        self.assertEqual(rfc4357.id_GostR3411_94_CryptoProParamSet, param1['digestParamSet'])

        self.assertEqual(8, len(ri['kari']['ukm']))
    
        alg2 = ri['kari']['keyEncryptionAlgorithm']
        self.assertEqual(rfc4490.id_GostR3410_2001_CryptoPro_ESDH, alg2['algorithm'])
        param2, rest = der_decoder(
            alg2['parameters'], asn1Spec=rfc4357.AlgorithmIdentifier())
        self.assertFalse(rest)
        self.assertTrue(param2.prettyPrint())
        self.assertEqual(alg2['parameters'], der_encoder(param2))

        self.assertEqual(rfc4490.id_Gost28147_89_None_KeyWrap, param2['algorithm'])
        kwa_p, rest = der_decoder(
            param2['parameters'], asn1Spec=rfc4490.Gost28147_89_KeyWrapParameters())
        self.assertFalse(rest)
        self.assertTrue(kwa_p.prettyPrint())
        self.assertEqual(param2['parameters'], der_encoder(kwa_p))
        self.assertEqual(rfc4357.id_Gost28147_89_CryptoPro_A_ParamSet, kwa_p['encryptionParamSet'])

        alg3 = ed['encryptedContentInfo']['contentEncryptionAlgorithm']        
        self.assertEqual(rfc4357.id_Gost28147_89, alg3['algorithm'])
        param3, rest = der_decoder(alg3['parameters'], asn1Spec=rfc4357.Gost28147_89_Parameters())
        self.assertFalse(rest)
        self.assertTrue(param3.prettyPrint())
        self.assertEqual(alg3['parameters'], der_encoder(param3))
        self.assertEqual(8, len(param3['iv']))
        self.assertEqual(rfc4357.id_Gost28147_89_CryptoPro_A_ParamSet, param3['encryptionParamSet'])

    def testOpenTypes(self):
        openTypeMap = {
            rfc4357.id_GostR3410_2001: rfc4357.GostR3410_2001_PublicKeyParameters(),
            rfc4357.id_Gost28147_89: rfc4357.Gost28147_89_Parameters(),
            rfc4490.id_GostR3410_2001_CryptoPro_ESDH: rfc5280.AlgorithmIdentifier(),
        }

        substrate = pem.readBase64fromText(self.keyagree_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec,
            openTypes=openTypeMap, decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(rfc5652.id_envelopedData, asn1Object['contentType'])

        ri = asn1Object['content']['recipientInfos'][0]
        alg1 = ri['kari']['originator']['originatorKey']['algorithm']
        self.assertEqual(rfc4357.id_GostR3410_2001, alg1['algorithm'])
        param1 = alg1['parameters']
        self.assertEqual(rfc4357.id_GostR3410_2001_CryptoPro_XchA_ParamSet, param1['publicKeyParamSet'])
        self.assertEqual(rfc4357.id_GostR3411_94_CryptoProParamSet, param1['digestParamSet'])

        self.assertEqual(8, len(ri['kari']['ukm']))

        alg2 = ri['kari']['keyEncryptionAlgorithm']
        self.assertEqual(rfc4490.id_GostR3410_2001_CryptoPro_ESDH, alg2['algorithm'])
        param2 = alg2['parameters']
        self.assertEqual(rfc4490.id_Gost28147_89_None_KeyWrap, param2['algorithm'])
        kwa_p = param2['parameters']
        self.assertEqual(rfc4357.id_Gost28147_89_CryptoPro_A_ParamSet, kwa_p['encryptionParamSet'])
    
        alg3 = asn1Object['content']['encryptedContentInfo']['contentEncryptionAlgorithm']        
        self.assertEqual(rfc4357.id_Gost28147_89, alg3['algorithm'])
        param3 = alg3['parameters']
        self.assertEqual(8, len(param3['iv']))
        self.assertEqual(rfc4357.id_Gost28147_89_CryptoPro_A_ParamSet, param3['encryptionParamSet'])

class KeyTransportTestCase(unittest.TestCase):
    keytrans_pem_text = """\
MIIBpwYJKoZIhvcNAQcDoIIBmDCCAZQCAQAxggFTMIIBTwIBADCBgTBtMR8wHQYD
VQQDDBZHb3N0UjM0MTAtMjAwMSBleGFtcGxlMRIwEAYDVQQKDAlDcnlwdG9Qcm8x
CzAJBgNVBAYTAlJVMSkwJwYJKoZIhvcNAQkBFhpHb3N0UjM0MTAtMjAwMUBleGFt
cGxlLmNvbQIQK/XGHsIRvRfH3NRiZrQuITAcBgYqhQMCAhMwEgYHKoUDAgIkAAYH
KoUDAgIeAQSBpzCBpDAoBCBqL6ghBpVon5/kR6qey2EVK35BYLxdjfv1PSgbGJr5
dQQENm2Yt6B4BgcqhQMCAh8BoGMwHAYGKoUDAgITMBIGByqFAwICJAAGByqFAwIC
HgEDQwAEQE0rLzOQ5tyj3VUqzd/g7/sx93N+Tv+/eImKK8PNMZQESw5gSJYf28dd
Em/askCKd7W96vLsNMsjn5uL3Z4SwPYECJeV4ywrrSsMMDgGCSqGSIb3DQEHATAd
BgYqhQMCAhUwEwQIvBCLHwv/NCkGByqFAwICHwGADKqOch3uT7Mu4w+hNw==
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.keytrans_pem_text)
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

        ri = ed['recipientInfos'][0]
        alg1 = ri['ktri']['keyEncryptionAlgorithm']
        self.assertEqual(rfc4357.id_GostR3410_2001, alg1['algorithm'])
        param1, rest = der_decoder(
            alg1['parameters'], asn1Spec=rfc4357.GostR3410_2001_PublicKeyParameters())
        self.assertFalse(rest)
        self.assertTrue(param1.prettyPrint())
        self.assertEqual(alg1['parameters'], der_encoder(param1))
        self.assertEqual(rfc4357.id_GostR3410_2001_CryptoPro_XchA_ParamSet, param1['publicKeyParamSet'])
        self.assertEqual(rfc4357.id_GostR3411_94_CryptoProParamSet, param1['digestParamSet'])

        alg2 = ed['encryptedContentInfo']['contentEncryptionAlgorithm']        
        self.assertEqual(rfc4357.id_Gost28147_89, alg2['algorithm'])
        param2, rest = der_decoder(
            alg2['parameters'], asn1Spec=rfc4357.Gost28147_89_Parameters())
        self.assertFalse(rest)
        self.assertTrue(param2.prettyPrint())
        self.assertEqual(alg2['parameters'], der_encoder(param2))
        self.assertEqual(8, len(param2['iv']))
        self.assertEqual(rfc4357.id_Gost28147_89_CryptoPro_A_ParamSet, param2['encryptionParamSet'])

    def testOpenTypes(self):
        openTypeMap = {
            rfc4357.id_GostR3410_2001: rfc4357.GostR3410_2001_PublicKeyParameters(),
            rfc4357.id_Gost28147_89: rfc4357.Gost28147_89_Parameters(),
        }

        substrate = pem.readBase64fromText(self.keytrans_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec,
            openTypes=openTypeMap, decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        ri = asn1Object['content']['recipientInfos'][0]
        alg1 = ri['ktri']['keyEncryptionAlgorithm']
        self.assertEqual(rfc4357.id_GostR3410_2001, alg1['algorithm'])
        param1 = alg1['parameters']
        self.assertEqual(rfc4357.id_GostR3410_2001_CryptoPro_XchA_ParamSet, param1['publicKeyParamSet'])
        self.assertEqual(rfc4357.id_GostR3411_94_CryptoProParamSet, param1['digestParamSet'])

        alg2 = asn1Object['content']['encryptedContentInfo']['contentEncryptionAlgorithm']        
        self.assertEqual(rfc4357.id_Gost28147_89, alg2['algorithm'])
        param2 = alg2['parameters']
        self.assertEqual(8, len(param2['iv']))
        self.assertEqual(rfc4357.id_Gost28147_89_CryptoPro_A_ParamSet, param2['encryptionParamSet'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
