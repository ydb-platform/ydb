#
# This file is part of pyasn1-modules software.
#
# Created by Russ Housley
# Copyright (c) 2019, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#

import sys
import unittest

from pyasn1.type import univ

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder

from pyasn1_modules import pem
from pyasn1_modules import rfc3058
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5751


class EnvelopedDataTestCase(unittest.TestCase):
    env_data_pem_text = """\
MIIFgwYJKoZIhvcNAQcDoIIFdDCCBXACAQIxXaJbAgEEMCMEEDiCUYXKXu8SzLos
n2xeYP4YDzIwMTkwOTEyMTIwMDAwWjAPBgsrBgEEAYE8BwEBBgUABCB0G/YBGH3L
3RhoG0mK33M8IvRYAOsnHB5MfUAOGF6kuDCCBQoGCSqGSIb3DQEHATAZBgsrBgEE
AYE8BwEBAjAKBAhCT0dVU19JVoCCBOBzx7F6GMkP+C0Q4iuDq0rkSZprg8nuXx/4
S3IMP999BrJdUAbPYxdQhAwTOZIuueyv00TJe/Eam9yyLuZXy0PFlTRi7KED8L8c
yHsRoEobWGMLvE3D4hEhTGttElqQxBvMxZZgm6kLnNG7j8Z72L4lU4aARLYTQvkt
lJnnfCaccDSiWzU8eXcXdnZAzcKR7CoDc0/XBpdDRddvQ7KXoarXYHuSybt649YD
cpy0SN9gEPqcFPrBB3nusAx4VOTlpx5Z3ZJv/TEymN8KDobNfykBZURTwupO9WaV
JZ3Hd/d8C1SCJn6DHuM1jwDp26WfzO8xCfea08MJrnQbNKsDHwmt4dFZIOvcOnwR
8nNSB/Lt1aUj3GzluHVMyQQyT4AdZDmwFdNmQOBUBLmbWYhtd7t3O7Eqx8bGNa7V
7LL0nvua04aj1oA6ph/G/8jxhByBYdN5Bwg7f1Ga3ZCwju2tFoQnWOCPYTVOjmBE
JshBbNC7KhLpp9+C7/13A9cIC3T7Reuc7m+Fopf9Fabu97yFiyJPS8jSF0EnesNG
R1L1Uvo2Wdc66iECoSrxvezaSgGKB2uLTnaFx4ASVMcP7gDipEOIwuUUuVCqgmWk
HAK0Q9mwhBLLrYrsn9OjDHFpvkWgWNRMLl/v3E9A+grFh2BQHkB4C7keB1ZOfj1S
qDi/+ylM9I1FOYMxVXJn2qHMl+QOkfdMoIATm3n3DiBI97/uX4x5KaX074v0dN31
WeDcsFsh2ze5Dhx8vLJCaXLzWqkmNHX5G/CjjqE6bSR/awgWLRZQuY/9fMvDpvVJ
uId/+OoWDtMVPIsyQ8w8yZzv+SkuZhsrJMHiKd5qxNQv5sOvC765LMUCNNwj7WzP
hajintFXLAEMpIjk5xt3eIy3hdYla3PQoFfqcHOVX4EFMLBoYwBTgik8Fg669yXt
MlbH84MGNs7jObhP/rrDkgbe0qmxUyzgm2uHya1VcItMGYoPPKMFU3ZfwAsZdqsi
1GAtruTzSUmOpMfAoKOIAyZP96HrsrPCaoGrn7ysm5eRrHQ2hdwO7rGQIw0dRAFh
2eyRomoLam7yEiw9M6uHuJ5hIS5yEW+7uUjQT6nvKlbrkIyLL5j9Gbk5Z4fOMqRT
kBs+3H8x7a+lBEKBo/ByJm6fHYi+LX5ZhQFTWkY0M7tfPtrxQdsNRGSHtv7jS7PZ
3thCMqCtkG/pjAsCbDUtMThtP08z2fstE6dfy7qSx6LzKLDyBl5W76mVYdsX7Q72
yIoCDFmUGdrRcWA+l3OMwNNL+x9MhhdaUWPtxqaGyZMNGOjkbYHbXZ69oqYqCHkA
stIVKTzpk3kq9C9x+ynzWO8kIGYNK2uxSBIzPLQ6Daq4c53rWFFNWVjPC8m98zMc
Yp0hbBhRsdk4qj8osSTcTfpT0+Q+hkYQvZl4IfgX1aHeaCDSScF8SaU+cZ7GYFvL
o1cYrtVbeXrFwmWl0xpco1Ux+XZgryT/fgfJ+3ToppgsQmzECqTWmYsSYaF1kLU4
Cqi9UH/VqBLOkwxoH05Zao2xOMNzu2QO3wFnvY2wBsIj1eaxfzVb42o9vom7V20j
T1ufXXctf9ls5J1WJxBxdKmXQWdNloeAcl1AtxTbw7vIUU5uWqu9wwqly11MDVPA
b0tcQW20auWmCNkXd52jQJ7PXR6kr5I=
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

        kwa = ed['recipientInfos'][0]['kekri']['keyEncryptionAlgorithm']
        self.assertEqual(rfc3058.id_alg_CMSIDEAwrap, kwa['algorithm'])
        self.assertEqual(kwa['parameters'], der_encoder(univ.Null("")))

        cea = ed['encryptedContentInfo']['contentEncryptionAlgorithm']
        self.assertEqual(rfc3058.id_IDEA_CBC, cea['algorithm'])
        param, rest = der_decoder(
            cea['parameters'], asn1Spec=rfc3058.IDEA_CBCPar())

        self.assertFalse(rest)
        self.assertTrue(param.prettyPrint())
        self.assertEqual(cea['parameters'], der_encoder(param))

        iv = univ.OctetString(hexValue='424f4755535f4956')
        self.assertEqual(iv, param['iv'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.env_data_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        kekri = asn1Object['content']['recipientInfos'][0]['kekri']
        kwa = kekri['keyEncryptionAlgorithm']
        self.assertEqual(rfc3058.id_alg_CMSIDEAwrap, kwa['algorithm'])
        self.assertEqual(univ.Null(""), kwa['parameters'])

        eci = asn1Object['content']['encryptedContentInfo']
        cea = eci['contentEncryptionAlgorithm']
        self.assertEqual(rfc3058.id_IDEA_CBC, cea['algorithm'])

        iv = univ.OctetString(hexValue='424f4755535f4956')
        self.assertEqual(iv, cea['parameters']['iv'])

class SMIMECapabilitiesTestCase(unittest.TestCase):
    smime_capabilities_pem_text = "MB4wDQYLKwYBBAGBPAcBAQIwDQYLKwYBBAGBPAcBAQY="

    def setUp(self):
        self.asn1Spec = rfc5751.SMIMECapabilities()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        alg_oid_list = [ ]
        for cap in asn1Object:
            self.assertFalse(cap['parameters'].hasValue())
            alg_oid_list.append(cap['capabilityID'])

        self.assertIn(rfc3058.id_IDEA_CBC, alg_oid_list)
        self.assertIn(rfc3058.id_alg_CMSIDEAwrap, alg_oid_list)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
