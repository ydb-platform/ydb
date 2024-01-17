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
from pyasn1_modules import rfc3370
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5751


class EnvelopedDataTestCase(unittest.TestCase):
    env_data_pem_text = """\
MIIFjAYJKoZIhvcNAQcDoIIFfTCCBXkCAQIxZqJkAgEEMCMEEH744tkBAA6gplAQ
nKYxCF8YDzIwMTkwOTEyMTIwMDAwWjAQBgsqhkiG9w0BCRADBwIBOgQocOaZ+1cB
94MzMPtx6HyFpCC9yZrwXSKvWg5I018xOJhsuq+0so1PNTCCBQoGCSqGSIb3DQEH
ATAZBggqhkiG9w0DAjANAgE6BAhCT0dVU19JVoCCBOBzx7F6GMkP+C0Q4iuDq0rk
SZprg8nuXx/4S3IMP999BrJdUAbPYxdQhAwTOZIuueyv00TJe/Eam9yyLuZXy0PF
lTRi7KED8L8cyHsRoEobWGMLvE3D4hEhTGttElqQxBvMxZZgm6kLnNG7j8Z72L4l
U4aARLYTQvktlJnnfCaccDSiWzU8eXcXdnZAzcKR7CoDc0/XBpdDRddvQ7KXoarX
YHuSybt649YDcpy0SN9gEPqcFPrBB3nusAx4VOTlpx5Z3ZJv/TEymN8KDobNfykB
ZURTwupO9WaVJZ3Hd/d8C1SCJn6DHuM1jwDp26WfzO8xCfea08MJrnQbNKsDHwmt
4dFZIOvcOnwR8nNSB/Lt1aUj3GzluHVMyQQyT4AdZDmwFdNmQOBUBLmbWYhtd7t3
O7Eqx8bGNa7V7LL0nvua04aj1oA6ph/G/8jxhByBYdN5Bwg7f1Ga3ZCwju2tFoQn
WOCPYTVOjmBEJshBbNC7KhLpp9+C7/13A9cIC3T7Reuc7m+Fopf9Fabu97yFiyJP
S8jSF0EnesNGR1L1Uvo2Wdc66iECoSrxvezaSgGKB2uLTnaFx4ASVMcP7gDipEOI
wuUUuVCqgmWkHAK0Q9mwhBLLrYrsn9OjDHFpvkWgWNRMLl/v3E9A+grFh2BQHkB4
C7keB1ZOfj1SqDi/+ylM9I1FOYMxVXJn2qHMl+QOkfdMoIATm3n3DiBI97/uX4x5
KaX074v0dN31WeDcsFsh2ze5Dhx8vLJCaXLzWqkmNHX5G/CjjqE6bSR/awgWLRZQ
uY/9fMvDpvVJuId/+OoWDtMVPIsyQ8w8yZzv+SkuZhsrJMHiKd5qxNQv5sOvC765
LMUCNNwj7WzPhajintFXLAEMpIjk5xt3eIy3hdYla3PQoFfqcHOVX4EFMLBoYwBT
gik8Fg669yXtMlbH84MGNs7jObhP/rrDkgbe0qmxUyzgm2uHya1VcItMGYoPPKMF
U3ZfwAsZdqsi1GAtruTzSUmOpMfAoKOIAyZP96HrsrPCaoGrn7ysm5eRrHQ2hdwO
7rGQIw0dRAFh2eyRomoLam7yEiw9M6uHuJ5hIS5yEW+7uUjQT6nvKlbrkIyLL5j9
Gbk5Z4fOMqRTkBs+3H8x7a+lBEKBo/ByJm6fHYi+LX5ZhQFTWkY0M7tfPtrxQdsN
RGSHtv7jS7PZ3thCMqCtkG/pjAsCbDUtMThtP08z2fstE6dfy7qSx6LzKLDyBl5W
76mVYdsX7Q72yIoCDFmUGdrRcWA+l3OMwNNL+x9MhhdaUWPtxqaGyZMNGOjkbYHb
XZ69oqYqCHkAstIVKTzpk3kq9C9x+ynzWO8kIGYNK2uxSBIzPLQ6Daq4c53rWFFN
WVjPC8m98zMcYp0hbBhRsdk4qj8osSTcTfpT0+Q+hkYQvZl4IfgX1aHeaCDSScF8
SaU+cZ7GYFvLo1cYrtVbeXrFwmWl0xpco1Ux+XZgryT/fgfJ+3ToppgsQmzECqTW
mYsSYaF1kLU4Cqi9UH/VqBLOkwxoH05Zao2xOMNzu2QO3wFnvY2wBsIj1eaxfzVb
42o9vom7V20jT1ufXXctf9ls5J1WJxBxdKmXQWdNloeAcl1AtxTbw7vIUU5uWqu9
wwqly11MDVPAb0tcQW20auWmCNkXd52jQJ7PXR6kr5I=
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
        self.assertEqual(rfc3370.id_alg_CMSRC2wrap, kwa['algorithm'])
        kwa_param, rest = der_decoder(
            kwa['parameters'], rfc3370.RC2wrapParameter())
        self.assertFalse(rest)
        self.assertTrue(kwa_param.prettyPrint())
        self.assertEqual(kwa['parameters'], der_encoder(kwa_param)) 
        self.assertEqual(58, kwa_param)

        cea = ed['encryptedContentInfo']['contentEncryptionAlgorithm']
        self.assertEqual(rfc3370.rc2CBC, cea['algorithm'])
        param, rest = der_decoder(
            cea['parameters'], rfc3370.RC2CBCParameter())
        self.assertFalse(rest)
        self.assertTrue(param.prettyPrint())
        self.assertEqual(cea['parameters'], der_encoder(param))

        iv = univ.OctetString(hexValue='424f4755535f4956')
        self.assertEqual(iv, param['iv'])
        self.assertEqual(58, param['rc2ParameterVersion'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.env_data_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertTrue(asn1Object['contentType'] in rfc5652.cmsContentTypesMap.keys())

        ri0 = asn1Object['content']['recipientInfos'][0]
        kwa = ri0['kekri']['keyEncryptionAlgorithm']
        self.assertEqual(rfc3370.id_alg_CMSRC2wrap, kwa['algorithm'])
        self.assertEqual(58, kwa['parameters'])

        eci = asn1Object['content']['encryptedContentInfo']
        cea = eci['contentEncryptionAlgorithm']
        self.assertEqual(rfc3370.rc2CBC, cea['algorithm'])

        iv = univ.OctetString(hexValue='424f4755535f4956')
        self.assertEqual(iv, cea['parameters']['iv'])
        self.assertEqual(58, cea['parameters']['rc2ParameterVersion'])

class DSAPublicKeyTestCase(unittest.TestCase):
    dsa_cert_pem_text = """\
MIIDpjCCA0ygAwIBAgIUY8xt3l0B9nIPWSpjs0hDJUJZmCkwCwYJYIZIAWUDBAMC
MD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIEwJWQTEQMA4GA1UEBxMHSGVybmRvbjER
MA8GA1UEChMIQm9ndXMgQ0EwHhcNMTkxMDIwMjAxMjMwWhcNMjAxMDE5MjAxMjMw
WjBwMQswCQYDVQQGEwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24x
EDAOBgNVBAoTB0V4YW1wbGUxDjAMBgNVBAsTBUFsaWNlMSAwHgYJKoZIhvcNAQkB
FhFhbGljZUBleGFtcGxlLmNvbTCCAbYwggErBgcqhkjOOAQBMIIBHgKBgQCLpR53
xHfe+SiknAK/L9lm/ZO1109c9iYkriPIW/5MMlM+qc/tdRkKpG6ELIpfXTPtKCJm
zqqVIyTmAJryyE8Xw0Ie2mzYPU5ULvKmllQkjTsWgPGgQBkciZ0AW9ggD9VwZilg
4qh3iSO7T97hVQFnpCh6vm8pOH6UP/5kpr9ZJQIVANzdbztBJlJfqCB1t4h/NvSu
wCFvAoGAITP+jhYk9Rngd98l+5ccgauQ+cLEUBgNG2Wq56zBXQbLou6eKkQi7ecL
NiRmExq3IU3LOj426wSxL72Kw6FPyOEv3edIFkJJEHL4Z+ZJeVe//dzya0ddOJ7k
k6qNF2ic+viD/5Vm8yRyKiig2uHH/MgIesLdZnvbzvX+f/P0z50DgYQAAoGALAUl
jkOi1PxjjFVvhGfK95yIsrfbfcIEKUBaTs9NR2rbGWUeP+93paoXwP39X9wrJx2M
SWeHWhWKszNgoiyqYT0k4R9mem3WClotxOvB5fHfwIp2kQYvE7H0/TPdGhfUpHQG
YpyLQgT6L80meSKMFnu4VXGzOANhWDxu3JxiADCjgZQwgZEwCwYDVR0PBAQDAgeA
MEIGCWCGSAGG+EIBDQQ1FjNUaGlzIGNlcnRpZmljYXRlIGNhbm5vdCBiZSB0cnVz
dGVkIGZvciBhbnkgcHVycG9zZS4wHQYDVR0OBBYEFO37wHcauyc03rDc6cDRRsHz
gcK+MB8GA1UdIwQYMBaAFM1IZQGDsqYHWwb+I4EMxHPk0bU4MAsGCWCGSAFlAwQD
AgNHADBEAiBBRbfMzLi7+SVyO8SM3xxwUsMf/k1B+Nkvf1kBTfCfGwIgSAx/6mI+
pNqdXqZZGESXy1MT1aBc4ynPGLFUr2r7cPY=
"""
    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.dsa_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki = asn1Object['tbsCertificate']['subjectPublicKeyInfo']
        self.assertEqual(rfc3370.id_dsa, spki['algorithm']['algorithm'])
        pk_substrate = spki['subjectPublicKey'].asOctets()

        pk, rest = der_decoder(pk_substrate, asn1Spec=rfc3370.Dss_Pub_Key())
        self.assertFalse(rest)
        self.assertTrue(pk.prettyPrint())
        self.assertEqual(pk_substrate, der_encoder(pk))

        self.assertEqual(48, pk % 1024)

class SMIMECapabilitiesTestCase(unittest.TestCase):
    smime_capabilities_pem_text = """\
MGIwDAYIKwYBBQUIAQIFADAfBgsqhkiG9w0BCRADBTAQBgsqhkiG9w0BCRADBwIB
OjAfBgsqhkiG9w0BCRADCjAQBgsqhkiG9w0BCRADBwIBOjAQBgsqhkiG9w0BCRAD
BwIBOg==
"""

    def setUp(self):
        self.asn1Spec = rfc5751.SMIMECapabilities()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        found_wrap_alg_param = False
        for cap in asn1Object:
            if cap['capabilityID'] in rfc5751.smimeCapabilityMap.keys():
                if cap['parameters'].hasValue():
                    param, rest = der_decoder(
                        cap['parameters'],
                        asn1Spec=rfc5751.smimeCapabilityMap[cap['capabilityID']])
                    self.assertFalse(rest)
                    self.assertTrue(param.prettyPrint())
                    self.assertEqual(cap['parameters'], der_encoder(param))

                    if cap['capabilityID'] == rfc3370.id_alg_ESDH:
                        kwa, rest = der_decoder(
                            cap['parameters'],
                            asn1Spec=rfc5751.smimeCapabilityMap[cap['capabilityID']])
                        self.assertFalse(rest)
                        self.assertTrue(kwa.prettyPrint())
                        self.assertEqual(cap['parameters'], der_encoder(kwa))

                        self.assertTrue(kwa['algorithm'] in rfc5280.algorithmIdentifierMap.keys())
                        self.assertEqual(rfc3370.id_alg_CMSRC2wrap, kwa['algorithm'])
                        kwa_p, rest = der_decoder(
                            kwa['parameters'],
                            asn1Spec=rfc5280.algorithmIdentifierMap[kwa['algorithm']])
                        self.assertFalse(rest)
                        self.assertTrue(kwa_p.prettyPrint())
                        self.assertEqual(kwa['parameters'], der_encoder(kwa_p))
                        self.assertEqual(58, kwa_p)
                        found_wrap_alg_param = True

        self.assertTrue(found_wrap_alg_param)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        found_wrap_alg_param = False
        for cap in asn1Object:
            if cap['capabilityID'] == rfc3370.id_alg_ESDH:
                self.assertEqual(rfc3370.id_alg_CMSRC2wrap, cap['parameters']['algorithm'])
                self.assertEqual(58, cap['parameters']['parameters'])
                found_wrap_alg_param = True

        self.assertTrue(found_wrap_alg_param)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())

