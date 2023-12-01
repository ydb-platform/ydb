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
from pyasn1.type import univ

from pyasn1_modules import pem
from pyasn1_modules import rfc4055
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5958
from pyasn1_modules import rfc8479


class ValidationParmTestCase(unittest.TestCase):
    pem_text = """\
MIIE/gIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCpPwXwfhDsWA3q
jN2BWg1xfDjvZDVNfgTV/b95g304Aty3z13xPXAhHZ3ROW3pgPxTj9fiq7ZMy4Ua
gMpPK81v3pHX1uokC2KcGXbgbAq2Q8ClxSXgEJllRwDENufjEdV10gArt8NlIP0N
lota1kQUuI1DMsqc5DTIa35Nq4j1GW+KmLtP0kCrGq9fMGwjDbPEpSp9DTquEMHJ
o7kyJIjB+93ikLvBUTgbxr+jcnTLXuhA8rC8r+KXre4NPPNPRyefRcALLt/URvfA
rTvFOQfi3vIjNhBZL5FdC+FVAr5QnF3r2+cuDPbnczr4/rr81kzFGWrwyAgF5FWu
pFtB5IYDAgMBAAECggEAHZ88vGNsNdmRkfhWupGW4cKCuo+Y7re8Q/H2Jd/4Nin2
FKvUPuloaztiSGDbVm+vejama/Nu5FEIumNJRYMeoVJcx2DDuUxO1ZB1aIEwfMct
/DWd0/JDzuCXB0Cu5GTWLhlz0zMGHXihIdQ0DtGKt++3Ncg5gy1D+cIqqJB515/z
jYdZmb0Wqmz7H3DisuxvnhiCAOuNrjcDau80hpMA9TQlb+XKNGHIBgKpJe6lnB0P
MsS/AjDiDoEpP9GG9mv9+96rAga4Nos6avYlwWwbC6d+hHIWvWEWsmrDfcJlm2gN
tjvG8omj00t5dAt7qGhfOoNDGr5tvJVo/g96O/0I8QKBgQDdzytVRulo9aKVdAYW
/Nj04thtnRaqsTyFH+7ibEVwNIUuld/Bp6NnuGrY+K1siX8+zA9f8mKxuXXV9KK4
O89Ypw9js2BxM7VYO9Gmp6e1RY3Rrd8w7pG7/KqoPWXkuixTay9eybrJMWu3TT36
q7NheNmBHqcFmSQQuUwEmvp3MQKBgQDDVaisMJkc/sIyQh3XrlfzmMLK+GlPDucD
w5e50fHl8Q5PmTcP20zVLhTevffCqeItSyeAno94Xdzc9vZ/rt69410kJEHyBO9L
CmhtYz94wvSdRhbqf4VzAl2WU184sIYiIZDGsnGScgIYvo6v6mITjRhc8AMdYoPR
rL6xp6frcwKBgFi1+avCj6mFzD+fxqu89nyCmXLFiAI+nmjTy7PM/7yPlNB76qDG
Dil2bW1Xj+y/1R9ld6S1CVnxRbqLe+TZLuVS82m5nRHJT3b5fbD8jquGJOE+e+xT
DgA0XoCpBa6D8yRt0uVDIyxCUsVd5DL0JusN7VehzcUEaZMyuL+CyDeRAoGBAImB
qH6mq3Kc6Komnwlw4ttJ436sxr1vuTKOIyYdZBNB0Zg5PGi+MWU0zl5LDroLi3vl
FwbVGBxcvxkSBU63FHhKMQw7Ne0gii+iQQcYQdtKKpb4ezNS1+exd55WTIcExTgL
tvYZMhgsh8tRgfLWpXor7kWmdBrgeflFiOxZIL1/AoGAeBP7sdE+gzsh8jqFnVRj
7nOg+YllJAlWsf7cTH4pLIy2Eo9D+cNjhL9LK6RaAd7PSZ1adm8HfaROA2cfCm84
RI4c7Ue0G+N6LZiFvC0Bfi5SaPVAExXOty8UqjOCoZavSaXBPuNcTXZuzswcgbxI
G5/kaJNHoEcdlVsPsYWKRNKgPzA9BgorBgEEAZIIEggBMS8wLQYJYIZIAWUDBAIC
BCCK9DKMh7687DHjA7j1U37/y2qR2UcITZmjaYI7NvAUYg==
"""

    def setUp(self):
        self.asn1Spec = rfc5958.OneAsymmetricKey()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        for attr in asn1Object['attributes']:
            self.assertIn(attr['attrType'], rfc5652.cmsAttributesMap)

            if attr['attrType'] == rfc8479.id_attr_validation_parameters:
                av, rest = der_decoder(
                    attr['attrValues'][0],
                    asn1Spec=rfc5652.cmsAttributesMap[attr['attrType']])
                self.assertFalse(rest)
                self.assertTrue(av.prettyPrint())
                self.assertEqual(attr['attrValues'][0], der_encoder(av))
                self.assertEqual(rfc4055.id_sha384, av['hashAlg'])

                seed = univ.OctetString(hexValue='8af4328c87bebcec31e303b8f55'
                                                 '37effcb6a91d947084d99a36982'
                                                 '3b36f01462')

                self.assertEqual(seed, av['seed'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        for attr in asn1Object['attributes']:
            self.assertIn(attr['attrType'], rfc5652.cmsAttributesMap)
            if attr['attrType'] == rfc8479.id_attr_validation_parameters:
                av = attr['attrValues'][0]

                self.assertEqual(av['hashAlg'], rfc4055.id_sha384)

                seed = univ.OctetString(hexValue='8af4328c87bebcec31e303b8f553'
                                                 '7effcb6a91d947084d99a369823b'
                                                 '36f01462')

                self.assertEqual(seed, av['seed'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
