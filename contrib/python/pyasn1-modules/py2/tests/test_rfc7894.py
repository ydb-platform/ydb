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
from pyasn1_modules import rfc6402
from pyasn1_modules import rfc7894


class AlternativeChallengePasswordTestCase(unittest.TestCase):
    otp_pem_text = """\
MIICsjCCAZwCAQAwJDELMAkGA1UEBhMCVVMxFTATBgNVBAMTDDRUUzJWMk5MWEE2
WjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKmF0oUj5+1rBB+pUO8X
7FPxer+1BhWOa54RTSucJmBaLx0H95qNaBCcctNDl1kcmIro/a0zMcEvj5Do29vQ
lStJdTeJ/B3X4qzOGShupxJcAhCreRZjN6Yz3T9z0zJ8OPnRvJOzcSiIzlubc9lK
Cpq4U0UsCLLfymOgL9NH4lZi96J+PFuJr0J+rTY38076U2jcPqNq5/L/d6NV9Sz2
IVOvCK1kqP/nElJVibIQZvj9YESLUKyVAfTNxLj3+IpioOOv2dT3kB9wdi4plAVi
UFEUvED1okRrI29+LdPV1UXglOCksyJIIw+DgDtutDE5Co6QkTNURFEdKIV9Sg13
zEECAwEAAaBLMBkGCyqGSIb3DQEJEAI4MQoTCDkwNTAzODQ2MC4GCSqGSIb3DQEJ
DjEhMB8wHQYDVR0OBBYEFBj12LVowM16Ed0D+AmoElKNYP/kMAsGCSqGSIb3DQEB
CwOCAQEAZZdDWKejs3UVfgZI3R9cMWGijmscVeZrjwFVkn7MI9pEDZ2aS1QaRYjY
1cu9j3i+LQp9LWPIW/ztYk11e/OcZp3fo8pZ+MT66n7YTWfDXNkqqA5xmI84DMEx
/cqenyzOBZWqpZGx7eyM9BtnrdeJ0r2qSc7LYU25FbIQFJJf8IvgMAXWMs50fvs2
Gzns447x952se2ReQ3vYhXdHvYYcgAZfSJZvK+nCmhzzqowv5p15Y5S+IHpBSXTO
a1qhNW4cjdicQZUeQ2R5kiuwZ+8vHaq9jKxAEk0hBeqG6RQaxvNOBQhHtTLNGw/C
NmaF8Y2Sl/MgvC5tjs0Ck0/r3lsoLQ==
"""

    def setUp(self):
        self.asn1Spec = rfc6402.CertificationRequest()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.otp_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(0, asn1Object['certificationRequestInfo']['version'])

        for attr in asn1Object['certificationRequestInfo']['attributes']:
            self.assertIn(
                attr['attrType'], rfc6402.cmcControlAttributesMap)

            av, rest = der_decoder(
                attr['attrValues'][0],
                rfc6402.cmcControlAttributesMap[attr['attrType']])

            self.assertFalse(rest)
            self.assertEqual(attr['attrValues'][0], der_encoder(av))

            if attr['attrType'] == rfc7894.id_aa_otpChallenge:
                self.assertEqual('90503846', av['printableString'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.otp_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        for attr in asn1Object['certificationRequestInfo']['attributes']:
            self.assertIn(attr['attrType'], rfc6402.cmcControlAttributesMap)
            if attr['attrType'] == rfc7894.id_aa_otpChallenge:
                self.assertEqual(
                    '90503846', attr['attrValues'][0]['printableString'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
