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

from pyasn1_modules import pem
from pyasn1_modules import rfc5480
from pyasn1_modules import rfc5751
from pyasn1_modules import rfc6664


class SMIMECapabilitiesTestCase(unittest.TestCase):
    smime_capabilities_pem_text = """\
MIICOjAJBgUrDgMCGgUAMA0GCWCGSAFlAwQCBAUAMA0GCWCGSAFlAwQCAQUAMA0G
CWCGSAFlAwQCAgUAMA0GCWCGSAFlAwQCAwUAMBUGCSqGSIb3DQEBATAIAgIEAAIC
EAAwFQYJKoZIhvcNAQEHMAgCAgQAAgIQADAVBgkqhkiG9w0BAQowCAICBAACAhAA
MBUGByqGSM44BAGgCjAIAgIEAAICDAAwggEvBgcqhkjOPgIBoYIBIjCCAR4CgYEA
i6Ued8R33vkopJwCvy/ZZv2TtddPXPYmJK4jyFv+TDJTPqnP7XUZCqRuhCyKX10z
7SgiZs6qlSMk5gCa8shPF8NCHtps2D1OVC7yppZUJI07FoDxoEAZHImdAFvYIA/V
cGYpYOKod4kju0/e4VUBZ6Qoer5vKTh+lD/+ZKa/WSUCFQDc3W87QSZSX6ggdbeI
fzb0rsAhbwKBgCEz/o4WJPUZ4HffJfuXHIGrkPnCxFAYDRtlqueswV0Gy6LunipE
Iu3nCzYkZhMatyFNyzo+NusEsS+9isOhT8jhL93nSBZCSRBy+GfmSXlXv/3c8mtH
XTie5JOqjRdonPr4g/+VZvMkcioooNrhx/zICHrC3WZ72871/n/z9M+dMCMGByqG
SM49AgEwGAYIKoZIzj0DAQcGBSuBBAAiBgUrgQQAIzAhBgUrgQQBDTAYBggqhkjO
PQMBBwYFK4EEACIGBSuBBAAjMBoGCSqGSIb3DQEBCDANBglghkgBZQMEAgEFAA==
"""

    def setUp(self):
        self.asn1Spec = rfc5751.SMIMECapabilities()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        count = 0
        for cap in asn1Object:
            if cap['capabilityID'] in rfc5751.smimeCapabilityMap.keys():
                substrate = cap['parameters']
                cap_p, rest = der_decoder(
                    substrate, asn1Spec=rfc5751.smimeCapabilityMap[cap['capabilityID']])
                self.assertFalse(rest)
                self.assertTrue(cap_p.prettyPrint())
                self.assertEqual(substrate, der_encoder(cap_p))
                count += 1

        self.assertEqual(8, count)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        parameterValue = {
            rfc6664.rsaEncryption: lambda x: x['maxKeySize'],
            rfc6664.id_RSAES_OAEP: lambda x: x['maxKeySize'],
            rfc6664.id_RSASSA_PSS: lambda x: x['minKeySize'],
            rfc6664.id_dsa: lambda x: x['keySizes']['maxKeySize'],
            rfc6664.dhpublicnumber: lambda x: x['keyParams']['q'] % 1023,
            rfc6664.id_ecPublicKey: lambda x: x[0]['namedCurve'],
            rfc6664.id_ecMQV: lambda x: x[1]['namedCurve'],
        }

        expectedValue = {
            rfc6664.rsaEncryption: 4096,
            rfc6664.id_RSAES_OAEP: 4096,
            rfc6664.id_RSASSA_PSS: 1024,
            rfc6664.id_dsa: 3072,
            rfc6664.dhpublicnumber: 257,
            rfc6664.id_ecPublicKey: rfc5480.secp256r1,
            rfc6664.id_ecMQV: rfc5480.secp384r1,
        }

        count = 0
        for cap in asn1Object:
            if cap['capabilityID'] in parameterValue.keys():
                pValue = parameterValue[cap['capabilityID']](cap['parameters'])
                eValue = expectedValue[cap['capabilityID']]
                self.assertEqual(eValue, pValue)
                count += 1

        self.assertEqual(7, count)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
