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
from pyasn1_modules import rfc3852
from pyasn1_modules import rfc6402


class ContentInfoTestCase(unittest.TestCase):
    pem_text = """\
MIIEJQYJKoZIhvcNAQcCoIIEFjCCBBICAQMxCzAJBgUrDgMCGgUAMIIDAgYIKwYBBQUHDAKgggL0
BIIC8DCCAuwweDB2AgECBgorBgEEAYI3CgoBMWUwYwIBADADAgEBMVkwVwYJKwYBBAGCNxUUMUow
SAIBBQwZcGl0dWNoYTEuZW1lYS5ocHFjb3JwLm5ldAwMRU1FQVxwaXR1Y2hhDBpDTUNSZXFHZW5l
cmF0b3IudnNob3N0LmV4ZTCCAmqgggJmAgEBMIICXzCCAcgCAQAwADCBnzANBgkqhkiG9w0BAQEF
AAOBjQAwgYkCgYEA0jm7SSSm2wyEAzuNKtFZFJKo91SrJq9wQwEhEKHDavZwMQOm1rZ2PF8NWCEb
PqrhToQ7rtiGLSZa4dF4bzgmBqQ9aoSfEX4jISt31Vy+skHidXjHHpbsjT24NPhrZgANivL7CxD6
Ft+s7qS1gL4HRm2twQkqSwOLrE/q2QeXl2UCAwEAAaCCAR0wGgYKKwYBBAGCNw0CAzEMFgo2LjIu
OTIwMC4yMD4GCSqGSIb3DQEJDjExMC8wHQYDVR0OBBYEFMW2skn88gxhONWZQA4sWGBDb68yMA4G
A1UdDwEB/wQEAwIHgDBXBgkrBgEEAYI3FRQxSjBIAgEFDBlwaXR1Y2hhMS5lbWVhLmhwcWNvcnAu
bmV0DAxFTUVBXHBpdHVjaGEMGkNNQ1JlcUdlbmVyYXRvci52c2hvc3QuZXhlMGYGCisGAQQBgjcN
AgIxWDBWAgECHk4ATQBpAGMAcgBvAHMAbwBmAHQAIABTAHQAcgBvAG4AZwAgAEMAcgB5AHAAdABv
AGcAcgBhAHAAaABpAGMAIABQAHIAbwB2AGkAZABlAHIDAQAwDQYJKoZIhvcNAQEFBQADgYEAJZlu
mxjtCxSOQi27jsVdd3y8NSIlzNv0b3LqmzvAly6L+CstXcnuG2MPQqPH9R7tbJonGUniBQO9sQ7C
KhYWj2gfhiEkSID82lV5chINVUFKoUlSiEhWr0tPGgvOaqdsKQcrHfzrsBbFkhDqrFSVy7Yivbnh
qYszKrOjJKiiCPMwADAAMYH5MIH2AgEDgBTFtrJJ/PIMYTjVmUAOLFhgQ2+vMjAJBgUrDgMCGgUA
oD4wFwYJKoZIhvcNAQkDMQoGCCsGAQUFBwwCMCMGCSqGSIb3DQEJBDEWBBTFTkK/OifaFjwqHiJu
xM7qXcg/VzANBgkqhkiG9w0BAQEFAASBgKfC6jOi1Wgy4xxDCQVK9+e5tktL8wE/j2cb9JSqq+aU
5UxEgXEw7q7BoYZCAzcxMRriGzakXr8aXHcgkRJ7XcFvLPUjpmGg9SOZ2sGW4zQdWAwImN/i8loc
xicQmJP+VoMHo/ZpjFY9fYCjNZUArgKsEwK/s+p9yrVVeB1Nf8Mn
"""

    def setUp(self):
        self.asn1Spec = rfc3852.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)

        layers = {
            rfc3852.id_ct_contentInfo: rfc3852.ContentInfo(),
            rfc3852.id_signedData: rfc3852.SignedData(),
            rfc6402.id_cct_PKIData: rfc6402.PKIData()
        }

        getNextLayer = {
            rfc3852.id_ct_contentInfo: lambda x: x['contentType'],
            rfc3852.id_signedData: lambda x: x['encapContentInfo']['eContentType'],
            rfc6402.id_cct_PKIData: lambda x: None
        }

        getNextSubstrate = {
            rfc3852.id_ct_contentInfo: lambda x: x['content'],
            rfc3852.id_signedData: lambda x: x['encapContentInfo']['eContent'],
            rfc6402.id_cct_PKIData: lambda x: None
        }

        alg_oids = (
            univ.ObjectIdentifier('1.3.14.3.2.26'),
            univ.ObjectIdentifier('1.2.840.113549.1.1.1'),
            univ.ObjectIdentifier('1.2.840.113549.1.1.5'),
            univ.ObjectIdentifier('1.2.840.113549.1.1.11'),
        )

        encoded_null = der_encoder(univ.Null(""))

        next_layer = rfc3852.id_ct_contentInfo

        count = 0

        while next_layer:
            asn1Object, rest = der_decoder(substrate, asn1Spec=layers[next_layer])

            self.assertFalse(rest)
            self.assertTrue(asn1Object.prettyPrint())
            self.assertEqual(substrate, der_encoder(asn1Object))
    
            if next_layer == rfc3852.id_signedData:
                for d in asn1Object['digestAlgorithms']:
                    self.assertIn(d['algorithm'], alg_oids)
                    self.assertEqual(encoded_null, d['parameters'])
                    count += 1

                for si in asn1Object['signerInfos']:
                    self.assertIn(si['digestAlgorithm']['algorithm'], alg_oids)
                    self.assertEqual(
                        encoded_null, si['digestAlgorithm']['parameters'])
                    count += 1

                    self.assertIn(si['signatureAlgorithm']['algorithm'], alg_oids)
                    self.assertEqual(
                        encoded_null, si['signatureAlgorithm']['parameters'])
                    count += 1

            if next_layer == rfc6402.id_cct_PKIData:
                for req in asn1Object['reqSequence']:
                    cr = req['tcr']['certificationRequest']
                    self.assertIn(cr['signatureAlgorithm']['algorithm'], alg_oids)
                    self.assertEqual(
                        encoded_null, cr['signatureAlgorithm']['parameters'])
                    count += 1

                    cri_spki = cr['certificationRequestInfo']['subjectPublicKeyInfo']
                    self.assertIn(cri_spki['algorithm']['algorithm'], alg_oids)
                    self.assertEqual(
                        encoded_null, cri_spki['algorithm']['parameters'])
                    count += 1

            substrate = getNextSubstrate[next_layer](asn1Object)
            next_layer = getNextLayer[next_layer](asn1Object)

        self.assertEqual(5, count)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
