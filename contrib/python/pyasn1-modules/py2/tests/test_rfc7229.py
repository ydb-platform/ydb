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
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc7229


class CertificatePolicyTestCase(unittest.TestCase):
    pem_text = """\
MIIDJDCCAqqgAwIBAgIJAKWzVCgbsG5AMAoGCCqGSM49BAMDMD8xCzAJBgNVBAYT
AlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9n
dXMgQ0EwHhcNMTkxMDEzMTkwNTUzWhcNMjAxMDEyMTkwNTUzWjBTMQswCQYDVQQG
EwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xJTAjBgNVBAoTHFRF
U1QgQ2VydGlmaWNhdGlvbiBBdXRob3JpdHkwdjAQBgcqhkjOPQIBBgUrgQQAIgNi
AATwUXZUseiOaqWdrClDCMbp9YFAM87LTmFirygpzKDU9cfqSCg7zBDIphXCwMcS
9zVWDoStCbcvN0jw5CljHcffzpHYX91P88SZRJ1w4hawHjOsWxvM3AkYgZ5nfdlL
7EajggFcMIIBWDAdBgNVHQ4EFgQU8jXbNATapVXyvWkDmbBi7OIVCMEwbwYDVR0j
BGgwZoAU8jXbNATapVXyvWkDmbBi7OIVCMGhQ6RBMD8xCzAJBgNVBAYTAlVTMQsw
CQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9ndXMgQ0GC
CQDokdYGkU/O8jAPBgNVHRMBAf8EBTADAQH/MAsGA1UdDwQEAwIBhjBCBglghkgB
hvhCAQ0ENRYzVGhpcyBjZXJ0aWZpY2F0ZSBjYW5ub3QgYmUgdHJ1c3RlZCBmb3Ig
YW55IHB1cnBvc2UuMCEGA1UdIAQaMBgwCgYIKwYBBQUHDQEwCgYIKwYBBQUHDQIw
CgYDVR02BAMCAQIwNQYDVR0hBC4wLDAUBggrBgEFBQcNAQYIKwYBBQUHDQcwFAYI
KwYBBQUHDQIGCCsGAQUFBw0IMAoGCCqGSM49BAMDA2gAMGUCMHaWskjS7MKQCMcn
zEKFOV3LWK8pL57vrECJd8ywKdwBJUNw9HhvSKkfUwL6rjlLpQIxAL2QO3CNoZRP
PZs8K3IjUA5+U73pA8lpaTOPscLY22WL9pAGmyVUyEJ8lM7E+r4iDg==
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        test_oids = [
            rfc7229.id_TEST_certPolicyOne,
            rfc7229.id_TEST_certPolicyTwo,
            rfc7229.id_TEST_certPolicyThree,
            rfc7229.id_TEST_certPolicyFour,
            rfc7229.id_TEST_certPolicyFive,
            rfc7229.id_TEST_certPolicySix,
            rfc7229.id_TEST_certPolicySeven,
            rfc7229.id_TEST_certPolicyEight,
        ]

        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        count = 0
        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] in rfc5280.certificateExtensionsMap.keys():
                s = extn['extnValue']
                ev, rest = der_decoder(
                    s, rfc5280.certificateExtensionsMap[extn['extnID']])

                self.assertFalse(rest)
                self.assertTrue(ev.prettyPrint())
                self.assertEqual(s, der_encoder(ev))

                if extn['extnID'] == rfc5280.id_ce_certificatePolicies:
                    for pol in ev:
                        if pol['policyIdentifier'] in test_oids:
                            count += 1

                if extn['extnID'] == rfc5280.id_ce_policyMappings:
                    for pmap in ev:
                        if pmap['issuerDomainPolicy'] in test_oids:
                            count += 1
                        if pmap['subjectDomainPolicy'] in test_oids:
                            count += 1

        self.assertEqual(6, count)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
