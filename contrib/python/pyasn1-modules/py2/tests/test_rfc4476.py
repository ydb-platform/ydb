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
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc5755
from pyasn1_modules import rfc4476


class AttributeCertificatePolicyTestCase(unittest.TestCase):
    pem_text = """\
MIID7zCCA1gCAQEwgY+gUTBKpEgwRjEjMCEGA1UEAwwaQUNNRSBJbnRlcm1lZGlh
dGUgRUNEU0EgQ0ExCzAJBgNVBAYTAkZJMRIwEAYDVQQKDAlBQ01FIEx0ZC4CAx7N
WqE6pDgwNjETMBEGA1UEAwwKQUNNRSBFQ0RTQTELMAkGA1UEBhMCRkkxEjAQBgNV
BAoMCUFDTUUgTHRkLqBWMFSkUjBQMQswCQYDVQQGEwJVUzELMAkGA1UECAwCVkEx
EDAOBgNVBAcMB0hlcm5kb24xIjAgBgNVBAoMGUJvZ3VzIEF0dHJpYnV0ZSBBdXRo
b3RpdHkwDQYJKoZIhvcNAQELBQACBAu1MO4wIhgPMjAxOTEyMTUxMjAwMDBaGA8y
MDE5MTIzMTEyMDAwMFowgfIwPAYIKwYBBQUHCgExMDAuhgt1cm46c2VydmljZaQV
MBMxETAPBgNVBAMMCHVzZXJuYW1lBAhwYXNzd29yZDAyBggrBgEFBQcKAjEmMCSG
C3VybjpzZXJ2aWNlpBUwEzERMA8GA1UEAwwIdXNlcm5hbWUwNQYIKwYBBQUHCgMx
KTAnoBikFjAUMRIwEAYDVQQDDAlBQ01FIEx0ZC4wCwwJQUNNRSBMdGQuMCAGCCsG
AQUFBwoEMRQwEjAQDAZncm91cDEMBmdyb3VwMjAlBgNVBEgxHjANoQuGCXVybjpy
b2xlMTANoQuGCXVybjpyb2xlMjCCATkwHwYDVR0jBBgwFoAUgJCMhskAsEBzvklA
X8yJBOXO500wCQYDVR04BAIFADA8BgNVHTcENTAzoAqGCHVybjp0ZXN0oBaCFEFD
TUUtTHRkLmV4YW1wbGUuY29toA2GC3Vybjphbm90aGVyMIHMBggrBgEFBQcBDwSB
vzCBvDCBuQYKKwYBBAGBrGAwCjCBqjBFBggrBgEFBQcCBBY5aHR0cHM6Ly93d3cu
ZXhhbXBsZS5jb20vYXR0cmlidXRlLWNlcnRpZmljYXRlLXBvbGljeS5odG1sMGEG
CCsGAQUFBwIFMFUwIwwZQm9ndXMgQXR0cmlidXRlIEF1dGhvcml0eTAGAgEKAgEU
Gi5URVNUIGF0dHJpYnV0ZSBjZXJ0aWZpY2F0ZSBwb2xpY3kgZGlzcGxheSB0ZXh0
MA0GCSqGSIb3DQEBCwUAA4GBACygfTs6TkPurZQTLufcE3B1H2707OXKsJlwRpuo
dR2oJbunSHZ94jcJHs5dfbzFs6vNfVLlBiDBRieX4p+4JcQ2P44bkgyiUTJu7g1b
6C1liB3vO6yH5hOZicOAaKd+c/myuGb9uFRoaXNfc2lnbmF0dXJlX2lzX2ludmFs
aWQh
"""

    def setUp(self):
        self.asn1Spec = rfc5755.AttributeCertificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(1, asn1Object['acinfo']['version'])

        found_ac_policy_qualifier1 = False
        found_ac_policy_qualifier2 = False
        for extn in asn1Object['acinfo']['extensions']:
            self.assertIn(extn['extnID'], rfc5280.certificateExtensionsMap)
            if extn['extnID'] == rfc4476.id_pe_acPolicies:
                ev, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']])

                self.assertFalse(rest)
                self.assertTrue(ev.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(ev))

                oid = univ.ObjectIdentifier((1, 3, 6, 1, 4, 1, 22112, 48, 10,))
                self.assertEqual(oid, ev[0]['policyIdentifier'])
        
                for pq in ev[0]['policyQualifiers']:
                    self.assertIn(
                        pq['policyQualifierId'], rfc5280.policyQualifierInfoMap)

                    pqv, rest = der_decoder(
                        pq['qualifier'],
                        asn1Spec=rfc5280.policyQualifierInfoMap[
                            pq['policyQualifierId']])
        
                    self.assertFalse(rest)
                    self.assertTrue(pqv.prettyPrint())
                    self.assertEqual(pq['qualifier'], der_encoder(pqv))

                    if pq['policyQualifierId'] == rfc4476.id_qt_acps:
                        self.assertIn('example.com', pqv)
                        found_ac_policy_qualifier1 = True

                    if pq['policyQualifierId'] == rfc4476.id_qt_acunotice:
                        self.assertIn(20, pqv[0]['noticeNumbers'])
                        found_ac_policy_qualifier2 = True

        assert found_ac_policy_qualifier1
        assert found_ac_policy_qualifier2

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(1, asn1Object['acinfo']['version'])

        found_ac_policy_qualifier1 = False
        found_ac_policy_qualifier2 = False
        for extn in asn1Object['acinfo']['extensions']:
            if extn['extnID'] == rfc4476.id_pe_acPolicies:
                ev, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']],
                    decodeOpenTypes=True)

                self.assertFalse(rest)
                self.assertTrue(ev.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(ev))

                oid = univ.ObjectIdentifier((1, 3, 6, 1, 4, 1, 22112, 48, 10,))
                self.assertEqual(oid, ev[0]['policyIdentifier'])
        
                for pq in ev[0]['policyQualifiers']:

                    if pq['policyQualifierId'] == rfc4476.id_qt_acps:
                        self.assertIn('example.com', pq['qualifier'])
                        found_ac_policy_qualifier1 = True

                    if pq['policyQualifierId'] == rfc4476.id_qt_acunotice:
                        self.assertIn(20, pq['qualifier'][0]['noticeNumbers'])
                        found_ac_policy_qualifier2 = True

        assert found_ac_policy_qualifier1
        assert found_ac_policy_qualifier2


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
