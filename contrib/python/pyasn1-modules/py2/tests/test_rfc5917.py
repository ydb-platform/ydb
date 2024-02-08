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
from pyasn1_modules import rfc5917


class ClearanceSponsorTestCase(unittest.TestCase):
    cert_pem_text = """\
MIID1DCCA1qgAwIBAgIUUc1IQGJpeYQ0XwOS2ZmVEb3aeZ0wCgYIKoZIzj0EAwMw
ZjELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAlZBMRAwDgYDVQQHEwdIZXJuZG9uMRAw
DgYDVQQKEwdFeGFtcGxlMQwwCgYDVQQLEwNQQ0ExGDAWBgNVBAMTD3BjYS5leGFt
cGxlLmNvbTAeFw0xOTExMDUyMjIwNDZaFw0yMDExMDQyMjIwNDZaMIGSMQswCQYD
VQQGEwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xEDAOBgNVBAoT
B0V4YW1wbGUxIjAgBgNVBAsTGUh1bWFuIFJlc291cmNlIERlcGFydG1lbnQxDTAL
BgNVBAMTBEZyZWQxHzAdBgkqhkiG9w0BCQEWEGZyZWRAZXhhbXBsZS5jb20wdjAQ
BgcqhkjOPQIBBgUrgQQAIgNiAAQObFslQ2EBP0xlDJ3sRnsNaqm/woQgKpBispSx
XxK5bWUVpfnWsZnjLWhtDuPcu1BcBlM2g7gwL/aw8nUSIK3D8Ja9rTUQQXc3zxnk
cl8+8znNXHMGByRjPUH87C+TOrqjggGaMIIBljAdBgNVHQ4EFgQU5m711OqFDNGR
SWMOSzTXjpTLIFUwbwYDVR0jBGgwZoAUJuolDwsyICik11oKjf8t3L1/VGWhQ6RB
MD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjER
MA8GA1UECgwIQm9ndXMgQ0GCCQCls1QoG7BuRjAPBgNVHRMBAf8EBTADAQH/MAsG
A1UdDwQEAwIBhjBCBglghkgBhvhCAQ0ENRYzVGhpcyBjZXJ0aWZpY2F0ZSBjYW5u
b3QgYmUgdHJ1c3RlZCBmb3IgYW55IHB1cnBvc2UuMBUGA1UdIAQOMAwwCgYIKwYB
BQUHDQIwCgYDVR02BAMCAQIwfwYDVR0JBHgwdjBJBgNVBDcxQjBABgsqhkiG9w0B
CRAHAwMCBeAxLTArgAsqhkiG9w0BCRAHBIEcMBoMGEhVTUFOIFJFU09VUkNFUyBV
U0UgT05MWTApBglghkgBZQIBBUQxHAwaSHVtYW4gUmVzb3VyY2VzIERlcGFydG1l
bnQwCgYIKoZIzj0EAwMDaAAwZQIwVh/RypULFgPpAN0I7OvuMomRWnm/Hea3Hk8P
tTRz2Zai8iYat7oeAmGVgMhSXy2jAjEAuJW4l/CFatBy4W/lZ7gS3weBdBa5WEDI
FFMC7GjGtCeLtXYqWfBnRdK26dOaHLB2
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        cs = rfc5917.DirectoryString()
        cs['utf8String'] = u'Human Resources Department'
        encoded_cs = der_encoder(cs)

        clearance_sponsor_found = False

        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5280.id_ce_subjectDirectoryAttributes:

                self.assertIn(extn['extnID'], rfc5280.certificateExtensionsMap)

                ev, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']])

                self.assertFalse(rest)
                self.assertTrue(ev.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(ev))

                for attr in ev:
                    if attr['type'] == rfc5917.id_clearanceSponsor:
                        self.assertEqual(encoded_cs, attr['values'][0])
                        clearance_sponsor_found = True

        self.assertTrue(clearance_sponsor_found)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        clearance_sponsor_found = False

        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5280.id_ce_subjectDirectoryAttributes:
                self.assertIn(extn['extnID'], rfc5280.certificateExtensionsMap)

                ev, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']],
                    decodeOpenTypes=True)

                self.assertFalse(rest)
                self.assertTrue(ev.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(ev))

                for attr in ev:
                    if attr['type'] == rfc5917.id_clearanceSponsor:
                        hrd = u'Human Resources Department'

                        self.assertEqual(hrd, attr['values'][0]['utf8String'])

                        clearance_sponsor_found = True

        self.assertTrue(clearance_sponsor_found)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
