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
from pyasn1_modules import rfc7585


class NAIRealmCertTestCase(unittest.TestCase):
    cert_pem_text = """\
MIIEZzCCA0+gAwIBAgIBBzANBgkqhkiG9w0BAQsFADCBkjELMAkGA1UEBhMCRlIx
DzANBgNVBAgMBlJhZGl1czESMBAGA1UEBwwJU29tZXdoZXJlMRQwEgYDVQQKDAtF
eGFtcGxlIEluYzEgMB4GCSqGSIb3DQEJARYRYWRtaW5AZXhhbXBsZS5vcmcxJjAk
BgNVBAMMHUV4YW1wbGUgQ2VydGlmaWNhdGUgQXV0aG9yaXR5MB4XDTE5MTExMTE4
MDQyMVoXDTIwMDExMDE4MDQyMVowezELMAkGA1UEBhMCRlIxDzANBgNVBAgMBlJh
ZGl1czEUMBIGA1UECgwLRXhhbXBsZSBJbmMxIzAhBgNVBAMMGkV4YW1wbGUgU2Vy
dmVyIENlcnRpZmljYXRlMSAwHgYJKoZIhvcNAQkBFhFhZG1pbkBleGFtcGxlLm9y
ZzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAM9HqbuyWpsTMKo739Dm
DwmQo2HUkNdQYbvsB+e7ILsw8fWa2qnsF1CoRr/1bcZqXUR1To/QbHse7xSMZH9t
F7rdlDMc7QtgdwVfn8TiL3hCg5LSE8iaBzfJUjrts/V5WOByP1DwJVM7W3Va/5dN
oOiceVeC7ThghMlwIx/wN5cy78a8fPYV2FvPR6e+U2HG35zaIv2PizYcliF/QmZG
gnw4Q9dYC1Lw/ogVBZBALlv+/MuGheb/xIuL8lu1PFZ0YbW65WLD9Cx4wvytAke7
tKlhL/Kd4OBSeOY3OYmpxbc1gEUmFoLTlZesY2NP9Jyl5mGsIHtPdvVkh/tSBy8o
VLUCAwEAAaOB3TCB2jAJBgNVHRMEAjAAMAsGA1UdDwQEAwIF4DATBgNVHSUEDDAK
BggrBgEFBQcDATA2BgNVHR8ELzAtMCugKaAnhiVodHRwOi8vd3d3LmV4YW1wbGUu
Y29tL2V4YW1wbGVfY2EuY3JsMDcGCCsGAQUFBwEBBCswKTAnBggrBgEFBQcwAYYb
aHR0cDovL3d3dy5leGFtcGxlLm9yZy9vY3NwMDoGA1UdEQQzMDGCEnJhZGl1cy5l
eGFtcGxlLm9yZ6AbBggrBgEFBQcICKAPDA0qLmV4YW1wbGUuY29tMA0GCSqGSIb3
DQEBCwUAA4IBAQBOhtH2Jpi0b0MZ8FBKTqDl44rIHL1rHG2mW/YYmRI4jZo8kFhA
yWm/T8ZpdaotJgRqbQbeXvTXIg4/JNFheyLG4yLOzS1esdMAYDD5EN9/dXE++jND
/wrfPU+QtTgzAjkgFDKuqO7gr1/vSizxLYTWLKBPRHhiQo7GGlEC6/CPb38x4mfQ
5Y9DsKCp6BEZu+LByCho/HMDzcIPCdtXRX7Fs8rtX4/zRpVIdm6D+vebuo6CwRKp
mIljfssCvZjb9YIxSVDmA/6Lapqsfsfo922kb+MTXvPrq2ynPx8LrPDrxKc8maYc
Jiw8B0yjkokwojxyRGftMT8uxNjWQVsMDbxl
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        nai_realm_oid = rfc7585.id_on_naiRealm
        nai_realm_found = False

        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5280.id_ce_subjectAltName:
                extnValue, rest = der_decoder(
                    extn['extnValue'], asn1Spec=rfc5280.SubjectAltName())

                self.assertFalse(rest)
                self.assertTrue(extnValue.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(extnValue))

                for gn in extnValue:
                    if gn['otherName'].hasValue():
                        self.assertEqual(
                            nai_realm_oid, gn['otherName']['type-id'])

                        onValue, rest = der_decoder(
                            gn['otherName']['value'], asn1Spec=rfc7585.NAIRealm())

                        self.assertFalse(rest)
                        self.assertTrue(onValue.prettyPrint())
                        self.assertEqual(
                            gn['otherName']['value'], der_encoder(onValue))
                        self.assertIn('example', onValue)

                        nai_realm_found = True

        self.assertTrue(nai_realm_found)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        nai_realm_oid = rfc7585.id_on_naiRealm
        nai_realm_found = False

        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5280.id_ce_subjectAltName:
                extnValue, rest = der_decoder(
                    extn['extnValue'], asn1Spec=rfc5280.SubjectAltName(),
                    decodeOpenTypes=True)

                self.assertFalse(rest)
                self.assertTrue(extnValue.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(extnValue))

                for gn in extnValue:
                    if gn['otherName'].hasValue():
                        self.assertEqual(
                            nai_realm_oid, gn['otherName']['type-id'])
                        self.assertIn('example', gn['otherName']['value'])

                        nai_realm_found = True

        self.assertTrue(nai_realm_found)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
