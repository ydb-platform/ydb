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
from pyasn1_modules import rfc4683


class SIMCertificateTestCase(unittest.TestCase):
    cert_pem_text = """\
MIIDOzCCAsCgAwIBAgIJAKWzVCgbsG5KMAoGCCqGSM49BAMDMD8xCzAJBgNVBAYT
AlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9n
dXMgQ0EwHhcNMTkxMjExMjIzODUwWhcNMjAxMjEwMjIzODUwWjBOMQswCQYDVQQG
EwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xEDAOBgNVBAoTB0V4
YW1wbGUxDjAMBgNVBAMTBUhlbnJ5MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEZj80
YyLeDb0arJY8ZxBUMMxPEMT9+5WFVBCC1dPpUn25MmEpb82Dz1inv3xmG6sFKIHj
achlvkNGDXTUzZ1DdCF0O7gU5Z+YctwczGQVSt/2Ox0NWTiHLDpbpyoTyK0Bo4IB
dzCCAXMwHQYDVR0OBBYEFOjxtcL2ucMoTjS5MNKKpdKzXtz/MG8GA1UdIwRoMGaA
FPI12zQE2qVV8r1pA5mwYuziFQjBoUOkQTA/MQswCQYDVQQGEwJVUzELMAkGA1UE
CAwCVkExEDAOBgNVBAcMB0hlcm5kb24xETAPBgNVBAoMCEJvZ3VzIENBggkA6JHW
BpFPzvIwDwYDVR0TAQH/BAUwAwEB/zALBgNVHQ8EBAMCAYYwQgYJYIZIAYb4QgEN
BDUWM1RoaXMgY2VydGlmaWNhdGUgY2Fubm90IGJlIHRydXN0ZWQgZm9yIGFueSBw
dXJwb3NlLjB/BgNVHREEeDB2oGEGCCsGAQUFBwgGoFUwUzANBglghkgBZQMEAgEF
AAQgnrmI6yL2lM5kmfLVn28A8PVIVgE2S7HEFtfLExhg7HsEIOaAn/Pq8hb4qn/K
imN3uyZrjAv3Uspg0VYEcetJdHSCgRFoZW5yeUBleGFtcGxlLmNvbTAKBggqhkjO
PQQDAwNpADBmAjEAiWhD493OGnqfdit6SRdBjn3N6HVaMxyVO0Lfosjf9+9FDWad
rYt3o64YQqGz9NTMAjEAmahE0EMiu/TyzRDidlG2SxmY2aHg9hQO0t38i1jInJyi
9LjB81zHEL6noTgBZsan
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        found_PEPSI = False
        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5280.id_ce_subjectAltName:
                extnValue, rest = der_decoder(
                    extn['extnValue'], asn1Spec=rfc5280.SubjectAltName())

                self.assertFalse(rest)
                self.assertTrue(extnValue.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(extnValue))

                for gn in extnValue:
                    if gn['otherName'].hasValue():
                        gn_on = gn['otherName']
                        if gn_on['type-id'] == rfc4683.id_on_SIM:
                            self.assertIn(
                                gn_on['type-id'], rfc5280.anotherNameMap)

                            spec = rfc5280.anotherNameMap[gn_on['type-id']]

                            on, rest = der_decoder(
                                gn_on['value'], asn1Spec=spec)

                            self.assertFalse(rest)
                            self.assertTrue(on.prettyPrint())
                            self.assertEqual(gn_on['value'], der_encoder(on))

                            self.assertEqual(
                                 'e6809ff3ea', on['pEPSI'].prettyPrint()[2:12])

                            found_PEPSI = True

        self.assertTrue(found_PEPSI)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        found_PEPSI = False
        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5280.id_ce_subjectAltName:
                extnValue, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.SubjectAltName(),
                    decodeOpenTypes=True)

                self.assertFalse(rest)
                self.assertTrue(extnValue.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(extnValue))

                for gn in extnValue:
                    if gn['otherName'].hasValue():
                        pepsi = gn['otherName']['value']['pEPSI']
                        self.assertEqual(
                            'e6809ff3ea', pepsi.prettyPrint()[2:12])

                        found_PEPSI = True

        self.assertTrue(found_PEPSI)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
