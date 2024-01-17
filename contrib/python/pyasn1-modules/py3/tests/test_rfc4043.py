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
from pyasn1_modules import rfc4043


class PermIdCertTestCase(unittest.TestCase):
    cert_pem_text = """\
MIIDDTCCApOgAwIBAgIJAKWzVCgbsG5HMAoGCCqGSM49BAMDMD8xCzAJBgNVBAYT
AlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9n
dXMgQ0EwHhcNMTkxMTEwMDA0MDIyWhcNMjAxMTA5MDA0MDIyWjBNMQswCQYDVQQG
EwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xEDAOBgNVBAoTB0V4
YW1wbGUxDTALBgNVBAMTBEdhaWwwdjAQBgcqhkjOPQIBBgUrgQQAIgNiAAQBoktg
/68xL+uEQaWBoHyOjw8EMLeMEng3R2H7yiEzTGoaMJgPOKvSfzB2P0paHYPL+B5y
Gc0CK5EHRujMl9ljH+Wydpk57rKBLo1ZzpWUS6anLGIkWs1sOakcgGGr7hGjggFL
MIIBRzAdBgNVHQ4EFgQU1pCNZuMzfEaJ9GGhH7RKy6Mvz+cwbwYDVR0jBGgwZoAU
8jXbNATapVXyvWkDmbBi7OIVCMGhQ6RBMD8xCzAJBgNVBAYTAlVTMQswCQYDVQQI
DAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9ndXMgQ0GCCQDokdYG
kU/O8jAPBgNVHRMBAf8EBTADAQH/MAsGA1UdDwQEAwIBhjBCBglghkgBhvhCAQ0E
NRYzVGhpcyBjZXJ0aWZpY2F0ZSBjYW5ub3QgYmUgdHJ1c3RlZCBmb3IgYW55IHB1
cnBvc2UuMFMGA1UdEQRMMEqgNgYIKwYBBQUHCAOgKjAoDBs4MjYyMDgtNDE3MDI4
LTU0ODE5NS0yMTUyMzMGCSsGAQQBgaxgMIEQZ2FpbEBleGFtcGxlLmNvbTAKBggq
hkjOPQQDAwNoADBlAjBT+36Y/LPaGSu+61P7kR97M8jAjtH5DtUwrWR02ChshvYJ
x0bpZq3PJaO0WlBgFicCMQCf+67wSvjxxtjI/OAg4t8NQIJW1LcehSXizlPDc772
/FC5OiUAxO+iFaSVMeDFsCo=
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        perm_id_oid = rfc4043.id_on_permanentIdentifier
        assigner_oid = univ.ObjectIdentifier('1.3.6.1.4.1.22112.48')
        permanent_identifier_found = False

        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5280.id_ce_subjectAltName:
                extnValue, rest = der_decoder(
                    extn['extnValue'], asn1Spec=rfc5280.SubjectAltName())

                self.assertFalse(rest)
                self.assertTrue(extnValue.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(extnValue))

                for gn in extnValue:
                    if gn['otherName'].hasValue():
                        self.assertEqual(perm_id_oid, gn['otherName']['type-id'])

                        onValue, rest = der_decoder(
                            gn['otherName']['value'],
                            asn1Spec=rfc4043.PermanentIdentifier())

                        self.assertFalse(rest)
                        self.assertTrue(onValue.prettyPrint())
                        self.assertEqual(gn['otherName']['value'], der_encoder(onValue))
                        self.assertEqual(assigner_oid, onValue['assigner'])
                        permanent_identifier_found = True

        self.assertTrue(permanent_identifier_found)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        perm_id_oid = rfc4043.id_on_permanentIdentifier
        assigner_oid = univ.ObjectIdentifier('1.3.6.1.4.1.22112.48')
        permanent_identifier_found = False

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
                        on = gn['otherName']
                        self.assertEqual(perm_id_oid, on['type-id'])
                        self.assertEqual(assigner_oid, on['value']['assigner'])
                        permanent_identifier_found = True

        self.assertTrue(permanent_identifier_found)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
