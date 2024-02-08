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
from pyasn1.compat.octets import str2octs

from pyasn1_modules import pem
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc6120


class XMPPCertificateTestCase(unittest.TestCase):
    xmpp_server_cert_pem_text = """\
MIIC6DCCAm+gAwIBAgIJAKWzVCgbsG5DMAoGCCqGSM49BAMDMD8xCzAJBgNVBAYT
AlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9n
dXMgQ0EwHhcNMTkxMDI0MjMxNjA0WhcNMjAxMDIzMjMxNjA0WjBNMQswCQYDVQQG
EwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xHzAdBgNVBAoTFkV4
YW1wbGUgUHJvZHVjdHMsIEluYy4wdjAQBgcqhkjOPQIBBgUrgQQAIgNiAAQZzQlk
03nJRPF6+w1NxFELmQ5vJTjTRz3eu03CRtahK4Wnwd4GwbDe8NVHAEG2qTzBXFDu
p6RZugsBdf9GcEZHG42rThYYOzIYzVFnI7tQgA+nTWSWZN6eoU/EXcknhgijggEn
MIIBIzAdBgNVHQ4EFgQUkQpUMYcbUesEn5buI03POFnktJgwHwYDVR0jBBgwFoAU
8jXbNATapVXyvWkDmbBi7OIVCMEwCwYDVR0PBAQDAgeAMIGPBgNVHREEgYcwgYSg
KQYIKwYBBQUHCAegHRYbX3htcHAtY2xpZW50LmltLmV4YW1wbGUuY29toCkGCCsG
AQUFBwgHoB0WG194bXBwLXNlcnZlci5pbS5leGFtcGxlLmNvbaAcBggrBgEFBQcI
BaAQDA5pbS5leGFtcGxlLmNvbYIOaW0uZXhhbXBsZS5jb20wQgYJYIZIAYb4QgEN
BDUWM1RoaXMgY2VydGlmaWNhdGUgY2Fubm90IGJlIHRydXN0ZWQgZm9yIGFueSBw
dXJwb3NlLjAKBggqhkjOPQQDAwNnADBkAjAEo4mhDGC6/R39HyNgzLseNAp36qBH
yQJ/AWsBojN0av8akeVv9IuM45yqLKdiCzcCMDCjh1lFnCvurahwp5D1j9pAZMsg
nOzhcMpnHs2U/eN0lHl/JNgnbftl6Dvnt59xdA==
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.xmpp_server_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        count = 0

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
                        if gn_on['type-id'] == rfc6120.id_on_xmppAddr:
                            self.assertIn(gn_on['type-id'], rfc5280.anotherNameMap)

                            spec = rfc5280.anotherNameMap[gn['otherName']['type-id']]
                            on, rest = der_decoder(gn_on['value'], asn1Spec=spec)

                            self.assertFalse(rest)
                            self.assertTrue(on.prettyPrint())
                            self.assertEqual(gn_on['value'], der_encoder(on))
                            self.assertEqual('im.example.com', on)

                            count += 1

        self.assertEqual(1, count)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.xmpp_server_cert_pem_text)
        asn1Object, rest = der_decoder(substrate,
                                       asn1Spec=self.asn1Spec,
                                       decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        count = 0

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
                        if gn['otherName']['type-id'] == rfc6120.id_on_xmppAddr:
                            self.assertEqual(
                                'im.example.com', gn['otherName']['value'])
                            count += 1

        self.assertEqual(1, count)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
