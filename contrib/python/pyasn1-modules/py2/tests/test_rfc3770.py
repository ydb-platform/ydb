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
from pyasn1_modules import rfc5480
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc3770


class CertificateTestCase(unittest.TestCase):
    cert_pem_text = """\
MIICqzCCAjCgAwIBAgIJAKWzVCgbsG4/MAoGCCqGSM49BAMDMD8xCzAJBgNVBAYT
AlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9n
dXMgQ0EwHhcNMTkwNzE5MTk0MjQ3WhcNMjAwNzE4MTk0MjQ3WjBjMQswCQYDVQQG
EwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xGzAZBgNVBAoTElZp
Z2lsIFNlY3VyaXR5IExMQzEYMBYGA1UEAxMPZWFwLmV4YW1wbGUuY29tMHYwEAYH
KoZIzj0CAQYFK4EEACIDYgAEMMbnIp2BUbuyMgH9HhNHrh7VBy7ql2lBjGRSsefR
Wa7+vCWs4uviW6On4eem5YoP9/UdO7DaIL+/J9/3DJHERI17oFxn+YWiE4JwXofy
QwfSu3cncVNMqpiDjEkUGGvBo4HTMIHQMAsGA1UdDwQEAwIHgDBCBglghkgBhvhC
AQ0ENRYzVGhpcyBjZXJ0aWZpY2F0ZSBjYW5ub3QgYmUgdHJ1c3RlZCBmb3IgYW55
IHB1cnBvc2UuMB0GA1UdDgQWBBSDjPGr7M742rsE4oQGwBvGvllZ+zAfBgNVHSME
GDAWgBTyNds0BNqlVfK9aQOZsGLs4hUIwTAeBggrBgEFBQcBDQQSMBAEB0V4YW1w
bGUEBUJvZ3VzMB0GA1UdJQQWMBQGCCsGAQUFBwMOBggrBgEFBQcDDTAKBggqhkjO
PQQDAwNpADBmAjEAmCPZnnlUQOKlcOIIOgFrRCkOqO0ESs+dobYwAc2rFCBtQyP7
C3N00xkX8WZZpiAZAjEAi1Z5+nGbJg5eJTc8fwudutN/HNwJEIS6mHds9kfcy26x
DAlVlhox680Jxy5J8Pkx
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        sig_alg = asn1Object['tbsCertificate']['signature']

        self.assertEqual(rfc5480.ecdsa_with_SHA384, sig_alg['algorithm'])
        self.assertFalse(sig_alg['parameters'].hasValue())
    
        spki_alg = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc5480.id_ecPublicKey, spki_alg['algorithm'])
        self.assertEqual(
            rfc5480.secp384r1, spki_alg['parameters']['namedCurve'])

        extn_list = []
        for extn in asn1Object['tbsCertificate']['extensions']:
            extn_list.append(extn['extnID'])
            if extn['extnID'] in rfc5280.certificateExtensionsMap.keys():
                extnValue, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']])

                self.assertEqual(extn['extnValue'], der_encoder(extnValue))

                if extn['extnID'] == rfc3770.id_pe_wlanSSID:
                    self.assertIn(str2octs('Example'), extnValue)

                if extn['extnID'] == rfc5280.id_ce_extKeyUsage:
                    self.assertIn(rfc3770.id_kp_eapOverLAN, extnValue)
                    self.assertIn(rfc3770.id_kp_eapOverPPP, extnValue)

        self.assertIn(rfc3770.id_pe_wlanSSID, extn_list)
        self.assertIn(rfc5280.id_ce_extKeyUsage, extn_list)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
