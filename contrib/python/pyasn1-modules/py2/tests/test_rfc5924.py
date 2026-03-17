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
from pyasn1_modules import rfc5924


class SIPDomainCertTestCase(unittest.TestCase):
    cert_pem_text = """\
MIICiTCCAg+gAwIBAgIJAKWzVCgbsG5EMAoGCCqGSM49BAMDMD8xCzAJBgNVBAYT
AlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9n
dXMgQ0EwHhcNMTkxMDMwMjEwMDM0WhcNMjAxMDI5MjEwMDM0WjBsMQswCQYDVQQG
EwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xEDAOBgNVBAoTB0V4
YW1wbGUxEjAQBgNVBAsTCVNJUCBQcm94eTEYMBYGA1UEAxMPc2lwLmV4YW1wbGUu
Y29tMHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEcY3ckttSa6z3CfOFwZvPmZY8C9Ml
D1XOydz00+Vqifh1lydhDuulHrJaQ+QgVjG1TzlTAssD9GeABit/M98DPS/IC3wi
TsTMSyQ9/Oz4hKAw7x7lYEvufvycsZ7pJGRso4GpMIGmMEIGCWCGSAGG+EIBDQQ1
FjNUaGlzIGNlcnRpZmljYXRlIGNhbm5vdCBiZSB0cnVzdGVkIGZvciBhbnkgcHVy
cG9zZS4wHQYDVR0OBBYEFEcJ8iFWmJOl3Hg/44UFgFWNbe7FMB8GA1UdIwQYMBaA
FPI12zQE2qVV8r1pA5mwYuziFQjBMAsGA1UdDwQEAwIHgDATBgNVHSUEDDAKBggr
BgEFBQcDFDAKBggqhkjOPQQDAwNoADBlAjAXEPPNyXBUj40dzy+ZOqafuM3/6Fy6
bkgiIObcQImra96X10fe6qacanrbu4uU6d8CMQCQ+BCjCnOP4dBbNC3vB0WypxLo
UwZ6TjS0Rfr+dRvlyilVjP+hPVwbyb7ZOSZR6zk=
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

        found_kp_sipDomain = False

        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5280.id_ce_extKeyUsage:
                self.assertIn(
                    extn['extnID'], rfc5280.certificateExtensionsMap)

                ev, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']])

                self.assertFalse(rest)
                self.assertTrue(ev.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(ev))
                self.assertIn(rfc5924.id_kp_sipDomain, ev)

                found_kp_sipDomain = True

        self.assertTrue(found_kp_sipDomain)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
