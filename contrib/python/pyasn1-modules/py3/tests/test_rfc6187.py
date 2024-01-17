#
# This file is part of pyasn1-modules software.
#
# Copyright (c) 2019, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder

from pyasn1_modules import pem
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc6187


class SSHClientCertificateTestCase(unittest.TestCase):
    cert_pem_text = """\
MIICkDCCAhegAwIBAgIJAKWzVCgbsG5BMAoGCCqGSM49BAMDMD8xCzAJBgNVBAYT
AlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9n
dXMgQ0EwHhcNMTkxMDI0MTgyNjA3WhcNMjAxMDIzMTgyNjA3WjB0MQswCQYDVQQG
EwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xEDAOBgNVBAoTB0V4
YW1wbGUxEDAOBgNVBAMTB0NoYXJsaWUxIjAgBgkqhkiG9w0BCQEWE2NoYXJsaWVA
ZXhhbXBsZS5jb20wdjAQBgcqhkjOPQIBBgUrgQQAIgNiAARfr1XPl5S0A/BwTOm4
/rO7mGVt2Tmfr3yvYnfN/ggMvyS3RiIXSsdzcAwzeqc907Jp7Dggab0PpaOKDOxD
WoK0g6B8+kC/VMsU23mfShlb9et8qcR3A8gdU6g8uvSMahWjgakwgaYwCwYDVR0P
BAQDAgeAMB0GA1UdDgQWBBQfwm5u0GoxiDcjhDt33UJYlvMPFTAfBgNVHSMEGDAW
gBTyNds0BNqlVfK9aQOZsGLs4hUIwTATBgNVHSUEDDAKBggrBgEFBQcDFTBCBglg
hkgBhvhCAQ0ENRYzVGhpcyBjZXJ0aWZpY2F0ZSBjYW5ub3QgYmUgdHJ1c3RlZCBm
b3IgYW55IHB1cnBvc2UuMAoGCCqGSM49BAMDA2cAMGQCMGEme38A3k8q4RGSEs2D
ThQQOQz3TBJrIW8zr92S8e8BNPkRcQDR+C72TEhL/qoPCQIwGpGaC4ERiUypETkC
voNP0ODFhhlpFo6lwVHd8Gu+6hShC2PKdAfs4QFDS9ZKgQeZ
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        ssh_eku_oids = [
            rfc6187.id_kp_secureShellClient,
            rfc6187.id_kp_secureShellServer,
        ]

        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        count = 0

        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5280.id_ce_extKeyUsage:
                extnValue, rest = der_decoder(
                    extn['extnValue'], asn1Spec=rfc5280.ExtKeyUsageSyntax())

                for oid in extnValue:
                    if oid in ssh_eku_oids:
                        count += 1

        self.assertEqual(1, count)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
