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
from pyasn1_modules import rfc8520


class MUDCertTestCase(unittest.TestCase):
    mud_cert_pem_text = """\
MIIFODCCAyCgAwIBAgICEEAwDQYJKoZIhvcNAQELBQAwZTELMAkGA1UEBhMCQ0gx
DzANBgNVBAgMBlp1cmljaDERMA8GA1UEBwwIV2V0emlrb24xEDAOBgNVBAoMB0lt
UmlnaHQxIDAeBgNVBAMMF0ltUmlnaHQgVGVzdCA4MDIuMUFSIENBMB4XDTE5MDUw
MTE4MDMyMVoXDTE5MDUzMTE4MDMyMVowZzELMAkGA1UEBhMCQ0gxEzARBgNVBAgM
ClNvbWUtU3RhdGUxITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDEg
MB4GA1UEAwwXTGlnaHRidWxiMjAwMCwgU04jMjAyMDIwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQCzntv6tCdkZWPUx+CK9A9PCgKF8zGCJwdU4eIjo0oe
A81i7iltOPnU416GJMEc2jGhlZPn2Rjjy8tPbyh1RVBfkgdq4UPWPnZPb+Gkq1c8
X8zLRrMSWKqkSGOPENieDuQpzcrkMfj7dCPcxTcJ5Gluv1jEI7bxoZOZXjNxaFXi
vsaZWFub7b+5zDLWpvmpKDaeCU+gad7rWpRE/Hjh3FX8paW8KE/hMF/au4xX2Qj/
rDwHSxgs3n8FtuFUELotSgL3Acy3aISmJILBx6XrSs3nLruZzamulwWupSryHo3L
U+GsOETiXwxiyrfOZo3aJNnWzlEvrYCQGyqd8Nd/XOENAgMBAAGjge8wgewwCQYD
VR0TBAIwADBABggrBgEFBQcBGQQ0FjJodHRwczovL3d3dy5vZmNvdXJzZWltcmln
aHQuY29tL0x1bWluYWlyZV8xNTAuanNvbjBdBggrBgEFBQcBHgRRME8xCzAJBgNV
BAYTAkNIMSswKQYJKoZIhvcNAQkBFhxhc2NlcnRpYUBvZmNvdXJzZWltcmlnaHQu
Y29tMRMwEQYDVQQDEwpFbGlvdCBMZWFyMB0GA1UdDgQWBBS00spi6cRFdqz95TQI
9AuPn5/DRjAfBgNVHSMEGDAWgBREKvrASIa7JJ41mQWDkJ06rXTCtTANBgkqhkiG
9w0BAQsFAAOCAgEAiS4OlazkDpgR4qhrq5Wpx6m3Bmkk5RkXnqey1yyhyfZlAGH7
ewQiybkF3nN6at/TcNWMRfGBLhRrQn1h75KEXKlc18RDorj72/bvkbJLoBmA43Mv
xMF0w4YX8pQwzb4hSt04p79P2RVVYM3ex/vdok0KkouhLTlxzY7vhv1T8WGTVQHJ
k2EyswS2nFa/OtIkwruXqJj+lotdV2yPgFav5j9lkw5VbOztlfSKT7qQInVm+VBI
/qddz/LOYrls1A7KHzWkTvOwmvQBqI4e9xLjc3r8K4pZyMd7EsmepYmLOU+pfINf
/sEjliCluR65mKcKGiUa5J31pzbVpCr6FM/NGEjqpp6F+slyNC8YM/UlaJK1W9ZI
W7JAhmfil5z1CtQILFSnUh4VneTVOaYg6+gXr169fXUDlMM4ECnuqWAE2PLhfhI8
+lY8u18rFiX0bNSiUySgxU3asCC92xNmvJHuL4QwiYaGtTne36NMN7dH/32nMKl+
G3XA8cX8yZIrIkmWLBSji8UwOXwVhYovmbhHjaUMTQommxYv/Cuqi5nJUJfh5YJr
APeEK6fTYpPMiZ6U1++qzZDp78MRAq7UQbluJHh8ujPuK6kQmSLXmvK5yGpnJ+Cw
izaUuU1EEwgOMELjeFL62Ssvq8X+x6hZFCLygI7GNeitlblNhCXhFFurqMs=
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.mud_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        extn_list = []
        for extn in asn1Object['tbsCertificate']['extensions']:
            extn_list.append(extn['extnID'])

            if extn['extnID'] == rfc8520.id_pe_mudsigner:
                mudsigner, rest = der_decoder(
                    extn['extnValue'], rfc8520.MUDsignerSyntax())

                self.assertEqual(extn['extnValue'], der_encoder(mudsigner))

                c = rfc5280.X520countryName(value="CH")

                self.assertEqual(mudsigner[0][0][0]['value'], der_encoder(c))

                e = rfc5280.EmailAddress(value="ascertia@ofcourseimright.com")

                self.assertEqual(mudsigner[0][1][0]['value'], der_encoder(e))

                cn = rfc5280.X520CommonName()
                cn['printableString'] = "Eliot Lear"

                self.assertEqual(mudsigner[0][2][0]['value'], der_encoder(cn))

            if extn['extnID'] == rfc8520.id_pe_mud_url:
                mudurl, rest = der_decoder(
                    extn['extnValue'], rfc8520.MUDURLSyntax())

                self.assertEqual(extn['extnValue'], der_encoder(mudurl))
                self.assertEqual(".json", mudurl[-5:])

        self.assertIn(rfc8520.id_pe_mudsigner, extn_list)
        self.assertIn(rfc8520.id_pe_mud_url, extn_list)

    def testExtensionsMap(self):
        substrate = pem.readBase64fromText(self.mud_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] in rfc5280.certificateExtensionsMap.keys():
                extnValue, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']])
                self.assertEqual(extn['extnValue'], der_encoder(extnValue))


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
