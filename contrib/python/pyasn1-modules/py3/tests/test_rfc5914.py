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
from pyasn1_modules import rfc5914
from pyasn1_modules import rfc5652


class TrustAnchorListTestCase(unittest.TestCase):
    trust_anchor_list_pem_text = """\
MIIGGQYLKoZIhvcNAQkQASKgggYIMIIGBKGCAvYwggLyoAMCAQICAgDJMA0GCSqG
SIb3DQEBCwUAMBYxFDASBgNVBAMTC3JpcGUtbmNjLXRhMCAXDTE3MTEyODE0Mzk1
NVoYDzIxMTcxMTI4MTQzOTU1WjAWMRQwEgYDVQQDEwtyaXBlLW5jYy10YTCCASIw
DQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANFEWEhqlM9psgbDs3ltY0OjbMTb
5SzMoVpJ755fDYgQrP0/0tl7jSkDWfsAWcSIDz1dqRQRXkAL6B/1ivNx8ANuldrI
sJvzGNpymfjpcPsJac5WdadyKY9njXCq5orfAcAQvMSJs7ghmldI5EQdBmdIaB+j
JdN7pi6a0bJ+r9MTj9PpekHNWRzBVRW9/OSEOxUEE3FSMa3XjLKMiavXjJBOg6HJ
R4RfzZUpZV7mwEkPSlFqidPjrd0Al6+C1xAjH5KZFUdk2U/r+b+ufGx1bOmcUQ9W
+lJNbkCgMh1G5/7V7z/Ja4wImxs1bFw09i9MeBHcfkHYsT4Do4t4ATMi9lcCAwEA
AaOCAV4wggFaMB0GA1UdDgQWBBToVSsf1tGk9+QExtjlaA0evBY/wzAPBgNVHRMB
Af8EBTADAQH/MA4GA1UdDwEB/wQEAwIBBjCBsQYIKwYBBQUHAQsEgaQwgaEwPAYI
KwYBBQUHMAqGMHJzeW5jOi8vcnBraS5yaXBlLm5ldC9yZXBvc2l0b3J5L3JpcGUt
bmNjLXRhLm1mdDAyBggrBgEFBQcwDYYmaHR0cHM6Ly9ycmRwLnJpcGUubmV0L25v
dGlmaWNhdGlvbi54bWwwLQYIKwYBBQUHMAWGIXJzeW5jOi8vcnBraS5yaXBlLm5l
dC9yZXBvc2l0b3J5LzAYBgNVHSABAf8EDjAMMAoGCCsGAQUFBw4CMCcGCCsGAQUF
BwEHAQH/BBgwFjAJBAIAATADAwEAMAkEAgACMAMDAQAwIQYIKwYBBQUHAQgBAf8E
EjAQoA4wDDAKAgEAAgUA/////zCCAgIwggGIoAMCAQICCQDokdYGkU/O8jAKBggq
hkjOPQQDAzA/MQswCQYDVQQGEwJVUzELMAkGA1UECAwCVkExEDAOBgNVBAcMB0hl
cm5kb24xETAPBgNVBAoMCEJvZ3VzIENBMB4XDTE5MDUxNDA4NTgxMVoXDTIxMDUx
MzA4NTgxMVowPzELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAlZBMRAwDgYDVQQHDAdI
ZXJuZG9uMREwDwYDVQQKDAhCb2d1cyBDQTB2MBAGByqGSM49AgEGBSuBBAAiA2IA
BPBRdlSx6I5qpZ2sKUMIxun1gUAzzstOYWKvKCnMoNT1x+pIKDvMEMimFcLAxxL3
NVYOhK0Jty83SPDkKWMdx9/Okdhf3U/zxJlEnXDiFrAeM6xbG8zcCRiBnmd92Uvs
RqNQME4wHQYDVR0OBBYEFPI12zQE2qVV8r1pA5mwYuziFQjBMB8GA1UdIwQYMBaA
FPI12zQE2qVV8r1pA5mwYuziFQjBMAwGA1UdEwQFMAMBAf8wCgYIKoZIzj0EAwMD
aAAwZQIwWlGNjb9NyqJSzUSdsEqDSvMZb8yFkxYCIbAVqQ9UqScUUb9tpJKGsPWw
bZsnLVvmAjEAt/ypozbUhQw4dSPpWzrn5BQ0kKbDM3DQJcBABEUBoIOol1/jYQPm
xajQuxcheFlkooIBADCB/TB2MBAGByqGSM49AgEGBSuBBAAiA2IABOIIQup32CTe
oCxkpBPOQJwjcqkCCg43PyE2uI1TFPbVkZVL85YCjXEexNjLp59e76Dmf1qSEZZT
b+vAyz+u/Vs/RyTnmgculr6oL7tXGK9xpL14Oh7oWzxrZBErzDQrjAQUo53mH/na
OU/AbuiRy5Wl2jHiCp8MFURpZ2lDZXJ0IFRydXN0IEFuY2hvcjBSMEwxCzAJBgNV
BAYTAlVTMRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxJjAkBgNVBAMTHURpZ2lDZXJ0
IEVDQyBTZWN1cmUgU2VydmVyIENBggIFIIICZW4=
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.trust_anchor_list_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(rfc5914.id_ct_trustAnchorList, asn1Object['contentType'])

        tal, rest = der_decoder(asn1Object['content'], rfc5914.TrustAnchorList())

        self.assertFalse(rest)
        self.assertTrue(tal.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(tal))
        self.assertEqual(3, sum(1 for _ in tal))


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
