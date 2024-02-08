#
# This file is part of pyasn1-modules software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder

from pyasn1_modules import pem
from pyasn1_modules import rfc2314


class CertificationRequestTestCase(unittest.TestCase):
    pem_text = """\
MIIDATCCAekCAQAwgZkxCzAJBgNVBAYTAlJVMRYwFAYDVQQIEw1Nb3Njb3cgUmVn
aW9uMQ8wDQYDVQQHEwZNb3Njb3cxGjAYBgNVBAoTEVNOTVAgTGFib3JhdG9yaWVz
MQwwCgYDVQQLFANSJkQxFTATBgNVBAMTDHNubXBsYWJzLmNvbTEgMB4GCSqGSIb3
DQEJARYRaW5mb0Bzbm1wbGFicy5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAw
ggEKAoIBAQC9n2NfGS98JDBmAXQn+vNUyPB3QPYC1cwpX8UMYh9MdAmBZJCnvXrQ
Pp14gNAv6AQKxefmGES1b+Yd+1we9HB8AKm1/8xvRDUjAvy4iO0sqFCPvIfSujUy
pBcfnR7QE2itvyrMxCDSEVnMhKdCNb23L2TptUmpvLcb8wfAMLFsSu2yaOtJysep
oH/mvGqlRv2ti2+E2YA0M7Pf83wyV1XmuEsc9tQ225rprDk2uyshUglkDD2235rf
0QyONq3Aw3BMrO9ss1qj7vdDhVHVsxHnTVbEgrxEWkq2GkVKh9QReMZ2AKxe40j4
og+OjKXguOCggCZHJyXKxccwqCaeCztbAgMBAAGgIjAgBgkqhkiG9w0BCQIxExMR
U05NUCBMYWJvcmF0b3JpZXMwDQYJKoZIhvcNAQEFBQADggEBAAihbwmN9M2bsNNm
9KfxqiGMqqcGCtzIlpDz/2NVwY93cEZsbz3Qscc0QpknRmyTSoDwIG+1nUH0vzkT
Nv8sBmp9I1GdhGg52DIaWwL4t9O5WUHgfHSJpPxZ/zMP2qIsdPJ+8o19BbXRlufc
73c03H1piGeb9VcePIaulSHI622xukI6f4Sis49vkDaoi+jadbEEb6TYkJQ3AMRD
WdApGGm0BePdLqboW1Yv70WRRFFD8sxeT7Yw4qrJojdnq0xMHPGfKpf6dJsqWkHk
b5DRbjil1Zt9pJuF680S9wtBzSi0hsMHXR9TzS7HpMjykL2nmCVY6A78MZapsCzn
GGbx7DI=
"""

    def setUp(self):
        self.asn1Spec = rfc2314.CertificationRequest()

    def testDerCodec(self):

        substrate = pem.readBase64fromText(self.pem_text)

        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
