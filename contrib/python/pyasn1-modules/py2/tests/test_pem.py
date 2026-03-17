#
# This file is part of pyasn1-modules software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.compat.octets import ints2octs
from pyasn1_modules import pem


class PemTestCase(unittest.TestCase):
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

    def testReadBase64fromText(self):

        binary = pem.readBase64fromText(self.pem_text)

        self.assertTrue(binary)

        expected = [
            48, 130, 3, 1, 48, 130, 1, 233, 2, 1, 0, 48, 129, 153, 49, 11, 48,
            9, 6, 3, 85, 4, 6, 19, 2, 82, 85, 49, 22, 48, 20, 6, 3, 85, 4, 8,
            19, 13, 77, 111, 115, 99, 111, 119, 32, 82, 101, 103, 105, 111,
            110, 49, 15, 48, 13, 6, 3, 85, 4, 7, 19, 6, 77, 111, 115, 99, 111,
            119, 49, 26, 48, 24, 6, 3, 85, 4, 10, 19, 17, 83, 78, 77, 80, 32,
            76, 97, 98, 111, 114, 97, 116, 111, 114, 105, 101, 115, 49, 12,
            48, 10, 6, 3, 85, 4, 11, 20, 3, 82, 38, 68, 49, 21, 48, 19, 6, 3,
            85, 4, 3, 19, 12, 115, 110, 109, 112, 108, 97, 98, 115, 46, 99,
            111, 109, 49, 32, 48, 30, 6, 9, 42, 134, 72, 134, 247, 13, 1, 9, 1,
            22, 17, 105, 110, 102, 111, 64, 115, 110, 109, 112, 108, 97, 98,
            115, 46, 99, 111, 109, 48, 130, 1, 34, 48, 13, 6, 9, 42, 134, 72,
            134, 247, 13, 1, 1, 1, 5, 0, 3, 130, 1, 15, 0, 48, 130, 1, 10, 2,
            130, 1, 1, 0, 189, 159, 99, 95, 25, 47, 124, 36, 48, 102, 1, 116,
            39, 250, 243, 84, 200, 240, 119, 64, 246, 2, 213, 204, 41, 95, 197,
            12, 98, 31, 76, 116, 9, 129, 100, 144, 167, 189, 122, 208, 62, 157,
            120, 128, 208, 47, 232, 4, 10, 197, 231, 230, 24, 68, 181, 111,
            230, 29, 251, 92, 30, 244, 112, 124, 0, 169, 181, 255, 204, 111,
            68, 53, 35, 2, 252, 184, 136, 237, 44, 168, 80, 143, 188, 135, 210,
            186, 53, 50, 164, 23, 31, 157, 30, 208, 19, 104, 173, 191, 42, 204,
            196, 32, 210, 17, 89, 204, 132, 167, 66, 53, 189, 183, 47, 100,
            233, 181, 73, 169, 188, 183, 27, 243, 7, 192, 48, 177, 108, 74,
            237, 178, 104, 235, 73, 202, 199, 169, 160, 127, 230, 188, 106,
            165, 70, 253, 173, 139, 111, 132, 217, 128, 52, 51, 179, 223, 243,
            124, 50, 87, 85, 230, 184, 75, 28, 246, 212, 54, 219, 154, 233,
            172, 57, 54, 187, 43, 33, 82, 9, 100, 12, 61, 182, 223, 154, 223,
            209, 12, 142, 54, 173, 192, 195, 112, 76, 172, 239, 108, 179, 90,
            163, 238, 247, 67, 133, 81, 213, 179, 17, 231, 77, 86, 196, 130,
            188, 68, 90, 74, 182, 26, 69, 74, 135, 212, 17, 120, 198, 118, 0,
            172, 94, 227, 72, 248, 162, 15, 142, 140, 165, 224, 184, 224, 160,
            128, 38, 71, 39, 37, 202, 197, 199, 48, 168, 38, 158, 11, 59, 91, 2,
            3, 1, 0, 1, 160, 34, 48, 32, 6, 9, 42, 134, 72, 134, 247, 13, 1, 9,
            2, 49, 19, 19, 17, 83, 78, 77, 80, 32, 76, 97, 98, 111, 114, 97,
            116, 111, 114, 105, 101, 115, 48, 13, 6, 9, 42, 134, 72, 134, 247,
            13, 1, 1, 5, 5, 0, 3, 130, 1, 1, 0, 8, 161, 111, 9, 141, 244, 205,
            155, 176, 211, 102, 244, 167, 241, 170, 33, 140, 170, 167, 6, 10,
            220, 200, 150, 144, 243, 255, 99, 85, 193, 143, 119, 112, 70, 108,
            111, 61, 208, 177, 199, 52, 66, 153, 39, 70, 108, 147, 74, 128, 240,
            32, 111, 181, 157, 65, 244, 191, 57, 19, 54, 255, 44, 6, 106, 125,
            35, 81, 157, 132, 104, 57, 216, 50, 26, 91, 2, 248, 183, 211, 185,
            89, 65, 224, 124, 116, 137, 164, 252, 89, 255, 51, 15, 218, 162,
            44, 116, 242, 126, 242, 141, 125, 5, 181, 209, 150, 231, 220, 239,
            119, 52, 220, 125, 105, 136, 103, 155, 245, 87, 30, 60, 134, 174,
            149, 33, 200, 235, 109, 177, 186, 66, 58, 127, 132, 162, 179, 143,
            111, 144, 54, 168, 139, 232, 218, 117, 177, 4, 111, 164, 216, 144,
            148, 55, 0, 196, 67, 89, 208, 41, 24, 105, 180, 5, 227, 221, 46,
            166, 232, 91, 86, 47, 239, 69, 145, 68, 81, 67, 242, 204, 94, 79,
            182, 48, 226, 170, 201, 162, 55, 103, 171, 76, 76, 28, 241, 159,
            42, 151, 250, 116, 155, 42, 90, 65, 228, 111, 144, 209, 110, 56,
            165, 213, 155, 125, 164, 155, 133, 235, 205, 18, 247, 11, 65, 205,
            40, 180, 134, 195, 7, 93, 31, 83, 205, 46, 199, 164, 200, 242, 144,
            189, 167, 152, 37, 88, 232, 14, 252, 49, 150, 169, 176, 44, 231,
            24, 102, 241, 236, 50
        ]

        self.assertEqual(ints2octs(expected), binary)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
