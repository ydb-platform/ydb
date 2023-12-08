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
from pyasn1_modules import rfc7773


class AuthenticationContextExtnTestCase(unittest.TestCase):
    pem_text = """\
MIIMUjCCCzqgAwIBAgIQevDaX+wRYAlpUgjTYjCCRjANBgkqhkiG9w0BAQsFADCBuDELMAkGA1UE
BhMCU0UxNTAzBgNVBAoTLERldiBURVNUIENBIG9yZyBBQiAoTk9UIEEgUkVBTCBPUkdBTklaQVRJ
T04pMSAwHgYDVQQLExdDZW50cmFsIFNpZ25pbmcgU2VydmljZTEVMBMGA1UEBRMMQTEyMzQ1Ni03
ODkwMTkwNwYDVQQDEzBDZW50cmFsIFNpZ25pbmcgQ0EwMDEgLSBFSUQgMi4wIERldiBURVNUIFNl
cnZpY2UwHhcNMTkxMDA5MDc0ODI2WhcNMjAxMDA5MDc0ODI2WjBgMRUwEwYDVQQFEwwxODg4MDMw
OTkzNjgxCzAJBgNVBAYTAlNFMQ0wCwYDVQQqEwRBZ2RhMRcwFQYDVQQDEw5BZ2RhIEFuZGVyc3Nv
bjESMBAGA1UEBBMJQW5kZXJzc29uMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAjl1H
7vveI/EUaF9z6EiL/AmTHDbpLAKoWh9JJjpRlb8lU0TseYOzZp6ySiAO8St2a/HxxhrNuAAELUwZ
3oICkmxM/NeYgI7EEaLVPUwBAWfGZrRWb/+h8C6SrivWc73M/LI1A0B9tcEpUuh0CHTSVIBZsH+L
IDyKW6n3T8YeI7+0CX391I/j3iyEBNFcfDaHaFChzkPxgPg6Xh1D1JWs+mUj1rOoTLxsyusWiIQk
IkjDgFNUCpS1+NUvkTU1uFewvluxjOzRVqzYZWesOL+V/lGnyVPw4o1INEKYpOurYii2TXElTmXO
iQdIG20S96uFH6vFFJ2cPwgYjWpory/K+QIDAQABo4IIrTCCCKkwCwYDVR0PBAQDAgZAMB0GA1Ud
DgQWBBQo71oFnxX2kapLl3ZoYOylnJo01TATBgNVHSAEDDAKMAgGBgQAizABATBLBgNVHR8ERDBC
MECgPqA8hjpodHRwczovL2VpZDJjc2lnLmtvbmtpLnNlL3B1Ymxpc2gvY3JsLzE4MTRiMGFiYzEx
NGM3YmEuY3JsMIIH6wYHKoVwgUkFAQSCB94wggfaMIIH1gwraHR0cDovL2lkLmVsZWduYW1uZGVu
LnNlL2F1dGgtY29udC8xLjAvc2FjaQyCB6U8c2FjaTpTQU1MQXV0aENvbnRleHQgeG1sbnM6c2Fj
aT0iaHR0cDovL2lkLmVsZWduYW1uZGVuLnNlL2F1dGgtY29udC8xLjAvc2FjaSI+PHNhY2k6QXV0
aENvbnRleHRJbmZvIElkZW50aXR5UHJvdmlkZXI9Imh0dHA6Ly9kZXYudGVzdC5zd2VkZW5jb25u
ZWN0LnNlL2lkcCIgQXV0aGVudGljYXRpb25JbnN0YW50PSIyMDE5LTEwLTA5VDA3OjU4OjI2LjAw
MFoiIFNlcnZpY2VJRD0iRmVkU2lnbmluZyIgQXV0aG5Db250ZXh0Q2xhc3NSZWY9Imh0dHA6Ly9p
ZC5lbGVnbmFtbmRlbi5zZS9sb2EvMS4wL2xvYTMtc2lnbWVzc2FnZSIgQXNzZXJ0aW9uUmVmPSJf
ZGM5MjM0Y2Y3Zjc5OWQwMDlmMjUwNWVhMzVlMWU0NmUiLz48c2FjaTpJZEF0dHJpYnV0ZXM+PHNh
Y2k6QXR0cmlidXRlTWFwcGluZyBUeXBlPSJyZG4iIFJlZj0iMi41LjQuNSI+PHNhbWw6QXR0cmli
dXRlIEZyaWVuZGx5TmFtZT0iU3dlZGlzaCBQZXJzb25udW1tZXIiIE5hbWU9InVybjpvaWQ6MS4y
Ljc1Mi4yOS40LjEzIiB4bWxuczpzYW1sPSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoyLjA6YXNz
ZXJ0aW9uIj48c2FtbDpBdHRyaWJ1dGVWYWx1ZSB4c2k6dHlwZT0ieHM6c3RyaW5nIiB4bWxuczp4
cz0iaHR0cDovL3d3dy53My5vcmcvMjAwMS9YTUxTY2hlbWEiIHhtbG5zOnhzaT0iaHR0cDovL3d3
dy53My5vcmcvMjAwMS9YTUxTY2hlbWEtaW5zdGFuY2UiPjE4ODgwMzA5OTM2ODwvc2FtbDpBdHRy
aWJ1dGVWYWx1ZT48L3NhbWw6QXR0cmlidXRlPjwvc2FjaTpBdHRyaWJ1dGVNYXBwaW5nPjxzYWNp
OkF0dHJpYnV0ZU1hcHBpbmcgVHlwZT0icmRuIiBSZWY9IjIuNS40LjQyIj48c2FtbDpBdHRyaWJ1
dGUgRnJpZW5kbHlOYW1lPSJHaXZlbiBOYW1lIiBOYW1lPSJ1cm46b2lkOjIuNS40LjQyIiB4bWxu
czpzYW1sPSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoyLjA6YXNzZXJ0aW9uIj48c2FtbDpBdHRy
aWJ1dGVWYWx1ZSB4c2k6dHlwZT0ieHM6c3RyaW5nIiB4bWxuczp4cz0iaHR0cDovL3d3dy53My5v
cmcvMjAwMS9YTUxTY2hlbWEiIHhtbG5zOnhzaT0iaHR0cDovL3d3dy53My5vcmcvMjAwMS9YTUxT
Y2hlbWEtaW5zdGFuY2UiPkFnZGE8L3NhbWw6QXR0cmlidXRlVmFsdWU+PC9zYW1sOkF0dHJpYnV0
ZT48L3NhY2k6QXR0cmlidXRlTWFwcGluZz48c2FjaTpBdHRyaWJ1dGVNYXBwaW5nIFR5cGU9InJk
biIgUmVmPSIyLjUuNC4zIj48c2FtbDpBdHRyaWJ1dGUgRnJpZW5kbHlOYW1lPSJEaXNwbGF5IE5h
bWUiIE5hbWU9InVybjpvaWQ6Mi4xNi44NDAuMS4xMTM3MzAuMy4xLjI0MSIgeG1sbnM6c2FtbD0i
dXJuOm9hc2lzOm5hbWVzOnRjOlNBTUw6Mi4wOmFzc2VydGlvbiI+PHNhbWw6QXR0cmlidXRlVmFs
dWUgeHNpOnR5cGU9InhzOnN0cmluZyIgeG1sbnM6eHM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDEv
WE1MU2NoZW1hIiB4bWxuczp4c2k9Imh0dHA6Ly93d3cudzMub3JnLzIwMDEvWE1MU2NoZW1hLWlu
c3RhbmNlIj5BZ2RhIEFuZGVyc3Nvbjwvc2FtbDpBdHRyaWJ1dGVWYWx1ZT48L3NhbWw6QXR0cmli
dXRlPjwvc2FjaTpBdHRyaWJ1dGVNYXBwaW5nPjxzYWNpOkF0dHJpYnV0ZU1hcHBpbmcgVHlwZT0i
cmRuIiBSZWY9IjIuNS40LjQiPjxzYW1sOkF0dHJpYnV0ZSBGcmllbmRseU5hbWU9IlN1cm5hbWUi
IE5hbWU9InVybjpvaWQ6Mi41LjQuNCIgeG1sbnM6c2FtbD0idXJuOm9hc2lzOm5hbWVzOnRjOlNB
TUw6Mi4wOmFzc2VydGlvbiI+PHNhbWw6QXR0cmlidXRlVmFsdWUgeHNpOnR5cGU9InhzOnN0cmlu
ZyIgeG1sbnM6eHM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDEvWE1MU2NoZW1hIiB4bWxuczp4c2k9
Imh0dHA6Ly93d3cudzMub3JnLzIwMDEvWE1MU2NoZW1hLWluc3RhbmNlIj5BbmRlcnNzb248L3Nh
bWw6QXR0cmlidXRlVmFsdWU+PC9zYW1sOkF0dHJpYnV0ZT48L3NhY2k6QXR0cmlidXRlTWFwcGlu
Zz48L3NhY2k6SWRBdHRyaWJ1dGVzPjwvc2FjaTpTQU1MQXV0aENvbnRleHQ+MAkGA1UdEwQCMAAw
HwYDVR0jBBgwFoAUqKv0QPwAYcLfcD/Vy1A2deHtiqcwDQYJKoZIhvcNAQELBQADggEBAETlZOIL
NknxlMiYHCxoYypyzYuza2l3M4+YWakT0vFPgXpCk+l0dNst7h9nWvKKHCboSj+YP5dUCSsuUXhb
7xTei/F2nj7q1oCPuVJGThZqhWgF/JkqOy34hHEM5VniJiQu2W9TjzRMSOSFzRlQsHcOuXzdTkhr
CQpD1TWxYL9sCy4YoCdE4edfgBGBMujxoijl3/xJ5uI1FjhlSPVP88p8Wsi8i7GdMYuxqjZMwrt2
PHIPgop3BNN9/BzW0cmdyNvFgcD9qR8Rv5aFBYuQbyg6fST8JdAOrbMrCST6v2U41OOXH5MC/kL6
tAGXsYdcuQpglUngmo/FV4Z9qjIDkYQ=
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        extn_list = []

        for extn in asn1Object['tbsCertificate']['extensions']:
            extn_list.append(extn['extnID'])

            if extn['extnID'] == rfc7773.id_ce_authContext:
                s = extn['extnValue']
                acs, rest = der_decoder(
                    s, asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']])
                self.assertFalse(rest)
                self.assertTrue(acs.prettyPrint())
                self.assertEqual(s, der_encoder(acs))
                self.assertIn('id.elegnamnden.se', acs[0]['contextType'])
                self.assertIn(
                    'AuthContextInfo IdentityProvider', acs[0]['contextInfo'])

        self.assertIn(rfc7773.id_ce_authContext, extn_list)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
