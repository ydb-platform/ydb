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
from pyasn1_modules import rfc3709


class CertificateExtnWithUrlTestCase(unittest.TestCase):
    pem_text = """\
MIIC9zCCAn2gAwIBAgIJAKWzVCgbsG46MAoGCCqGSM49BAMDMD8xCzAJBgNVBAYT
AlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9n
dXMgQ0EwHhcNMTkwNTE0MTAwMjAwWhcNMjAwNTEzMTAwMjAwWjBlMQswCQYDVQQG
EwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xGzAZBgNVBAoTElZp
Z2lsIFNlY3VyaXR5IExMQzEaMBgGA1UEAxMRbWFpbC52aWdpbHNlYy5jb20wdjAQ
BgcqhkjOPQIBBgUrgQQAIgNiAATwUXZUseiOaqWdrClDCMbp9YFAM87LTmFirygp
zKDU9cfqSCg7zBDIphXCwMcS9zVWDoStCbcvN0jw5CljHcffzpHYX91P88SZRJ1w
4hawHjOsWxvM3AkYgZ5nfdlL7EajggEdMIIBGTALBgNVHQ8EBAMCB4AwQgYJYIZI
AYb4QgENBDUWM1RoaXMgY2VydGlmaWNhdGUgY2Fubm90IGJlIHRydXN0ZWQgZm9y
IGFueSBwdXJwb3NlLjAdBgNVHQ4EFgQU8jXbNATapVXyvWkDmbBi7OIVCMEwHwYD
VR0jBBgwFoAU8jXbNATapVXyvWkDmbBi7OIVCMEwgYUGCCsGAQUFBwEMBHkwd6J1
oHMwcTBvMG0WCWltYWdlL3BuZzAzMDEwDQYJYIZIAWUDBAIBBQAEIJtBNrMSSNo+
6Rwqwctmcy0qf68ilRuKEmlf3GLwGiIkMCsWKWh0dHA6Ly93d3cudmlnaWxzZWMu
Y29tL3ZpZ2lsc2VjX2xvZ28ucG5nMAoGCCqGSM49BAMDA2gAMGUCMGhfLH4kZaCD
H43A8m8mHCUpYt9unT0qYu4TCMaRuOTYEuqj3qtuwyLcfAGuXKp/oAIxAIrPY+3y
Pj22pmfmQi5w21UljqoTj/+lQLkU3wfy5BdVKBwI0GfEA+YL3ctSzPNqAA==
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

            if extn['extnID'] == rfc3709.id_pe_logotype:
                s = extn['extnValue']
                logotype, rest = der_decoder(s, rfc3709.LogotypeExtn())

                self.assertFalse(rest)
                self.assertTrue(logotype.prettyPrint())
                self.assertEqual(s, der_encoder(logotype))

                ids = logotype['subjectLogo']['direct']['image'][0]['imageDetails']

                self.assertEqual( "image/png", ids['mediaType'])

                expected = "http://www.vigilsec.com/vigilsec_logo.png"
                self.assertEqual(expected, ids['logotypeURI'][0])

        self.assertIn(rfc3709.id_pe_logotype, extn_list)

    def testExtensionsMap(self):
        substrate = pem.readBase64fromText(self.pem_text)
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


class CertificateExtnWithDataTestCase(unittest.TestCase):
    pem_text = """\
MIIJJDCCCAygAwIBAgIRAPIGo/5ScWbpAAAAAFwQBqkwDQYJKoZIhvcNAQELBQAw
gbkxCzAJBgNVBAYTAlVTMRYwFAYDVQQKEw1FbnRydXN0LCBJbmMuMSgwJgYDVQQL
Ex9TZWUgd3d3LmVudHJ1c3QubmV0L2xlZ2FsLXRlcm1zMTkwNwYDVQQLEzAoYykg
MjAxOCBFbnRydXN0LCBJbmMuIC0gZm9yIGF1dGhvcml6ZWQgdXNlIG9ubHkxLTAr
BgNVBAMTJEVudHJ1c3QgQ2VydGlmaWNhdGUgQXV0aG9yaXR5IC0gVk1DMTAeFw0x
OTA4MzAxNDMyMzlaFw0yMDAyMjUxNTAyMzZaMIIBjTEOMAwGA1UEERMFMTAwMTcx
CzAJBgNVBAYTAlVTMREwDwYDVQQIEwhOZXcgWW9yazERMA8GA1UEBxMITmV3IFlv
cmsxGDAWBgNVBAkTDzI3MCBQYXJrIEF2ZW51ZTETMBEGCysGAQQBgjc8AgEDEwJV
UzEZMBcGCysGAQQBgjc8AgECEwhEZWxhd2FyZTEfMB0GA1UEChMWSlBNb3JnYW4g
Q2hhc2UgYW5kIENvLjEdMBsGA1UEDxMUUHJpdmF0ZSBPcmdhbml6YXRpb24xNzA1
BgNVBAsTLkpQTUMgRmlyc3QgVmVyaWZpZWQgTWFyayBDZXJ0aWZpY2F0ZSBXb3Js
ZHdpZGUxDzANBgNVBAUTBjY5MTAxMTEXMBUGCisGAQQBg55fAQQTBzIwMTUzODkx
EjAQBgorBgEEAYOeXwEDEwJVUzEmMCQGCisGAQQBg55fAQITFmh0dHBzOi8vd3d3
LnVzcHRvLmdvdi8xHzAdBgNVBAMTFkpQTW9yZ2FuIENoYXNlIGFuZCBDby4wggEi
MA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCNLY+etlX06q1MxA1VT/P20h1i
eFGTzX4fqSQNG+ypmjNfLa8YXraO1v1hahenkRUWrVPW0Hq3zKNJcCDmosox6+tB
59u0b1xgN8y8D05AEC7qoVVdbaWKENMxCN4CDfST6d3YOqApjqEFAGZ71s39tRRG
kmWGJb4jKXcUX8FWV8w/vjKrpipZ8JsX2tuOp2uxFLkmi+V7gvN8tpbHUipP5K7L
190VOBytSWPudXefnYG3UWRfwah7Fq1bKYT/cCwStUm8XlfA8nUumeVsAiyC6phs
adn26MYiSddsBU08TGthmunLAO0+shaBy6jHYZxMa37S67vVlDpxbeF+TPVXAgMB
AAGjggROMIIESjATBgorBgEEAdZ5AgQDAQH/BAIFADCCArAGCCsGAQUFBwEMBIIC
ojCCAp6iggKaoIICljCCApIwggKOMIICihYNaW1hZ2Uvc3ZnK3htbDAzMDEwDQYJ
YIZIAWUDBAIBBQAEIBnwW6ChGgWWIRn3qn/xGAOlhDflA3z5jhZcZTNDlxF5MIIC
QhaCAj5kYXRhOmltYWdlL3N2Zyt4bWw7YmFzZTY0LEg0c0lBQUFBQUFBQUFJV1Iz
V3JqTUJCR3I1dW5tR3F2Rml4NUpQODBObkZLRTVhbTRFSmhJYmVMazZpT1dhOXRa
TWQyOXVrN2NsTG9SV25CMHNENGNPYVR0TGdmLzVYUWE5TVdkWlV3S1pDQnJ2YjFv
YWp5aEoyNlZ6NW45OHZaNHBaemVOU1ZObGxYbXhnZUR2Vk93MU5abnRwdWFvRlNB
b1YwNFBmMkVYNk5UVzA2ZUNsUE9YK3FRRXpON1dWR0RLRkFoTldwS0ErQVB3RTRK
MzNiNXg5REtBYTdyTlV2cG40dFNwMndycWpPRElwRHd0THNyTTBmeVlCaVYyM0Nq
bDNYeEs0N0RJTVlQRkdiM0ZXSTZKTHZpc1JqV1ZSL1B3TmxGRVh1OUpmTmJtQk1H
RFlqZy9PMTlvVWVWclh0QWtJWTBEY0o0N2JKOXBTb01iclZwdGVNd3VmTDJjMml5
Ym9qVU5veVlUOFFnL1VxWWtCNW41VW5QQWZYU2pub0tPbEl1eW5oOVRJVTh1Z3JF
YVMrVC9lRzZRWDh6OXl2YkdIZ0VLZjJ5S1h3dU9Sa2VsOGJQeFJoUHhtSnN0TDBT
bi9qOUtXWU8yR3dsM2EremNhbmhOYTV0YzZORkdHcVVFUUVwVmY0R3lVNnhOMnRx
WGgwWXQrM1BpcEhlK2l0cElRMGg0VHBoWnRrQ3plM0d6M2NjdllHbkp0cjZKVUNB
QUE9MCIGA1UdEQQbMBmCF2V4Y2hhZGRldi5sYWJtb3JnYW4uY29tMBMGA1UdJQQM
MAoGCCsGAQUFBwMfMA4GA1UdDwEB/wQEAwIHgDBmBggrBgEFBQcBAQRaMFgwIwYI
KwYBBQUHMAGGF2h0dHA6Ly9vY3NwLmVudHJ1c3QubmV0MDEGCCsGAQUFBzAChiVo
dHRwOi8vYWlhLmVudHJ1c3QubmV0L3ZtYzEtY2hhaW4uY2VyMDIGA1UdHwQrMCkw
J6AloCOGIWh0dHA6Ly9jcmwuZW50cnVzdC5uZXQvdm1jMWNhLmNybDBPBgNVHSAE
SDBGMDYGCmCGSAGG+mwKAQswKDAmBggrBgEFBQcCARYaaHR0cDovL3d3dy5lbnRy
dXN0Lm5ldC9ycGEwDAYKKwYBBAGDnl8BATAfBgNVHSMEGDAWgBSLtjl20DSQpj9i
4WTqPrz0fEahczAdBgNVHQ4EFgQUxAJ+yoDhzpPUzAPWKBYxg108dU0wCQYDVR0T
BAIwADANBgkqhkiG9w0BAQsFAAOCAQEAnqdB/vcwxFcxAlyCK0W5HOthXUdXRg9a
GwPDupqmLq2rKfyysZXonJJfr8jqO0f3l6TWTTJlXHljAwwXMtg3T3ngLyEzip5p
g0zH7s5eXjmWRhOeuHt21o611bXDbUNFTF0IpbYBTgOwAz/+k3XLVehf8dW7Y0Lr
VkzxJ6U82NxmqjaAnkm+H127x5/jPAr4LLD4gZfqFaHzw/ZLoS+fXFGs+dpuYE4s
n+xe0msYMu8qWABiMGA+MCKl45Dp5di+c2fyXtKyQ3rKI8XXZ0nN4bXK7DZd+3E3
kbpmR6cDliloU808Bi/erMkrfUHRoZ2d586lkmwkLcoDkJ/yPD+Jhw==
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

            if extn['extnID'] == rfc3709.id_pe_logotype:
                s = extn['extnValue']
                logotype, rest = der_decoder(s, rfc3709.LogotypeExtn())
                self.assertFalse(rest)

                self.assertTrue(logotype.prettyPrint())
                self.assertEqual(s, der_encoder(logotype))

                ids = logotype['subjectLogo']['direct']['image'][0]['imageDetails']

                self.assertEqual("image/svg+xml", ids['mediaType'])
                self.assertEqual(
                    "data:image/svg+xml;base64", ids['logotypeURI'][0][0:25])

        self.assertIn(rfc3709.id_pe_logotype, extn_list)

    def testExtensionsMap(self):
        substrate = pem.readBase64fromText(self.pem_text)
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
