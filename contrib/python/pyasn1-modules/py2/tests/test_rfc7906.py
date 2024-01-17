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
from pyasn1.type import univ

from pyasn1_modules import pem
from pyasn1_modules import rfc2985
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc7906


class AttributeSetTestCase(unittest.TestCase):
    attr_set_pem_text = """\
MYIRmDAQBglghkgBZQIBDQcxA4IBATAQBglghkgBZQIBDQ0xAwoBUzAQBglghkgB
ZQIBDQ4xAwoBAjAQBglghkgBZQIBDQ8xAwoBATARBglghkgBZQIBBUIxBAQCeQYw
EgYJYIZIAWUCAQ0LMQUwAwoBATAVBglghkgBZQIBDQUxCDAGAgReAA//MBUGCyqG
SIb3DQEJEAIuMQYCBF1qowYwGQYJYIZIAWUCAQVHMQwGCisGAQQBgaxgME0wGgYJ
YIZIAWUCAQ0BMQ0wCwYJYIZIAWUDBAEtMBoGCWCGSAFlAgENDDENBgsqhkiG9w0B
CRABGTAaBglghkgBZQIBDRUxDTALBglghkgBZQMEAS0wGwYJYIZIAWUCAQ0GMQ4w
DAIEXQAAAAIEXwAP/zAdBgsqhkiG9w0BCRACKDEOMAwGCisGAQQBgaxgMDAwLQYJ
YIZIAWUCAQVGMSAwHoYcaHR0cDovL3JlcG8uZXhhbXBsZS5jb20vcGtpLzAvBglg
hkgBZQIBDQMxIjAgExFCb2d1cyBTaG9ydCBUaXRsZYEFQm9ndXOFATCHAU0wNAYJ
YIZIAWUCAQVIMScwJRMRQm9ndXMgU2hvcnQgVGl0bGUTEEZha2UgU2hvcnQgVGl0
bGUwOAYIKwYBBQUHAQsxLDAqMCgGCCsGAQUFBzAFhhxodHRwOi8vcmVwby5leGFt
cGxlLmNvbS9wa2kvMEEGCyqGSIb3DQEJEAIEMTIwMAwjVGhlc2UgUkZDIDc5MDYg
YXR0cmlidXRlcyBhcmUgYm9ndXMGCSqGSIb3DQEHATCBggYLKoZIhvcNAQkQAgIx
czFxAgEBBgorBgEEAYGsYAEBMUwwJIAKYIZIAWUCAQgDA4EWMBQGCisGAQQBgaxg
MEkxBgIBMAIBSTAkgApghkgBZQIBCAMEgRYwFAYKKwYBBAGBrGAwRTEGAgEwAgFF
ExJCb2d1cyBQcml2YWN5IE1hcmswgYQGCWCGSAFlAgENFjF3MHUwMAYKYIZIAWUC
AQJOAjAiMCAGCyqGSIb3DQEJEAwLMREMD2t0YS5leGFtcGxlLmNvbTAxBgsqhkiG
9w0BCRABGTAiMCAGCyqGSIb3DQEJEAwLMREMD2t0YS5leGFtcGxlLmNvbTAOBgkq
hkiG9w0BBwEKAQEwgaAGCWCGSAFlAgENEDGBkjCBj6EMBgorBgEEAYGsYDAwoH8G
CWCGSAFlAgEQAARyMHAxCzAJBgNVBAYTAlVTMQswCQYDVQQIEwJWQTEQMA4GA1UE
BxMHSGVybmRvbjEQMA4GA1UEChMHRXhhbXBsZTEOMAwGA1UEAxMFQWxpY2UxIDAe
BgkqhkiG9w0BCQEWEWFsaWNlQGV4YW1wbGUuY29tMIIBvwYJYIZIAWUCAQVBMYIB
sDCCAawEFO1lDTbJmd4voc2GDuaMzYO+XJSmMIIBkqCB/jB/BglghkgBZQIBEAAE
cjBwMQswCQYDVQQGEwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24x
EDAOBgNVBAoTB0V4YW1wbGUxDjAMBgNVBAMTBUFsaWNlMSAwHgYJKoZIhvcNAQkB
FhFhbGljZUBleGFtcGxlLmNvbTB7BglghkgBZQIBEAAEbjBsMQswCQYDVQQGEwJV
UzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xEDAOBgNVBAoTB0V4YW1w
bGUxDDAKBgNVBAMTA0JvYjEeMBwGCSqGSIb3DQEJARYPYm9iQGV4YW1wbGUuY29t
MIGOMIGLBglghkgBZQIBEAAEfjB8MQswCQYDVQQGEwJVUzELMAkGA1UECBMCVkEx
EDAOBgNVBAcTB0hlcm5kb24xGzAZBgNVBAoTElZpZ2lsIFNlY3VyaXR5IExMQzEX
MBUGA1UECxMOS2V5IE1hbmFnZW1lbnQxGDAWBgNVBAMTD2t0YS5leGFtcGxlLmNv
bTCCAoUGA1UEJDGCAnwwggJ4MIIB/qADAgECAgkApbNUKBuwbjswCgYIKoZIzj0E
AwMwPzELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAlZBMRAwDgYDVQQHDAdIZXJuZG9u
MREwDwYDVQQKDAhCb2d1cyBDQTAeFw0xOTA1MjkxNDQ1NDFaFw0yMDA1MjgxNDQ1
NDFaMHAxCzAJBgNVBAYTAlVTMQswCQYDVQQIEwJWQTEQMA4GA1UEBxMHSGVybmRv
bjEQMA4GA1UEChMHRXhhbXBsZTEOMAwGA1UEAxMFQWxpY2UxIDAeBgkqhkiG9w0B
CQEWEWFsaWNlQGV4YW1wbGUuY29tMHYwEAYHKoZIzj0CAQYFK4EEACIDYgAE+M2f
By/sRA6V1pKFqecRTE8+LuAHtZxes1wmJZrBBg+bz7uYZfYQxI3dVB0YCSD6Mt3y
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
G8zcCRiBnmd92UvsRqNQME4wHQYDVR0OBBYEFPI12zQE2qVV8r1pA5mwYuziFQjB
MB8GA1UdIwQYMBaAFPI12zQE2qVV8r1pA5mwYuziFQjBMAwGA1UdEwQFMAMBAf8w
CgYIKoZIzj0EAwMDaAAwZQIwWlGNjb9NyqJSzUSdsEqDSvMZb8yFkxYCIbAVqQ9U
qScUUb9tpJKGsPWwbZsnLVvmAjEAt/ypozbUhQw4dSPpWzrn5BQ0kKbDM3DQJcBA
BEUBoIOol1/jYQPmxajQuxcheFlkMIICgzCCAgqgAwIBAgIJAKWzVCgbsG49MAoG
CCqGSM49BAMDMD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwH
SGVybmRvbjERMA8GA1UECgwIQm9ndXMgQ0EwHhcNMTkwNjEyMTQzMTA0WhcNMjAw
NjExMTQzMTA0WjB8MQswCQYDVQQGEwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcT
B0hlcm5kb24xGzAZBgNVBAoTElZpZ2lsIFNlY3VyaXR5IExMQzEXMBUGA1UECxMO
S2V5IE1hbmFnZW1lbnQxGDAWBgNVBAMTD2t0YS5leGFtcGxlLmNvbTB2MBAGByqG
SM49AgEGBSuBBAAiA2IABJf2XsTdVLcYASKJGtWjOAIFB8sXrsiE7G1tC+IP+iOE
dJCZ+UvJ9Enx7v6dtaU4uy1FzuWCar45BVpKVK2TNWT8E7XATkGBTIXGN76yJ5S0
9FdWp+hVkIkmyCJJujXzV6OBlDCBkTALBgNVHQ8EBAMCB4AwQgYJYIZIAYb4QgEN
BDUWM1RoaXMgY2VydGlmaWNhdGUgY2Fubm90IGJlIHRydXN0ZWQgZm9yIGFueSBw
dXJwb3NlLjAdBgNVHQ4EFgQUbZtc/QOvtbnVi/FknxpW4LWtTQ8wHwYDVR0jBBgw
FoAU8jXbNATapVXyvWkDmbBi7OIVCMEwCgYIKoZIzj0EAwMDZwAwZAIwBniWpO11
toMsV8fLBpBjA5YGQvd3TAcSw1lNbWpArL+hje1dzQ7pxslnkklv3CTxAjBuVebz
4mN0Qkew2NK/itwlmi7i+QxPs/MSZ7YFsyTA5Z4h2GbLW+zN3xNCC91vfpc=
"""

    def setUp(self):
        self.asn1Spec = rfc2985.AttributeSet()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.attr_set_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        for attr in asn1Object:
            self.assertIn(attr['type'], rfc5652.cmsAttributesMap)

            av, rest = der_decoder(
                attr['values'][0],
                asn1Spec=rfc5652.cmsAttributesMap[attr['type']])

            self.assertFalse(rest)
            self.assertTrue(av.prettyPrint())
            self.assertEqual(attr['values'][0], der_encoder(av))

            if attr['type'] == rfc7906.id_aa_KP_contentDecryptKeyID:
                self.assertEqual(univ.OctetString(hexValue='7906'), av)

    def testOpenTypes(self):
        openTypesMap = rfc5280.certificateAttributesMap.copy()
        openTypesMap.update(rfc5652.cmsAttributesMap)

        substrate = pem.readBase64fromText(self.attr_set_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, openTypes=openTypesMap,
            decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        for attr in asn1Object:
            if attr['type'] == rfc7906.id_aa_KP_contentDecryptKeyID:
                self.assertEqual(
                    univ.OctetString(hexValue='7906'), attr['values'][0])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
