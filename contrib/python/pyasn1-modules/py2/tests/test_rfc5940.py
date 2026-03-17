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
from pyasn1_modules import rfc2560
from pyasn1_modules import rfc5940
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5280


class CRLandOCSPResponseTestCase(unittest.TestCase):
    pem_text = """\
MIIHWQYJKoZIhvcNAQcCoIIHSjCCB0YCAQExDTALBglghkgBZQMEAgEwUwYJKoZI
hvcNAQcBoEYERENvbnRlbnQtVHlwZTogdGV4dC9wbGFpbg0KDQpXYXRzb24sIGNv
bWUgaGVyZSAtIEkgd2FudCB0byBzZWUgeW91Lg0KoIIBaDCCAWQwggEKoAMCAQIC
CQClWUKCJkwnGTAKBggqhkjOPQQDAjAkMRQwEgYDVQQKDAtleGFtcGxlLm9yZzEM
MAoGA1UEAwwDQm9iMB4XDTE3MTIyMDIzMDc0OVoXDTE4MTIyMDIzMDc0OVowJDEU
MBIGA1UECgwLZXhhbXBsZS5vcmcxDDAKBgNVBAMMA0JvYjBZMBMGByqGSM49AgEG
CCqGSM49AwEHA0IABIZP//xT8ah2ymmxfidIegeccVKuGxN+OTuvGq69EnQ8fUFD
ov2KNw8Cup0DtzAfHaZOMFWUu2+Vy3H6SLbQo4OjJTAjMCEGA1UdEQEB/wQXMBWG
E3NpcDpib2JAZXhhbXBsZS5vcmcwCgYIKoZIzj0EAwIDSAAwRQIhALIkjJJAKCI4
nsklf2TM/RBvuguWwRkHMDTVGxAvczlsAiAVjrFR8IW5vS4EzyePDVIua7b+Tzb3
THcQsVpPR53kDaGCBGQwggIbMIIBAwIBATANBgkqhkiG9w0BAQsFADBsMQswCQYD
VQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3d3cuZGln
aWNlcnQuY29tMSswKQYDVQQDEyJEaWdpQ2VydCBIaWdoIEFzc3VyYW5jZSBFViBS
b290IENBFw0xOTA1MDIyMjE1NTRaFw0xOTA1MjMyMjE1NTRaMDEwLwIQDPWCOBgZ
nlb4K9ZS7Sft6RcNMTgxMDI1MTYxMTM4WjAMMAoGA1UdFQQDCgEAoDAwLjAfBgNV
HSMEGDAWgBSxPsNpA/i/RwHUmCYaCALvY2QrwzALBgNVHRQEBAICAcQwDQYJKoZI
hvcNAQELBQADggEBABPO3OA0OkQZ+RLVxz/cNx5uNVEO416oOePkN0A4DxFztf33
7caS4OyfS9Wyu1j5yUdWJVpAKXSQeN95MqHkpSpYDssuqbuYjv8ViJfseGBgtXTc
zUzzNeNdY2uxMbCxuhmPkgacAo1lx9LkK2ScYHWVbfFRF1UQ/dcmavaZsEOBNuLW
OxQYA9MqfVNAymHe7vPqwm/8IY2FbHe9HsiJZfGxNWMDP5lmJiXmpntTeDQ2Ujdi
yXwGGKjyiSTFk2jVRutrGINufaoA/f7eCmIb4UDPbpMjVfD215dW8eBKouypCVoE
vmCSSTacdiBI2yOluvMN0PzvPve0ECAE+D4em9ahggJBBggrBgEFBQcQAjCCAjMK
AQCgggIsMIICKAYJKwYBBQUHMAEBBIICGTCCAhUwZqEgMB4xHDAJBgNVBAYTAlJV
MA8GA1UEAx4IAFQAZQBzAHQYEzIwMTkwNTA5MTU1MDQ4LjI1OVowLTArMBIwBwYF
Kw4DAhoEAQEEAQECAQGAABgTMjAxOTA1MDkxNTUwNDguMjYxWjAKBggqhkjOPQQD
AgNJADBGAiEAujFVH+NvuTLYa8RW3pvWSUwZfjOW5H5171JI+/50BjcCIQDhwige
wl+ts6TIvhU+CFoOipQBNKyKXKh7ngJkUtpZ86CCAVIwggFOMIIBSjCB8aADAgEC
AgEBMAoGCCqGSM49BAMCMB4xHDAJBgNVBAYTAlJVMA8GA1UEAx4IAFQAZQBzAHQw
HhcNMTkwMjAxMDUwMDAwWhcNMjIwMjAxMDUwMDAwWjAeMRwwCQYDVQQGEwJSVTAP
BgNVBAMeCABUAGUAcwB0MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEM0jxEYgg
RxC/r87uV/h6iZ8BAdHT/6fxRuzG0PRMIlFBy38skFUXJJulKV9JW16YJqOkVsqv
xwMM61z7p1vQ/qMgMB4wDwYDVR0TBAgwBgEB/wIBAzALBgNVHQ8EBAMCAAYwCgYI
KoZIzj0EAwIDSAAwRQIhAIdpCt5g89ofSADXmBD3KXQGnTghwbAMeWrKXqTGww+x
AiAl8NQgfUk4xMymZ3VtCLJ2MdczDps4Zh2KPOqAR5fZAjGCAQcwggEDAgEBMDEw
JDEUMBIGA1UECgwLZXhhbXBsZS5vcmcxDDAKBgNVBAMMA0JvYgIJAKVZQoImTCcZ
MAsGCWCGSAFlAwQCAaBpMBgGCSqGSIb3DQEJAzELBgkqhkiG9w0BBwEwHAYJKoZI
hvcNAQkFMQ8XDTE5MDEyNDIzNTI1NlowLwYJKoZIhvcNAQkEMSIEIO93j8lA1ebc
JXb0elmbMSYZWp8aInra81+iLAUNjRlaMAoGCCqGSM49BAMCBEcwRQIhAPeI7URq
tw//LB/6TAN0/Qh3/WHukXwxRbOJpnYVx0b6AiB3lK3FfwBhx4S5YSPMblS7goJl
ttTMEpl2prH8bbwo1g==
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)

        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(rfc5652.id_signedData, asn1Object['contentType'])

        sd, rest = der_decoder(
            asn1Object['content'], asn1Spec=rfc5652.SignedData())

        self.assertTrue(sd.prettyPrint())

        self.assertEqual(
            rfc5652.id_data, sd['encapContentInfo']['eContentType'])
        self.assertTrue(sd['encapContentInfo']['eContent'])

        v2 = rfc5280.Version(value='v2')

        self.assertEqual(v2, sd['crls'][0]['crl']['tbsCertList']['version'])

        ocspr_oid = rfc5940.id_ri_ocsp_response

        self.assertEqual(ocspr_oid, sd['crls'][1]['other']['otherRevInfoFormat'])

        ocspr, rest = der_decoder(
            sd['crls'][1]['other']['otherRevInfo'],
            asn1Spec=rfc5940.OCSPResponse())

        self.assertTrue(ocspr.prettyPrint())

        success = rfc2560.OCSPResponseStatus(value='successful')

        self.assertEqual(success, ocspr['responseStatus'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(rfc5652.id_signedData, asn1Object['contentType'])

        sd_eci = asn1Object['content']['encapContentInfo']

        self.assertEqual(rfc5652.id_data, sd_eci['eContentType'])
        self.assertTrue(sd_eci['eContent'].hasValue())

        for ri in asn1Object['content']['crls']:
            if ri.getName() == 'crl':
                v2 = rfc5280.Version(value='v2')
                self.assertEqual(v2, ri['crl']['tbsCertList']['version'])

            if ri.getName() == 'other':
                ori = ri['other']
                ocspr_oid = rfc5940.id_ri_ocsp_response

                self.assertEqual(ocspr_oid, ori['otherRevInfoFormat'])

                ocspr_status = ori['otherRevInfo']['responseStatus']
                success = rfc2560.OCSPResponseStatus(value='successful')

                self.assertEqual(success, ocspr_status)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
