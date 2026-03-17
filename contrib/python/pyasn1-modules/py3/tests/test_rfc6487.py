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
from pyasn1_modules import rfc6487


class CertificateWithManifestTestCase(unittest.TestCase):
    rpki_cert_pem_text = """\
MIIGCTCCBPGgAwIBAgICKJgwDQYJKoZIhvcNAQELBQAwRjERMA8GA1UEAxMIQTkwREM1QkUx
MTAvBgNVBAUTKDBDRkNFNzc4NTdGQ0YwMUYzOUQ5OUE2MkI0QUE2MkU2MTU5RTc2RjgwHhcN
MTkwODA2MDQwMzIyWhcNMjAxMDMxMDAwMDAwWjBGMREwDwYDVQQDEwhBOTFEMTY5MTExMC8G
A1UEBRMoREMwNEFGMTk4Qzk3RjI1ODJGMTVBRERFRUU3QzY4MjYxMUNBREE1MTCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAMksR6bPbZFpxlXID/2dhYFuS11agb6ACDUFJpII
41uw65tFIPT+Y4laccnYRcWPWMTvHLyj0ggU+bc2zJCTYfmGD/GW/Q3WW0A3niBCdXDfkrp2
DXvSTASJ5+wtVb+AE74C4Mr3UiMOXhJre1rRd5Lq7o6+TEKbVkmUrmTlbsz2Vs2F4//t5sCr
WjAVP9D5jUBGH2MInbleBP1Bwf+kIxD16OKftRb/vGLzk1UhLsbq22GGE0vZ2hnJP3CbyXkN
dLBraErzvyCnqYF7/yA0JL0KWRDwr7a9y37s8O3xOxhA/dL8hLZXllzJmoxvxHmq8D+5CjHv
2/EmH8ODGm2aAzcCAwEAAaOCAv8wggL7MB0GA1UdDgQWBBTcBK8ZjJfyWC8Vrd7ufGgmEcra
UTAfBgNVHSMEGDAWgBQM/Od4V/zwHznZmmK0qmLmFZ52+DAOBgNVHQ8BAf8EBAMCAQYwDwYD
VR0TAQH/BAUwAwEB/zBzBgNVHR8EbDBqMGigZqBkhmJyc3luYzovL3Jwa2kuYXBuaWMubmV0
L3JlcG9zaXRvcnkvQjMyMkE1RjQxRDY2MTFFMkEzRjI3RjdDNzJGRDFGRjIvRFB6bmVGZjg4
Qjg1MlpwaXRLcGk1aFdlZHZnLmNybDB+BggrBgEFBQcBAQRyMHAwbgYIKwYBBQUHMAKGYnJz
eW5jOi8vcnBraS5hcG5pYy5uZXQvcmVwb3NpdG9yeS85ODA2NTJFMEI3N0UxMUU3QTk2QTM5
NTIxQTRGNEZCNC9EUHpuZUZmODhCODUyWnBpdEtwaTVoV2VkdmcuY2VyMEoGA1UdIAEB/wRA
MD4wPAYIKwYBBQUHDgIwMDAuBggrBgEFBQcCARYiaHR0cHM6Ly93d3cuYXBuaWMubmV0L1JQ
S0kvQ1BTLnBkZjCCASgGCCsGAQUFBwELBIIBGjCCARYwXwYIKwYBBQUHMAWGU3JzeW5jOi8v
cnBraS5hcG5pYy5uZXQvbWVtYmVyX3JlcG9zaXRvcnkvQTkxRDE2OTEvNTBDNjkyOTI5RDI0
MTFFNzg2MUEyMjZCQzRGOUFFMDIvMH4GCCsGAQUFBzAKhnJyc3luYzovL3Jwa2kuYXBuaWMu
bmV0L21lbWJlcl9yZXBvc2l0b3J5L0E5MUQxNjkxLzUwQzY5MjkyOUQyNDExRTc4NjFBMjI2
QkM0RjlBRTAyLzNBU3ZHWXlYOGxndkZhM2U3bnhvSmhISzJsRS5tZnQwMwYIKwYBBQUHMA2G
J2h0dHBzOi8vcnJkcC5hcG5pYy5uZXQvbm90aWZpY2F0aW9uLnhtbDArBggrBgEFBQcBBwEB
/wQcMBowGAQCAAEwEgMEAdQI5gMEAdQI/gMEAdRcZjANBgkqhkiG9w0BAQsFAAOCAQEAGvJ+
s7VgIZk8LDSz6uvsyX80KzZgaqMF7sMsqln0eo5KiGGBHjwvZuiDf46xbNseWW2nwAHmjLda
osCbcTGVu0JzFYBdkimgyHiq2l8yEchh5BUXr8x4CQIxwGEZEOlEp5mRa/AfHVEfDeMm7mob
eiCfyTC8q8KH9Tb/rY192kBe+n9MuRyn7TkimV5eYMdwWMyT/VSBCQzzfJ0r+S9o0rBYWH9k
HDFd3u1ztO8WGjH/LOehoO30xsm52kbxZjc4SJWubgBgxTMIWyjPHbKqCF44NwYev/6eFcOC
+KTEQ/hydcURm3YtX7EZLDtksWB2me576J8opeLsbNeNgzfJpg==
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        access_methods = [
            rfc6487.id_ad_rpkiManifest,
            rfc6487.id_ad_signedObject,
        ]

        substrate = pem.readBase64fromText(self.rpki_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        count = 0

        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5280.id_pe_subjectInfoAccess:
                extnValue, rest = der_decoder(
                    extn['extnValue'], asn1Spec=rfc5280.SubjectInfoAccessSyntax())
                for ad in extnValue:
                    if ad['accessMethod'] in access_methods:
                        uri = ad['accessLocation']['uniformResourceIdentifier']
                        self.assertIn('rpki.apnic.net', uri)
                        count += 1

        self.assertEqual(1, count)


class CertificateWithSignedObjectTestCase(unittest.TestCase):
    rpki_cert_pem_text = """\
MIIEuDCCA6CgAwIBAgICBhgwDQYJKoZIhvcNAQELBQAwMzExMC8GA1UEAxMoNmQ2
ZmJmYTk3NTNkYjhkODQ2NDMzZGI1MzUxZDlhOWVjMDdjOTZiZDAeFw0xOTA4MjAw
MDQ5MjlaFw0yMDA3MDEwMDAwMDBaMDMxMTAvBgNVBAMTKDVCODNERDg3REU5QUM3
QzZFMzRCODc3REY1MDFBMkIxMjMwQTgxQjQwggEiMA0GCSqGSIb3DQEBAQUAA4IB
DwAwggEKAoIBAQCXJw4ElLYkHnpRkLE8djruXIJn6uij0hIDM/18ma0c48HKlEPw
8jGB8kSSvLpimY3zLvqyLaD0pBNulovT3Ep37mPkskvXIUwLTThjBEujG7R4zU5g
RV2q+QhRjPLXiyER5QnEnHrIEke6tnioEPnC4i02Z6JdDKZrn9jSLPlet2OB5/0+
0DqLnYPZ1LZrId9YVDeIyBDRFxbzQ6L5K2stua5fWqhX1vnbZKDbXSY6d+u5zVwn
adxnRP989EiKk/MJ4Reu7YEdtpsM3sd7prXkAcJjPokdvL7hy+BOY8ESgaIhIBj2
Kqu4G35HKBbUdwFekBikitmiVJlIvvVYXku/AgMBAAGjggHUMIIB0DAdBgNVHQ4E
FgQUW4Pdh96ax8bjS4d99QGisSMKgbQwHwYDVR0jBBgwFoAUbW+/qXU9uNhGQz21
NR2ansB8lr0wGAYDVR0gAQH/BA4wDDAKBggrBgEFBQcOAjBQBgNVHR8ESTBHMEWg
Q6BBhj9yc3luYzovL2NhLnJnLm5ldC9ycGtpL1JHbmV0LU9VL2JXLV9xWFU5dU5o
R1F6MjFOUjJhbnNCOGxyMC5jcmwwZAYIKwYBBQUHAQEEWDBWMFQGCCsGAQUFBzAC
hkhyc3luYzovL3Jwa2kucmlwZS5uZXQvcmVwb3NpdG9yeS9ERUZBVUxUL2JXLV9x
WFU5dU5oR1F6MjFOUjJhbnNCOGxyMC5jZXIwDgYDVR0PAQH/BAQDAgeAMIGKBggr
BgEFBQcBCwR+MHwwSwYIKwYBBQUHMAuGP3JzeW5jOi8vY2EucmcubmV0L3Jwa2kv
UkduZXQtT1UvVzRQZGg5NmF4OGJqUzRkOTlRR2lzU01LZ2JRLnJvYTAtBggrBgEF
BQcwDYYhaHR0cHM6Ly9jYS5yZy5uZXQvcnJkcC9ub3RpZnkueG1sMB8GCCsGAQUF
BwEHAQH/BBAwDjAMBAIAATAGAwQAkxwtMA0GCSqGSIb3DQEBCwUAA4IBAQCoYaCd
17R3o7xul5BWgk8SXItdIDoDb7zxVqs/gnzl9i5gdDd0IWIy4gGW32EjsTUXsi+G
1gyv7aWYFQNlR7kvBgfHyPPp2rkFIj9/KK1VygG3FFMaO1JBDB8UOU+tRbV6xGcf
IYCk5bH6H9BtkPm2kiczVdjCFIB5krMy+DMf3x1F7/G+5f+ZmUG3b93GfUmzgxw9
IjlQMyt35h3rgOK6EjpOJgUA1jUWNTpPsR/xzA0HaDlbW38ue02SNluztrsXxJSr
8XwXhHPUzmlqg89Mb5iem3WZ5lkbr6lteO+ZocYtLPyOHhNmXWgop764K4JQaf46
WYtY4rWNeHcfgNTz
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        access_methods = [
            rfc6487.id_ad_rpkiManifest,
            rfc6487.id_ad_signedObject,
        ]

        substrate = pem.readBase64fromText(self.rpki_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        count = 0
        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5280.id_pe_subjectInfoAccess:
                extnValue, rest = der_decoder(
                    extn['extnValue'], asn1Spec=rfc5280.SubjectInfoAccessSyntax())
                for ad in extnValue:
                    if ad['accessMethod'] in access_methods:
                        uri = ad['accessLocation']['uniformResourceIdentifier']
                        self.assertIn('ca.rg.net', uri)
                        count += 1

        self.assertEqual(1, count)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
