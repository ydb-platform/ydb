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
from pyasn1_modules import rfc3657
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5751


class EnvelopedDataTestCase(unittest.TestCase):
    env_data_pem_text = """\
MIIFfwYJKoZIhvcNAQcDoIIFcDCCBWwCAQIxU6JRAgEEMCMEECBlcTFnxBsPlsug
4KOCj78YDzIwMTkwOTEyMTIwMDAwWjANBgsqgwiMmks9AQEDAgQYS3mK9jQmvth1
iuBV8PEa89ICvmoomJCvMIIFEAYJKoZIhvcNAQcBMB8GCyqDCIyaSz0BAQECBBBC
T0dVU0lWX0JPR1VTSVYhgIIE4HPHsXoYyQ/4LRDiK4OrSuRJmmuDye5fH/hLcgw/
330Gsl1QBs9jF1CEDBM5ki657K/TRMl78Rqb3LIu5lfLQ8WVNGLsoQPwvxzIexGg
ShtYYwu8TcPiESFMa20SWpDEG8zFlmCbqQuc0buPxnvYviVThoBEthNC+S2Umed8
JpxwNKJbNTx5dxd2dkDNwpHsKgNzT9cGl0NF129Dspehqtdge5LJu3rj1gNynLRI
32AQ+pwU+sEHee6wDHhU5OWnHlndkm/9MTKY3woOhs1/KQFlRFPC6k71ZpUlncd3
93wLVIImfoMe4zWPAOnbpZ/M7zEJ95rTwwmudBs0qwMfCa3h0Vkg69w6fBHyc1IH
8u3VpSPcbOW4dUzJBDJPgB1kObAV02ZA4FQEuZtZiG13u3c7sSrHxsY1rtXssvSe
+5rThqPWgDqmH8b/yPGEHIFh03kHCDt/UZrdkLCO7a0WhCdY4I9hNU6OYEQmyEFs
0LsqEumn34Lv/XcD1wgLdPtF65zub4Wil/0Vpu73vIWLIk9LyNIXQSd6w0ZHUvVS
+jZZ1zrqIQKhKvG97NpKAYoHa4tOdoXHgBJUxw/uAOKkQ4jC5RS5UKqCZaQcArRD
2bCEEsutiuyf06MMcWm+RaBY1EwuX+/cT0D6CsWHYFAeQHgLuR4HVk5+PVKoOL/7
KUz0jUU5gzFVcmfaocyX5A6R90yggBObefcOIEj3v+5fjHkppfTvi/R03fVZ4Nyw
WyHbN7kOHHy8skJpcvNaqSY0dfkb8KOOoTptJH9rCBYtFlC5j/18y8Om9Um4h3/4
6hYO0xU8izJDzDzJnO/5KS5mGyskweIp3mrE1C/mw68LvrksxQI03CPtbM+FqOKe
0VcsAQykiOTnG3d4jLeF1iVrc9CgV+pwc5VfgQUwsGhjAFOCKTwWDrr3Je0yVsfz
gwY2zuM5uE/+usOSBt7SqbFTLOCba4fJrVVwi0wZig88owVTdl/ACxl2qyLUYC2u
5PNJSY6kx8Cgo4gDJk/3oeuys8JqgaufvKybl5GsdDaF3A7usZAjDR1EAWHZ7JGi
agtqbvISLD0zq4e4nmEhLnIRb7u5SNBPqe8qVuuQjIsvmP0ZuTlnh84ypFOQGz7c
fzHtr6UEQoGj8HImbp8diL4tflmFAVNaRjQzu18+2vFB2w1EZIe2/uNLs9ne2EIy
oK2Qb+mMCwJsNS0xOG0/TzPZ+y0Tp1/LupLHovMosPIGXlbvqZVh2xftDvbIigIM
WZQZ2tFxYD6Xc4zA00v7H0yGF1pRY+3GpobJkw0Y6ORtgdtdnr2ipioIeQCy0hUp
POmTeSr0L3H7KfNY7yQgZg0ra7FIEjM8tDoNqrhznetYUU1ZWM8Lyb3zMxxinSFs
GFGx2TiqPyixJNxN+lPT5D6GRhC9mXgh+BfVod5oINJJwXxJpT5xnsZgW8ujVxiu
1Vt5esXCZaXTGlyjVTH5dmCvJP9+B8n7dOimmCxCbMQKpNaZixJhoXWQtTgKqL1Q
f9WoEs6TDGgfTllqjbE4w3O7ZA7fAWe9jbAGwiPV5rF/NVvjaj2+ibtXbSNPW59d
dy1/2WzknVYnEHF0qZdBZ02Wh4ByXUC3FNvDu8hRTm5aq73DCqXLXUwNU8BvS1xB
bbRq5aYI2Rd3naNAns9dHqSvkg==
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.env_data_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(rfc5652.id_envelopedData, asn1Object['contentType'])

        ed, rest = der_decoder(
            asn1Object['content'], asn1Spec=rfc5652.EnvelopedData())

        self.assertFalse(rest)
        self.assertTrue(ed.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(ed))

        kwa = ed['recipientInfos'][0]['kekri']['keyEncryptionAlgorithm']
        self.assertEqual(rfc3657.id_camellia128_wrap, kwa['algorithm'])

        cea = ed['encryptedContentInfo']['contentEncryptionAlgorithm']
        self.assertEqual(rfc3657.id_camellia128_cbc, cea['algorithm'])
        param, rest = der_decoder(
            cea['parameters'], asn1Spec=rfc3657.Camellia_IV())

        self.assertFalse(rest)
        self.assertTrue(param.prettyPrint())
        self.assertEqual(cea['parameters'], der_encoder(param))

        iv = rfc3657.Camellia_IV(hexValue='424f47555349565f424f475553495621')
        self.assertEqual(iv, param)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.env_data_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertTrue(asn1Object['contentType'] in rfc5652.cmsContentTypesMap.keys())

        kekri = asn1Object['content']['recipientInfos'][0]['kekri']
        kwa = kekri['keyEncryptionAlgorithm']
        self.assertEqual(rfc3657.id_camellia128_wrap, kwa['algorithm'])

        eci = asn1Object['content']['encryptedContentInfo']
        cea = eci['contentEncryptionAlgorithm']
        self.assertEqual(rfc3657.id_camellia128_cbc, cea['algorithm'])

        iv = rfc3657.Camellia_IV(hexValue='424f47555349565f424f475553495621')
        self.assertEqual(iv, cea['parameters'])

class SMIMECapabilitiesTestCase(unittest.TestCase):
    smime_capabilities_pem_text = """\
MGYwDwYLKoMIjJpLPQEBAQIFADAPBgsqgwiMmks9AQEBAwUAMA8GCyqDCIyaSz0B
AQEEBQAwDwYLKoMIjJpLPQEBAwIFADAPBgsqgwiMmks9AQEDAwUAMA8GCyqDCIya
Sz0BAQMEBQA=
"""

    def setUp(self):
        self.asn1Spec = rfc5751.SMIMECapabilities()

    def testDerCodec(self):
        alg_oid_list = [
            rfc3657.id_camellia128_cbc,
            rfc3657.id_camellia192_cbc,
            rfc3657.id_camellia256_cbc,
            rfc3657.id_camellia128_wrap,
            rfc3657.id_camellia192_wrap,
            rfc3657.id_camellia256_wrap,
        ]

        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
 
        param = der_encoder(rfc3657.CamelliaSMimeCapability(""))
        count = 0
        for cap in asn1Object:
            self.assertEqual(cap['parameters'], param)
            self.assertTrue(cap['capabilityID'] in alg_oid_list)
            count += 1

        self.assertEqual(count, 6)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
 
        param = rfc3657.CamelliaSMimeCapability("")
        count = 0
        for cap in asn1Object:
            self.assertTrue(cap['capabilityID'] in rfc5751.smimeCapabilityMap.keys())
            self.assertEqual(cap['parameters'], param)
            count += 1

        self.assertEqual(count, 6)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
