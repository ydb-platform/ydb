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
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc6486


class SignedManifestTestCase(unittest.TestCase):
    manifest_pem_text = """\
MIIHVAYJKoZIhvcNAQcCoIIHRTCCB0ECAQMxDTALBglghkgBZQMEAgEwgYwGCyqGSIb3DQEJ
EAEaoH0EezB5AgIK5xgPMjAxMjEwMjMyMjI2MDNaGA8yMDEyMTAyNTIyMjYwM1oGCWCGSAFl
AwQCATBGMEQWH1pYU0dCREJrTDgyVEZHSHVFNFZPWXRKUC1FNC5jcmwDIQCzTdC3GsuONsRq
RFnYf8+AJ2NnCIgmnc3O8PyfGvn18aCCBO4wggTqMIID0qADAgECAgIK5zANBgkqhkiG9w0B
AQsFADATMREwDwYDVQQDEwhBOTE5OTg4NTAeFw0xMjEwMjMyMjI2MDNaFw0xMjEwMjUyMjI2
MDNaMBgxFjAUBgNVBAMTDTUwODcxOTdjLTIwZjcwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAw
ggEKAoIBAQDEl4R4LiCs6zyR/IAeaRCfz0O0mXXAUKt8bmG6DXzaDYNG8dnBjbrsM1L05sb4
2Ti4TyE1UXtwFFEwatsFQ2uRBn9gsKmDGOjW8TH1AYObmZW+hZlEN7OLSz2bmPLtxIMwiCq/
vqmBJlMWPyCSym4iPnjzwWbJechqHSiTMOYGICF1QSW5xjJDAhRfeZG3nRY7TqfW8R2KJXeN
cKSYSGNKzv79B8GCswmwU8J8kcuryIiqb7WtcK2B6VBsROIQHGXM0UV4Zbnvv9m9Fl0SjvZJ
XyrzRjGzV2C00hM0f4jAplD9nJhAJ7nOTe8OnadrFABRga+Ge1HooeDQJGmTekLXAgMBAAGj
ggJBMIICPTAdBgNVHQ4EFgQUbcbOyNBHkRXXDaMq51jC7vOSHFUwHwYDVR0jBBgwFoAUZXSG
BDBkL82TFGHuE4VOYtJP+E4wDgYDVR0PAQH/BAQDAgeAMIGDBgNVHR8EfDB6MHigdqB0hnJy
c3luYzovL3Jwa2kuYXBuaWMubmV0L21lbWJlcl9yZXBvc2l0b3J5L0E5MTk5ODg1LzY1RkQ0
M0FBNUJFRjExREZBQjYxQjNFNzU1QUZFN0NGL1pYU0dCREJrTDgyVEZHSHVFNFZPWXRKUC1F
NC5jcmwwfgYIKwYBBQUHAQEEcjBwMG4GCCsGAQUFBzAChmJyc3luYzovL3Jwa2kuYXBuaWMu
bmV0L3JlcG9zaXRvcnkvQTNDMzhBMjRENjAzMTFEQ0FCMDhGMzE5NzlCREJFMzkvWlhTR0JE
QmtMODJURkdIdUU0Vk9ZdEpQLUU0LmNlcjAYBgNVHSABAf8EDjAMMAoGCCsGAQUFBw4CMIGQ
BggrBgEFBQcBCwSBgzCBgDB+BggrBgEFBQcwC4ZycnN5bmM6Ly9ycGtpLmFwbmljLm5ldC9t
ZW1iZXJfcmVwb3NpdG9yeS9BOTE5OTg4NS82NUZENDNBQTVCRUYxMURGQUI2MUIzRTc1NUFG
RTdDRi9aWFNHQkRCa0w4MlRGR0h1RTRWT1l0SlAtRTQubWZ0MBUGCCsGAQUFBwEIAQH/BAYw
BKACBQAwIQYIKwYBBQUHAQcBAf8EEjAQMAYEAgABBQAwBgQCAAIFADANBgkqhkiG9w0BAQsF
AAOCAQEAyBl1J+ql1O3d6JiaQEG2UAjDSKHSMVau++QcB6/yd4RuWv2KpQxk1cp+awf4Ttoh
GYakbUZQl7lJaXzbluG5siRSv6AowEWxf99iLhDx+pE1htklRfmmTE9oFpKnITAYZAUjarNC
sYGCZ00vSwRu27OdpSQbZQ7WdyDAhyHS0Sun0pkImVSqPO11gqyKV9ZCwCJUa5U/zsWDMNrj
MSZl1I3VoPs2rx997rLoiQiMqwGeoqfl7snpsL9OR/CazPmepuq3SyZNWcCrUGcGRhRdGScj
Tm2EHne1GiRHapn46HWQ3am8jumEKv5u0gLT4Mi9CyZwkDyhotGTJZmdAmN7zzGCAaowggGm
AgEDgBRtxs7I0EeRFdcNoyrnWMLu85IcVTALBglghkgBZQMEAgGgazAaBgkqhkiG9w0BCQMx
DQYLKoZIhvcNAQkQARowHAYJKoZIhvcNAQkFMQ8XDTEyMTAyMzIyMjYwNFowLwYJKoZIhvcN
AQkEMSIEIIu2XV8dT+rqQy5Cbpm3Tv5I1dwkLK8n2GesMGOr6/pEMA0GCSqGSIb3DQEBAQUA
BIIBAFsd0zkl4dIHrqZts441T+w/5/ekymDLFwftk6W+Mi35Htjvm2IHOthnKHQsK5h6dnEh
6DfNfc6tACmzLnM+UG7ve+uAhfpA+CUJIoVhpQvDH7Ntql0cD1X3d9ng484jpkVoHhbUIYNR
TyxvV4DV5EBbLYpx2HYf6wWa8TCobxUXNtw53OVA24ceavS+KvuDa0JQPFpbYUCS0UPMt/Im
mtKrWTmRUr8sYWdIQn+SStUh8iAR5rmSVr+Pe7aFbe2ju2FPf08gnIjH/SdCrJuFK8q7Z5MT
C9ijmXiajracUe+7eCluqgXRE8yRtnscWoA/9fVFz1lPwgEeNHLoaK7Sqew=
"""

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.manifest_pem_text)

        layers = rfc5652.cmsContentTypesMap.copy()

        getNextLayer = {
            rfc5652.id_ct_contentInfo: lambda x: x['contentType'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContentType'],
            rfc6486.id_ct_rpkiManifest: lambda x: None
        }

        getNextSubstrate = {
            rfc5652.id_ct_contentInfo: lambda x: x['content'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContent'],
            rfc6486.id_ct_rpkiManifest: lambda x: None
        }

        next_layer = rfc5652.id_ct_contentInfo

        while next_layer:
            asn1Object, rest = der_decoder(substrate, asn1Spec=layers[next_layer])

            self.assertFalse(rest)
            self.assertTrue(asn1Object.prettyPrint())
            self.assertEqual(substrate, der_encoder(asn1Object))

            substrate = getNextSubstrate[next_layer](asn1Object)
            next_layer = getNextLayer[next_layer](asn1Object)

        self.assertEqual(0, asn1Object['version'])

        for f in asn1Object['fileList']:
            self.assertEqual('ZXSGBDBkL82TFGHuE4VOYtJP-E4.crl', f['file'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.manifest_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=rfc5652.ContentInfo(), decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        oid = asn1Object['content']['encapContentInfo']['eContentType']
        substrate = asn1Object['content']['encapContentInfo']['eContent']

        self.assertIn(oid, rfc5652.cmsContentTypesMap)

        asn1Object, rest = der_decoder(
            substrate, asn1Spec=rfc5652.cmsContentTypesMap[oid],
            decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(0, asn1Object['version'])

        for f in asn1Object['fileList']:
            self.assertEqual('ZXSGBDBkL82TFGHuE4VOYtJP-E4.crl', f['file'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
