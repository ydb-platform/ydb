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
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5751


class SignedMessageTestCase(unittest.TestCase):
    pem_text = """\
MIIGigYJKoZIhvcNAQcCoIIGezCCBncCAQExCTAHBgUrDgMCGjArBgkqhkiG9w0B
BwGgHgQcVGhpcyBpcyBzb21lIHNhbXBsZSBjb250ZW50LqCCAuAwggLcMIICm6AD
AgECAgIAyDAJBgcqhkjOOAQDMBIxEDAOBgNVBAMTB0NhcmxEU1MwHhcNOTkwODE3
MDExMDQ5WhcNMzkxMjMxMjM1OTU5WjATMREwDwYDVQQDEwhBbGljZURTUzCCAbYw
ggErBgcqhkjOOAQBMIIBHgKBgQCBjc3tg+oKnjk+wkgoo+RHk90O16gO7FPFq4QI
T/+U4XNIfgzW80RI0f6fr6ShiS/h2TDINt4/m7+3TNxfaYrkddA3DJEIlZvep175
/PSfL91DqItU8T+wBwhHTV2Iw8O1s+NVCHXVOXYQxHi9/52whJc38uRRG7XkCZZc
835b2wIVAOJHphpFZrgTxtqPuDchK2KL95PNAoGAJjjQFIkyqjn7Pm3ZS1lqTHYj
OQQCNVzyyxowwx5QXd2bWeLNqgU9WMB7oja4bgevfYpCJaf0dc9KCF5LPpD4beqc
ySGKO3YU6c4uXaMHzSOFuC8wAXxtSYkRiTZEvfjIlUpTVrXi+XPsGmE2HxF/wr3t
0VD/mHTC0YFKYDm6NjkDgYQAAoGAXOO5WnUUlgupet3jP6nsrF7cvbcTETSmFoko
ESPZNIZndXUTEj1DW2/lUb/6ifKiGz4kfT0HjVtjyLtFpaBK44XWzgaAP+gjfhry
JKtTGrgnDR7vCL9mFIBcYqxl+hWL8bs01NKWN/ZhR7LEMoTwfkFA/UanY04z8qXi
9PKD5bijgYEwfzAMBgNVHRMBAf8EAjAAMA4GA1UdDwEB/wQEAwIGwDAfBgNVHSME
GDAWgBRwRD6CLm+H3krTdeM9ILxDK5PxHzAdBgNVHQ4EFgQUvmyhs+PB9+1DcKTO
EwHi/eOX/s0wHwYDVR0RBBgwFoEUQWxpY2VEU1NAZXhhbXBsZS5jb20wCQYHKoZI
zjgEAwMwADAtAhRVDKQZH0IriXEiM42DarU9Z2u/RQIVAJ9hU1JUC1yy3drndh3i
EFJbQ169MYIDVDCCA1ACAQEwGDASMRAwDgYDVQQDEwdDYXJsRFNTAgIAyDAHBgUr
DgMCGqCCAuowGAYJKoZIhvcNAQkDMQsGCSqGSIb3DQEHATAjBgkqhkiG9w0BCQQx
FgQUQGrsCFJ5um4WAi2eBinAIpaH3UgwOAYDKqszMTEEL1RoaXMgaXMgYSB0ZXN0
IEdlbmVyYWwgQVNOIEF0dHJpYnV0ZSwgbnVtYmVyIDEuMD4GCyqGSIb3DQEJEAIE
MS8wLQwgQ29udGVudCBIaW50cyBEZXNjcmlwdGlvbiBCdWZmZXIGCSqGSIb3DQEH
ATBKBgkqhkiG9w0BCQ8xPTA7MAcGBSoDBAUGMDAGBioDBAUGTQQmU21pbWUgQ2Fw
YWJpbGl0aWVzIHBhcmFtZXRlcnMgYnVmZmVyIDIwbwYLKoZIhvcNAQkQAgoxYDBe
BgUqAwQFBgQrQ29udGVudCBSZWZlcmVuY2UgQ29udGVudCBJZGVudGlmaWVyIEJ1
ZmZlcgQoQ29udGVudCBSZWZlcmVuY2UgU2lnbmF0dXJlIFZhbHVlIEJ1ZmZlcjBz
BgsqhkiG9w0BCRACCzFkoGIwWjELMAkGA1UEBhMCVVMxFjAUBgNVBAoTDVVTIEdv
dmVybm1lbnQxETAPBgNVBAsTCFZEQSBTaXRlMQwwCgYDVQQLEwNWREExEjAQBgNV
BAMTCURhaXN5IFJTQQIEClVEMzCB/AYLKoZIhvcNAQkQAgMxgewwgekwgeYEBzU3
MzgyOTkYDzE5OTkwMzExMTA0NDMzWqGByTCBxqRhMF8xCzAJBgNVBAYTAlVTMRYw
FAYDVQQKEw1VUyBHb3Zlcm5tZW50MREwDwYDVQQLEwhWREEgU2l0ZTEMMAoGA1UE
CxMDVkRBMRcwFQYDVQQDEw5CdWdzIEJ1bm55IERTQaRhMF8xCzAJBgNVBAYTAlVT
MRYwFAYDVQQKEw1VUyBHb3Zlcm5tZW50MREwDwYDVQQLEwhWREEgU2l0ZTEMMAoG
A1UECxMDVkRBMRcwFQYDVQQDEw5FbG1lciBGdWRkIERTQTAJBgcqhkjOOAQDBC8w
LQIVALwzN2XE93BcF0kTqkyFyrtSkUhZAhRjlqIUi89X3rBIX2xk3YQESV8cyg==
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        smimeCapMap = {
            univ.ObjectIdentifier('1.2.3.4.5.6.77'): univ.OctetString(),
        }
        smimeCapMap.update(rfc5751.smimeCapabilityMap)

        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder (substrate,
                                        asn1Spec=self.asn1Spec,
                                        decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(rfc5652.id_signedData, asn1Object['contentType'])
        self.assertEqual(1, asn1Object['content']['version'])

        for si in asn1Object['content']['signerInfos']:
            self.assertEqual(1, si['version'])

            for attr in si['signedAttrs']:

                if attr['attrType'] == rfc5751.smimeCapabilities:
                    for scap in attr['attrValues'][0]:
                        if scap['capabilityID'] in smimeCapMap.keys():
                            scap_p, rest = der_decoder(scap['parameters'],
                                                       asn1Spec=smimeCapMap[scap['capabilityID']])
                            self.assertFalse(rest)
                            self.assertEqual(scap['parameters'], der_encoder(scap_p))
                            self.assertIn('parameters', scap_p.prettyPrint())

                if attr['attrType'] == rfc5751.id_aa_encrypKeyPref:
                    ekp_issuer_serial = attr['attrValues'][0]['issuerAndSerialNumber']

                    self.assertEqual(173360179, ekp_issuer_serial['serialNumber'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
