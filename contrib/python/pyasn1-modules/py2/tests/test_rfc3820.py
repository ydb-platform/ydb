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
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc3820


class ProxyCertificateTestCase(unittest.TestCase):
    pem_text = """\
MIID9DCCAtygAwIBAgIEDODd4TANBgkqhkiG9w0BAQUFADCBjTESMBAGCgmSJomT
8ixkARkWAm5sMRcwFQYKCZImiZPyLGQBGRYHZS1pbmZyYTEaMBgGA1UEChMRVHJh
aW5pbmcgU2VydmljZXMxDjAMBgNVBAsTBXVzZXJzMRowGAYDVQQLExFTZWN1cml0
eSBUcmFpbmluZzEWMBQGA1UEAxMNUGlldGplIFB1ayA0MjAeFw0xOTExMjcwODMz
NDZaFw0xOTExMjcyMDM4NDZaMIGhMRIwEAYKCZImiZPyLGQBGRYCbmwxFzAVBgoJ
kiaJk/IsZAEZFgdlLWluZnJhMRowGAYDVQQKExFUcmFpbmluZyBTZXJ2aWNlczEO
MAwGA1UECxMFdXNlcnMxGjAYBgNVBAsTEVNlY3VyaXR5IFRyYWluaW5nMRYwFAYD
VQQDEw1QaWV0amUgUHVrIDQyMRIwEAYDVQQDEwkyMTYwNjM0NTcwggEiMA0GCSqG
SIb3DQEBAQUAA4IBDwAwggEKAoIBAQCu2b1j1XQXAgNazmTtdp6jjzvNQT8221/c
dSIv2ftxr3UochHbazTfoR7wDT5PGlp2v99M0kZQvAEJ96CJpBDte4pwio7xHK3w
s5h7lH3W2ydrxAMSnZp0NHxyo3DNenTV5HavGjraOZDLt/k1aPJ8C68CBbrGDQxH
wzTs21Z+7lAy4C1ZNyOhkNF4qD5qy9Q2SHOPD+uc2QZE8IadZyxbeW/lEWHjESI1
5y55oLZhe3leb2NswvppgdwM8KW4Pbtya6mDKGH4e1qQfNfxsqlxbIBr4UaM8iSM
5BhJhe7VCny2iesGCJWz3NNoTJKBehN5o2xs7+fHv+sOW2Yuc3MnAgMBAAGjRjBE
MBMGA1UdJQQMMAoGCCsGAQUFBwMCMA4GA1UdDwEB/wQEAwIEsDAdBggrBgEFBQcB
DgEB/wQOMAwwCgYIKwYBBQUHFQEwDQYJKoZIhvcNAQEFBQADggEBAJbeKv3yQ9Yc
GHT4r64gVkKd4do7+cRS9dfWg8pcLRn3aBzTCBIznkg+OpzjteOJCuw6AxDsDPmf
n0Ms7LaAqegW8vcYgcZTxeABE5kgg5HTMUSMo39kFNTYHlNgsVfnOhpePnWX+e0Y
gPpQU7w1npAhr23lXn9DNWgWMMT6T3z+NngcJ9NQdEee9D4rzY5Oo9W/2OAPuMne
w5dGF7wVCUBRi6vrMnWYN8E3sHiFDJJrOsPWZzjRCa/W3N9A/OdgjitKQc3X4dlS
tP2J7Yxv/B/6+VxVEa9WtVXsm/wJnhwvICBscB1/4WkI0PfJ7Nh4ZqQplPdlDEKe
FOuri/fKBe0=
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        found_ppl = False
        for extn in asn1Object['tbsCertificate']['extensions']:
           if extn['extnID'] == rfc3820.id_pe_proxyCertInfo:
               self.assertTrue(rfc3820.id_pe_proxyCertInfo in rfc5280.certificateExtensionsMap.keys())
               pci, rest = der_decoder(
                   extn['extnValue'],
                   asn1Spec=rfc5280.certificateExtensionsMap[rfc3820.id_pe_proxyCertInfo])
               self.assertFalse(rest)
               self.assertTrue(pci.prettyPrint())
               self.assertEqual(extn['extnValue'], der_encoder(pci))

               self.assertEqual(rfc3820.id_ppl_inheritAll, pci['proxyPolicy']['policyLanguage'])
               found_ppl = True

        self.assertTrue(found_ppl)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
