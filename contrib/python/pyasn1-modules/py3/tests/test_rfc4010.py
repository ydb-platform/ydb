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
from pyasn1_modules import rfc4010
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5751


class EnvelopedDataTestCase(unittest.TestCase):
    env_data_pem_text = """\
MIIFewYJKoZIhvcNAQcDoIIFbDCCBWgCAQIxUqJQAgEEMCMEEKBBI2KxDUPS5TCo
RCEDJo4YDzIwMTkwOTEyMTIwMDAwWjAMBgoqgxqMmkQHAQEBBBipFE2DxCLAx2Og
E53Jt21V8kAoscU7K3wwggUNBgkqhkiG9w0BBwEwHAYIKoMajJpEAQQEEEJPR1VT
SVZfQk9HVVNJViGAggTgc8exehjJD/gtEOIrg6tK5Emaa4PJ7l8f+EtyDD/ffQay
XVAGz2MXUIQMEzmSLrnsr9NEyXvxGpvcsi7mV8tDxZU0YuyhA/C/HMh7EaBKG1hj
C7xNw+IRIUxrbRJakMQbzMWWYJupC5zRu4/Ge9i+JVOGgES2E0L5LZSZ53wmnHA0
ols1PHl3F3Z2QM3CkewqA3NP1waXQ0XXb0Oyl6Gq12B7ksm7euPWA3KctEjfYBD6
nBT6wQd57rAMeFTk5aceWd2Sb/0xMpjfCg6GzX8pAWVEU8LqTvVmlSWdx3f3fAtU
giZ+gx7jNY8A6duln8zvMQn3mtPDCa50GzSrAx8JreHRWSDr3Dp8EfJzUgfy7dWl
I9xs5bh1TMkEMk+AHWQ5sBXTZkDgVAS5m1mIbXe7dzuxKsfGxjWu1eyy9J77mtOG
o9aAOqYfxv/I8YQcgWHTeQcIO39Rmt2QsI7trRaEJ1jgj2E1To5gRCbIQWzQuyoS
6affgu/9dwPXCAt0+0XrnO5vhaKX/RWm7ve8hYsiT0vI0hdBJ3rDRkdS9VL6NlnX
OuohAqEq8b3s2koBigdri052hceAElTHD+4A4qRDiMLlFLlQqoJlpBwCtEPZsIQS
y62K7J/Towxxab5FoFjUTC5f79xPQPoKxYdgUB5AeAu5HgdWTn49Uqg4v/spTPSN
RTmDMVVyZ9qhzJfkDpH3TKCAE5t59w4gSPe/7l+MeSml9O+L9HTd9Vng3LBbIds3
uQ4cfLyyQmly81qpJjR1+Rvwo46hOm0kf2sIFi0WULmP/XzLw6b1SbiHf/jqFg7T
FTyLMkPMPMmc7/kpLmYbKyTB4ineasTUL+bDrwu+uSzFAjTcI+1sz4Wo4p7RVywB
DKSI5Ocbd3iMt4XWJWtz0KBX6nBzlV+BBTCwaGMAU4IpPBYOuvcl7TJWx/ODBjbO
4zm4T/66w5IG3tKpsVMs4Jtrh8mtVXCLTBmKDzyjBVN2X8ALGXarItRgLa7k80lJ
jqTHwKCjiAMmT/eh67KzwmqBq5+8rJuXkax0NoXcDu6xkCMNHUQBYdnskaJqC2pu
8hIsPTOrh7ieYSEuchFvu7lI0E+p7ypW65CMiy+Y/Rm5OWeHzjKkU5AbPtx/Me2v
pQRCgaPwciZunx2Ivi1+WYUBU1pGNDO7Xz7a8UHbDURkh7b+40uz2d7YQjKgrZBv
6YwLAmw1LTE4bT9PM9n7LROnX8u6ksei8yiw8gZeVu+plWHbF+0O9siKAgxZlBna
0XFgPpdzjMDTS/sfTIYXWlFj7camhsmTDRjo5G2B212evaKmKgh5ALLSFSk86ZN5
KvQvcfsp81jvJCBmDStrsUgSMzy0Og2quHOd61hRTVlYzwvJvfMzHGKdIWwYUbHZ
OKo/KLEk3E36U9PkPoZGEL2ZeCH4F9Wh3mgg0knBfEmlPnGexmBby6NXGK7VW3l6
xcJlpdMaXKNVMfl2YK8k/34Hyft06KaYLEJsxAqk1pmLEmGhdZC1OAqovVB/1agS
zpMMaB9OWWqNsTjDc7tkDt8BZ72NsAbCI9XmsX81W+NqPb6Ju1dtI09bn113LX/Z
bOSdVicQcXSpl0FnTZaHgHJdQLcU28O7yFFOblqrvcMKpctdTA1TwG9LXEFttGrl
pgjZF3edo0Cez10epK+S
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

        ed, rest = der_decoder(asn1Object['content'], rfc5652.EnvelopedData())
        self.assertFalse(rest)
        self.assertTrue(ed.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(ed))

        kwa = ed['recipientInfos'][0]['kekri']['keyEncryptionAlgorithm']
        self.assertEqual(rfc4010.id_npki_app_cmsSeed_wrap, kwa['algorithm'])

        cea = ed['encryptedContentInfo']['contentEncryptionAlgorithm']
        self.assertEqual(rfc4010.id_seedCBC, cea['algorithm'])
        param, rest = der_decoder(
            cea['parameters'], asn1Spec=rfc4010.SeedCBCParameter())
        self.assertFalse(rest)
        self.assertTrue(param.prettyPrint())
        self.assertEqual(cea['parameters'], der_encoder(param))

        iv = univ.OctetString(hexValue='424f47555349565f424f475553495621')
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
        self.assertEqual(rfc4010.id_npki_app_cmsSeed_wrap, kwa['algorithm'])

        eci = asn1Object['content']['encryptedContentInfo']
        cea = eci['contentEncryptionAlgorithm']
        self.assertEqual(rfc4010.id_seedCBC, cea['algorithm'])

        iv = univ.OctetString(hexValue='424f47555349565f424f475553495621')
        self.assertEqual(iv, cea['parameters'])

class SMIMECapabilitiesTestCase(unittest.TestCase):
    smime_capabilities_pem_text = "MB4wDAYIKoMajJpEAQQFADAOBgoqgxqMmkQHAQEBBQA="

    def setUp(self):
        self.asn1Spec = rfc5751.SMIMECapabilities()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        alg_oid_list = [ ]
        for cap in asn1Object:
            self.assertTrue(cap['parameters'].hasValue())
            self.assertEqual(cap['parameters'], der_encoder(rfc4010.SeedSMimeCapability("")))
            alg_oid_list.append(cap['capabilityID'])

        self.assertIn(rfc4010.id_seedCBC, alg_oid_list)
        self.assertIn(rfc4010.id_npki_app_cmsSeed_wrap, alg_oid_list)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())

