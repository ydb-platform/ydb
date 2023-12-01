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
from pyasn1_modules import rfc5275
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc6402


class GLUseKEKTestCase(unittest.TestCase):
    pem_text = """\
MIIMVAYJKoZIhvcNAQcCoIIMRTCCDEECAQMxDTALBglghkgBZQMEAgIwggY7Bggr
BgEFBQcMAqCCBi0EggYpMIIGJTCCBhswggYXAgEBBgsqhkiG9w0BCRAIATGCBgMw
ggX/MEaGLGh0dHBzOi8vd3d3LmV4YW1wbGUuY29tL2xpc3QtaW5mby9ncm91cC1s
aXN0gRZncm91cC1saXN0QGV4YW1wbGUuY29tMIIFmzCCBZekQTA/MQswCQYDVQQG
EwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xETAPBgNVBAoTCEJv
Z3VzIENBgRxncm91cC1saXN0LW93bmVyQGV4YW1wbGUuY29tMIIFMqCCBS4wggTU
oAMCAQICFCVehe2QOuzvkY+pMECid/MyYVKJMAsGCWCGSAFlAwQDAjA/MQswCQYD
VQQGEwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xETAPBgNVBAoT
CEJvZ3VzIENBMB4XDTE5MTAyMDE5MzE1MloXDTIxMTAxOTE5MzE1MlowPzELMAkG
A1UEBhMCVVMxCzAJBgNVBAgTAlZBMRAwDgYDVQQHEwdIZXJuZG9uMREwDwYDVQQK
EwhCb2d1cyBDQTCCA0cwggI5BgcqhkjOOAQBMIICLAKCAQEAt9x/0iwGww3k19h+
wbODVK1yqjFzEY2pyfXthHcn+nEw+DpURJ+iOhYPr68E3XO5sB48r5xTZhPN5+Ye
jD3T8qhnDtiq4qrrSH7BOaEzqCDpHE2Bpoy3SodQ5Obaiu9Kx1ixBRk/oRZUH+F+
ATZmF0rPKrZGZOnmsh0IZm3dlmRR9FRGn0aJlZKXveqp+hZ97/r0cbSo6wdT47AP
focgweZMvgWu1IQBs6FiunRgaeX3RyLr4fnkvCzUM7TmxpRJYtL6myAp007QvtgQ
0AdEwVfNl3jQ0IIW7TtpXVxDDQaKZZe9yYrY4GV3etlYk8a4cpjNrBxBCCTMASE4
+iVtPQIhAJGPJRq8r3GSP6cV7V8EmlxC9ne+xkhiAjBmWtcDibXRAoIBACDebX29
ZzVOUeaR6ovCC8c3RR93LDlrFa1zyogkZnUx7OHIvIPhFTRUUJKhwkIJ7aTaRLY/
a9ARFllhyf+cJi6KzAKM4JufAqjN9pNncVzUo4K1vgQRy6T+2Hlc2FYJgknsdqzK
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
"""

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)

        layers = { }
        layers.update(rfc5652.cmsContentTypesMap)

        getNextLayer = {
            rfc5652.id_ct_contentInfo: lambda x: x['contentType'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContentType'],
            rfc6402.id_cct_PKIData: lambda x: None
        }

        getNextSubstrate = {
            rfc5652.id_ct_contentInfo: lambda x: x['content'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContent'],
            rfc6402.id_cct_PKIData: lambda x: None
        }

        next_layer = rfc5652.id_ct_contentInfo
        while next_layer:
            asn1Object, rest = der_decoder(
                substrate, asn1Spec=layers[next_layer])

            self.assertFalse(rest)
            self.assertTrue(asn1Object.prettyPrint())
            self.assertEqual(substrate, der_encoder(asn1Object))

            substrate = getNextSubstrate[next_layer](asn1Object)
            next_layer = getNextLayer[next_layer](asn1Object)

        found_gl_use_kek = False
        for ctrl in asn1Object['controlSequence']:
            if ctrl['attrType'] == rfc5275.id_skd_glUseKEK:
                cv, rest = der_decoder(
                    ctrl['attrValues'][0],
                    asn1Spec=rfc5652.cmsAttributesMap[ctrl['attrType']])

                self.assertFalse(rest)
                self.assertTrue(cv.prettyPrint())
                self.assertEqual(ctrl['attrValues'][0], der_encoder(cv))

                self.assertIn(
                    'example.com',
                    cv['glInfo']['glAddress']['rfc822Name'])

                self.assertIn(
                    'example.com',
                    cv['glOwnerInfo'][0]['glOwnerAddress']['rfc822Name'])

                self.assertEqual(31, cv['glKeyAttributes']['duration'])
                found_gl_use_kek = True

        self.assertTrue(found_gl_use_kek)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=rfc5652.ContentInfo(), decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        sd = asn1Object['content']
        self.assertEqual(
            rfc6402.id_cct_PKIData, sd['encapContentInfo']['eContentType'])

        pkid, rest = der_decoder(
            sd['encapContentInfo']['eContent'],
            asn1Spec=rfc6402.PKIData(),
            decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(pkid.prettyPrint())
        self.assertEqual(sd['encapContentInfo']['eContent'], der_encoder(pkid))

        found_gl_use_kek = False
        for ctrl in pkid['controlSequence']:
            if ctrl['attrType'] == rfc5275.id_skd_glUseKEK:
                cv = ctrl['attrValues'][0]

                self.assertIn(
                    'example.com',
                    cv['glInfo']['glAddress']['rfc822Name'])

                self.assertIn(
                    'example.com',
                    cv['glOwnerInfo'][0]['glOwnerAddress']['rfc822Name'])

                self.assertEqual(31, cv['glKeyAttributes']['duration'])
                found_gl_use_kek = True

        self.assertTrue(found_gl_use_kek)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    import sys

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
