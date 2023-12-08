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
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc5480
from pyasn1_modules import rfc5639


class ECCertTestCase(unittest.TestCase):
    brainpool_ec_cert_pem_text = """\
MIIB0jCCAXmgAwIBAgITPUXQAyl3ZE5iAHYGZYSp1FkqzTAKBggqhkjOPQQDAjA/
MQswCQYDVQQGEwJVUzELMAkGA1UECAwCVkExEDAOBgNVBAcMB0hlcm5kb24xETAP
BgNVBAoMCEJvZ3VzIENBMB4XDTE5MTIwOTIxNDM0NFoXDTIxMTIwODIxNDM0NFow
PzELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAlZBMRAwDgYDVQQHDAdIZXJuZG9uMREw
DwYDVQQKDAhCb2d1cyBDQTBaMBQGByqGSM49AgEGCSskAwMCCAEBBwNCAASBvvOk
WNZlGAf5O3V94qgC3IUUR/6uxFxT6To0ULFmrVVndXiVP6DE5h5QHGXPwKfO+4Yt
n0OVnGHp68dPS37Go1MwUTAdBgNVHQ4EFgQUiRFFVcdn6Fp9+sEP1GVRtwl9XgIw
HwYDVR0jBBgwFoAUiRFFVcdn6Fp9+sEP1GVRtwl9XgIwDwYDVR0TAQH/BAUwAwEB
/zAKBggqhkjOPQQDAgNHADBEAiB3d+P64Dh5YzwyM++uOL6zHUeLbNpW2sF1eJsm
l3M5uQIgGxpbAXOt/o1xtyhEGLNUBE7ObgQpm7tHMMQGUHo4wV8=
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.brainpool_ec_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki = asn1Object['tbsCertificate']['subjectPublicKeyInfo']
        algid = spki['algorithm']

        self.assertEqual(rfc5480.id_ecPublicKey, algid['algorithm'])

        param, rest = der_decoder(
            algid['parameters'], asn1Spec=rfc5480.ECParameters())

        self.assertFalse(rest)
        self.assertTrue(param.prettyPrint())
        self.assertEqual(algid['parameters'], der_encoder(param))

        self.assertEqual(rfc5639.brainpoolP256r1, param['namedCurve'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.brainpool_ec_cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
    
        spki = asn1Object['tbsCertificate']['subjectPublicKeyInfo']
        algid = spki['algorithm']

        self.assertEqual(rfc5480.id_ecPublicKey, algid['algorithm'])
        self.assertEqual(
            rfc5639.brainpoolP256r1, algid['parameters']['namedCurve'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
