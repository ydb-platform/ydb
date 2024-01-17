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
from pyasn1_modules import rfc5915
from pyasn1_modules import rfc5480


class MUDCertTestCase(unittest.TestCase):
    private_key_pem_text = """\
MIGkAgEBBDDLjzGbbLrR3T13lrrVum7WC/4Ua4Femc1RhhNVe1Q5XsArQ33kn9kx
3lOUfOcG+qagBwYFK4EEACKhZANiAAT4zZ8HL+xEDpXWkoWp5xFMTz4u4Ae1nF6z
XCYlmsEGD5vPu5hl9hDEjd1UHRgJIPoy3fJcWWeZ8FHCirICtuMgFisNscG/aTwK
yDYOFDuqz/C2jyEwqgWCRyxyohuJXtk=
"""

    def setUp(self):
        self.asn1Spec = rfc5915.ECPrivateKey()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.private_key_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(
            rfc5480.secp384r1, asn1Object['parameters']['namedCurve'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
