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
from pyasn1_modules import rfc5958
from pyasn1_modules import rfc7914
from pyasn1_modules import rfc8018


# From RFC 7914, Section 13

class MultiprimeRSAPrivateKeyTestCase(unittest.TestCase):
    pem_text = """\
MIHiME0GCSqGSIb3DQEFDTBAMB8GCSsGAQQB2kcECzASBAVNb3VzZQIDEAAAAgEI
AgEBMB0GCWCGSAFlAwQBKgQQyYmguHMsOwzGMPoyObk/JgSBkJb47EWd5iAqJlyy
+ni5ftd6gZgOPaLQClL7mEZc2KQay0VhjZm/7MbBUNbqOAXNM6OGebXxVp6sHUAL
iBGY/Dls7B1TsWeGObE0sS1MXEpuREuloZjcsNVcNXWPlLdZtkSH6uwWzR0PyG/Z
+ZXfNodZtd/voKlvLOw5B3opGIFaLkbtLZQwMiGtl42AS89lZg==
"""

    def setUp(self):
        self.asn1Spec = rfc5958.EncryptedPrivateKeyInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        ea = asn1Object['encryptionAlgorithm']

        self.assertEqual(rfc8018.id_PBES2, ea['algorithm'])
        self.assertIn(ea['algorithm'], rfc5280.algorithmIdentifierMap)

        params, rest = der_decoder(
            ea['parameters'],
            asn1Spec=rfc5280.algorithmIdentifierMap[ea['algorithm']])

        self.assertFalse(rest)
        self.assertTrue(params.prettyPrint())
        self.assertEqual(ea['parameters'], der_encoder(params))

        kdf = params['keyDerivationFunc']

        self.assertEqual(rfc7914.id_scrypt, kdf['algorithm'])
        self.assertIn(kdf['algorithm'], rfc5280.algorithmIdentifierMap)

        kdfp, rest = der_decoder(
            kdf['parameters'],
            asn1Spec=rfc5280.algorithmIdentifierMap[kdf['algorithm']])

        self.assertFalse(rest)
        self.assertTrue(kdfp.prettyPrint())
        self.assertTrue(kdf['parameters'], der_encoder(kdfp))
        self.assertEqual(1048576, kdfp['costParameter'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        ea = asn1Object['encryptionAlgorithm']

        self.assertEqual(rfc8018.id_PBES2, ea['algorithm'])

        params = asn1Object['encryptionAlgorithm']['parameters']

        self.assertEqual(
            rfc7914.id_scrypt, params['keyDerivationFunc']['algorithm'])

        kdfp = params['keyDerivationFunc']['parameters']

        self.assertEqual(1048576, kdfp['costParameter'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
