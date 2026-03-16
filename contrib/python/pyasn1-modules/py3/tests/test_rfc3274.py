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
from pyasn1_modules import rfc3274
from pyasn1_modules import rfc5652


class CompressedDataTestCase(unittest.TestCase):
    compressed_data_pem_text = """\
MIIB7wYLKoZIhvcNAQkQAQmgggHeMIIB2gIBADANBgsqhkiG9w0BCRADCDCCAcQG
CSqGSIb3DQEHAaCCAbUEggGxeJxVksGO1DAQRO/+ir4xK4VlNSAhcUPRrgRiLgw/
0Il7Egu7bdntMOHraSezMJyixOWq19XpIwuxvP2xJvoEQld5lzw6Nub7Sw/vjx8/
dJDq4F2ZyYJj+FqZ4Pj0dOzA0sUxFUC4xBxQ2gNqcTzBGEPKVApZY1EQsKn6vCaJ
U8Y0uxFOeowTwXllwSsc+tP5Qe9tOCCK8wjQ32zUcvcZSDMIJCOX4PQgMqQcF2c3
Dq5hoAzxAmgXVN+JSqfUo6+2YclMhrwLjlHaVRVutplsZYs8rvBL2WblqN7CTD4B
MqAIjj8pd1ASUXMyNbXccWeDYd0sxlsGYIhVp3i1l6jgr3qtUeUehbIpQqnAoVSN
1IqKm7hZaI3EY2tLIR86RbD//ONCGb2HsPdnivvdqvrsZY51mlu+NjTjQhpKWz0p
FvRlWw9ae7+fVgKKie0SeFpIZYemoyuG5HUS2QY6fTk9N6zz+dsuUyr9Xghs5Ddi
1LbZbVoNHDyFNv19jL7qiv9uuLK/XTD3Kqct1JS822vS8vWXpMzYBtal/083rMap
XQ7u2qbaKFtZ7V96NH8ApkUFkg==
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.compressed_data_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(rfc3274.id_ct_compressedData, asn1Object['contentType'])

        cd, rest = der_decoder(
            asn1Object['content'], asn1Spec=rfc3274.CompressedData())

        self.assertFalse(rest)
        self.assertTrue(cd.prettyPrint())
        self.assertEqual(asn1Object['content'], der_encoder(cd))

        self.assertEqual(rfc3274.id_alg_zlibCompress,
                         cd['compressionAlgorithm']['algorithm'])
        self.assertEqual(rfc5652.id_data, cd['encapContentInfo']['eContentType'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.compressed_data_pem_text)
        asn1Object, rest = der_decoder(substrate,
                                       asn1Spec=self.asn1Spec,
                                       decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(
            rfc3274.id_ct_compressedData, asn1Object['contentType'])

        cd = asn1Object['content']

        self.assertEqual(rfc3274.id_alg_zlibCompress,
                         cd['compressionAlgorithm']['algorithm'])
        self.assertEqual(rfc5652.id_data, cd['encapContentInfo']['eContentType'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
