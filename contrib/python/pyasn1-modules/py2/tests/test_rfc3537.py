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
from pyasn1_modules import rfc3537
from pyasn1_modules import rfc5751


class SMIMECapabilitiesTestCase(unittest.TestCase):
    smime_capabilities_pem_text = "MCIwDwYLKoZIhvcNAQkQAwwFADAPBgsqhkiG9w0BCRADCwUA"

    def setUp(self):
        self.asn1Spec = rfc5751.SMIMECapabilities()

    def testDerCodec(self):
        alg_oid_list = [
            rfc3537.id_alg_HMACwithAESwrap,
            rfc3537.id_alg_HMACwith3DESwrap,
        ]

        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        count = 0
        for cap in asn1Object:
            self.assertEqual(der_encoder(univ.Null("")), cap['parameters'])
            self.assertTrue(cap['capabilityID'] in alg_oid_list)
            count += 1

        self.assertEqual(count, 2)

    def testOpenTypes(self):
        openTypesMap = {
            rfc3537.id_alg_HMACwithAESwrap: univ.Null(""),
            rfc3537.id_alg_HMACwith3DESwrap: univ.Null(""),
        }

        asn1Spec=rfc5751.SMIMECapabilities()
        substrate = pem.readBase64fromText(self.smime_capabilities_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec,
            openTypes=openTypesMap, decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        count = 0
        for cap in asn1Object:
            self.assertEqual(univ.Null(""), cap['parameters'])
            self.assertTrue(cap['capabilityID'] in openTypesMap.keys())
            count += 1

        self.assertEqual(count, 2)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
