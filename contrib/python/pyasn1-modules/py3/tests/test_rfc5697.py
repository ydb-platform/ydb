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
from pyasn1_modules import rfc5697


class OtherCertTestCase(unittest.TestCase):
    cert_pem_text = """\
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
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        other_cert_found = False

        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5697.id_pe_otherCerts:
                extnValue, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5697.OtherCertificates())

                self.assertFalse(rest)
                self.assertTrue(extnValue.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(extnValue))

                self.assertEqual(
                    11939979568329289287,
                    extnValue[0]['issuerSerial']['serialNumber'])

                other_cert_found = True

        self.assertTrue(other_cert_found)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        other_cert_found = False

        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc5697.id_pe_otherCerts:
                self.assertIn(extn['extnID'], rfc5280.certificateExtensionsMap)

                extnValue, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']],
                    decodeOpenTypes=True)

                self.assertFalse(rest)
                self.assertTrue(extnValue.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(extnValue))

                self.assertEqual(
                    11939979568329289287,
                    extnValue[0]['issuerSerial']['serialNumber'])

                other_cert_found = True

        self.assertTrue(other_cert_found)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
