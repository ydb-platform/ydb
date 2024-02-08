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
from pyasn1_modules import rfc4055
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5126


class SignedAttributesTestCase(unittest.TestCase):
    pem_text = """\
MYIBUzAYBgkqhkiG9w0BCQMxCwYJKoZIhvcNAQcBMCsGCSqGSIb3DQEJNDEeMBww
DQYJYIZIAWUDBAIBBQChCwYJKoZIhvcNAQELMC8GCSqGSIb3DQEJBDEiBCCyqtCC
Gosj/GT4YPPAqKheze4A1QBU5O3tniTsVPGr7jBBBgsqhkiG9w0BCRACETEyMDCg
BBMCVVOhBBMCVkGiIjAgExExMjMgU29tZXBsYWNlIFdheRMLSGVybmRvbiwgVkEw
RgYLKoZIhvcNAQkQAi8xNzA1MDMwMTANBglghkgBZQMEAgEFAAQgJPmqUmGQnQ4q
RkVtUHecJXIkozOzX8+pZQj/UD5JcnQwTgYLKoZIhvcNAQkQAg8xPzA9BgorBgEE
AYGsYDAUMC8wCwYJYIZIAWUDBAIBBCDWjjVmAeXgZBkE/rG8Pf8pTCs4Ikowc8Vm
l+AOeKdFgg==
"""

    def setUp(self):
        self.asn1Spec = rfc5652.SignedAttributes()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        found_spid_oid = False

        for attr in asn1Object:
            if attr['attrType'] in rfc5652.cmsAttributesMap.keys():
                av, rest = der_decoder(
                    attr['attrValues'][0],
                    asn1Spec=rfc5652.cmsAttributesMap[attr['attrType']])

                self.assertFalse(rest)
                self.assertTrue(av.prettyPrint())
                self.assertEqual(attr['attrValues'][0], der_encoder(av))

                if attr['attrType'] == rfc5126.id_aa_ets_sigPolicyId:
                    spid_oid = rfc5126.SigPolicyId('1.3.6.1.4.1.22112.48.20')

                    self.assertEqual(
                        spid_oid, av['signaturePolicyId']['sigPolicyId'])

                    found_spid_oid = True

        self.assertTrue(found_spid_oid)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        attr_type_list = []
        spid_oid = rfc5126.SigPolicyId('1.3.6.1.4.1.22112.48.20')

        for attr in asn1Object:
            if attr['attrType'] == rfc5126.id_aa_ets_sigPolicyId:
                spid = attr['attrValues'][0]['signaturePolicyId']
                self.assertEqual(spid_oid, spid['sigPolicyId'])
                attr_type_list.append(rfc5126.id_aa_ets_sigPolicyId)

            if attr['attrType'] == rfc5126.id_aa_ets_signerLocation:
                cn = attr['attrValues'][0]['countryName']
                self.assertEqual('US', cn['printableString'])
                attr_type_list.append(rfc5126.id_aa_ets_signerLocation)

            if attr['attrType'] == rfc5126.id_aa_signingCertificateV2:
                ha = attr['attrValues'][0]['certs'][0]['hashAlgorithm']
                self.assertEqual(rfc4055.id_sha256, ha['algorithm'])
                attr_type_list.append(rfc5126.id_aa_signingCertificateV2)

        self.assertIn(rfc5126.id_aa_ets_sigPolicyId, attr_type_list)
        self.assertIn(rfc5126.id_aa_ets_signerLocation, attr_type_list)
        self.assertIn(rfc5126.id_aa_signingCertificateV2, attr_type_list)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    import sys

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
