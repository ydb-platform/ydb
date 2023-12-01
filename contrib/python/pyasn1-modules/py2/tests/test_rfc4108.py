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
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc4108


class CMSFirmwareWrapperTestCase(unittest.TestCase):
    pem_text = """\
MIIEvAYJKoZIhvcNAQcCoIIErTCCBKkCAQExDTALBglghkgBZQMEAgEwggIVBgsq
hkiG9w0BCRABEKCCAgQEggIA3ntqPr5kDpx+//pgWGfHCH/Ht4pbenGwXv80txyE
Y0I2mT9BUGz8ILkbhD7Xz89pBS5KhEJpthxH8WREJtvS+wL4BqYLt23wjWoZy5Gt
5dPzWgaNlV/aQ5AdfAY9ljmnNYnK8D8r8ur7bQM4cKUdxry+QA0nqXHMAOSpx4Um
8impCc0BICXaFfL3zBrNxyPubbFO9ofbYOAWaNmmIAhzthXf12vDrLostIqmYrP4
LMRCjTr4LeYaVrAWfKtbUbByN6IuBef3Qt5cJaChr74udz3JvbYFsUvCpl64kpRq
g2CT6R+xE4trO/pViJlI15dvJVz04BBYQ2jQsutJwChi97/DDcjIv03VBmrwRE0k
RJNFP9vpDM8CxJIqcobC5Kuv8b0GqGfGl6ouuQKEVMfBcrupgjk3oc3KL1iVdSr1
+74amb1vDtTMWNm6vWRqh+Kk17NGEi2mNvYkkZUTIHNGH7OgiDclFU8dSMZd1fun
/D9dmiFiErDB3Fzr4+8Qz0aKedNE/1uvM+dhu9qjuRdkDzZ4S7txTfk6y9pG9iyk
aEeTV2kElKXblgi+Cf0Ut4f5he8rt6jveHdMo9X36YiUQVvevj2cgN7lFivEnFYV
QY0xugpP7lvEFDfsi2+0ozgP8EKOLYaCUKpuvttlYJ+vdtUFEijizEZ4cx02RsXm
EesxggJ6MIICdgIBA4AUnutnybladNRNLxY5ZoDoAbXLpJwwCwYJYIZIAWUDBAIB
oIG8MBoGCSqGSIb3DQEJAzENBgsqhkiG9w0BCRABEDArBgsqhkiG9w0BCRACJDEc
MBoGCysGAQQBjb9BAQEqBgsrBgEEAY2/QQEBMDAvBgkqhkiG9w0BCQQxIgQgAJfv
uasB4P6WDLOkOyvj33YPgZW4olHbidzyh1EKP9YwQAYLKoZIhvcNAQkQAikxMTAv
MAsGCWCGSAFlAwQCAQQgAJfvuasB4P6WDLOkOyvj33YPgZW4olHbidzyh1EKP9Yw
CwYJKoZIhvcNAQELBIIBgDivAlSLbMPPu+zV+pPcYpNp+A1mwVOytjMBzSo31kR/
qEu+hVrDknAOk9IdCaDvcz612CcfNT85/KzrYvWWxOP2woU/vZj253SnndALpfNN
n3/crJjF6hKgkjUwoXebI7kuj5WCh2q5lkd6xUa+jkCw+CINcN43thtS66UsVI4d
mv02EvsS2cxPY/508uaQZ6AYAacm667bgX8xEjbzACMOeMCuvKQXWAuh3DkNk+gV
xizHDw7xZxXgMGMAnJglAeBtd3Si5ztILw9U2gKUqFn/nOgy+eW63JuU/q31/Hgg
ZATjyBznSzneTZrw8/ePoSCj7E9vBeCTUkeFbVB2tJK1iYDMblp6HUuwgYuGKXy/
ZwKL3GvB11qg7ntdEyjdLq0xcVrht/K0d2dPo4iO4Ac7c1xbFMDAlWOt4FMPWh6O
iTh55YvT7hAJjTbB5ebgMA9QJnAczQPFnaIePnlFrkETd3YyLK4yHwnoIGo1GiW/
dsnhVtIdkPtfJIvcYteYJg==
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)

        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(rfc5652.id_signedData, asn1Object['contentType'])

        inner, rest = der_decoder(asn1Object['content'], asn1Spec=rfc5652.SignedData())

        self.assertEqual(
            rfc4108.id_ct_firmwarePackage, inner['encapContentInfo']['eContentType'])

        self.assertTrue(inner['encapContentInfo']['eContent'])

        attribute_list = []

        for attr in inner['signerInfos'][0]['signedAttrs']:
            attribute_list.append(attr['attrType'])
            if attr['attrType'] == rfc4108.id_aa_targetHardwareIDs:
                av, rest = der_decoder(attr['attrValues'][0],
                                       asn1Spec=rfc4108.TargetHardwareIdentifiers())
                self.assertEqual(2, len(av))

                for oid in av:
                    self.assertIn('1.3.6.1.4.1.221121.1.1.', oid.prettyPrint())

        self.assertIn( rfc5652.id_contentType, attribute_list)
        self.assertIn( rfc5652.id_messageDigest, attribute_list)
        self.assertIn(rfc4108.id_aa_targetHardwareIDs, attribute_list)
        self.assertIn(rfc4108.id_aa_fwPkgMessageDigest, attribute_list)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(asn1Object['contentType'], rfc5652.id_signedData)

        sd_eci = asn1Object['content']['encapContentInfo']

        self.assertEqual(sd_eci['eContentType'], rfc4108.id_ct_firmwarePackage)
        self.assertTrue(sd_eci['eContent'].hasValue())

        for attr in asn1Object['content']['signerInfos'][0]['signedAttrs']:
            self.assertIn(attr['attrType'], rfc5652.cmsAttributesMap)
            if attr['attrType'] == rfc4108.id_aa_targetHardwareIDs:
               for oid in attr['attrValues'][0]:
                   self.assertIn('1.3.6.1.4.1.221121.1.1.', oid.prettyPrint())


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
