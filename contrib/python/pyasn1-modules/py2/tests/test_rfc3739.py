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

from pyasn1.type import error
from pyasn1.type import univ

from pyasn1_modules import pem
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc3739


class QCCertificateTestCase(unittest.TestCase):
    pem_text = """\
MIIFLTCCBBWgAwIBAgIMVRaIE9MInBkG6aUaMA0GCSqGSIb3DQEBCwUAMHMxCzAJ
BgNVBAYTAkJFMRkwFwYDVQQKExBHbG9iYWxTaWduIG52LXNhMRowGAYDVQQLExFG
b3IgRGVtbyBVc2UgT25seTEtMCsGA1UEAxMkR2xvYmFsU2lnbiBEZW1vIElzc3Vp
bmcgQ0EgLSBTdGFnaW5nMB4XDTE4MDYxNTA1MTgxNFoXDTE5MDYxNjA1MTgxNFow
WjELMAkGA1UEBhMCQkUxGTAXBgNVBAMTEFRlc3QgQ2VydGlmaWNhdGUxEjAQBgNV
BAUTCTEyMzQ1Njc4OTENMAsGA1UEKhMEVGVzdDENMAsGA1UEBBMEVGVzdDCCASIw
DQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAL/tsE2EIVQhpkZU5XmFR6FAq9ou
k8FWbyku5M7S2JT3c6OFMQiVgu6nfqdsl4rzojhUXQtMOnO7sUqcIedmwqRIR/jd
X+ELqGGRHodZt94Tjf6Qgn2Wv/EgG0EIwsOAisGKr4qTNs6ZmVMqQ3I4+l9Ik5eM
whr9JfrhSxrXDzoh8Prc9lNjQbk+YKXw0zLmVxW7GAu9zTr98GF+HapIhNQbvqOc
fHoY5svla5MqoRXagfrw/w2fSaO/LT+AFsZYODVpvCg/X3xsknoG7TDIeZ8Hmlgq
Mvg9l9VA2JbSv1C38SeOm0Hfv0l0fspZPSrtmbYlvBtQoO1X/GhQXvE7UvMCAwEA
AaOCAdgwggHUMA4GA1UdDwEB/wQEAwIGQDCBkQYIKwYBBQUHAQEEgYQwgYEwQQYI
KwYBBQUHMAKGNWh0dHA6Ly9zZWN1cmUuc3RhZ2luZy5nbG9iYWxzaWduLmNvbS9n
c2RlbW9zaGEyZzMuY3J0MDwGCCsGAQUFBzABhjBodHRwOi8vb2NzcDIuc3RhZ2lu
Zy5nbG9iYWxzaWduLmNvbS9nc2RlbW9zaGEyZzMwWQYDVR0gBFIwUDBDBgsrBgEE
AaAyASgjAjA0MDIGCCsGAQUFBwIBFiZodHRwczovL3d3dy5nbG9iYWxzaWduLmNv
bS9yZXBvc2l0b3J5LzAJBgcEAIvsQAECMAkGA1UdEwQCMAAwQwYDVR0fBDwwOjA4
oDagNIYyaHR0cDovL2NybC5zdGFnaW5nLmdsb2JhbHNpZ24uY29tL2dzZGVtb3No
YTJnMy5jcmwwLQYIKwYBBQUHAQMEITAfMAgGBgQAjkYBATATBgYEAI5GAQYwCQYH
BACORgEGATAUBgNVHSUEDTALBgkqhkiG9y8BAQUwHQYDVR0OBBYEFNRFutzxY2Jg
qilbYWe86em0QQC+MB8GA1UdIwQYMBaAFBcYifCc7R2iN5qLgGGRDT/RWZN6MA0G
CSqGSIb3DQEBCwUAA4IBAQCMJeiaEAu45PetKSoPEnJ5t4MYr4dUl/HdnV13WEUW
/34yHDGuubTFqJ6sM7P7dO25kdNOr75mR8yc0+gsGJv5K5C7LXfk36ofDlVQm0RJ
3LTRhCvnJIzvuc5R52QW3MvB0EEPd1sfkpGgyTdK8zYZkwCXrWgMuPhBG/kgTiN0
65qitL/WfkcX9SXmsYuV1a3Tsxz+6/rTtxdZfXSJgaVCOWHGyXCvpAQM/4eH5hSj
UfTNwEMrE4sw4k9F90Sp8Wx24sMRDTIpnEXh3ceZSzBN2OYCIO84GaiZDpSvvkYN
Iwtui+Wql/HveMqbAtXkiv9GDXYZms3HBoIaCVuDaUf6
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        count = 0
        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc3739.id_pe_qcStatements:
                s = extn['extnValue']
                qc_stmts, rest = der_decoder(s, rfc3739.QCStatements())
                self.assertFalse(rest)
                self.assertTrue(qc_stmts.prettyPrint())
                self.assertEqual(s, der_encoder(qc_stmts))

                for qcs in qc_stmts:
                    count += 1

        self.assertEqual(2, count)

    def testExtensionsMap(self):

        class SequenceOfOID(univ.SequenceOf):
            componentType = univ.ObjectIdentifier()

        openTypesMap = {
            univ.ObjectIdentifier('0.4.0.1862.1.6'): SequenceOfOID()
        }

        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        count = 0
        found_qc_stmt_oid = False
        for extn in asn1Object['tbsCertificate']['extensions']:
            if extn['extnID'] == rfc3739.id_pe_qcStatements:
                qc_stmts, rest = der_decoder(
                    extn['extnValue'],
                    asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']],
                    openTypes=openTypesMap,
                    decodeOpenTypes=True)
                self.assertFalse(rest)
                self.assertTrue(qc_stmts.prettyPrint())
                self.assertEqual(extn['extnValue'], der_encoder(qc_stmts))

                for qcs in qc_stmts:
                    count += 1
                    if qcs['statementId'] in openTypesMap.keys():
                        for oid in qcs['statementInfo']:
                            if oid == univ.ObjectIdentifier('0.4.0.1862.1.6.1'):
                                found_qc_stmt_oid = True

        self.assertEqual(2, count)
        self.assertTrue(found_qc_stmt_oid)

class WithComponentsTestCase(unittest.TestCase):

    def testDerCodec(self):
        si = rfc3739.SemanticsInformation()
        self.assertRaises(error.PyAsn1Error, der_encoder, si)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
