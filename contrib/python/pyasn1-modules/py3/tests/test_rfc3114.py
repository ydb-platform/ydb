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
from pyasn1_modules import rfc3114
from pyasn1_modules import rfc5035
from pyasn1_modules import rfc5083
from pyasn1_modules import rfc5280
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc5755


class SecurityLabelTestCase(unittest.TestCase):
    pem_text = """\
MIITHAYJKoZIhvcNAQcCoIITDTCCEwkCAQMxDTALBglghkgBZQMEAgIwggeUBgsq
hkiG9w0BCRABF6CCB4MEggd/MIIHewIBADGCAk8wggJLAgEAMDMwJjEUMBIGA1UE
CgwLZXhhbXBsZS5jb20xDjAMBgNVBAMMBUFsaWNlAgkAg/ULtwvVxA4wDQYJKoZI
hvcNAQEBBQAEggIAdZphtN3x8a8kZoAFY15HYRD6JyPBueRUhLbTPoOH3pZ9xeDK
+zVXGlahl1y1UOe+McEx2oD7cxAkhFuruNZMrCYEBCTZMwVhyEOZlBXdZEs8rZUH
L3FFE5PJnygsSIO9DMxd1UuTFGTgCm5V5ZLFGmjeEGJRbsfTyo52S7iseJqIN3dl
743DbApu0+yuUoXKxqKdUFlEVxmhvc+Qbg/zfiwu8PTsYiUQDMBi4cdIlju8iLjj
389xQHNyndXHWD51is89GG8vpBe+IsN8mnbGtCcpqtJ/c65ErJhHTR7rSJSMEqQD
0LPOCKIY1q9FaSSJfMXJZk9t/rPxgUEVjfw7hAkKpgOAqoZRN+FpnFyBl0FnnXo8
kLp55tfVyNibtUpmdCPkOwt9b3jAtKtnvDQ2YqY1/llfEUnFOVDKwuC6MYwifm92
qNlAQA/T0+ocjs6gA9zOLx+wD1zqM13hMD/L+T2OHL/WgvGb62JLrNHXuPWA8RSh
O4kIlPtARKXap2S3+MX/kpSUUrNa65Y5uK1jwFFclczG+CPCIBBn6iJiQT/vOX1I
97YUP4Qq6OGkjK064Bq6o8+e5+NmIOBcygYRv6wA7vGkmPLSWbnw99qD728bBh84
fC3EjItdusqGIwjzL0eSUWXJ5eu0Z3mYhJGN1pe0R/TEB5ibiJsMLpWAr3gwggUP
BgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEGMBEEDE2HVyIurFKUEX8MEgIBEICCBOD+
L7PeC/BpmMOb9KlS+r+LD+49fi6FGBrs8aieGi7ezZQEiFYS38aYQzTYYCt3SbJQ
TkX1fDsGZiaw/HRiNh7sJnxWATm+XNKGoq+Wls9RhSJ45Sw4GMqwpoxZjeT84Uoz
OITk3l3fV+3XiGcCejHkp8DAKZFExd5rrjlpnnAOBX6w8NrXO4s2n0LrMhtBU4eB
2YKhGgs5Q6wQyXtU7rc7OOwTGvxWEONzSHJ01pyvqVQZAohsZPaWLULrM/kEGkrh
G4jcaVjVPfULi7Uqo14imYhdCq5Ba4bwqI0Ot6mB27KD6LlOnVC/YmXCNIoYoWmq
y1o3pSm9ovnLEO/dzxQjEJXYeWRje9M/sTxotM/5oZBpYMHqIwHTJbehXFgp8+oD
jyTfayMYA3fTcTH3XbGPQfnYW2U9+ka/JhcSYybM8cuDNFd1I1LIQXoJRITXtkvP
UbJqm+s6DtS5yvG9I8aQxlT365zphS4vbQaO74ujO8bE3dynrvTTV0c318TcHpN3
DY9PIt6mHXMIPDLEA4wes90zg6iah5XiQcLtfLaAdYwEEGlImGD8n0kOhSNgclSL
Mklpj5mVOs8exli3qoXlVMRJcBptSwOe0QPcRY30spywS4zt1UDIQ0jaecGGVtUY
j586nkubhAxwZkuQKWxgt6yYTpGNSKCdvd+ygfyGJRDbWdn6nck/EPnG1773KTHR
hMrXrBPBpSlfyJ/ju3644CCFqCjFoTh4bmB63k9ejUEVkJIJuoeKeTBaUxbCIink
K4htBkgchHP51RJp4q9jQbziD3aOhg13hO1GFQ4E/1DNIJxbEnURNp/ga8SqmnLY
8f5Pzwhm1mSzZf+obowbQ+epISrswWyjUKKO+uJfrAVN2TS/5+X6T3U6pBWWjH6+
xDngrAJwtIdKBo0iSEwJ2eir4X8TcrSy9l8RSOiTPtqS5dF3RWSWOzkcO72fHCf/
42+DLgUVX8Oe5mUvp7QYiXXsXGezLJ8hPIrGuOEypafDv3TwFkBc2MIB0QUhk+GG
1ENY3jiNcyEbovF5Lzz+ubvechHSb1arBuEczJzN4riM2Dc3c+r8N/2Ft6eivK7H
UuYX1uAcArhunZpA8yBGLF1m+DUXFtzWAUvfMKYPdfwGMckghF7YwLrTXd8ZhPIk
HNO1KdwQKIRfgIlUPfTxRB7eNrG/Ma9a/IwrcI1QtkXU59uIZIw+7+FHZRWPsOjT
u1Pdy+JtcSTG4dmS+DIwqpUzdu6MaBCVaOhXHwybvaSPTfMG/nR/NxF1FI8xgydn
zXZs8HtFDL9iytKnvXHx+IIz8Rahp/PK8S80vPQNIeef/JgnIhtosID/A614LW1t
B4cWdveYlD5U8T/XXInAtCY78Q9WJD+ecu87OJmlOdmjrFvitpQAo8+NGWxc7Wl7
LtgDuYel7oXFCVtI2npbA7R+K5/kzUvDCY6GTgzn1Gfamc1/Op6Ue17qd/emvhbI
x+ng3swf8TJVnCNDIXucKVA4boXSlCEhCGzfoZZYGVvm1/hrypiBtpUIKWTxLnz4
AQJdZ5LGiCQJQU1wMyHsg6vWmNaJVhGHE6D/EnKsvJptFIkAx0wWkh35s48p7EbU
8QBg//5eNru6yvLRutfdBX7T4w681pCD+dOiom75C3UdahrfoFkNsZ2hB88+qNsE
EPb/xuGu8ZzSPZhakhl2NS2ggglpMIICAjCCAYigAwIBAgIJAOiR1gaRT87yMAoG
CCqGSM49BAMDMD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwH
SGVybmRvbjERMA8GA1UECgwIQm9ndXMgQ0EwHhcNMTkwNTE0MDg1ODExWhcNMjEw
NTEzMDg1ODExWjA/MQswCQYDVQQGEwJVUzELMAkGA1UECAwCVkExEDAOBgNVBAcM
B0hlcm5kb24xETAPBgNVBAoMCEJvZ3VzIENBMHYwEAYHKoZIzj0CAQYFK4EEACID
YgAE8FF2VLHojmqlnawpQwjG6fWBQDPOy05hYq8oKcyg1PXH6kgoO8wQyKYVwsDH
Evc1Vg6ErQm3LzdI8OQpYx3H386R2F/dT/PEmUSdcOIWsB4zrFsbzNwJGIGeZ33Z
S+xGo1AwTjAdBgNVHQ4EFgQU8jXbNATapVXyvWkDmbBi7OIVCMEwHwYDVR0jBBgw
FoAU8jXbNATapVXyvWkDmbBi7OIVCMEwDAYDVR0TBAUwAwEB/zAKBggqhkjOPQQD
AwNoADBlAjBaUY2Nv03KolLNRJ2wSoNK8xlvzIWTFgIhsBWpD1SpJxRRv22kkoaw
9bBtmyctW+YCMQC3/KmjNtSFDDh1I+lbOufkFDSQpsMzcNAlwEAERQGgg6iXX+Nh
A+bFqNC7FyF4WWQwggOHMIIDDqADAgECAgkApbNUKBuwbkYwCgYIKoZIzj0EAwMw
PzELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAlZBMRAwDgYDVQQHDAdIZXJuZG9uMREw
DwYDVQQKDAhCb2d1cyBDQTAeFw0xOTExMDIxODQyMThaFw0yMDExMDExODQyMTha
MGYxCzAJBgNVBAYTAlVTMQswCQYDVQQIEwJWQTEQMA4GA1UEBxMHSGVybmRvbjEQ
MA4GA1UEChMHRXhhbXBsZTEMMAoGA1UECxMDUENBMRgwFgYDVQQDEw9wY2EuZXhh
bXBsZS5jb20wdjAQBgcqhkjOPQIBBgUrgQQAIgNiAAQ9/m9uACpsTl2frBuILHiw
IJyfUEpKseYJ+JYL1AtIZU0YeJ9DA+32h0ZeNGJDtDClnbBEPpn3W/5+TzldcsTe
QlAJB08gcVRjkQym9LtPq7rGubCeVWlRRE9M7F9znk6jggGtMIIBqTAdBgNVHQ4E
FgQUJuolDwsyICik11oKjf8t3L1/VGUwbwYDVR0jBGgwZoAU8jXbNATapVXyvWkD
mbBi7OIVCMGhQ6RBMD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJWQTEQMA4GA1UE
BwwHSGVybmRvbjERMA8GA1UECgwIQm9ndXMgQ0GCCQDokdYGkU/O8jAPBgNVHRMB
Af8EBTADAQH/MAsGA1UdDwQEAwIBhjBCBglghkgBhvhCAQ0ENRYzVGhpcyBjZXJ0
aWZpY2F0ZSBjYW5ub3QgYmUgdHJ1c3RlZCBmb3IgYW55IHB1cnBvc2UuMBUGA1Ud
IAQOMAwwCgYIKwYBBQUHDQIwCgYDVR02BAMCAQIwgZEGCCsGAQUFBwEVBIGEMIGB
MFkGCyqGSIb3DQEJEAcDAwIF4DFGMESACyqGSIb3DQEJEAcEgTUwMwwXTEFXIERF
UEFSVE1FTlQgVVNFIE9OTFkMGEhVTUFOIFJFU09VUkNFUyBVU0UgT05MWTARBgsq
hkiG9w0BCRAHAgMCBPAwEQYLKoZIhvcNAQkQBwEDAgXgMAoGCCqGSM49BAMDA2cA
MGQCMBlIP4FWrNzWXR8OgfcvCLGPG+110EdsmwznIF6ThT1vbJYvYoSbBXTZ9OCh
/cCMMQIwJOySybHl/eLkNJh971DWF4mUQkt3WGBmZ+9Rg2cJTdat2ZjPKg101NuD
tkUyjGxfMIID1DCCA1qgAwIBAgIUUc1IQGJpeYQ0XwOS2ZmVEb3aeZ0wCgYIKoZI
zj0EAwMwZjELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAlZBMRAwDgYDVQQHEwdIZXJu
ZG9uMRAwDgYDVQQKEwdFeGFtcGxlMQwwCgYDVQQLEwNQQ0ExGDAWBgNVBAMTD3Bj
YS5leGFtcGxlLmNvbTAeFw0xOTExMDUyMjIwNDZaFw0yMDExMDQyMjIwNDZaMIGS
MQswCQYDVQQGEwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xEDAO
BgNVBAoTB0V4YW1wbGUxIjAgBgNVBAsTGUh1bWFuIFJlc291cmNlIERlcGFydG1l
bnQxDTALBgNVBAMTBEZyZWQxHzAdBgkqhkiG9w0BCQEWEGZyZWRAZXhhbXBsZS5j
b20wdjAQBgcqhkjOPQIBBgUrgQQAIgNiAAQObFslQ2EBP0xlDJ3sRnsNaqm/woQg
KpBispSxXxK5bWUVpfnWsZnjLWhtDuPcu1BcBlM2g7gwL/aw8nUSIK3D8Ja9rTUQ
QXc3zxnkcl8+8znNXHMGByRjPUH87C+TOrqjggGaMIIBljAdBgNVHQ4EFgQU5m71
1OqFDNGRSWMOSzTXjpTLIFUwbwYDVR0jBGgwZoAUJuolDwsyICik11oKjf8t3L1/
VGWhQ6RBMD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVy
bmRvbjERMA8GA1UECgwIQm9ndXMgQ0GCCQCls1QoG7BuRjAPBgNVHRMBAf8EBTAD
AQH/MAsGA1UdDwQEAwIBhjBCBglghkgBhvhCAQ0ENRYzVGhpcyBjZXJ0aWZpY2F0
ZSBjYW5ub3QgYmUgdHJ1c3RlZCBmb3IgYW55IHB1cnBvc2UuMBUGA1UdIAQOMAww
CgYIKwYBBQUHDQIwCgYDVR02BAMCAQIwfwYDVR0JBHgwdjBJBgNVBDcxQjBABgsq
hkiG9w0BCRAHAwMCBeAxLTArgAsqhkiG9w0BCRAHBIEcMBoMGEhVTUFOIFJFU09V
UkNFUyBVU0UgT05MWTApBglghkgBZQIBBUQxHAwaSHVtYW4gUmVzb3VyY2VzIERl
cGFydG1lbnQwCgYIKoZIzj0EAwMDaAAwZQIwVh/RypULFgPpAN0I7OvuMomRWnm/
Hea3Hk8PtTRz2Zai8iYat7oeAmGVgMhSXy2jAjEAuJW4l/CFatBy4W/lZ7gS3weB
dBa5WEDIFFMC7GjGtCeLtXYqWfBnRdK26dOaHLB2MYIB7jCCAeoCAQEwfjBmMQsw
CQYDVQQGEwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xEDAOBgNV
BAoTB0V4YW1wbGUxDDAKBgNVBAsTA1BDQTEYMBYGA1UEAxMPcGNhLmV4YW1wbGUu
Y29tAhRRzUhAYml5hDRfA5LZmZURvdp5nTALBglghkgBZQMEAgKggeIwGgYJKoZI
hvcNAQkDMQ0GCyqGSIb3DQEJEAEXMBwGCSqGSIb3DQEJBTEPFw0xOTExMDgyMDA4
MzFaMD8GCSqGSIb3DQEJBDEyBDCd5WyvIB0VdXgPBWPtI152MIJLg5o68IRimCXx
bVY0j3YyAKbi0egiZ/UunkyCfv0wZQYLKoZIhvcNAQkQAgIxVjFUAgEIBgsqhkiG
9w0BCRAHAzEtMCuACyqGSIb3DQEJEAcEgRwwGgwYSFVNQU4gUkVTT1VSQ0VTIFVT
RSBPTkxZExNCb2FndXMgUHJpdmFjeSBNYXJrMAoGCCqGSM49BAMDBGcwZQIwWkD7
03QoNrKL5HJnuGJqvML1KlUXZDHnFpnJ+QMzXi8gocyfpRXWm6h0NjXieE0XAjEA
uuDSOoaUIz+G9aemAE0ldpo1c0avNGa7BtynUTHmwosD6Sjfj0epAg9OnMedOjbr
"""

    def testDerCodec(self):
        layers = { }
        layers.update(rfc5652.cmsContentTypesMap)

        getNextLayer = {
            rfc5652.id_ct_contentInfo: lambda x: x['contentType'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContentType'],
            rfc5083.id_ct_authEnvelopedData: lambda x: None
        }

        getNextSubstrate = {
            rfc5652.id_ct_contentInfo: lambda x: x['content'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContent'],
            rfc5083.id_ct_authEnvelopedData: lambda x: None
        }

        substrate = pem.readBase64fromText(self.pem_text)

        next_layer = rfc5652.id_ct_contentInfo
        while next_layer:
            asn1Object, rest = der_decoder(substrate, asn1Spec=layers[next_layer])

            self.assertFalse(rest)
            self.assertTrue(asn1Object.prettyPrint())
            self.assertEqual(substrate, der_encoder(asn1Object))

            if next_layer == rfc5652.id_signedData:
                attrs = asn1Object['signerInfos'][0]['signedAttrs']
                certs = asn1Object['certificates']

            substrate = getNextSubstrate[next_layer](asn1Object)
            next_layer = getNextLayer[next_layer](asn1Object)

        spid = rfc3114.id_tsp_TEST_Whirlpool
        catid = rfc3114.id_tsp_TEST_Whirlpool_Categories
        conf = rfc3114.Whirlpool_SecurityClassification(value='whirlpool-confidential')

        self.assertIn(catid, rfc5755.securityCategoryMap)
        self.assertIn(rfc5755.id_at_clearance, rfc5280.certificateAttributesMap)
        self.assertIn(rfc5280.id_ce_subjectDirectoryAttributes, rfc5280.certificateExtensionsMap)

        security_label_okay = False

        for attr in attrs:
            if attr['attrType'] == rfc5035.id_aa_securityLabel:
                esssl, rest = der_decoder(
                    attr['attrValues'][0], asn1Spec=rfc5035.ESSSecurityLabel())

                self.assertFalse(rest)
                self.assertTrue(esssl.prettyPrint())
                self.assertEqual(attr['attrValues'][0], der_encoder(esssl))

                self.assertEqual(spid, esssl['security-policy-identifier'])
                self.assertEqual(conf, esssl['security-classification'])

                for cat in esssl['security-categories']:
                    if cat['type'] == catid:
                        scv, rest = der_decoder(
                            cat['value'], asn1Spec=rfc3114.SecurityCategoryValues())

                        self.assertFalse(rest)
                        self.assertTrue(scv.prettyPrint())
                        self.assertEqual(cat['value'], der_encoder(scv))

                        for scv_str in scv:
                            self.assertIn('USE ONLY', scv_str)
                            security_label_okay = True

        self.assertTrue(security_label_okay)

        clearance_okay = False
        for cert_choice in certs:
            for extn in cert_choice['certificate']['tbsCertificate']['extensions']:
                if extn['extnID'] == rfc5280.id_ce_subjectDirectoryAttributes:
                    ev, rest = der_decoder(
                        extn['extnValue'],
                        asn1Spec=rfc5280.certificateExtensionsMap[extn['extnID']])

                    self.assertFalse(rest)
                    self.assertTrue(ev.prettyPrint())
                    self.assertEqual(extn['extnValue'], der_encoder(ev))

                    for attr in ev:

                        if attr['type'] == rfc5755.id_at_clearance:
                            av, rest = der_decoder(
                                attr['values'][0],
                                asn1Spec=rfc5280.certificateAttributesMap[attr['type']])

                            self.assertEqual(spid, av['policyId'])

                            for cat in av['securityCategories']:

                                self.assertEqual(catid, cat['type'])

                                scv, rest = der_decoder(
                                    cat['value'],
                                    asn1Spec=rfc5755.securityCategoryMap[cat['type']])

                                self.assertFalse(rest)
                                self.assertTrue(scv.prettyPrint())
                                self.assertEqual(cat['value'], der_encoder(scv))

                                for scv_str in scv:
                                    self.assertIn('USE ONLY', scv_str)
                                    clearance_okay = True

        self.assertTrue(clearance_okay)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
