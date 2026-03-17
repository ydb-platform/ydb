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
from pyasn1_modules import rfc6482


class RPKIROATestCase(unittest.TestCase):
    roa_pem_text = """\
MIIGvwYJKoZIhvcNAQcCoIIGsDCCBqwCAQMxDTALBglghkgBZQMEAgEwKgYLKoZIhvcNAQkQ
ARigGwQZMBcCAwDj+zAQMA4EAgABMAgwBgMEAJMcLaCCBLwwggS4MIIDoKADAgECAgIGGDAN
BgkqhkiG9w0BAQsFADAzMTEwLwYDVQQDEyg2ZDZmYmZhOTc1M2RiOGQ4NDY0MzNkYjUzNTFk
OWE5ZWMwN2M5NmJkMB4XDTE5MDgyMDAwNDkyOVoXDTIwMDcwMTAwMDAwMFowMzExMC8GA1UE
AxMoNUI4M0REODdERTlBQzdDNkUzNEI4NzdERjUwMUEyQjEyMzBBODFCNDCCASIwDQYJKoZI
hvcNAQEBBQADggEPADCCAQoCggEBAJcnDgSUtiQeelGQsTx2Ou5cgmfq6KPSEgMz/XyZrRzj
wcqUQ/DyMYHyRJK8umKZjfMu+rItoPSkE26Wi9PcSnfuY+SyS9chTAtNOGMES6MbtHjNTmBF
Xar5CFGM8teLIRHlCcScesgSR7q2eKgQ+cLiLTZnol0Mpmuf2NIs+V63Y4Hn/T7QOoudg9nU
tmsh31hUN4jIENEXFvNDovkray25rl9aqFfW+dtkoNtdJjp367nNXCdp3GdE/3z0SIqT8wnh
F67tgR22mwzex3umteQBwmM+iR28vuHL4E5jwRKBoiEgGPYqq7gbfkcoFtR3AV6QGKSK2aJU
mUi+9VheS78CAwEAAaOCAdQwggHQMB0GA1UdDgQWBBRbg92H3prHxuNLh331AaKxIwqBtDAf
BgNVHSMEGDAWgBRtb7+pdT242EZDPbU1HZqewHyWvTAYBgNVHSABAf8EDjAMMAoGCCsGAQUF
Bw4CMFAGA1UdHwRJMEcwRaBDoEGGP3JzeW5jOi8vY2EucmcubmV0L3Jwa2kvUkduZXQtT1Uv
YlctX3FYVTl1TmhHUXoyMU5SMmFuc0I4bHIwLmNybDBkBggrBgEFBQcBAQRYMFYwVAYIKwYB
BQUHMAKGSHJzeW5jOi8vcnBraS5yaXBlLm5ldC9yZXBvc2l0b3J5L0RFRkFVTFQvYlctX3FY
VTl1TmhHUXoyMU5SMmFuc0I4bHIwLmNlcjAOBgNVHQ8BAf8EBAMCB4AwgYoGCCsGAQUFBwEL
BH4wfDBLBggrBgEFBQcwC4Y/cnN5bmM6Ly9jYS5yZy5uZXQvcnBraS9SR25ldC1PVS9XNFBk
aDk2YXg4YmpTNGQ5OVFHaXNTTUtnYlEucm9hMC0GCCsGAQUFBzANhiFodHRwczovL2NhLnJn
Lm5ldC9ycmRwL25vdGlmeS54bWwwHwYIKwYBBQUHAQcBAf8EEDAOMAwEAgABMAYDBACTHC0w
DQYJKoZIhvcNAQELBQADggEBAKhhoJ3XtHejvG6XkFaCTxJci10gOgNvvPFWqz+CfOX2LmB0
N3QhYjLiAZbfYSOxNReyL4bWDK/tpZgVA2VHuS8GB8fI8+nauQUiP38orVXKAbcUUxo7UkEM
HxQ5T61FtXrEZx8hgKTlsfof0G2Q+baSJzNV2MIUgHmSszL4Mx/fHUXv8b7l/5mZQbdv3cZ9
SbODHD0iOVAzK3fmHeuA4roSOk4mBQDWNRY1Ok+xH/HMDQdoOVtbfy57TZI2W7O2uxfElKvx
fBeEc9TOaWqDz0xvmJ6bdZnmWRuvqW1475mhxi0s/I4eE2ZdaCinvrgrglBp/jpZi1jitY14
dx+A1PMxggGqMIIBpgIBA4AUW4Pdh96ax8bjS4d99QGisSMKgbQwCwYJYIZIAWUDBAIBoGsw
GgYJKoZIhvcNAQkDMQ0GCyqGSIb3DQEJEAEYMBwGCSqGSIb3DQEJBTEPFw0xOTA4MjAwMDQ5
MjlaMC8GCSqGSIb3DQEJBDEiBCCfuHnOmhF2iBF3JXMOnoZCJzmE+Tcf8b+zObvDUpUddzAN
BgkqhkiG9w0BAQEFAASCAQBDlJIMKCqWsFV/tQj/XvpSJUxJybG+zwjrUKm4yTKv8QEGOzOD
aIL6irSOhhXeax6Lw0P2J7x+L3jGW1we1qWslumEDTr9kTE+kN/6rZuptUhwdrXcu3p9G6gJ
mAUQtzqe2jRN1T3eSBfz1CNU3C7+jSHXOc+4Tea5mKiVddsjotYHXX0PbSCS/ZZ1yzdeES0o
KWhXhW9ogS0bwtXWVTrciSekaRpp2n/pqcVEDxWg/5NpPiDlPNrRL/9eTEHFp940RAUfhbBh
pbC2J02N0KgxUJxIJnGnpZ7rXKpG4jMiTVry7XB9bnFxCvZGBdjQW1Hagrfpl2TiVxQFvJWl
IzU1
"""

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.roa_pem_text)

        layers = {}
        layers.update(rfc5652.cmsContentTypesMap)

        getNextLayer = {
            rfc5652.id_ct_contentInfo: lambda x: x['contentType'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContentType'],
            rfc6482.id_ct_routeOriginAuthz: lambda x: None
        }

        getNextSubstrate = {
            rfc5652.id_ct_contentInfo: lambda x: x['content'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContent'],
            rfc6482.id_ct_routeOriginAuthz: lambda x: None
        }

        next_layer = rfc5652.id_ct_contentInfo
        while next_layer:
            asn1Object, rest = der_decoder(substrate, asn1Spec=layers[next_layer])

            self.assertFalse(rest)
            self.assertTrue(asn1Object.prettyPrint())
            self.assertEqual(substrate, der_encoder(asn1Object))

            substrate = getNextSubstrate[next_layer](asn1Object)
            next_layer = getNextLayer[next_layer](asn1Object)

        self.assertEqual(0, asn1Object['version'])
        self.assertEqual(58363, asn1Object['asID'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.roa_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=rfc5652.ContentInfo(), decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        oid = asn1Object['content']['encapContentInfo']['eContentType']
        substrate = asn1Object['content']['encapContentInfo']['eContent']

        self.assertIn(oid, rfc5652.cmsContentTypesMap)

        asn1Object, rest = der_decoder(
            substrate, asn1Spec=rfc5652.cmsContentTypesMap[oid],
            decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))
        self.assertEqual(0, asn1Object['version'])
        self.assertEqual(58363, asn1Object['asID'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
