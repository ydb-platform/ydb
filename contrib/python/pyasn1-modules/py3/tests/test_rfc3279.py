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
from pyasn1_modules import rfc3279


class RSACertificateTestCase(unittest.TestCase):
    rsa_cert_pem_text = """\
MIIE8TCCA9mgAwIBAgIQbyXcFa/fXqMIVgw7ek/H+DANBgkqhkiG9w0BAQUFADBv
MQswCQYDVQQGEwJTRTEUMBIGA1UEChMLQWRkVHJ1c3QgQUIxJjAkBgNVBAsTHUFk
ZFRydXN0IEV4dGVybmFsIFRUUCBOZXR3b3JrMSIwIAYDVQQDExlBZGRUcnVzdCBF
eHRlcm5hbCBDQSBSb290MB4XDTAwMDUzMDEwNDgzOFoXDTIwMDUzMDEwNDgzOFow
gYExCzAJBgNVBAYTAkdCMRswGQYDVQQIExJHcmVhdGVyIE1hbmNoZXN0ZXIxEDAO
BgNVBAcTB1NhbGZvcmQxGjAYBgNVBAoTEUNPTU9ETyBDQSBMaW1pdGVkMScwJQYD
VQQDEx5DT01PRE8gQ2VydGlmaWNhdGlvbiBBdXRob3JpdHkwggEiMA0GCSqGSIb3
DQEBAQUAA4IBDwAwggEKAoIBAQDQQIuLcuORG/dRwRtUBJjTqb/B5opdO4f7u4jO
DeMvPwaW8KIpUJmu2zuhV7B0UXHN7UKRTUH+qcjYaoZ3RLtZZpdQXrTULHBEz9o3
lUJpPDDEcbNS8CFNodi6OXwcnqMknfKDFpiqFnxDmxVbt640kf7UYiYYRpo/68H5
8ZBX66x6DYvbcjBqZtXgRqNw3GjZ/wRIiXfeten7Z21B6bw5vTLZYgLxsag9bjec
4i/i06Imi8a4VUOI4SM+pdIkOWpHqwDUobOpJf4NP6cdutNRwQuk2qw471VQJAVl
RpM0Ty2NrcbUIRnSjsoFYXEHc0flihkSvQRNzk6cpUisuyb3AgMBAAGjggF0MIIB
cDAfBgNVHSMEGDAWgBStvZh6NLQm9/rEJlTvA73gJMtUGjAdBgNVHQ4EFgQUC1jl
i8ZMFTekQKkwqSG+RzZaVv8wDgYDVR0PAQH/BAQDAgEGMA8GA1UdEwEB/wQFMAMB
Af8wEQYDVR0gBAowCDAGBgRVHSAAMEQGA1UdHwQ9MDswOaA3oDWGM2h0dHA6Ly9j
cmwudXNlcnRydXN0LmNvbS9BZGRUcnVzdEV4dGVybmFsQ0FSb290LmNybDCBswYI
KwYBBQUHAQEEgaYwgaMwPwYIKwYBBQUHMAKGM2h0dHA6Ly9jcnQudXNlcnRydXN0
LmNvbS9BZGRUcnVzdEV4dGVybmFsQ0FSb290LnA3YzA5BggrBgEFBQcwAoYtaHR0
cDovL2NydC51c2VydHJ1c3QuY29tL0FkZFRydXN0VVROU0dDQ0EuY3J0MCUGCCsG
AQUFBzABhhlodHRwOi8vb2NzcC51c2VydHJ1c3QuY29tMA0GCSqGSIb3DQEBBQUA
A4IBAQAHYJOZqs7Q00fQNzPeP2S35S6jJQzVMx0Njav2fkZ7WQaS44LE5/X289kF
z0k0LTdf9CXH8PtrI3fx8UDXTLtJRTHdAChntylMdagfeTHJNjcPyjVPjPF+3vxG
q79om3AjMC63xVx7ivsYE3lLkkKM3CyrbCK3KFOzGkrOG/soDrc6pNoN90AyT99v
uwFQ/IfTdtn8+7aEA8rJNhj33Wzbu7qBHKat/ij5z7micV0ZBepKRtxzQe+JlEKx
Q4hvNRevHmCDrHqMEHufyfaDbZ76iO4+3e6esL/garnQnweyCROa9aTlyFt5p0c1
M2jlVZ6qW8swC53HD79oRIGXi1FK
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.rsa_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc3279.rsaEncryption, spki_a['algorithm'])

        spki_pk = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['subjectPublicKey'].asOctets()
        pk, rest = der_decoder(spki_pk, asn1Spec=rfc3279.RSAPublicKey())

        self.assertFalse(rest)
        self.assertTrue(pk.prettyPrint())
        self.assertEqual(spki_pk, der_encoder(pk))
        self.assertEqual(65537, pk['publicExponent'])
        self.assertEqual(rfc3279.sha1WithRSAEncryption,
                         asn1Object['tbsCertificate']['signature']['algorithm'])
        self.assertEqual(rfc3279.sha1WithRSAEncryption,
                         asn1Object['signatureAlgorithm']['algorithm'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.rsa_cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc3279.rsaEncryption, spki_a['algorithm'])
        self.assertEqual(univ.Null(""), spki_a['parameters'])


class ECCertificateTestCase(unittest.TestCase):
    ec_cert_pem_text = """\
MIIDrDCCApSgAwIBAgIQCssoukZe5TkIdnRw883GEjANBgkqhkiG9w0BAQwFADBh
MQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3
d3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBD
QTAeFw0xMzAzMDgxMjAwMDBaFw0yMzAzMDgxMjAwMDBaMEwxCzAJBgNVBAYTAlVT
MRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxJjAkBgNVBAMTHURpZ2lDZXJ0IEVDQyBT
ZWN1cmUgU2VydmVyIENBMHYwEAYHKoZIzj0CAQYFK4EEACIDYgAE4ghC6nfYJN6g
LGSkE85AnCNyqQIKDjc/ITa4jVMU9tWRlUvzlgKNcR7E2Munn17voOZ/WpIRllNv
68DLP679Wz9HJOeaBy6Wvqgvu1cYr3GkvXg6HuhbPGtkESvMNCuMo4IBITCCAR0w
EgYDVR0TAQH/BAgwBgEB/wIBADAOBgNVHQ8BAf8EBAMCAYYwNAYIKwYBBQUHAQEE
KDAmMCQGCCsGAQUFBzABhhhodHRwOi8vb2NzcC5kaWdpY2VydC5jb20wQgYDVR0f
BDswOTA3oDWgM4YxaHR0cDovL2NybDMuZGlnaWNlcnQuY29tL0RpZ2lDZXJ0R2xv
YmFsUm9vdENBLmNybDA9BgNVHSAENjA0MDIGBFUdIAAwKjAoBggrBgEFBQcCARYc
aHR0cHM6Ly93d3cuZGlnaWNlcnQuY29tL0NQUzAdBgNVHQ4EFgQUo53mH/naOU/A
buiRy5Wl2jHiCp8wHwYDVR0jBBgwFoAUA95QNVbRTLtm8KPiGxvDl7I90VUwDQYJ
KoZIhvcNAQEMBQADggEBAMeKoENL7HTJxavVHzA1Nm6YVntIrAVjrnuaVyRXzG/6
3qttnMe2uuzO58pzZNvfBDcKAEmzP58mrZGMIOgfiA4q+2Y3yDDo0sIkp0VILeoB
UEoxlBPfjV/aKrtJPGHzecicZpIalir0ezZYoyxBEHQa0+1IttK7igZFcTMQMHp6
mCHdJLnsnLWSB62DxsRq+HfmNb4TDydkskO/g+l3VtsIh5RHFPVfKK+jaEyDj2D3
loB5hWp2Jp2VDCADjT7ueihlZGak2YPqmXTNbk19HOuNssWvFhtOyPNV6og4ETQd
Ea8/B6hPatJ0ES8q/HO3X8IVQwVs1n3aAr0im0/T+Xc=
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.ec_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc3279.id_ecPublicKey, spki_a['algorithm'])

        spki_a_p, rest = der_decoder(
            spki_a['parameters'], asn1Spec=rfc3279.EcpkParameters())

        self.assertFalse(rest)
        self.assertTrue(spki_a_p.prettyPrint())
        self.assertEqual(spki_a['parameters'], der_encoder(spki_a_p))
        self.assertEqual(univ.ObjectIdentifier('1.3.132.0.34'), spki_a_p['namedCurve'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.ec_cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc3279.id_ecPublicKey, spki_a['algorithm'])
        self.assertEqual(
            univ.ObjectIdentifier('1.3.132.0.34'), spki_a['parameters']['namedCurve'])


class DSACertificateTestCase(unittest.TestCase):
    dsa_cert_pem_text = """\
MIIDpjCCA0ygAwIBAgIUY8xt3l0B9nIPWSpjs0hDJUJZmCkwCwYJYIZIAWUDBAMC
MD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIEwJWQTEQMA4GA1UEBxMHSGVybmRvbjER
MA8GA1UEChMIQm9ndXMgQ0EwHhcNMTkxMDIwMjAxMjMwWhcNMjAxMDE5MjAxMjMw
WjBwMQswCQYDVQQGEwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24x
EDAOBgNVBAoTB0V4YW1wbGUxDjAMBgNVBAsTBUFsaWNlMSAwHgYJKoZIhvcNAQkB
FhFhbGljZUBleGFtcGxlLmNvbTCCAbYwggErBgcqhkjOOAQBMIIBHgKBgQCLpR53
xHfe+SiknAK/L9lm/ZO1109c9iYkriPIW/5MMlM+qc/tdRkKpG6ELIpfXTPtKCJm
zqqVIyTmAJryyE8Xw0Ie2mzYPU5ULvKmllQkjTsWgPGgQBkciZ0AW9ggD9VwZilg
4qh3iSO7T97hVQFnpCh6vm8pOH6UP/5kpr9ZJQIVANzdbztBJlJfqCB1t4h/NvSu
wCFvAoGAITP+jhYk9Rngd98l+5ccgauQ+cLEUBgNG2Wq56zBXQbLou6eKkQi7ecL
NiRmExq3IU3LOj426wSxL72Kw6FPyOEv3edIFkJJEHL4Z+ZJeVe//dzya0ddOJ7k
k6qNF2ic+viD/5Vm8yRyKiig2uHH/MgIesLdZnvbzvX+f/P0z50DgYQAAoGALAUl
jkOi1PxjjFVvhGfK95yIsrfbfcIEKUBaTs9NR2rbGWUeP+93paoXwP39X9wrJx2M
SWeHWhWKszNgoiyqYT0k4R9mem3WClotxOvB5fHfwIp2kQYvE7H0/TPdGhfUpHQG
YpyLQgT6L80meSKMFnu4VXGzOANhWDxu3JxiADCjgZQwgZEwCwYDVR0PBAQDAgeA
MEIGCWCGSAGG+EIBDQQ1FjNUaGlzIGNlcnRpZmljYXRlIGNhbm5vdCBiZSB0cnVz
dGVkIGZvciBhbnkgcHVycG9zZS4wHQYDVR0OBBYEFO37wHcauyc03rDc6cDRRsHz
gcK+MB8GA1UdIwQYMBaAFM1IZQGDsqYHWwb+I4EMxHPk0bU4MAsGCWCGSAFlAwQD
AgNHADBEAiBBRbfMzLi7+SVyO8SM3xxwUsMf/k1B+Nkvf1kBTfCfGwIgSAx/6mI+
pNqdXqZZGESXy1MT1aBc4ynPGLFUr2r7cPY=
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.dsa_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc3279.id_dsa, spki_a['algorithm'])

        spki_a_p, rest = der_decoder(spki_a['parameters'],
                                     asn1Spec=rfc3279.Dss_Parms())
        self.assertFalse(rest)
        self.assertTrue(spki_a_p.prettyPrint())
        self.assertEqual(spki_a['parameters'], der_encoder(spki_a_p))

        q_value = 1260916123897116834511257683105158021801897369967

        self.assertEqual(q_value, spki_a_p['q'])

        sig_value, rest = der_decoder(
            asn1Object['signature'].asOctets(), asn1Spec=rfc3279.Dss_Sig_Value())

        self.assertFalse(rest)
        self.assertTrue(sig_value.prettyPrint())
        self.assertEqual(asn1Object['signature'].asOctets(), der_encoder(sig_value))
        self.assertTrue(sig_value['r'].hasValue())
        self.assertTrue(sig_value['s'].hasValue())

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.dsa_cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc3279.id_dsa, spki_a['algorithm'])

        q_value = 1260916123897116834511257683105158021801897369967

        self.assertEqual(q_value, spki_a['parameters']['q'])


class KEACertificateTestCase(unittest.TestCase):
    kea_cert_pem_text = """\
MIICizCCAjOgAwIBAgIUY8xt3l0B9nIPWSpjs0hDJUJZmCgwCQYHKoZIzjgEAzA/
MQswCQYDVQQGEwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24xETAP
BgNVBAoTCEJvZ3VzIENBMB4XDTE5MTAyMDIwMDkyMVoXDTIwMTAxOTIwMDkyMVow
cDELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAlZBMRAwDgYDVQQHEwdIZXJuZG9uMRAw
DgYDVQQKEwdFeGFtcGxlMQ4wDAYDVQQDEwVBbGljZTEgMB4GCSqGSIb3DQEJARYR
YWxpY2VAZXhhbXBsZS5jb20wgaAwFwYJYIZIAWUCAQEWBApc+PEn5ladbYizA4GE
AAKBgB9Lc2QcoSW0E9/VnQ2xGBtpYh9MaDUBzIixbN8rhDwh0BBesD2TwHjzBpDM
2PJ6DD1ZbBcz2M3vJaIKoZ8hA2EUtbbHX1BSnVfAdeqr5St5gfnuxSdloUjLQlWO
rOYfpFVEp6hJoKAZiYfiXz0fohNXn8+fiU5k214byxlCPlU0o4GUMIGRMAsGA1Ud
DwQEAwIDCDBCBglghkgBhvhCAQ0ENRYzVGhpcyBjZXJ0aWZpY2F0ZSBjYW5ub3Qg
YmUgdHJ1c3RlZCBmb3IgYW55IHB1cnBvc2UuMB0GA1UdDgQWBBSE49bkPB9sQm27
Rs2jgAPMyY6UCDAfBgNVHSMEGDAWgBTNSGUBg7KmB1sG/iOBDMRz5NG1ODAJBgcq
hkjOOAQDA0cAMEQCIE9PWhUbnJVdNQcVYSc36BMZ+23uk2ITLsgSXtkScF6TAiAf
TPnJ5Wym0hv2fOpnPPsWTgqvLFYfX27GGTquuOd/6A==
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.kea_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc3279.id_keyExchangeAlgorithm, spki_a['algorithm'])

        spki_a_p, rest = der_decoder(spki_a['parameters'],
                                     asn1Spec=rfc3279.KEA_Parms_Id())
        self.assertFalse(rest)
        self.assertTrue(spki_a_p.prettyPrint())

        self.assertEqual(spki_a['parameters'], der_encoder(spki_a_p))
        self.assertEqual(univ.OctetString(hexValue='5cf8f127e6569d6d88b3'), spki_a_p)
        self.assertEqual(
            rfc3279.id_dsa_with_sha1, asn1Object['tbsCertificate']['signature']['algorithm'])
        self.assertEqual(
            rfc3279.id_dsa_with_sha1, asn1Object['signatureAlgorithm']['algorithm'])

        sig_value, rest = der_decoder(asn1Object['signature'].asOctets(),
                                      asn1Spec=rfc3279.Dss_Sig_Value())
        self.assertFalse(rest)
        self.assertTrue(sig_value.prettyPrint())
        self.assertEqual(asn1Object['signature'].asOctets(), der_encoder(sig_value))
        self.assertTrue(sig_value['r'].hasValue())
        self.assertTrue(sig_value['s'].hasValue())

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.kea_cert_pem_text)
        asn1Object, rest = der_decoder(
            substrate, asn1Spec=self.asn1Spec, decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc3279.id_keyExchangeAlgorithm, spki_a['algorithm'])
        self.assertEqual(
            univ.OctetString(hexValue='5cf8f127e6569d6d88b3'), spki_a['parameters'])

        self.assertEqual(rfc3279.id_dsa_with_sha1,
                         asn1Object['tbsCertificate']['signature']['algorithm'])
        self.assertEqual(
            rfc3279.id_dsa_with_sha1, asn1Object['signatureAlgorithm']['algorithm'])


class DHCertificateTestCase(unittest.TestCase):
    dh_cert_pem_text = """\
MIIEtDCCBFqgAwIBAgIUY8xt3l0B9nIPWSpjs0hDJUJZmCkwCwYJYIZIAWUDBAMC
MD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIEwJWQTEQMA4GA1UEBxMHSGVybmRvbjER
MA8GA1UEChMIQm9ndXMgQ0EwHhcNMTkxMDIwMjAxMjMwWhcNMjAxMDE5MjAxMjMw
WjBwMQswCQYDVQQGEwJVUzELMAkGA1UECBMCVkExEDAOBgNVBAcTB0hlcm5kb24x
EDAOBgNVBAoTB0V4YW1wbGUxDjAMBgNVBAsTBUFsaWNlMSAwHgYJKoZIhvcNAQkB
FhFhbGljZUBleGFtcGxlLmNvbTCCAsQwggI5BgcqhkjOPgIBMIICLAKCAQEAt9x/
0iwGww3k19h+wbODVK1yqjFzEY2pyfXthHcn+nEw+DpURJ+iOhYPr68E3XO5sB48
r5xTZhPN5+YejD3T8qhnDtiq4qrrSH7BOaEzqCDpHE2Bpoy3SodQ5Obaiu9Kx1ix
BRk/oRZUH+F+ATZmF0rPKrZGZOnmsh0IZm3dlmRR9FRGn0aJlZKXveqp+hZ97/r0
cbSo6wdT47APfocgweZMvgWu1IQBs6FiunRgaeX3RyLr4fnkvCzUM7TmxpRJYtL6
myAp007QvtgQ0AdEwVfNl3jQ0IIW7TtpXVxDDQaKZZe9yYrY4GV3etlYk8a4cpjN
rBxBCCTMASE4+iVtPQKCAQAg3m19vWc1TlHmkeqLwgvHN0Ufdyw5axWtc8qIJGZ1
MezhyLyD4RU0VFCSocJCCe2k2kS2P2vQERZZYcn/nCYuiswCjOCbnwKozfaTZ3Fc
1KOCtb4EEcuk/th5XNhWCYJJ7Hasym8zuPaqh5TLcsHXp0/lQUiOV2uVHnAt503A
HY1v4PhlZ3G0CRZMenafU0Ky7a6zhrqFvWgtSdo+vN0S9xS/KJuTaWsYgOAt4r2I
K1uwuWuvA5L1Qrdj8pDzMLkdlyHU1Jgjzk0rNQDTbUkZX9CAi/xKUGZysjWfOn1F
HC1vJ1sbP9nTXpWRain1/6yatB2RxLTvWYyAq9IsL/8PAiEAkY8lGryvcZI/pxXt
XwSaXEL2d77GSGICMGZa1wOJtdEDgYQAAoGALAUljkOi1PxjjFVvhGfK95yIsrfb
fcIEKUBaTs9NR2rbGWUeP+93paoXwP39X9wrJx2MSWeHWhWKszNgoiyqYT0k4R9m
em3WClotxOvB5fHfwIp2kQYvE7H0/TPdGhfUpHQGYpyLQgT6L80meSKMFnu4VXGz
OANhWDxu3JxiADCjgZQwgZEwCwYDVR0PBAQDAgMIMEIGCWCGSAGG+EIBDQQ1FjNU
aGlzIGNlcnRpZmljYXRlIGNhbm5vdCBiZSB0cnVzdGVkIGZvciBhbnkgcHVycG9z
ZS4wHQYDVR0OBBYEFO37wHcauyc03rDc6cDRRsHzgcK+MB8GA1UdIwQYMBaAFM1I
ZQGDsqYHWwb+I4EMxHPk0bU4MAsGCWCGSAFlAwQDAgNHADBEAiB1LU0esRdHDvSj
kqAm+3viU2a+hl66sLrK5lYBOYqGYAIgWG7bDxqFVP6/stHfdbeMovLejquEl9tr
iPEBA+EDHjk=
"""

    def setUp(self):
        self.asn1Spec = rfc5280.Certificate()

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.dh_cert_pem_text)
        asn1Object, rest = der_decoder(substrate, asn1Spec=self.asn1Spec)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc3279.dhpublicnumber, spki_a['algorithm'])

        spki_a_p, rest = der_decoder(
            spki_a['parameters'], asn1Spec=rfc3279.DomainParameters())

        self.assertFalse(rest)
        self.assertTrue(spki_a_p.prettyPrint())
        self.assertEqual(spki_a['parameters'], der_encoder(spki_a_p))

        q_value = 65838278260281264030127352144753816831178774189428428256716126077244217603537

        self.assertEqual(q_value, spki_a_p['q'])

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.dh_cert_pem_text)
        asn1Object, rest = der_decoder(substrate,
                                       asn1Spec=self.asn1Spec,
                                       decodeOpenTypes=True)

        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        spki_a = asn1Object['tbsCertificate']['subjectPublicKeyInfo']['algorithm']

        self.assertEqual(rfc3279.dhpublicnumber, spki_a['algorithm'])

        q_value = 65838278260281264030127352144753816831178774189428428256716126077244217603537

        self.assertEqual(q_value, spki_a['parameters']['q'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
