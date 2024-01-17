#
# This file is part of pyasn1-modules software.
#
# Copyright (c) 2020, Vigil Security, LLC
# License: http://snmplabs.com/pyasn1/license.html
#
import sys
import unittest

from pyasn1.codec.der.decoder import decode as der_decoder
from pyasn1.codec.der.encoder import encode as der_encoder

from pyasn1_modules import pem
from pyasn1_modules import rfc5652
from pyasn1_modules import rfc8769


class CBORContentTestCase(unittest.TestCase):
    pem_text = """\
MIIEHwYJKoZIhvcNAQcCoIIEEDCCBAwCAQMxDTALBglghkgBZQMEAgIwIQYLKoZIhvcNAQkQ
ASygEgQQgw9kUnVzc/tADzMzMzMzM6CCAnwwggJ4MIIB/qADAgECAgkApbNUKBuwbjswCgYI
KoZIzj0EAwMwPzELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAlZBMRAwDgYDVQQHDAdIZXJuZG9u
MREwDwYDVQQKDAhCb2d1cyBDQTAeFw0xOTA1MjkxNDQ1NDFaFw0yMDA1MjgxNDQ1NDFaMHAx
CzAJBgNVBAYTAlVTMQswCQYDVQQIEwJWQTEQMA4GA1UEBxMHSGVybmRvbjEQMA4GA1UEChMH
RXhhbXBsZTEOMAwGA1UEAxMFQWxpY2UxIDAeBgkqhkiG9w0BCQEWEWFsaWNlQGV4YW1wbGUu
Y29tMHYwEAYHKoZIzj0CAQYFK4EEACIDYgAE+M2fBy/sRA6V1pKFqecRTE8+LuAHtZxes1wm
JZrBBg+bz7uYZfYQxI3dVB0YCSD6Mt3yXFlnmfBRwoqyArbjIBYrDbHBv2k8Csg2DhQ7qs/w
to8hMKoFgkcscqIbiV7Zo4GUMIGRMAsGA1UdDwQEAwIHgDBCBglghkgBhvhCAQ0ENRYzVGhp
cyBjZXJ0aWZpY2F0ZSBjYW5ub3QgYmUgdHJ1c3RlZCBmb3IgYW55IHB1cnBvc2UuMB0GA1Ud
DgQWBBTEuloOPnrjPIGw9AKqaLsW4JYONTAfBgNVHSMEGDAWgBTyNds0BNqlVfK9aQOZsGLs
4hUIwTAKBggqhkjOPQQDAwNoADBlAjBjuR/RNbgL3kRhmn+PJTeKaL9sh/oQgHOYTgLmSnv3
+NDCkhfKuMNoo/tHrkmihYgCMQC94MaerDIrQpi0IDh+v0QSAv9rMife8tClafXWtDwwL8MS
7oAh0ymT446Uizxx3PUxggFTMIIBTwIBATBMMD8xCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJW
QTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9ndXMgQ0ECCQCls1QoG7BuOzALBglg
hkgBZQMEAgKgezAaBgkqhkiG9w0BCQMxDQYLKoZIhvcNAQkQASwwHAYJKoZIhvcNAQkFMQ8X
DTIwMDExNDIyMjIxNVowPwYJKoZIhvcNAQkEMTIEMADSWdHn4vsesm9XnjJq1WxkoV6EtD+f
qDAs1JEpZMZ+n8AtUxvC5SFobYpGCl+fsDAKBggqhkjOPQQDAwRmMGQCMGclPwvZLwVJqgON
mOfnxSF8Cqn3AC+ZFBg7VplspiuhKPNIyu3IofqZjCxw0TzSpAIwEK0JxNlY28KDb5te0iN6
I2hw+am26W+PRyltVVGUAISHM2kA4tG39HcxEQi+6HJx
"""

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        
        layers = { }
        layers.update(rfc5652.cmsContentTypesMap)

        getNextLayer = {
            rfc5652.id_ct_contentInfo: lambda x: x['contentType'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContentType'],
        }

        getNextSubstrate = {
            rfc5652.id_ct_contentInfo: lambda x: x['content'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContent'],
        }

        next_layer = rfc5652.id_ct_contentInfo
        while next_layer in layers:
            asn1Object, rest = der_decoder(
                substrate, asn1Spec=layers[next_layer])

            self.assertFalse(rest)
            self.assertTrue(asn1Object.prettyPrint())
            self.assertEqual(substrate, der_encoder(asn1Object))

            substrate = getNextSubstrate[next_layer](asn1Object)
            next_layer = getNextLayer[next_layer](asn1Object)

        self.assertEqual(rfc8769.id_ct_cbor, next_layer)


class CBORSequenceContentTestCase(unittest.TestCase):
    pem_text = """\
MIIEKQYJKoZIhvcNAQcCoIIEGjCCBBYCAQMxDTALBglghkgBZQMEAgIwKgYLKoZIhvcNAQkQ
AS2gGwQZgw9kUnVzc/tADzMzMzMzM6MDCSD1YWFhYqCCAnwwggJ4MIIB/qADAgECAgkApbNU
KBuwbjswCgYIKoZIzj0EAwMwPzELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAlZBMRAwDgYDVQQH
DAdIZXJuZG9uMREwDwYDVQQKDAhCb2d1cyBDQTAeFw0xOTA1MjkxNDQ1NDFaFw0yMDA1Mjgx
NDQ1NDFaMHAxCzAJBgNVBAYTAlVTMQswCQYDVQQIEwJWQTEQMA4GA1UEBxMHSGVybmRvbjEQ
MA4GA1UEChMHRXhhbXBsZTEOMAwGA1UEAxMFQWxpY2UxIDAeBgkqhkiG9w0BCQEWEWFsaWNl
QGV4YW1wbGUuY29tMHYwEAYHKoZIzj0CAQYFK4EEACIDYgAE+M2fBy/sRA6V1pKFqecRTE8+
LuAHtZxes1wmJZrBBg+bz7uYZfYQxI3dVB0YCSD6Mt3yXFlnmfBRwoqyArbjIBYrDbHBv2k8
Csg2DhQ7qs/wto8hMKoFgkcscqIbiV7Zo4GUMIGRMAsGA1UdDwQEAwIHgDBCBglghkgBhvhC
AQ0ENRYzVGhpcyBjZXJ0aWZpY2F0ZSBjYW5ub3QgYmUgdHJ1c3RlZCBmb3IgYW55IHB1cnBv
c2UuMB0GA1UdDgQWBBTEuloOPnrjPIGw9AKqaLsW4JYONTAfBgNVHSMEGDAWgBTyNds0BNql
VfK9aQOZsGLs4hUIwTAKBggqhkjOPQQDAwNoADBlAjBjuR/RNbgL3kRhmn+PJTeKaL9sh/oQ
gHOYTgLmSnv3+NDCkhfKuMNoo/tHrkmihYgCMQC94MaerDIrQpi0IDh+v0QSAv9rMife8tCl
afXWtDwwL8MS7oAh0ymT446Uizxx3PUxggFUMIIBUAIBATBMMD8xCzAJBgNVBAYTAlVTMQsw
CQYDVQQIDAJWQTEQMA4GA1UEBwwHSGVybmRvbjERMA8GA1UECgwIQm9ndXMgQ0ECCQCls1Qo
G7BuOzALBglghkgBZQMEAgKgezAaBgkqhkiG9w0BCQMxDQYLKoZIhvcNAQkQAS0wHAYJKoZI
hvcNAQkFMQ8XDTIwMDExNDIyMjIxNVowPwYJKoZIhvcNAQkEMTIEMOsEu3dGU5j6fKZbsZPL
LDA8QWxpP36CPDZWr3BVJ3R5mMCKCSmoWtVRnB7XASQcjTAKBggqhkjOPQQDAwRnMGUCMBLW
PyYw4c11nrH97KHnEmx3BSDX/SfepFNM6PoPR5HCI+OR/v/wlIIByuhyrIl8xAIxAK8dEwOe
I06um+ATKQzUcbgq0PCKA7T31pAq46fsWc5tA+mMARTrxZjSXsDneeAWpw==
"""

    def testDerCodec(self):
        substrate = pem.readBase64fromText(self.pem_text)
        
        layers = { }
        layers.update(rfc5652.cmsContentTypesMap)

        getNextLayer = {
            rfc5652.id_ct_contentInfo: lambda x: x['contentType'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContentType'],
        }

        getNextSubstrate = {
            rfc5652.id_ct_contentInfo: lambda x: x['content'],
            rfc5652.id_signedData: lambda x: x['encapContentInfo']['eContent'],
        }

        next_layer = rfc5652.id_ct_contentInfo
        while next_layer in layers:
            asn1Object, rest = der_decoder(
                substrate, asn1Spec=layers[next_layer])

            self.assertFalse(rest)
            self.assertTrue(asn1Object.prettyPrint())
            self.assertEqual(substrate, der_encoder(asn1Object))

            substrate = getNextSubstrate[next_layer](asn1Object)
            next_layer = getNextLayer[next_layer](asn1Object)

        self.assertEqual(rfc8769.id_ct_cborSequence, next_layer)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    import sys

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
