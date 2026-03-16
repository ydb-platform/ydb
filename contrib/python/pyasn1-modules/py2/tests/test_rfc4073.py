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
from pyasn1.compat.octets import str2octs

from pyasn1_modules import pem
from pyasn1_modules import rfc2634
from pyasn1_modules import rfc4073
from pyasn1_modules import rfc5652


class ContentCollectionTestCase(unittest.TestCase):
    pem_text = """\
MIIG/QYLKoZIhvcNAQkQAROgggbsMIIG6DCCAWcGCyqGSIb3DQEJEAEUoIIBVjCC
AVIwgfEGCSqGSIb3DQEHAaCB4wSB4ENvbnRlbnQtVHlwZTogdGV4dC9wbGFpbgoK
UkZDIDQwNzMsIHB1Ymxpc2hlZCBpbiBNYXkgMjAwNSwgZGVzY3JpYmVzIGEgY29u
dmVudGlvbiBmb3IgdXNpbmcgdGhlCkNyeXB0b2dyYXBoaWMgTWVzc2FnZSBTeW50
YXggKENNUykgdG8gcHJvdGVjdCBhIGNvbnRlbnQgY29sbGVjdGlvbi4gIElmCmRl
c2lyZWQsIGF0dHJpYnV0ZXMgY2FuIGJlIGFzc29jaWF0ZWQgd2l0aCB0aGUgY29u
dGVudC4KMFwwMwYLKoZIhvcNAQkQAgQxJDAiDBVBYnN0cmFjdCBmb3IgUkZDIDQw
NzMGCSqGSIb3DQEHATAlBgsqhkiG9w0BCRACBzEWBBSkLSXBiRWvbwnJKb4EGb1X
FwCa3zCCBXkGCyqGSIb3DQEJEAEUoIIFaDCCBWQwggT9BgkqhkiG9w0BBwGgggTu
BIIE6kNvbnRlbnQtVHlwZTogdGV4dC9wbGFpbgoKVGhlIGZvbGxvd2luZyBBU04u
MSBtb2R1bGUgZGVmaW5lcyB0aGUgc3RydWN0dXJlcyB0aGF0IGFyZSBuZWVkZWQg
dG8KaW1wbGVtZW50IHRoZSBzcGVjaWZpY2F0aW9uIGluIFJGQyA0MDczLiAgSXQg
aXMgZXhwZWN0ZWQgdG8gYmUgdXNlZCBpbgpjb25qdW5jdGlvbiB3aXRoIHRoZSBB
U04uMSBtb2R1bGVzIGluIFJGQyA1NjUyIGFuZCBSRkMgMzI3NC4KCiAgIENvbnRl
bnRDb2xsZWN0aW9uTW9kdWxlCiAgICAgeyBpc28oMSkgbWVtYmVyLWJvZHkoMikg
dXMoODQwKSByc2Fkc2koMTEzNTQ5KSBwa2NzKDEpCiAgICAgICBwa2NzLTkoOSkg
c21pbWUoMTYpIG1vZHVsZXMoMCkgMjYgfQoKICAgREVGSU5JVElPTlMgSU1QTElD
SVQgVEFHUyA6Oj0KICAgQkVHSU4KCiAgIElNUE9SVFMKICAgICBBdHRyaWJ1dGUs
IENvbnRlbnRJbmZvCiAgICAgICBGUk9NIENyeXB0b2dyYXBoaWNNZXNzYWdlU3lu
dGF4MjAwNCAtLSBbQ01TXQogICAgICAgICB7IGlzbygxKSBtZW1iZXItYm9keSgy
KSB1cyg4NDApIHJzYWRzaSgxMTM1NDkpCiAgICAgICAgICAgcGtjcygxKSBwa2Nz
LTkoOSkgc21pbWUoMTYpIG1vZHVsZXMoMCkgY21zLTIwMDEoMTQpIH07CgoKICAg
LS0gQ29udGVudCBDb2xsZWN0aW9uIENvbnRlbnQgVHlwZSBhbmQgT2JqZWN0IElk
ZW50aWZpZXIKCiAgIGlkLWN0LWNvbnRlbnRDb2xsZWN0aW9uIE9CSkVDVCBJREVO
VElGSUVSIDo6PSB7CiAgICAgICAgICAgaXNvKDEpIG1lbWJlci1ib2R5KDIpIHVz
KDg0MCkgcnNhZHNpKDExMzU0OSkgcGtjcygxKQogICAgICAgICAgIHBrY3M5KDkp
IHNtaW1lKDE2KSBjdCgxKSAxOSB9CgogICBDb250ZW50Q29sbGVjdGlvbiA6Oj0g
U0VRVUVOQ0UgU0laRSAoMS4uTUFYKSBPRiBDb250ZW50SW5mbwoKICAgLS0gQ29u
dGVudCBXaXRoIEF0dHJpYnV0ZXMgQ29udGVudCBUeXBlIGFuZCBPYmplY3QgSWRl
bnRpZmllcgoKICAgaWQtY3QtY29udGVudFdpdGhBdHRycyBPQkpFQ1QgSURFTlRJ
RklFUiA6Oj0gewogICAgICAgICAgIGlzbygxKSBtZW1iZXItYm9keSgyKSB1cyg4
NDApIHJzYWRzaSgxMTM1NDkpIHBrY3MoMSkKICAgICAgICAgICBwa2NzOSg5KSBz
bWltZSgxNikgY3QoMSkgMjAgfQoKICAgQ29udGVudFdpdGhBdHRyaWJ1dGVzIDo6
PSBTRVFVRU5DRSB7CiAgICAgICBjb250ZW50ICAgICBDb250ZW50SW5mbywKICAg
ICAgIGF0dHJzICAgICAgIFNFUVVFTkNFIFNJWkUgKDEuLk1BWCkgT0YgQXR0cmli
dXRlIH0KCiAgIEVORAowYTA4BgsqhkiG9w0BCRACBDEpMCcMGkFTTi4xIE1vZHVs
ZSBmcm9tIFJGQyA0MDczBgkqhkiG9w0BBwEwJQYLKoZIhvcNAQkQAgcxFgQUMbeK
buWO3egPDL8Kf7tBhzjIKLw=
"""

    def setUp(self):
        self.asn1Spec = rfc5652.ContentInfo()

    def testDerCodec(self):

        def test_layer(substrate, content_type):
            asn1Object, rest = der_decoder(substrate, asn1Spec=layers[content_type])
            self.assertFalse(rest)
            self.assertTrue(asn1Object.prettyPrint())
            self.assertEqual(substrate, der_encoder(asn1Object))

            if content_type == rfc4073.id_ct_contentWithAttrs:
                for attr in asn1Object['attrs']:
                    self.assertIn(attr['attrType'], rfc5652.cmsAttributesMap)
    
            return asn1Object

        layers = rfc5652.cmsContentTypesMap

        getNextLayer = {
            rfc5652.id_ct_contentInfo: lambda x: x['contentType'],
            rfc4073.id_ct_contentCollection: lambda x: x[0]['contentType'],
            rfc4073.id_ct_contentWithAttrs: lambda x: x['content']['contentType'],
            rfc5652.id_data: lambda x: None,
        }

        getNextSubstrate = {
            rfc5652.id_ct_contentInfo: lambda x: x['content'],
            rfc4073.id_ct_contentCollection: lambda x: x[0]['content'],
            rfc4073.id_ct_contentWithAttrs: lambda x: x['content']['content'],
            rfc5652.id_data: lambda x: None,
        }

        substrate = pem.readBase64fromText(self.pem_text)

        this_layer = rfc5652.id_ct_contentInfo

        while this_layer != rfc5652.id_data:
            if this_layer == rfc4073.id_ct_contentCollection:
                asn1Object = test_layer(substrate, this_layer)
                for ci in asn1Object:
                    substrate = ci['content']
                    this_layer = ci['contentType']
                    while this_layer != rfc5652.id_data:
                        asn1Object = test_layer(substrate, this_layer)
                        substrate = getNextSubstrate[this_layer](asn1Object)
                        this_layer = getNextLayer[this_layer](asn1Object)
            else:
                asn1Object = test_layer(substrate, this_layer)
                substrate = getNextSubstrate[this_layer](asn1Object)
                this_layer = getNextLayer[this_layer](asn1Object)

    def testOpenTypes(self):
        substrate = pem.readBase64fromText(self.pem_text)
        asn1Object, rest = der_decoder(substrate,
                                       asn1Spec=rfc5652.ContentInfo(),
                                       decodeOpenTypes=True)
        self.assertFalse(rest)
        self.assertTrue(asn1Object.prettyPrint())
        self.assertEqual(substrate, der_encoder(asn1Object))

        self.assertEqual(rfc4073.id_ct_contentCollection, asn1Object['contentType'])

        for ci in asn1Object['content']:
            self.assertIn(ci['contentType'], rfc5652.cmsContentTypesMap)
            self.assertEqual(rfc4073.id_ct_contentWithAttrs, ci['contentType'])

            next_ci = ci['content']['content']

            self.assertIn(next_ci['contentType'], rfc5652.cmsContentTypesMap)
            self.assertEqual(rfc5652.id_data, next_ci['contentType'])
            self.assertIn(str2octs('Content-Type: text'), next_ci['content'])

            for attr in ci['content']['attrs']:
                self.assertIn(attr['attrType'], rfc5652.cmsAttributesMap)
                if attr['attrType'] == rfc2634.id_aa_contentHint:
                    self.assertIn('RFC 4073', attr['attrValues'][0]['contentDescription'])


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
