# -*- coding: utf-8 -*-
import os
from time import time
from unittest.mock import patch

import jwt

from oauthlib.common import Request
from oauthlib.oauth2 import ServiceApplicationClient

from tests.unittest import TestCase


class ServiceApplicationClientTest(TestCase):

    gt = ServiceApplicationClient.grant_type

    private_key = """
-----BEGIN RSA PRIVATE KEY-----
MIICXgIBAAKBgQDk1/bxyS8Q8jiheHeYYp/4rEKJopeQRRKKpZI4s5i+UPwVpupG
AlwXWfzXwSMaKPAoKJNdu7tqKRniqst5uoHXw98gj0x7zamu0Ck1LtQ4c7pFMVah
5IYGhBi2E9ycNS329W27nJPWNCbESTu7snVlG8V8mfvGGg3xNjTMO7IdrwIDAQAB
AoGBAOQ2KuH8S5+OrsL4K+wfjoCi6MfxCUyqVU9GxocdM1m30WyWRFMEz2nKJ8fR
p3vTD4w8yplTOhcoXdQZl0kRoaDzrcYkm2VvJtQRrX7dKFT8dR8D/Tr7dNQLOXfC
DY6xveQczE7qt7Vk7lp4FqmxBsaaEuokt78pOOjywZoInjZhAkEA9wz3zoZNT0/i
rf6qv2qTIeieUB035N3dyw6f1BGSWYaXSuerDCD/J1qZbAPKKhyHZbVawFt3UMhe
542UftBaxQJBAO0iJy1I8GQjGnS7B3yvyH3CcLYGy296+XO/2xKp/d/ty1OIeovx
C60pLNwuFNF3z9d2GVQAdoQ89hUkOtjZLeMCQQD0JO6oPHUeUjYT+T7ImAv7UKVT
Suy30sKjLzqoGw1kR+wv7C5PeDRvscs4wa4CW9s6mjSrMDkDrmCLuJDtmf55AkEA
kmaMg2PNrjUR51F0zOEFycaaqXbGcFwe1/xx9zLmHzMDXd4bsnwt9kk+fe0hQzVS
JzatanQit3+feev1PN3QewJAWv4RZeavEUhKv+kLe95Yd0su7lTLVduVgh4v5yLT
Ga6FHdjGPcfajt+nrpB1n8UQBEH9ZxniokR/IPvdMlxqXA==
-----END RSA PRIVATE KEY-----
"""

    public_key = """
-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDk1/bxyS8Q8jiheHeYYp/4rEKJ
opeQRRKKpZI4s5i+UPwVpupGAlwXWfzXwSMaKPAoKJNdu7tqKRniqst5uoHXw98g
j0x7zamu0Ck1LtQ4c7pFMVah5IYGhBi2E9ycNS329W27nJPWNCbESTu7snVlG8V8
mfvGGg3xNjTMO7IdrwIDAQAB
-----END PUBLIC KEY-----
"""

    subject = 'resource-owner@provider.com'

    issuer = 'the-client@provider.com'

    audience = 'https://provider.com/token'

    client_id = "someclientid"
    scope = ["/profile"]
    kwargs = {
        "some": "providers",
        "require": "extra arguments"
    }

    body = "isnot=empty"

    body_up = "not=empty&grant_type=%s" % gt
    body_kwargs = body_up + "&some=providers&require=extra+arguments"

    token_json = ('{   "access_token":"2YotnFZFEjr1zCsicMWpAA",'
                  '    "token_type":"example",'
                  '    "expires_in":3600,'
                  '    "scope":"/profile",'
                  '    "example_parameter":"example_value"}')
    token = {
        "access_token": "2YotnFZFEjr1zCsicMWpAA",
        "token_type": "example",
        "expires_in": 3600,
        "scope": ["/profile"],
        "example_parameter": "example_value"
    }

    @patch('time.time')
    def test_request_body(self, t):
        t.return_value = time()
        self.token['expires_at'] = self.token['expires_in'] + t.return_value

        client = ServiceApplicationClient(
                self.client_id, private_key=self.private_key)

        # Basic with min required params
        body = client.prepare_request_body(issuer=self.issuer,
                                           subject=self.subject,
                                           audience=self.audience,
                                           body=self.body)
        r = Request('https://a.b', body=body)
        self.assertEqual(r.isnot, 'empty')
        self.assertEqual(r.grant_type, ServiceApplicationClient.grant_type)

        claim = jwt.decode(r.assertion, self.public_key, audience=self.audience, algorithms=['RS256'])

        self.assertEqual(claim['iss'], self.issuer)
        # audience verification is handled during decode now
        self.assertEqual(claim['sub'], self.subject)
        self.assertEqual(claim['iat'], int(t.return_value))
        self.assertNotIn('nbf', claim)
        self.assertNotIn('jti', claim)

        # Missing issuer parameter
        self.assertRaises(ValueError, client.prepare_request_body,
            issuer=None, subject=self.subject, audience=self.audience, body=self.body)

        # Missing subject parameter
        self.assertRaises(ValueError, client.prepare_request_body,
            issuer=self.issuer, subject=None, audience=self.audience, body=self.body)

        # Missing audience parameter
        self.assertRaises(ValueError, client.prepare_request_body,
            issuer=self.issuer, subject=self.subject, audience=None, body=self.body)

        # Optional kwargs
        not_before = time() - 3600
        jwt_id = '8zd15df4s35f43sd'
        body = client.prepare_request_body(issuer=self.issuer,
                                           subject=self.subject,
                                           audience=self.audience,
                                           body=self.body,
                                           not_before=not_before,
                                           jwt_id=jwt_id)

        r = Request('https://a.b', body=body)
        self.assertEqual(r.isnot, 'empty')
        self.assertEqual(r.grant_type, ServiceApplicationClient.grant_type)

        claim = jwt.decode(r.assertion, self.public_key, audience=self.audience, algorithms=['RS256'])

        self.assertEqual(claim['iss'], self.issuer)
        # audience verification is handled during decode now
        self.assertEqual(claim['sub'], self.subject)
        self.assertEqual(claim['iat'], int(t.return_value))
        self.assertEqual(claim['nbf'], not_before)
        self.assertEqual(claim['jti'], jwt_id)

    @patch('time.time')
    def test_request_body_no_initial_private_key(self, t):
        t.return_value = time()
        self.token['expires_at'] = self.token['expires_in'] + t.return_value

        client = ServiceApplicationClient(
                self.client_id, private_key=None)

        # Basic with private key provided
        body = client.prepare_request_body(issuer=self.issuer,
                                           subject=self.subject,
                                           audience=self.audience,
                                           body=self.body,
                                           private_key=self.private_key)
        r = Request('https://a.b', body=body)
        self.assertEqual(r.isnot, 'empty')
        self.assertEqual(r.grant_type, ServiceApplicationClient.grant_type)

        claim = jwt.decode(r.assertion, self.public_key, audience=self.audience, algorithms=['RS256'])

        self.assertEqual(claim['iss'], self.issuer)
        # audience verification is handled during decode now
        self.assertEqual(claim['sub'], self.subject)
        self.assertEqual(claim['iat'], int(t.return_value))

        # No private key provided
        self.assertRaises(ValueError, client.prepare_request_body,
            issuer=self.issuer, subject=self.subject, audience=self.audience, body=self.body)

    @patch('time.time')
    def test_parse_token_response(self, t):
        t.return_value = time()
        self.token['expires_at'] = self.token['expires_in'] + t.return_value

        client = ServiceApplicationClient(self.client_id)

        # Parse code and state
        response = client.parse_request_body_response(self.token_json, scope=self.scope)
        self.assertEqual(response, self.token)
        self.assertEqual(client.access_token, response.get("access_token"))
        self.assertEqual(client.refresh_token, response.get("refresh_token"))
        self.assertEqual(client.token_type, response.get("token_type"))

        # Mismatching state
        self.assertRaises(Warning, client.parse_request_body_response, self.token_json, scope="invalid")
        os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '2'
        token = client.parse_request_body_response(self.token_json, scope="invalid")
        self.assertTrue(token.scope_changed)
        del os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE']
