# -*- coding: utf-8 -*-
from json import loads
from unittest.mock import MagicMock

from oauthlib.common import urlencode
from oauthlib.oauth2 import IntrospectEndpoint, RequestValidator

from tests.unittest import TestCase


class IntrospectEndpointTest(TestCase):

    def setUp(self):
        self.validator = MagicMock(wraps=RequestValidator())
        self.validator.client_authentication_required.return_value = True
        self.validator.authenticate_client.return_value = True
        self.validator.validate_bearer_token.return_value = True
        self.validator.introspect_token.return_value = {}
        self.endpoint = IntrospectEndpoint(self.validator)

        self.uri = 'should_not_matter'
        self.headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        self.resp_h = {
            'Cache-Control': 'no-store',
            'Content-Type': 'application/json',
            'Pragma': 'no-cache'
        }
        self.resp_b = {
            "active": True
        }

    def test_introspect_token(self):
        for token_type in ('access_token', 'refresh_token', 'invalid'):
            body = urlencode([('token', 'foo'),
                              ('token_type_hint', token_type)])
            h, b, s = self.endpoint.create_introspect_response(self.uri,
                    headers=self.headers, body=body)
            self.assertEqual(h, self.resp_h)
            self.assertEqual(loads(b), self.resp_b)
            self.assertEqual(s, 200)

    def test_introspect_token_nohint(self):
        # don't specify token_type_hint
        body = urlencode([('token', 'foo')])
        h, b, s = self.endpoint.create_introspect_response(self.uri,
                headers=self.headers, body=body)
        self.assertEqual(h, self.resp_h)
        self.assertEqual(loads(b), self.resp_b)
        self.assertEqual(s, 200)

    def test_introspect_token_false(self):
        self.validator.introspect_token.return_value = None
        body = urlencode([('token', 'foo')])
        h, b, s = self.endpoint.create_introspect_response(self.uri,
                headers=self.headers, body=body)
        self.assertEqual(h, self.resp_h)
        self.assertEqual(loads(b), {"active": False})
        self.assertEqual(s, 200)

    def test_introspect_token_claims(self):
        self.validator.introspect_token.return_value = {"foo": "bar"}
        body = urlencode([('token', 'foo')])
        h, b, s = self.endpoint.create_introspect_response(self.uri,
                headers=self.headers, body=body)
        self.assertEqual(h, self.resp_h)
        self.assertEqual(loads(b), {"active": True, "foo": "bar"})
        self.assertEqual(s, 200)

    def test_introspect_token_claims_spoof_active(self):
        self.validator.introspect_token.return_value = {"foo": "bar", "active": False}
        body = urlencode([('token', 'foo')])
        h, b, s = self.endpoint.create_introspect_response(self.uri,
                headers=self.headers, body=body)
        self.assertEqual(h, self.resp_h)
        self.assertEqual(loads(b), {"active": True, "foo": "bar"})
        self.assertEqual(s, 200)

    def test_introspect_token_client_authentication_failed(self):
        self.validator.authenticate_client.return_value = False
        body = urlencode([('token', 'foo'),
                          ('token_type_hint', 'access_token')])
        h, b, s = self.endpoint.create_introspect_response(self.uri,
                headers=self.headers, body=body)
        self.assertEqual(h, {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-store',
            'Pragma': 'no-cache',
            "WWW-Authenticate": 'Bearer error="invalid_client"'
        })
        self.assertEqual(loads(b)['error'], 'invalid_client')
        self.assertEqual(s, 401)

    def test_introspect_token_public_client_authentication(self):
        self.validator.client_authentication_required.return_value = False
        self.validator.authenticate_client_id.return_value = True
        for token_type in ('access_token', 'refresh_token', 'invalid'):
            body = urlencode([('token', 'foo'),
                              ('token_type_hint', token_type)])
            h, b, s = self.endpoint.create_introspect_response(self.uri,
                    headers=self.headers, body=body)
            self.assertEqual(h, self.resp_h)
            self.assertEqual(loads(b), self.resp_b)
            self.assertEqual(s, 200)

    def test_introspect_token_public_client_authentication_failed(self):
        self.validator.client_authentication_required.return_value = False
        self.validator.authenticate_client_id.return_value = False
        body = urlencode([('token', 'foo'),
                          ('token_type_hint', 'access_token')])
        h, b, s = self.endpoint.create_introspect_response(self.uri,
                headers=self.headers, body=body)
        self.assertEqual(h, {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-store',
            'Pragma': 'no-cache',
            "WWW-Authenticate": 'Bearer error="invalid_client"'
        })
        self.assertEqual(loads(b)['error'], 'invalid_client')
        self.assertEqual(s, 401)

    def test_introspect_unsupported_token(self):
        endpoint = IntrospectEndpoint(self.validator,
                                      supported_token_types=['access_token'])
        body = urlencode([('token', 'foo'),
                          ('token_type_hint', 'refresh_token')])
        h, b, s = endpoint.create_introspect_response(self.uri,
                headers=self.headers, body=body)
        self.assertEqual(h, self.resp_h)
        self.assertEqual(loads(b)['error'], 'unsupported_token_type')
        self.assertEqual(s, 400)

        h, b, s = endpoint.create_introspect_response(self.uri,
                headers=self.headers, body='')
        self.assertEqual(h, self.resp_h)
        self.assertEqual(loads(b)['error'], 'invalid_request')
        self.assertEqual(s, 400)

    def test_introspect_invalid_request_method(self):
        endpoint = IntrospectEndpoint(self.validator,
                                      supported_token_types=['access_token'])
        test_methods = ['GET', 'pUt', 'dEleTe', 'paTcH']
        test_methods = test_methods + [x.lower() for x in test_methods] + [x.upper() for x in test_methods]
        for method in test_methods:
            body = urlencode([('token', 'foo'),
                              ('token_type_hint', 'refresh_token')])
            h, b, s = endpoint.create_introspect_response(self.uri,
                    http_method = method, headers=self.headers, body=body)
            self.assertEqual(h, self.resp_h)
            self.assertEqual(loads(b)['error'], 'invalid_request')
            self.assertIn('Unsupported request method', loads(b)['error_description'])
            self.assertEqual(s, 400)

    def test_introspect_bad_post_request(self):
        endpoint = IntrospectEndpoint(self.validator,
                                      supported_token_types=['access_token'])
        for param in ['token', 'secret', 'code', 'foo']:
            uri = 'http://some.endpoint?' + urlencode([(param, 'secret')])
            body = urlencode([('token', 'foo'),
                              ('token_type_hint', 'access_token')])
            h, b, s = endpoint.create_introspect_response(
                uri,
                headers=self.headers, body=body)
            self.assertEqual(h, self.resp_h)
            self.assertEqual(loads(b)['error'], 'invalid_request')
            self.assertIn('query parameters are not allowed', loads(b)['error_description'])
            self.assertEqual(s, 400)
