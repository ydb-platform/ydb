# -*- coding: utf-8 -*-
from unittest import mock

from oauthlib.oauth2.rfc6749 import errors
from oauthlib.oauth2.rfc6749.tokens import BearerToken
from oauthlib.openid.connect.core.grant_types.hybrid import HybridGrant

from __tests__.oauth2.rfc6749.grant_types.test_authorization_code import (
    AuthorizationCodeGrantTest,
)

from .test_authorization_code import OpenIDAuthCodeTest


class OpenIDHybridInterferenceTest(AuthorizationCodeGrantTest):
    """Test that OpenID don't interfere with normal OAuth 2 flows."""

    def setUp(self):
        super().setUp()
        self.auth = HybridGrant(request_validator=self.mock_validator)


class OpenIDHybridCodeTokenTest(OpenIDAuthCodeTest):

    def setUp(self):
        super().setUp()
        self.request.response_type = 'code token'
        self.request.nonce = None
        self.auth = HybridGrant(request_validator=self.mock_validator)
        self.url_query = 'https://a.b/cb?code=abc&state=abc&token_type=Bearer&expires_in=3600&scope=hello+openid&access_token=abc'
        self.url_fragment = 'https://a.b/cb#code=abc&state=abc&token_type=Bearer&expires_in=3600&scope=hello+openid&access_token=abc'

    @mock.patch('oauthlib.common.generate_token')
    def test_optional_nonce(self, generate_token):
        generate_token.return_value = 'abc'
        self.request.nonce = 'xyz'
        scope, info = self.auth.validate_authorization_request(self.request)

        bearer = BearerToken(self.mock_validator)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertURLEqual(h['Location'], self.url_fragment, parse_fragment=True)
        self.assertIsNone(b)
        self.assertEqual(s, 302)


class OpenIDHybridCodeIdTokenTest(OpenIDAuthCodeTest):

    def setUp(self):
        super().setUp()
        self.mock_validator.get_code_challenge.return_value = None
        self.request.response_type = 'code id_token'
        self.request.nonce = 'zxc'
        self.auth = HybridGrant(request_validator=self.mock_validator)
        token = 'MOCKED_TOKEN'
        self.url_query = 'https://a.b/cb?code=abc&state=abc&id_token=%s' % token
        self.url_fragment = 'https://a.b/cb#code=abc&state=abc&id_token=%s' % token

    @mock.patch('oauthlib.common.generate_token')
    def test_required_nonce(self, generate_token):
        generate_token.return_value = 'abc'
        self.request.nonce = None
        self.assertRaises(errors.InvalidRequestError, self.auth.validate_authorization_request, self.request)

        bearer = BearerToken(self.mock_validator)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=invalid_request', h['Location'])
        self.assertIsNone(b)
        self.assertEqual(s, 302)

    def test_id_token_contains_nonce(self):
        token = {}
        self.mock_validator.get_id_token.side_effect = None
        self.mock_validator.get_id_token.return_value = None
        token = self.auth.add_id_token(token, None, self.request)
        assert self.mock_validator.finalize_id_token.call_count == 1
        claims = self.mock_validator.finalize_id_token.call_args[0][0]
        assert "nonce" in claims


class OpenIDHybridCodeIdTokenTokenTest(OpenIDAuthCodeTest):

    def setUp(self):
        super().setUp()
        self.mock_validator.get_code_challenge.return_value = None
        self.request.response_type = 'code id_token token'
        self.request.nonce = 'xyz'
        self.auth = HybridGrant(request_validator=self.mock_validator)
        token = 'MOCKED_TOKEN'
        self.url_query = 'https://a.b/cb?code=abc&state=abc&token_type=Bearer&expires_in=3600&scope=hello+openid&access_token=abc&id_token=%s' % token
        self.url_fragment = 'https://a.b/cb#code=abc&state=abc&token_type=Bearer&expires_in=3600&scope=hello+openid&access_token=abc&id_token=%s' % token

    @mock.patch('oauthlib.common.generate_token')
    def test_required_nonce(self, generate_token):
        generate_token.return_value = 'abc'
        self.request.nonce = None
        self.assertRaises(errors.InvalidRequestError, self.auth.validate_authorization_request, self.request)

        bearer = BearerToken(self.mock_validator)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=invalid_request', h['Location'])
        self.assertIsNone(b)
        self.assertEqual(s, 302)
