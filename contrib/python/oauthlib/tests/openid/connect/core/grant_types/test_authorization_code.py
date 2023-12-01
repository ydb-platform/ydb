# -*- coding: utf-8 -*-
import json
from unittest import mock

from oauthlib.common import Request
from oauthlib.oauth2.rfc6749.errors import (
    ConsentRequired, InvalidRequestError, LoginRequired,
)
from oauthlib.oauth2.rfc6749.tokens import BearerToken
from oauthlib.openid.connect.core.grant_types.authorization_code import (
    AuthorizationCodeGrant,
)

from __tests__.oauth2.rfc6749.grant_types.test_authorization_code import (
    AuthorizationCodeGrantTest,
)
from tests.unittest import TestCase


def get_id_token_mock(token, token_handler, request):
    return "MOCKED_TOKEN"


class OpenIDAuthCodeInterferenceTest(AuthorizationCodeGrantTest):
    """Test that OpenID don't interfere with normal OAuth 2 flows."""

    def setUp(self):
        super().setUp()
        self.auth = AuthorizationCodeGrant(request_validator=self.mock_validator)


class OpenIDAuthCodeTest(TestCase):

    def setUp(self):
        self.request = Request('http://a.b/path')
        self.request.scopes = ('hello', 'openid')
        self.request.expires_in = 1800
        self.request.client_id = 'abcdef'
        self.request.code = '1234'
        self.request.response_type = 'code'
        self.request.grant_type = 'authorization_code'
        self.request.redirect_uri = 'https://a.b/cb'
        self.request.state = 'abc'
        self.request.nonce = None

        self.mock_validator = mock.MagicMock()
        self.mock_validator.authenticate_client.side_effect = self.set_client
        self.mock_validator.get_code_challenge.return_value = None
        self.mock_validator.get_id_token.side_effect = get_id_token_mock
        self.auth = AuthorizationCodeGrant(request_validator=self.mock_validator)

        self.url_query = 'https://a.b/cb?code=abc&state=abc'
        self.url_fragment = 'https://a.b/cb#code=abc&state=abc'

    def set_client(self, request):
        request.client = mock.MagicMock()
        request.client.client_id = 'mocked'
        return True

    @mock.patch('oauthlib.common.generate_token')
    def test_authorization(self, generate_token):

        scope, info = self.auth.validate_authorization_request(self.request)

        generate_token.return_value = 'abc'
        bearer = BearerToken(self.mock_validator)
        self.request.response_mode = 'query'
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertURLEqual(h['Location'], self.url_query)
        self.assertIsNone(b)
        self.assertEqual(s, 302)

        self.request.response_mode = 'fragment'
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertURLEqual(h['Location'], self.url_fragment, parse_fragment=True)
        self.assertIsNone(b)
        self.assertEqual(s, 302)

    @mock.patch('oauthlib.common.generate_token')
    def test_no_prompt_authorization(self, generate_token):
        generate_token.return_value = 'abc'
        self.request.prompt = 'none'

        bearer = BearerToken(self.mock_validator)

        self.request.response_mode = 'query'
        self.request.id_token_hint = 'me@email.com'
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertURLEqual(h['Location'], self.url_query)
        self.assertIsNone(b)
        self.assertEqual(s, 302)

        # Test alternative response modes
        self.request.response_mode = 'fragment'
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertURLEqual(h['Location'], self.url_fragment, parse_fragment=True)

        # Ensure silent authentication and authorization is done
        self.mock_validator.validate_silent_login.return_value = False
        self.mock_validator.validate_silent_authorization.return_value = True
        self.assertRaises(LoginRequired,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=login_required', h['Location'])

        self.mock_validator.validate_silent_login.return_value = True
        self.mock_validator.validate_silent_authorization.return_value = False
        self.assertRaises(ConsentRequired,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=consent_required', h['Location'])

        # ID token hint must match logged in user
        self.mock_validator.validate_silent_authorization.return_value = True
        self.mock_validator.validate_user_match.return_value = False
        self.assertRaises(LoginRequired,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=login_required', h['Location'])

    def test_none_multi_prompt(self):
        bearer = BearerToken(self.mock_validator)

        self.request.prompt = 'none login'
        self.assertRaises(InvalidRequestError,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=invalid_request', h['Location'])

        self.request.prompt = 'none consent'
        self.assertRaises(InvalidRequestError,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=invalid_request', h['Location'])

        self.request.prompt = 'none select_account'
        self.assertRaises(InvalidRequestError,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=invalid_request', h['Location'])

        self.request.prompt = 'consent none login'
        self.assertRaises(InvalidRequestError,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=invalid_request', h['Location'])

    def set_scopes(self, client_id, code, client, request):
        request.scopes = self.request.scopes
        request.user = 'bob'
        return True

    def test_create_token_response(self):
        self.request.response_type = None
        self.mock_validator.validate_code.side_effect = self.set_scopes

        bearer = BearerToken(self.mock_validator)

        h, token, s = self.auth.create_token_response(self.request, bearer)
        token = json.loads(token)
        self.assertEqual(self.mock_validator.save_token.call_count, 1)
        self.assertIn('access_token', token)
        self.assertIn('refresh_token', token)
        self.assertIn('expires_in', token)
        self.assertIn('scope', token)
        self.assertIn('id_token', token)
        self.assertIn('openid', token['scope'])

        self.mock_validator.reset_mock()

        self.request.scopes = ('hello', 'world')
        h, token, s = self.auth.create_token_response(self.request, bearer)
        token = json.loads(token)
        self.assertEqual(self.mock_validator.save_token.call_count, 1)
        self.assertIn('access_token', token)
        self.assertIn('refresh_token', token)
        self.assertIn('expires_in', token)
        self.assertIn('scope', token)
        self.assertNotIn('id_token', token)
        self.assertNotIn('openid', token['scope'])

    @mock.patch('oauthlib.common.generate_token')
    def test_optional_nonce(self, generate_token):
        generate_token.return_value = 'abc'
        self.request.nonce = 'xyz'
        scope, info = self.auth.validate_authorization_request(self.request)

        bearer = BearerToken(self.mock_validator)
        self.request.response_mode = 'query'
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertURLEqual(h['Location'], self.url_query)
        self.assertIsNone(b)
        self.assertEqual(s, 302)
