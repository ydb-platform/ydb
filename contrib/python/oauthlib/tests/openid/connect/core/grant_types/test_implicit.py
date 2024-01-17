# -*- coding: utf-8 -*-
from unittest import mock

from oauthlib.common import Request
from oauthlib.oauth2.rfc6749 import errors
from oauthlib.oauth2.rfc6749.tokens import BearerToken
from oauthlib.openid.connect.core.grant_types.implicit import ImplicitGrant

from __tests__.oauth2.rfc6749.grant_types.test_implicit import ImplicitGrantTest
from tests.unittest import TestCase

from .test_authorization_code import get_id_token_mock


class OpenIDImplicitInterferenceTest(ImplicitGrantTest):
    """Test that OpenID don't interfere with normal OAuth 2 flows."""

    def setUp(self):
        super().setUp()
        self.auth = ImplicitGrant(request_validator=self.mock_validator)


class OpenIDImplicitTest(TestCase):

    def setUp(self):
        self.request = Request('http://a.b/path')
        self.request.scopes = ('hello', 'openid')
        self.request.expires_in = 1800
        self.request.client_id = 'abcdef'
        self.request.response_type = 'id_token token'
        self.request.redirect_uri = 'https://a.b/cb'
        self.request.state = 'abc'
        self.request.nonce = 'xyz'

        self.mock_validator = mock.MagicMock()
        self.mock_validator.get_id_token.side_effect = get_id_token_mock
        self.auth = ImplicitGrant(request_validator=self.mock_validator)

        token = 'MOCKED_TOKEN'
        self.url_query = 'https://a.b/cb?state=abc&token_type=Bearer&expires_in=3600&scope=hello+openid&access_token=abc&id_token=%s' % token
        self.url_fragment = 'https://a.b/cb#state=abc&token_type=Bearer&expires_in=3600&scope=hello+openid&access_token=abc&id_token=%s' % token

    @mock.patch('oauthlib.common.generate_token')
    def test_authorization(self, generate_token):
        scope, info = self.auth.validate_authorization_request(self.request)

        generate_token.return_value = 'abc'
        bearer = BearerToken(self.mock_validator)

        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertURLEqual(h['Location'], self.url_fragment, parse_fragment=True)
        self.assertIsNone(b)
        self.assertEqual(s, 302)

        self.request.response_type = 'id_token'
        token = 'MOCKED_TOKEN'
        url = 'https://a.b/cb#state=abc&id_token=%s' % token
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertURLEqual(h['Location'], url, parse_fragment=True)
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
        self.assertRaises(errors.LoginRequired,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=login_required', h['Location'])

        self.mock_validator.validate_silent_login.return_value = True
        self.mock_validator.validate_silent_authorization.return_value = False
        self.assertRaises(errors.ConsentRequired,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=consent_required', h['Location'])

        # ID token hint must match logged in user
        self.mock_validator.validate_silent_authorization.return_value = True
        self.mock_validator.validate_user_match.return_value = False
        self.assertRaises(errors.LoginRequired,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=login_required', h['Location'])

    def test_none_multi_prompt(self):
        bearer = BearerToken(self.mock_validator)

        self.request.prompt = 'none login'
        self.assertRaises(errors.InvalidRequestError,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=invalid_request', h['Location'])

        self.request.prompt = 'none consent'
        self.assertRaises(errors.InvalidRequestError,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=invalid_request', h['Location'])

        self.request.prompt = 'none select_account'
        self.assertRaises(errors.InvalidRequestError,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=invalid_request', h['Location'])

        self.request.prompt = 'consent none login'
        self.assertRaises(errors.InvalidRequestError,
                          self.auth.validate_authorization_request,
                          self.request)
        h, b, s = self.auth.create_authorization_response(self.request, bearer)
        self.assertIn('error=invalid_request', h['Location'])

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


class OpenIDImplicitNoAccessTokenTest(OpenIDImplicitTest):
    def setUp(self):
        super().setUp()
        self.request.response_type = 'id_token'
        token = 'MOCKED_TOKEN'
        self.url_query = 'https://a.b/cb?state=abc&id_token=%s' % token
        self.url_fragment = 'https://a.b/cb#state=abc&id_token=%s' % token

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
