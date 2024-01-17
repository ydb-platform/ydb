# -*- coding: utf-8 -*-
import json
from unittest import mock

from oauthlib.oauth2.rfc6749 import errors
from oauthlib.oauth2.rfc6749.endpoints.authorization import (
    AuthorizationEndpoint,
)
from oauthlib.oauth2.rfc6749.endpoints.token import TokenEndpoint
from oauthlib.oauth2.rfc6749.tokens import BearerToken
from oauthlib.openid.connect.core.grant_types.authorization_code import (
    AuthorizationCodeGrant,
)
from oauthlib.openid.connect.core.grant_types.hybrid import HybridGrant
from oauthlib.openid.connect.core.grant_types.implicit import ImplicitGrant

from tests.unittest import TestCase


class AuthorizationEndpointTest(TestCase):

    def setUp(self):
        self.mock_validator = mock.MagicMock()
        self.mock_validator.get_code_challenge.return_value = None
        self.addCleanup(setattr, self, 'mock_validator', mock.MagicMock())
        auth_code = AuthorizationCodeGrant(request_validator=self.mock_validator)
        auth_code.save_authorization_code = mock.MagicMock()
        implicit = ImplicitGrant(
            request_validator=self.mock_validator)
        implicit.save_token = mock.MagicMock()
        hybrid = HybridGrant(self.mock_validator)

        response_types = {
            'code': auth_code,
            'token': implicit,
            'id_token': implicit,
            'id_token token': implicit,
            'code token': hybrid,
            'code id_token': hybrid,
            'code token id_token': hybrid,
            'none': auth_code
        }
        self.expires_in = 1800
        token = BearerToken(
            self.mock_validator,
            expires_in=self.expires_in
        )
        self.endpoint = AuthorizationEndpoint(
            default_response_type='code',
            default_token_type=token,
            response_types=response_types
        )

    # TODO: Add hybrid grant test

    @mock.patch('oauthlib.common.generate_token', new=lambda: 'abc')
    def test_authorization_grant(self):
        uri = 'http://i.b/l?response_type=code&client_id=me&scope=all+of+them&state=xyz'
        uri += '&redirect_uri=http%3A%2F%2Fback.to%2Fme'
        headers, body, status_code = self.endpoint.create_authorization_response(
            uri, scopes=['all', 'of', 'them'])
        self.assertIn('Location', headers)
        self.assertURLEqual(headers['Location'], 'http://back.to/me?code=abc&state=xyz')

    @mock.patch('oauthlib.common.generate_token', new=lambda: 'abc')
    def test_implicit_grant(self):
        uri = 'http://i.b/l?response_type=token&client_id=me&scope=all+of+them&state=xyz'
        uri += '&redirect_uri=http%3A%2F%2Fback.to%2Fme'
        headers, body, status_code = self.endpoint.create_authorization_response(
            uri, scopes=['all', 'of', 'them'])
        self.assertIn('Location', headers)
        self.assertURLEqual(headers['Location'], 'http://back.to/me#access_token=abc&expires_in=' + str(self.expires_in) + '&token_type=Bearer&state=xyz&scope=all+of+them', parse_fragment=True)

    def test_none_grant(self):
        uri = 'http://i.b/l?response_type=none&client_id=me&scope=all+of+them&state=xyz'
        uri += '&redirect_uri=http%3A%2F%2Fback.to%2Fme'
        headers, body, status_code = self.endpoint.create_authorization_response(
            uri, scopes=['all', 'of', 'them'])
        self.assertIn('Location', headers)
        self.assertURLEqual(headers['Location'], 'http://back.to/me?state=xyz', parse_fragment=True)
        self.assertIsNone(body)
        self.assertEqual(status_code, 302)

        # and without the state parameter
        uri = 'http://i.b/l?response_type=none&client_id=me&scope=all+of+them'
        uri += '&redirect_uri=http%3A%2F%2Fback.to%2Fme'
        headers, body, status_code = self.endpoint.create_authorization_response(
            uri, scopes=['all', 'of', 'them'])
        self.assertIn('Location', headers)
        self.assertURLEqual(headers['Location'], 'http://back.to/me', parse_fragment=True)
        self.assertIsNone(body)
        self.assertEqual(status_code, 302)

    def test_missing_type(self):
        uri = 'http://i.b/l?client_id=me&scope=all+of+them'
        uri += '&redirect_uri=http%3A%2F%2Fback.to%2Fme'
        self.mock_validator.validate_request = mock.MagicMock(
            side_effect=errors.InvalidRequestError())
        headers, body, status_code = self.endpoint.create_authorization_response(
            uri, scopes=['all', 'of', 'them'])
        self.assertIn('Location', headers)
        self.assertURLEqual(headers['Location'], 'http://back.to/me?error=invalid_request&error_description=Missing+response_type+parameter.')

    def test_invalid_type(self):
        uri = 'http://i.b/l?response_type=invalid&client_id=me&scope=all+of+them'
        uri += '&redirect_uri=http%3A%2F%2Fback.to%2Fme'
        self.mock_validator.validate_request = mock.MagicMock(
            side_effect=errors.UnsupportedResponseTypeError())
        headers, body, status_code = self.endpoint.create_authorization_response(
            uri, scopes=['all', 'of', 'them'])
        self.assertIn('Location', headers)
        self.assertURLEqual(headers['Location'], 'http://back.to/me?error=unsupported_response_type')


class TokenEndpointTest(TestCase):

    def setUp(self):
        def set_user(request):
            request.user = mock.MagicMock()
            request.client = mock.MagicMock()
            request.client.client_id = 'mocked_client_id'
            return True

        self.mock_validator = mock.MagicMock()
        self.mock_validator.authenticate_client.side_effect = set_user
        self.mock_validator.get_code_challenge.return_value = None
        self.addCleanup(setattr, self, 'mock_validator', mock.MagicMock())
        auth_code = AuthorizationCodeGrant(
            request_validator=self.mock_validator)
        supported_types = {
            'authorization_code': auth_code,
        }
        self.expires_in = 1800
        token = BearerToken(
            self.mock_validator,
            expires_in=self.expires_in
        )
        self.endpoint = TokenEndpoint(
            'authorization_code',
            default_token_type=token,
            grant_types=supported_types
        )

    @mock.patch('oauthlib.common.generate_token', new=lambda: 'abc')
    def test_authorization_grant(self):
        body = 'grant_type=authorization_code&code=abc&scope=all+of+them'
        headers, body, status_code = self.endpoint.create_token_response(
            '', body=body)
        token = {
            'token_type': 'Bearer',
            'expires_in': self.expires_in,
            'access_token': 'abc',
            'refresh_token': 'abc',
            'scope': 'all of them'
        }
        self.assertEqual(json.loads(body), token)

        body = 'grant_type=authorization_code&code=abc'
        headers, body, status_code = self.endpoint.create_token_response(
            '', body=body)
        token = {
            'token_type': 'Bearer',
            'expires_in': self.expires_in,
            'access_token': 'abc',
            'refresh_token': 'abc'
        }
        self.assertEqual(json.loads(body), token)

        # ignore useless fields
        body = 'grant_type=authorization_code&code=abc&state=foobar'
        headers, body, status_code = self.endpoint.create_token_response(
            '', body=body)
        self.assertEqual(json.loads(body), token)

    def test_missing_type(self):
        _, body, _ = self.endpoint.create_token_response('', body='')
        token = {'error': 'unsupported_grant_type'}
        self.assertEqual(json.loads(body), token)

    def test_invalid_type(self):
        body = 'grant_type=invalid'
        _, body, _ = self.endpoint.create_token_response('', body=body)
        token = {'error': 'unsupported_grant_type'}
        self.assertEqual(json.loads(body), token)
