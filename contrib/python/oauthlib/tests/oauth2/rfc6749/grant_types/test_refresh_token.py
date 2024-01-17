# -*- coding: utf-8 -*-
import json
from unittest import mock

from oauthlib.common import Request
from oauthlib.oauth2.rfc6749 import errors
from oauthlib.oauth2.rfc6749.grant_types import RefreshTokenGrant
from oauthlib.oauth2.rfc6749.tokens import BearerToken

from tests.unittest import TestCase


class RefreshTokenGrantTest(TestCase):

    def setUp(self):
        mock_client = mock.MagicMock()
        mock_client.user.return_value = 'mocked user'
        self.request = Request('http://a.b/path')
        self.request.grant_type = 'refresh_token'
        self.request.refresh_token = 'lsdkfhj230'
        self.request.client_id = 'abcdef'
        self.request.client = mock_client
        self.request.scope = 'foo'
        self.mock_validator = mock.MagicMock()
        self.auth = RefreshTokenGrant(
                request_validator=self.mock_validator)

    def test_create_token_response(self):
        self.mock_validator.get_original_scopes.return_value = ['foo', 'bar']
        bearer = BearerToken(self.mock_validator)
        headers, body, status_code = self.auth.create_token_response(
                self.request, bearer)
        token = json.loads(body)
        self.assertEqual(self.mock_validator.save_token.call_count, 1)
        self.assertIn('access_token', token)
        self.assertIn('token_type', token)
        self.assertIn('expires_in', token)
        self.assertEqual(token['scope'], 'foo')

    def test_custom_auth_validators_unsupported(self):
        authval1, authval2 = mock.Mock(), mock.Mock()
        expected = ('RefreshTokenGrant does not support authorization '
                    'validators. Use token validators instead.')
        with self.assertRaises(ValueError) as caught:
            RefreshTokenGrant(self.mock_validator, pre_auth=[authval1])
        self.assertEqual(caught.exception.args[0], expected)
        with self.assertRaises(ValueError) as caught:
            RefreshTokenGrant(self.mock_validator, post_auth=[authval2])
        self.assertEqual(caught.exception.args[0], expected)
        with self.assertRaises(AttributeError):
            self.auth.custom_validators.pre_auth.append(authval1)
        with self.assertRaises(AttributeError):
            self.auth.custom_validators.pre_auth.append(authval2)

    def test_custom_token_validators(self):
        tknval1, tknval2 = mock.Mock(), mock.Mock()
        self.auth.custom_validators.pre_token.append(tknval1)
        self.auth.custom_validators.post_token.append(tknval2)

        bearer = BearerToken(self.mock_validator)
        self.auth.create_token_response(self.request, bearer)
        self.assertTrue(tknval1.called)
        self.assertTrue(tknval2.called)

    def test_create_token_inherit_scope(self):
        self.request.scope = None
        self.mock_validator.get_original_scopes.return_value = ['foo', 'bar']
        bearer = BearerToken(self.mock_validator)
        headers, body, status_code = self.auth.create_token_response(
                self.request, bearer)
        token = json.loads(body)
        self.assertEqual(self.mock_validator.save_token.call_count, 1)
        self.assertIn('access_token', token)
        self.assertIn('token_type', token)
        self.assertIn('expires_in', token)
        self.assertEqual(token['scope'], 'foo bar')

    def test_create_token_within_original_scope(self):
        self.mock_validator.get_original_scopes.return_value = ['baz']
        self.mock_validator.is_within_original_scope.return_value = True
        bearer = BearerToken(self.mock_validator)
        headers, body, status_code = self.auth.create_token_response(
                self.request, bearer)
        token = json.loads(body)
        self.assertEqual(self.mock_validator.save_token.call_count, 1)
        self.assertIn('access_token', token)
        self.assertIn('token_type', token)
        self.assertIn('expires_in', token)
        self.assertEqual(token['scope'], 'foo')

    def test_invalid_scope(self):
        self.mock_validator.get_original_scopes.return_value = ['baz']
        self.mock_validator.is_within_original_scope.return_value = False
        bearer = BearerToken(self.mock_validator)
        headers, body, status_code = self.auth.create_token_response(
                self.request, bearer)
        token = json.loads(body)
        self.assertEqual(self.mock_validator.save_token.call_count, 0)
        self.assertEqual(token['error'], 'invalid_scope')
        self.assertEqual(status_code, 400)

    def test_invalid_token(self):
        self.mock_validator.validate_refresh_token.return_value = False
        bearer = BearerToken(self.mock_validator)
        headers, body, status_code = self.auth.create_token_response(
                self.request, bearer)
        token = json.loads(body)
        self.assertEqual(self.mock_validator.save_token.call_count, 0)
        self.assertEqual(token['error'], 'invalid_grant')
        self.assertEqual(status_code, 400)

    def test_invalid_client(self):
        self.mock_validator.authenticate_client.return_value = False
        bearer = BearerToken(self.mock_validator)
        headers, body, status_code = self.auth.create_token_response(
                self.request, bearer)
        token = json.loads(body)
        self.assertEqual(self.mock_validator.save_token.call_count, 0)
        self.assertEqual(token['error'], 'invalid_client')
        self.assertEqual(status_code, 401)

    def test_authentication_required(self):
        """
        ensure client_authentication_required() is properly called
        """
        self.mock_validator.authenticate_client.return_value = False
        self.mock_validator.authenticate_client_id.return_value = False
        self.request.code = 'waffles'
        self.assertRaises(errors.InvalidClientError, self.auth.validate_token_request,
                          self.request)
        self.mock_validator.client_authentication_required.assert_called_once_with(self.request)

    def test_invalid_grant_type(self):
        self.request.grant_type = 'wrong_type'
        self.assertRaises(errors.UnsupportedGrantTypeError,
                          self.auth.validate_token_request, self.request)

    def test_authenticate_client_id(self):
        self.mock_validator.client_authentication_required.return_value = False
        self.request.refresh_token = mock.MagicMock()
        self.mock_validator.authenticate_client_id.return_value = False
        self.assertRaises(errors.InvalidClientError,
                          self.auth.validate_token_request, self.request)

    def test_invalid_refresh_token(self):
        # invalid refresh token
        self.mock_validator.authenticate_client_id.return_value = True
        self.mock_validator.validate_refresh_token.return_value = False
        self.assertRaises(errors.InvalidGrantError,
                          self.auth.validate_token_request, self.request)
        # no token provided
        del self.request.refresh_token
        self.assertRaises(errors.InvalidRequestError,
                          self.auth.validate_token_request, self.request)

    def test_invalid_scope_original_scopes_empty(self):
        self.mock_validator.validate_refresh_token.return_value = True
        self.mock_validator.is_within_original_scope.return_value = False
        self.assertRaises(errors.InvalidScopeError,
                          self.auth.validate_token_request, self.request)

    def test_valid_token_request(self):
        self.request.scope = 'foo bar'
        self.mock_validator.get_original_scopes = mock.Mock()
        self.mock_validator.get_original_scopes.return_value = 'foo bar baz'
        self.auth.validate_token_request(self.request)
        self.assertEqual(self.request.scopes, self.request.scope.split())
        # all ok but without request.scope
        del self.request.scope
        self.auth.validate_token_request(self.request)
        self.assertEqual(self.request.scopes, 'foo bar baz'.split())

    # CORS

    def test_create_cors_headers(self):
        bearer = BearerToken(self.mock_validator)
        self.request.headers['origin'] = 'https://foo.bar'
        self.mock_validator.is_origin_allowed.return_value = True

        headers = self.auth.create_token_response(self.request, bearer)[0]
        self.assertEqual(
            headers['Access-Control-Allow-Origin'], 'https://foo.bar'
        )
        self.mock_validator.is_origin_allowed.assert_called_once_with(
            'abcdef', 'https://foo.bar', self.request
        )

    def test_create_cors_headers_no_origin(self):
        bearer = BearerToken(self.mock_validator)
        headers = self.auth.create_token_response(self.request, bearer)[0]
        self.assertNotIn('Access-Control-Allow-Origin', headers)
        self.mock_validator.is_origin_allowed.assert_not_called()

    def test_create_cors_headers_insecure_origin(self):
        bearer = BearerToken(self.mock_validator)
        self.request.headers['origin'] = 'http://foo.bar'

        headers = self.auth.create_token_response(self.request, bearer)[0]
        self.assertNotIn('Access-Control-Allow-Origin', headers)
        self.mock_validator.is_origin_allowed.assert_not_called()

    def test_create_cors_headers_invalid_origin(self):
        bearer = BearerToken(self.mock_validator)
        self.request.headers['origin'] = 'https://foo.bar'
        self.mock_validator.is_origin_allowed.return_value = False

        headers = self.auth.create_token_response(self.request, bearer)[0]
        self.assertNotIn('Access-Control-Allow-Origin', headers)
        self.mock_validator.is_origin_allowed.assert_called_once_with(
            'abcdef', 'https://foo.bar', self.request
        )
