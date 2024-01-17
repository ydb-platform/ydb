# -*- coding: utf-8 -*-
import json
from unittest import mock

from oauthlib.common import Request
from oauthlib.oauth2.rfc6749.grant_types import ClientCredentialsGrant
from oauthlib.oauth2.rfc6749.tokens import BearerToken

from tests.unittest import TestCase


class ClientCredentialsGrantTest(TestCase):

    def setUp(self):
        mock_client = mock.MagicMock()
        mock_client.user.return_value = 'mocked user'
        self.request = Request('http://a.b/path')
        self.request.grant_type = 'client_credentials'
        self.request.client = mock_client
        self.request.scopes = ('mocked', 'scopes')
        self.mock_validator = mock.MagicMock()
        self.auth = ClientCredentialsGrant(
                request_validator=self.mock_validator)

    def test_custom_auth_validators_unsupported(self):
        authval1, authval2 = mock.Mock(), mock.Mock()
        expected = ('ClientCredentialsGrant does not support authorization '
                    'validators. Use token validators instead.')
        with self.assertRaises(ValueError) as caught:
            ClientCredentialsGrant(self.mock_validator, pre_auth=[authval1])
        self.assertEqual(caught.exception.args[0], expected)
        with self.assertRaises(ValueError) as caught:
            ClientCredentialsGrant(self.mock_validator, post_auth=[authval2])
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

    def test_create_token_response(self):
        bearer = BearerToken(self.mock_validator)
        headers, body, status_code = self.auth.create_token_response(
                self.request, bearer)
        token = json.loads(body)
        self.assertEqual(self.mock_validator.save_token.call_count, 1)
        self.assertIn('access_token', token)
        self.assertIn('token_type', token)
        self.assertIn('expires_in', token)
        self.assertIn('Content-Type', headers)
        self.assertEqual(headers['Content-Type'], 'application/json')

    def test_error_response(self):
        bearer = BearerToken(self.mock_validator)
        self.mock_validator.authenticate_client.return_value = False
        headers, body, status_code = self.auth.create_token_response(
            self.request, bearer)
        self.assertEqual(self.mock_validator.save_token.call_count, 0)
        error_msg = json.loads(body)
        self.assertIn('error', error_msg)
        self.assertEqual(error_msg['error'], 'invalid_client')
        self.assertIn('Content-Type', headers)
        self.assertEqual(headers['Content-Type'], 'application/json')

    def test_validate_token_response(self):
        # wrong grant type, scope
        pass
