import json
from unittest import mock

from oauthlib.common import Request
from oauthlib.oauth2.rfc6749.tokens import BearerToken
from oauthlib.openid.connect.core.grant_types import RefreshTokenGrant

from __tests__.oauth2.rfc6749.grant_types.test_refresh_token import (
    RefreshTokenGrantTest,
)
from tests.unittest import TestCase


def get_id_token_mock(token, token_handler, request):
    return "MOCKED_TOKEN"


class OpenIDRefreshTokenInterferenceTest(RefreshTokenGrantTest):
    """Test that OpenID don't interfere with normal OAuth 2 flows."""

    def setUp(self):
        super().setUp()
        self.auth = RefreshTokenGrant(request_validator=self.mock_validator)


class OpenIDRefreshTokenTest(TestCase):

    def setUp(self):
        self.request = Request('http://a.b/path')
        self.request.grant_type = 'refresh_token'
        self.request.refresh_token = 'lsdkfhj230'
        self.request.scope = ('hello', 'openid')
        self.mock_validator = mock.MagicMock()

        self.mock_validator = mock.MagicMock()
        self.mock_validator.authenticate_client.side_effect = self.set_client
        self.mock_validator.get_id_token.side_effect = get_id_token_mock
        self.auth = RefreshTokenGrant(request_validator=self.mock_validator)

    def set_client(self, request):
        request.client = mock.MagicMock()
        request.client.client_id = 'mocked'
        return True

    def test_refresh_id_token(self):
        self.mock_validator.get_original_scopes.return_value = [
            'hello', 'openid'
        ]
        bearer = BearerToken(self.mock_validator)

        headers, body, status_code = self.auth.create_token_response(
            self.request, bearer
        )

        token = json.loads(body)
        self.assertEqual(self.mock_validator.save_token.call_count, 1)
        self.assertIn('access_token', token)
        self.assertIn('refresh_token', token)
        self.assertIn('id_token', token)
        self.assertIn('token_type', token)
        self.assertIn('expires_in', token)
        self.assertEqual(token['scope'], 'hello openid')
        self.mock_validator.refresh_id_token.assert_called_once_with(
            self.request
        )

    def test_refresh_id_token_false(self):
        self.mock_validator.refresh_id_token.return_value = False
        self.mock_validator.get_original_scopes.return_value = [
            'hello', 'openid'
        ]
        bearer = BearerToken(self.mock_validator)

        headers, body, status_code = self.auth.create_token_response(
            self.request, bearer
        )

        token = json.loads(body)
        self.assertEqual(self.mock_validator.save_token.call_count, 1)
        self.assertIn('access_token', token)
        self.assertIn('refresh_token', token)
        self.assertIn('token_type', token)
        self.assertIn('expires_in', token)
        self.assertEqual(token['scope'], 'hello openid')
        self.assertNotIn('id_token', token)
        self.mock_validator.refresh_id_token.assert_called_once_with(
            self.request
        )

    def test_refresh_token_without_openid_scope(self):
        self.request.scope = "hello"
        bearer = BearerToken(self.mock_validator)

        headers, body, status_code = self.auth.create_token_response(
            self.request, bearer
        )

        token = json.loads(body)
        self.assertEqual(self.mock_validator.save_token.call_count, 1)
        self.assertIn('access_token', token)
        self.assertIn('refresh_token', token)
        self.assertIn('token_type', token)
        self.assertIn('expires_in', token)
        self.assertNotIn('id_token', token)
        self.assertEqual(token['scope'], 'hello')
