"""Client authentication tests across all endpoints.

Client authentication in OAuth2 serve two purposes, to authenticate
confidential clients and to ensure public clients are in fact public. The
latter is achieved with authenticate_client_id and the former with
authenticate_client.

We make sure authentication is done by requiring a client object to be set
on the request object with a client_id parameter. The client_id attribute
prevents this check from being circumvented with a client form parameter.
"""
import json
from unittest import mock

from oauthlib.oauth2 import (
    BackendApplicationServer, LegacyApplicationServer, MobileApplicationServer,
    RequestValidator, WebApplicationServer,
)

from tests.unittest import TestCase

from .test_utils import get_fragment_credentials


class ClientAuthenticationTest(TestCase):

    def inspect_client(self, request, refresh_token=False):
        if not request.client or not request.client.client_id:
            raise ValueError()
        return 'abc'

    def setUp(self):
        self.validator = mock.MagicMock(spec=RequestValidator)
        self.validator.is_pkce_required.return_value = False
        self.validator.get_code_challenge.return_value = None
        self.validator.get_default_redirect_uri.return_value = 'http://i.b./path'
        self.web = WebApplicationServer(self.validator,
                token_generator=self.inspect_client)
        self.mobile = MobileApplicationServer(self.validator,
                token_generator=self.inspect_client)
        self.legacy = LegacyApplicationServer(self.validator,
                token_generator=self.inspect_client)
        self.backend = BackendApplicationServer(self.validator,
                token_generator=self.inspect_client)
        self.token_uri = 'http://example.com/path'
        self.auth_uri = 'http://example.com/path?client_id=abc&response_type=token'
        # should be base64 but no added value in this unittest
        self.basicauth_client_creds = {"Authorization": "john:doe"}
        self.basicauth_client_id = {"Authorization": "john:"}

    def set_client(self, request):
        request.client = mock.MagicMock()
        request.client.client_id = 'mocked'
        return True

    def set_client_id(self, client_id, request):
        request.client = mock.MagicMock()
        request.client.client_id = 'mocked'
        return True

    def basicauth_authenticate_client(self, request):
        assert "Authorization" in request.headers
        assert "john:doe" in request.headers["Authorization"]
        request.client = mock.MagicMock()
        request.client.client_id = 'mocked'
        return True

    def test_client_id_authentication(self):
        token_uri = 'http://example.com/path'

        # authorization code grant
        self.validator.authenticate_client.return_value = False
        self.validator.authenticate_client_id.return_value = False
        _, body, _ = self.web.create_token_response(token_uri,
                body='grant_type=authorization_code&code=mock')
        self.assertEqual(json.loads(body)['error'], 'invalid_client')

        self.validator.authenticate_client_id.return_value = True
        self.validator.authenticate_client.side_effect = self.set_client
        _, body, _ = self.web.create_token_response(token_uri,
                body='grant_type=authorization_code&code=mock')
        self.assertIn('access_token', json.loads(body))

        # implicit grant
        auth_uri = 'http://example.com/path?client_id=abc&response_type=token'
        self.assertRaises(ValueError, self.mobile.create_authorization_response,
                auth_uri, scopes=['random'])

        self.validator.validate_client_id.side_effect = self.set_client_id
        h, _, s = self.mobile.create_authorization_response(auth_uri, scopes=['random'])
        self.assertEqual(302, s)
        self.assertIn('Location', h)
        self.assertIn('access_token', get_fragment_credentials(h['Location']))

    def test_basicauth_web(self):
        self.validator.authenticate_client.side_effect = self.basicauth_authenticate_client
        _, body, _ = self.web.create_token_response(
            self.token_uri,
            body='grant_type=authorization_code&code=mock',
            headers=self.basicauth_client_creds
        )
        self.assertIn('access_token', json.loads(body))

    def test_basicauth_legacy(self):
        self.validator.authenticate_client.side_effect = self.basicauth_authenticate_client
        _, body, _ = self.legacy.create_token_response(
            self.token_uri,
            body='grant_type=password&username=abc&password=secret',
            headers=self.basicauth_client_creds
        )
        self.assertIn('access_token', json.loads(body))

    def test_basicauth_backend(self):
        self.validator.authenticate_client.side_effect = self.basicauth_authenticate_client
        _, body, _ = self.backend.create_token_response(
            self.token_uri,
            body='grant_type=client_credentials',
            headers=self.basicauth_client_creds
        )
        self.assertIn('access_token', json.loads(body))

    def test_basicauth_revoke(self):
        self.validator.authenticate_client.side_effect = self.basicauth_authenticate_client

        # legacy or any other uses the same RevocationEndpoint
        _, body, status = self.legacy.create_revocation_response(
            self.token_uri,
            body='token=foobar',
            headers=self.basicauth_client_creds
        )
        self.assertEqual(status, 200, body)

    def test_basicauth_introspect(self):
        self.validator.authenticate_client.side_effect = self.basicauth_authenticate_client

        # legacy or any other uses the same IntrospectEndpoint
        _, body, status = self.legacy.create_introspect_response(
            self.token_uri,
            body='token=foobar',
            headers=self.basicauth_client_creds
        )
        self.assertEqual(status, 200, body)

    def test_custom_authentication(self):
        token_uri = 'http://example.com/path'

        # authorization code grant
        self.assertRaises(NotImplementedError,
                self.web.create_token_response, token_uri,
                body='grant_type=authorization_code&code=mock')

        # password grant
        self.validator.authenticate_client.return_value = True
        self.assertRaises(NotImplementedError,
                self.legacy.create_token_response, token_uri,
                body='grant_type=password&username=abc&password=secret')

        # client credentials grant
        self.validator.authenticate_client.return_value = True
        self.assertRaises(NotImplementedError,
                self.backend.create_token_response, token_uri,
                body='grant_type=client_credentials')
