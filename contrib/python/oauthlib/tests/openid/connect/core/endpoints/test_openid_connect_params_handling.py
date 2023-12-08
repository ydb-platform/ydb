from unittest import mock
from urllib.parse import urlencode

from oauthlib.oauth2 import InvalidRequestError
from oauthlib.oauth2.rfc6749.endpoints.authorization import (
    AuthorizationEndpoint,
)
from oauthlib.oauth2.rfc6749.tokens import BearerToken
from oauthlib.openid.connect.core.grant_types import AuthorizationCodeGrant

from tests.unittest import TestCase


class OpenIDConnectEndpointTest(TestCase):

    def setUp(self):
        self.mock_validator = mock.MagicMock()
        self.mock_validator.authenticate_client.side_effect = self.set_client
        grant = AuthorizationCodeGrant(request_validator=self.mock_validator)
        bearer = BearerToken(self.mock_validator)
        self.endpoint = AuthorizationEndpoint(grant, bearer,
                                              response_types={'code': grant})
        params = {
            'prompt': 'consent',
            'display': 'touch',
            'nonce': 'abcd',
            'state': 'abc',
            'redirect_uri': 'https://a.b/cb',
            'response_type': 'code',
            'client_id': 'abcdef',
            'scope': 'hello openid'
        }
        self.url = 'http://a.b/path?' + urlencode(params)

    def set_client(self, request):
        request.client = mock.MagicMock()
        request.client.client_id = 'mocked'
        return True

    @mock.patch('oauthlib.common.generate_token')
    def test_authorization_endpoint_handles_prompt(self, generate_token):
        generate_token.return_value = "MOCK_CODE"
        # In the GET view:
        scopes, creds = self.endpoint.validate_authorization_request(self.url)
        # In the POST view:
        creds['scopes'] = scopes
        h, b, s = self.endpoint.create_authorization_response(self.url,
                                                        credentials=creds)
        expected = 'https://a.b/cb?state=abc&code=MOCK_CODE'
        self.assertURLEqual(h['Location'], expected)
        self.assertIsNone(b)
        self.assertEqual(s, 302)

    def test_prompt_none_exclusiveness(self):
        """
        Test that prompt=none can't be used with another prompt value.
        """
        params = {
            'prompt': 'none consent',
            'state': 'abc',
            'redirect_uri': 'https://a.b/cb',
            'response_type': 'code',
            'client_id': 'abcdef',
            'scope': 'hello openid'
        }
        url = 'http://a.b/path?' + urlencode(params)
        with self.assertRaises(InvalidRequestError):
            self.endpoint.validate_authorization_request(url)

    def test_oidc_params_preservation(self):
        """
        Test that the nonce parameter is passed through.
        """
        scopes, creds = self.endpoint.validate_authorization_request(self.url)

        self.assertEqual(creds['prompt'], {'consent'})
        self.assertEqual(creds['nonce'], 'abcd')
        self.assertEqual(creds['display'], 'touch')
