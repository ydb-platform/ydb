"""Ensure OpenID Connect Authorization Request 'claims' are preserved across authorization.

The claims parameter is an optional query param for the Authorization Request endpoint
 but if it is provided and is valid it needs to be deserialized (from urlencoded JSON)
 and persisted with the authorization code itself, then in the subsequent Access Token
 request the claims should be transferred (via the oauthlib request) to be persisted
 with the Access Token when it is created.
"""
from unittest import mock

from oauthlib.openid import RequestValidator
from oauthlib.openid.connect.core.endpoints.pre_configured import Server

from __tests__.oauth2.rfc6749.endpoints.test_utils import get_query_credentials
from tests.unittest import TestCase


class TestClaimsHandling(TestCase):

    DEFAULT_REDIRECT_URI = 'http://i.b./path'

    def set_scopes(self, scopes):
        def set_request_scopes(client_id, code, client, request):
            request.scopes = scopes
            return True
        return set_request_scopes

    def set_user(self, request):
        request.user = 'foo'
        request.client_id = 'bar'
        request.client = mock.MagicMock()
        request.client.client_id = 'mocked'
        return True

    def set_client(self, request):
        request.client = mock.MagicMock()
        request.client.client_id = 'mocked'
        return True

    def save_claims_with_code(self, client_id, code, request, *args, **kwargs):
        # a real validator would save the claims with the code during save_authorization_code()
        self.claims_from_auth_code_request = request.claims
        self.scopes = request.scopes.split()

    def retrieve_claims_saved_with_code(self, client_id, code, client, request, *args, **kwargs):
        request.claims = self.claims_from_auth_code_request
        request.scopes = self.scopes

        return True

    def save_claims_with_bearer_token(self, token, request, *args, **kwargs):
        # a real validator would save the claims with the access token during save_bearer_token()
        self.claims_saved_with_bearer_token = request.claims

    def setUp(self):
        self.validator = mock.MagicMock(spec=RequestValidator)
        self.validator.get_code_challenge.return_value = None
        self.validator.get_default_redirect_uri.return_value = TestClaimsHandling.DEFAULT_REDIRECT_URI
        self.validator.authenticate_client.side_effect = self.set_client

        self.validator.save_authorization_code.side_effect = self.save_claims_with_code
        self.validator.validate_code.side_effect = self.retrieve_claims_saved_with_code
        self.validator.save_token.side_effect = self.save_claims_with_bearer_token

        self.server = Server(self.validator)

    def test_claims_stored_on_code_creation(self):

        claims = {
            "id_token": {
                "claim_1": None,
                "claim_2": {
                    "essential": True
                }
            },
            "userinfo": {
                "claim_3": {
                    "essential": True
                },
                "claim_4": None
            }
        }

        claims_urlquoted = '%7B%22id_token%22%3A%20%7B%22claim_2%22%3A%20%7B%22essential%22%3A%20true%7D%2C%20%22claim_1%22%3A%20null%7D%2C%20%22userinfo%22%3A%20%7B%22claim_4%22%3A%20null%2C%20%22claim_3%22%3A%20%7B%22essential%22%3A%20true%7D%7D%7D'
        uri = 'http://example.com/path?client_id=abc&scope=openid+test_scope&response_type=code&claims=%s'

        h, b, s = self.server.create_authorization_response(uri % claims_urlquoted, scopes='openid test_scope')

        self.assertDictEqual(self.claims_from_auth_code_request, claims)

        code = get_query_credentials(h['Location'])['code'][0]
        token_uri = 'http://example.com/path'
        _, body, _ = self.server.create_token_response(
            token_uri,
            body='client_id=me&redirect_uri=http://back.to/me&grant_type=authorization_code&code=%s' % code
        )

        self.assertDictEqual(self.claims_saved_with_bearer_token, claims)

    def test_invalid_claims(self):
        uri = 'http://example.com/path?client_id=abc&scope=openid+test_scope&response_type=code&claims=this-is-not-json'

        h, b, s = self.server.create_authorization_response(uri, scopes='openid test_scope')
        error = get_query_credentials(h['Location'])['error'][0]
        error_desc = get_query_credentials(h['Location'])['error_description'][0]
        self.assertEqual(error, 'invalid_request')
        self.assertEqual(error_desc, "Malformed claims parameter")
