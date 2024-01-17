from unittest.mock import ANY, MagicMock

from oauthlib.oauth1 import RequestValidator
from oauthlib.oauth1.rfc5849 import Client
from oauthlib.oauth1.rfc5849.endpoints import SignatureOnlyEndpoint

from tests.unittest import TestCase


class SignatureOnlyEndpointTest(TestCase):

    def setUp(self):
        self.validator = MagicMock(wraps=RequestValidator())
        self.validator.check_client_key.return_value = True
        self.validator.allowed_signature_methods = ['HMAC-SHA1']
        self.validator.get_client_secret.return_value = 'bar'
        self.validator.timestamp_lifetime = 600
        self.validator.validate_client_key.return_value = True
        self.validator.validate_timestamp_and_nonce.return_value = True
        self.validator.dummy_client = 'dummy'
        self.validator.dummy_secret = 'dummy'
        self.endpoint = SignatureOnlyEndpoint(self.validator)
        self.client = Client('foo', client_secret='bar')
        self.uri, self.headers, self.body = self.client.sign(
                'https://i.b/protected_resource')

    def test_missing_parameters(self):
        v, r = self.endpoint.validate_request(
                self.uri)
        self.assertFalse(v)

    def test_validate_client_key(self):
        self.validator.validate_client_key.return_value = False
        v, r = self.endpoint.validate_request(
                self.uri, headers=self.headers)
        self.assertFalse(v)

    def test_validate_signature(self):
        client = Client('foo')
        _, headers, _ = client.sign(self.uri + '/extra')
        v, r = self.endpoint.validate_request(
                self.uri, headers=headers)
        self.assertFalse(v)

    def test_valid_request(self):
        v, r = self.endpoint.validate_request(
                self.uri, headers=self.headers)
        self.assertTrue(v)
        self.validator.validate_timestamp_and_nonce.assert_called_once_with(
             self.client.client_key, ANY, ANY, ANY)
