import json
from unittest import TestCase, mock

from oauthlib.common import Request, urlencode
from oauthlib.oauth2.rfc6749 import errors
from oauthlib.oauth2.rfc8628.endpoints.pre_configured import DeviceApplicationServer
from oauthlib.oauth2.rfc8628.request_validator import RequestValidator


class ErrorResponseTest(TestCase):
    def set_client(self, request):
        request.client = mock.MagicMock()
        request.client.client_id = "mocked"
        return True

    def build_request(self, uri="https://example.com/device_authorize", client_id="foo"):
        body = ""
        if client_id:
            body = f"client_id={client_id}"
        return Request(
            uri,
            http_method="POST",
            body=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    def assert_request_raises(self, error, request):
        """Test that the request fails similarly on the validation and response endpoint."""
        self.assertRaises(
            error,
            self.device.validate_device_authorization_request,
            request,
        )
        self.assertRaises(
            error,
            self.device.create_device_authorization_response,
            uri=request.uri,
            http_method=request.http_method,
            body=request.body,
            headers=request.headers,
        )

    def setUp(self):
        self.validator = mock.MagicMock(spec=RequestValidator)
        self.validator.get_default_redirect_uri.return_value = None
        self.validator.get_code_challenge.return_value = None
        self.device = DeviceApplicationServer(self.validator, "https://example.com/verify")

    def test_missing_client_id(self):
        # Device code grant
        request = self.build_request(client_id=None)
        self.assert_request_raises(errors.MissingClientIdError, request)

    def test_empty_client_id(self):
        # Device code grant
        self.assertRaises(
            errors.MissingClientIdError,
            self.device.create_device_authorization_response,
            "https://i.l/",
            "POST",
            "client_id=",
            {"Content-Type": "application/x-www-form-urlencoded"},
        )

    def test_invalid_client_id(self):
        request = self.build_request(client_id="foo")
        # Device code grant
        self.validator.validate_client_id.return_value = False
        self.assert_request_raises(errors.InvalidClientIdError, request)

    def test_duplicate_client_id(self):
        request = self.build_request()
        request.body = "client_id=foo&client_id=bar"
        # Device code grant
        self.validator.validate_client_id.return_value = False
        self.assert_request_raises(errors.InvalidRequestFatalError, request)

    def test_unauthenticated_confidential_client(self):
        self.validator.client_authentication_required.return_value = True
        self.validator.authenticate_client.return_value = False
        request = self.build_request()
        self.assert_request_raises(errors.InvalidClientError, request)

    def test_unauthenticated_public_client(self):
        self.validator.client_authentication_required.return_value = False
        self.validator.authenticate_client_id.return_value = False
        request = self.build_request()
        self.assert_request_raises(errors.InvalidClientError, request)

    def test_duplicate_scope_parameter(self):
        request = self.build_request()
        request.body = "client_id=foo&scope=foo&scope=bar"
        # Device code grant
        self.validator.validate_client_id.return_value = False
        self.assert_request_raises(errors.InvalidRequestFatalError, request)
