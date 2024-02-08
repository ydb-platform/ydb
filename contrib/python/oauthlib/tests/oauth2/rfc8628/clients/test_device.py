import os
from unittest.mock import patch

from oauthlib import signals
from oauthlib.oauth2 import DeviceClient

from tests.unittest import TestCase


class DeviceClientTest(TestCase):

    client_id = "someclientid"
    kwargs = {
        "some": "providers",
        "require": "extra arguments"
    }

    client_secret = "asecret"

    device_code = "somedevicecode"

    scope = ["profile", "email"]

    body = "not=empty"

    body_up = "not=empty&grant_type=urn:ietf:params:oauth:grant-type:device_code"
    body_code = body_up + "&device_code=somedevicecode"
    body_kwargs = body_code + "&some=providers&require=extra+arguments"

    uri = "https://example.com/path?query=world"
    uri_id = uri + "&client_id=" + client_id
    uri_grant = uri_id + "&grant_type=urn:ietf:params:oauth:grant-type:device_code"
    uri_secret = uri_grant + "&client_secret=asecret"
    uri_scope = uri_secret + "&scope=profile+email"

    def test_request_body(self):
        client = DeviceClient(self.client_id)

        # Basic, no extra arguments
        body = client.prepare_request_body(self.device_code, body=self.body)
        self.assertFormBodyEqual(body, self.body_code)

        rclient = DeviceClient(self.client_id)
        body = rclient.prepare_request_body(self.device_code, body=self.body)
        self.assertFormBodyEqual(body, self.body_code)

        # With extra parameters
        body = client.prepare_request_body(
            self.device_code, body=self.body, **self.kwargs)
        self.assertFormBodyEqual(body, self.body_kwargs)

    def test_request_uri(self):
        client = DeviceClient(self.client_id)

        uri = client.prepare_request_uri(self.uri)
        self.assertURLEqual(uri, self.uri_grant)

        client = DeviceClient(self.client_id, client_secret=self.client_secret)
        uri = client.prepare_request_uri(self.uri)
        self.assertURLEqual(uri, self.uri_secret)

        uri = client.prepare_request_uri(self.uri, scope=self.scope)
        self.assertURLEqual(uri, self.uri_scope)
