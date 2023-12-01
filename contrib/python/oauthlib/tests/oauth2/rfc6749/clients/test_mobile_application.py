# -*- coding: utf-8 -*-
import os
from unittest.mock import patch

from oauthlib import signals
from oauthlib.oauth2 import MobileApplicationClient

from tests.unittest import TestCase


@patch('time.time', new=lambda: 1000)
class MobileApplicationClientTest(TestCase):

    client_id = "someclientid"
    uri = "https://example.com/path?query=world"
    uri_id = uri + "&response_type=token&client_id=" + client_id
    uri_redirect = uri_id + "&redirect_uri=http%3A%2F%2Fmy.page.com%2Fcallback"
    redirect_uri = "http://my.page.com/callback"
    scope = ["/profile"]
    state = "xyz"
    uri_scope = uri_id + "&scope=%2Fprofile"
    uri_state = uri_id + "&state=" + state
    kwargs = {
        "some": "providers",
        "require": "extra arguments"
    }
    uri_kwargs = uri_id + "&some=providers&require=extra+arguments"

    code = "zzzzaaaa"

    response_uri = ('https://client.example.com/cb?#'
                    'access_token=2YotnFZFEjr1zCsicMWpAA&'
                    'token_type=example&'
                    'expires_in=3600&'
                    'scope=%2Fprofile&'
                    'example_parameter=example_value')
    token = {
        "access_token": "2YotnFZFEjr1zCsicMWpAA",
        "token_type": "example",
        "expires_in": 3600,
        "expires_at": 4600,
        "scope": scope,
        "example_parameter": "example_value"
    }

    def test_implicit_token_uri(self):
        client = MobileApplicationClient(self.client_id)

        # Basic, no extra arguments
        uri = client.prepare_request_uri(self.uri)
        self.assertURLEqual(uri, self.uri_id)

        # With redirection uri
        uri = client.prepare_request_uri(self.uri, redirect_uri=self.redirect_uri)
        self.assertURLEqual(uri, self.uri_redirect)

        # With scope
        uri = client.prepare_request_uri(self.uri, scope=self.scope)
        self.assertURLEqual(uri, self.uri_scope)

        # With state
        uri = client.prepare_request_uri(self.uri, state=self.state)
        self.assertURLEqual(uri, self.uri_state)

        # With extra parameters through kwargs
        uri = client.prepare_request_uri(self.uri, **self.kwargs)
        self.assertURLEqual(uri, self.uri_kwargs)

    def test_populate_attributes(self):

        client = MobileApplicationClient(self.client_id)

        response_uri = (self.response_uri + "&code=EVIL-CODE")

        client.parse_request_uri_response(response_uri, scope=self.scope)

        # We must not accidentally pick up any further security
        # credentials at this point.
        self.assertIsNone(client.code)

    def test_parse_token_response(self):
        client = MobileApplicationClient(self.client_id)

        # Parse code and state
        response = client.parse_request_uri_response(self.response_uri, scope=self.scope)
        self.assertEqual(response, self.token)
        self.assertEqual(client.access_token, response.get("access_token"))
        self.assertEqual(client.refresh_token, response.get("refresh_token"))
        self.assertEqual(client.token_type, response.get("token_type"))

        # Mismatching scope
        self.assertRaises(Warning, client.parse_request_uri_response, self.response_uri, scope="invalid")
        os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '4'
        token = client.parse_request_uri_response(self.response_uri, scope='invalid')
        self.assertTrue(token.scope_changed)

        scope_changes_recorded = []
        def record_scope_change(sender, message, old, new):
            scope_changes_recorded.append((message, old, new))

        signals.scope_changed.connect(record_scope_change)
        try:
            client.parse_request_uri_response(self.response_uri, scope="invalid")
            self.assertEqual(len(scope_changes_recorded), 1)
            message, old, new = scope_changes_recorded[0]
            self.assertEqual(message, 'Scope has changed from "invalid" to "/profile".')
            self.assertEqual(old, ['invalid'])
            self.assertEqual(new, ['/profile'])
        finally:
            signals.scope_changed.disconnect(record_scope_change)
        del os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE']
