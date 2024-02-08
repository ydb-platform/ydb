# -*- coding: utf-8 -*-
import os
from unittest.mock import patch

from oauthlib import signals
from oauthlib.oauth2 import BackendApplicationClient

from tests.unittest import TestCase


@patch('time.time', new=lambda: 1000)
class BackendApplicationClientTest(TestCase):

    client_id = "someclientid"
    client_secret = 'someclientsecret'
    scope = ["/profile"]
    kwargs = {
        "some": "providers",
        "require": "extra arguments"
    }

    body = "not=empty"

    body_up = "not=empty&grant_type=client_credentials"
    body_kwargs = body_up + "&some=providers&require=extra+arguments"

    token_json = ('{   "access_token":"2YotnFZFEjr1zCsicMWpAA",'
                  '    "token_type":"example",'
                  '    "expires_in":3600,'
                  '    "scope":"/profile",'
                  '    "example_parameter":"example_value"}')
    token = {
        "access_token": "2YotnFZFEjr1zCsicMWpAA",
        "token_type": "example",
        "expires_in": 3600,
        "expires_at": 4600,
        "scope": ["/profile"],
        "example_parameter": "example_value"
    }

    def test_request_body(self):
        client = BackendApplicationClient(self.client_id)

        # Basic, no extra arguments
        body = client.prepare_request_body(body=self.body)
        self.assertFormBodyEqual(body, self.body_up)

        rclient = BackendApplicationClient(self.client_id)
        body = rclient.prepare_request_body(body=self.body)
        self.assertFormBodyEqual(body, self.body_up)

        # With extra parameters
        body = client.prepare_request_body(body=self.body, **self.kwargs)
        self.assertFormBodyEqual(body, self.body_kwargs)

    def test_parse_token_response(self):
        client = BackendApplicationClient(self.client_id)

        # Parse code and state
        response = client.parse_request_body_response(self.token_json, scope=self.scope)
        self.assertEqual(response, self.token)
        self.assertEqual(client.access_token, response.get("access_token"))
        self.assertEqual(client.refresh_token, response.get("refresh_token"))
        self.assertEqual(client.token_type, response.get("token_type"))

        # Mismatching state
        self.assertRaises(Warning, client.parse_request_body_response, self.token_json, scope="invalid")
        os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '3'
        token = client.parse_request_body_response(self.token_json, scope="invalid")
        self.assertTrue(token.scope_changed)

        scope_changes_recorded = []
        def record_scope_change(sender, message, old, new):
            scope_changes_recorded.append((message, old, new))

        signals.scope_changed.connect(record_scope_change)
        try:
            client.parse_request_body_response(self.token_json, scope="invalid")
            self.assertEqual(len(scope_changes_recorded), 1)
            message, old, new = scope_changes_recorded[0]
            self.assertEqual(message, 'Scope has changed from "invalid" to "/profile".')
            self.assertEqual(old, ['invalid'])
            self.assertEqual(new, ['/profile'])
        finally:
            signals.scope_changed.disconnect(record_scope_change)
        del os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE']
