# -*- coding: utf-8 -*-
import os
import urllib.parse as urlparse
from unittest.mock import patch

from oauthlib import signals
from oauthlib.oauth2 import LegacyApplicationClient

from tests.unittest import TestCase


@patch('time.time', new=lambda: 1000)
class LegacyApplicationClientTest(TestCase):

    client_id = "someclientid"
    client_secret = 'someclientsecret'
    scope = ["/profile"]
    kwargs = {
        "some": "providers",
        "require": "extra arguments"
    }

    username = "user_username"
    password = "user_password"
    body = "not=empty"

    body_up = "not=empty&grant_type=password&username={}&password={}".format(username, password)
    body_kwargs = body_up + "&some=providers&require=extra+arguments"

    token_json = ('{   "access_token":"2YotnFZFEjr1zCsicMWpAA",'
                  '    "token_type":"example",'
                  '    "expires_in":3600,'
                  '    "scope":"/profile",'
                  '    "refresh_token":"tGzv3JOkF0XG5Qx2TlKWIA",'
                  '    "example_parameter":"example_value"}')
    token = {
        "access_token": "2YotnFZFEjr1zCsicMWpAA",
        "token_type": "example",
        "expires_in": 3600,
        "expires_at": 4600,
        "scope": scope,
        "refresh_token": "tGzv3JOkF0XG5Qx2TlKWIA",
        "example_parameter": "example_value"
    }

    def test_request_body(self):
        client = LegacyApplicationClient(self.client_id)

        # Basic, no extra arguments
        body = client.prepare_request_body(self.username, self.password,
                body=self.body)
        self.assertFormBodyEqual(body, self.body_up)

        # With extra parameters
        body = client.prepare_request_body(self.username, self.password,
                body=self.body, **self.kwargs)
        self.assertFormBodyEqual(body, self.body_kwargs)

    def test_parse_token_response(self):
        client = LegacyApplicationClient(self.client_id)

        # Parse code and state
        response = client.parse_request_body_response(self.token_json, scope=self.scope)
        self.assertEqual(response, self.token)
        self.assertEqual(client.access_token, response.get("access_token"))
        self.assertEqual(client.refresh_token, response.get("refresh_token"))
        self.assertEqual(client.token_type, response.get("token_type"))

        # Mismatching state
        self.assertRaises(Warning, client.parse_request_body_response, self.token_json, scope="invalid")
        os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '5'
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

    def test_prepare_request_body(self):
        """
        see issue #585
            https://github.com/oauthlib/oauthlib/issues/585
        """
        client = LegacyApplicationClient(self.client_id)

        # scenario 1, default behavior to not include `client_id`
        r1 = client.prepare_request_body(username=self.username, password=self.password)
        self.assertIn(r1, ('grant_type=password&username={}&password={}'.format(self.username, self.password),
                           'grant_type=password&password={}&username={}'.format(self.password, self.username),
                          ))

        # scenario 2, include `client_id` in the body
        r2 = client.prepare_request_body(username=self.username, password=self.password, include_client_id=True)
        r2_params = dict(urlparse.parse_qsl(r2, keep_blank_values=True))
        self.assertEqual(len(r2_params.keys()), 4)
        self.assertEqual(r2_params['grant_type'], 'password')
        self.assertEqual(r2_params['username'], self.username)
        self.assertEqual(r2_params['password'], self.password)
        self.assertEqual(r2_params['client_id'], self.client_id)

        # scenario 3, include `client_id` + `client_secret` in the body
        r3 = client.prepare_request_body(username=self.username, password=self.password, include_client_id=True, client_secret=self.client_secret)
        r3_params = dict(urlparse.parse_qsl(r3, keep_blank_values=True))
        self.assertEqual(len(r3_params.keys()), 5)
        self.assertEqual(r3_params['grant_type'], 'password')
        self.assertEqual(r3_params['username'], self.username)
        self.assertEqual(r3_params['password'], self.password)
        self.assertEqual(r3_params['client_id'], self.client_id)
        self.assertEqual(r3_params['client_secret'], self.client_secret)

        # scenario 4, `client_secret` is an empty string
        r4 = client.prepare_request_body(username=self.username, password=self.password, include_client_id=True, client_secret='')
        r4_params = dict(urlparse.parse_qsl(r4, keep_blank_values=True))
        self.assertEqual(len(r4_params.keys()), 5)
        self.assertEqual(r4_params['grant_type'], 'password')
        self.assertEqual(r4_params['username'], self.username)
        self.assertEqual(r4_params['password'], self.password)
        self.assertEqual(r4_params['client_id'], self.client_id)
        self.assertEqual(r4_params['client_secret'], '')

        # scenario 4b`,` client_secret is `None`
        r4b = client.prepare_request_body(username=self.username, password=self.password, include_client_id=True, client_secret=None)
        r4b_params = dict(urlparse.parse_qsl(r4b, keep_blank_values=True))
        self.assertEqual(len(r4b_params.keys()), 4)
        self.assertEqual(r4b_params['grant_type'], 'password')
        self.assertEqual(r4b_params['username'], self.username)
        self.assertEqual(r4b_params['password'], self.password)
        self.assertEqual(r4b_params['client_id'], self.client_id)
