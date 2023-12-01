# -*- coding: utf-8 -*-
import os
import urllib.parse as urlparse
import warnings
from unittest.mock import patch

from oauthlib import common, signals
from oauthlib.oauth2 import (
    BackendApplicationClient, Client, LegacyApplicationClient,
    MobileApplicationClient, WebApplicationClient,
)
from oauthlib.oauth2.rfc6749 import errors, utils
from oauthlib.oauth2.rfc6749.clients import AUTH_HEADER, BODY, URI_QUERY

from tests.unittest import TestCase


@patch('time.time', new=lambda: 1000)
class WebApplicationClientTest(TestCase):

    client_id = "someclientid"
    client_secret = 'someclientsecret'
    uri = "https://example.com/path?query=world"
    uri_id = uri + "&response_type=code&client_id=" + client_id
    uri_redirect = uri_id + "&redirect_uri=http%3A%2F%2Fmy.page.com%2Fcallback"
    redirect_uri = "http://my.page.com/callback"
    code_verifier = "code_verifier"
    scope = ["/profile"]
    state = "xyz"
    code_challenge = "code_challenge"
    code_challenge_method = "S256"
    uri_scope = uri_id + "&scope=%2Fprofile"
    uri_state = uri_id + "&state=" + state
    uri_code_challenge = uri_id + "&code_challenge=" + code_challenge + "&code_challenge_method=" + code_challenge_method
    uri_code_challenge_method = uri_id + "&code_challenge=" + code_challenge + "&code_challenge_method=plain"
    kwargs = {
        "some": "providers",
        "require": "extra arguments"
    }
    uri_kwargs = uri_id + "&some=providers&require=extra+arguments"
    uri_authorize_code = uri_redirect + "&scope=%2Fprofile&state=" + state

    code = "zzzzaaaa"
    body = "not=empty"

    body_code = "not=empty&grant_type=authorization_code&code={}&client_id={}".format(code, client_id)
    body_redirect = body_code + "&redirect_uri=http%3A%2F%2Fmy.page.com%2Fcallback"
    body_code_verifier = body_code + "&code_verifier=code_verifier"
    body_kwargs = body_code + "&some=providers&require=extra+arguments"

    response_uri = "https://client.example.com/cb?code=zzzzaaaa&state=xyz"
    response = {"code": "zzzzaaaa", "state": "xyz"}

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

    def test_auth_grant_uri(self):
        client = WebApplicationClient(self.client_id)

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

        # with code_challenge and code_challenge_method
        uri = client.prepare_request_uri(self.uri, code_challenge=self.code_challenge, code_challenge_method=self.code_challenge_method)
        self.assertURLEqual(uri, self.uri_code_challenge)

        # with no code_challenge_method
        uri = client.prepare_request_uri(self.uri, code_challenge=self.code_challenge)
        self.assertURLEqual(uri, self.uri_code_challenge_method)

        # With extra parameters through kwargs
        uri = client.prepare_request_uri(self.uri, **self.kwargs)
        self.assertURLEqual(uri, self.uri_kwargs)

    def test_request_body(self):
        client = WebApplicationClient(self.client_id, code=self.code)

        # Basic, no extra arguments
        body = client.prepare_request_body(body=self.body)
        self.assertFormBodyEqual(body, self.body_code)

        rclient = WebApplicationClient(self.client_id)
        body = rclient.prepare_request_body(code=self.code, body=self.body)
        self.assertFormBodyEqual(body, self.body_code)

        # With redirection uri
        body = client.prepare_request_body(body=self.body, redirect_uri=self.redirect_uri)
        self.assertFormBodyEqual(body, self.body_redirect)

        # With code verifier
        body = client.prepare_request_body(body=self.body, code_verifier=self.code_verifier)
        self.assertFormBodyEqual(body, self.body_code_verifier)

        # With extra parameters
        body = client.prepare_request_body(body=self.body, **self.kwargs)
        self.assertFormBodyEqual(body, self.body_kwargs)

    def test_parse_grant_uri_response(self):
        client = WebApplicationClient(self.client_id)

        # Parse code and state
        response = client.parse_request_uri_response(self.response_uri, state=self.state)
        self.assertEqual(response, self.response)
        self.assertEqual(client.code, self.code)

        # Mismatching state
        self.assertRaises(errors.MismatchingStateError,
                client.parse_request_uri_response,
                self.response_uri,
                state="invalid")

    def test_populate_attributes(self):

        client = WebApplicationClient(self.client_id)

        response_uri = (self.response_uri +
                        "&access_token=EVIL-TOKEN"
                        "&refresh_token=EVIL-TOKEN"
                        "&mac_key=EVIL-KEY")

        client.parse_request_uri_response(response_uri, self.state)

        self.assertEqual(client.code, self.code)

        # We must not accidentally pick up any further security
        # credentials at this point.
        self.assertIsNone(client.access_token)
        self.assertIsNone(client.refresh_token)
        self.assertIsNone(client.mac_key)

    def test_parse_token_response(self):
        client = WebApplicationClient(self.client_id)

        # Parse code and state
        response = client.parse_request_body_response(self.token_json, scope=self.scope)
        self.assertEqual(response, self.token)
        self.assertEqual(client.access_token, response.get("access_token"))
        self.assertEqual(client.refresh_token, response.get("refresh_token"))
        self.assertEqual(client.token_type, response.get("token_type"))

        # Mismatching state
        self.assertRaises(Warning, client.parse_request_body_response, self.token_json, scope="invalid")
        os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '1'
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

    def test_prepare_authorization_requeset(self):
        client = WebApplicationClient(self.client_id)

        url, header, body = client.prepare_authorization_request(
            self.uri, redirect_url=self.redirect_uri, state=self.state, scope=self.scope)
        self.assertURLEqual(url, self.uri_authorize_code)
        # verify default header and body only
        self.assertEqual(header, {'Content-Type': 'application/x-www-form-urlencoded'})
        self.assertEqual(body, '')

    def test_prepare_request_body(self):
        """
        see issue #585
            https://github.com/oauthlib/oauthlib/issues/585

        `prepare_request_body` should support the following scenarios:
            1. Include client_id alone in the body (default)
            2. Include client_id and client_secret in auth and not include them in the body (RFC preferred solution)
            3. Include client_id and client_secret in the body (RFC alternative solution)
            4. Include client_id in the body and an empty string for client_secret.
        """
        client = WebApplicationClient(self.client_id)

        # scenario 1, default behavior to include `client_id`
        r1 = client.prepare_request_body()
        self.assertEqual(r1, 'grant_type=authorization_code&client_id=%s' % self.client_id)

        r1b = client.prepare_request_body(include_client_id=True)
        self.assertEqual(r1b, 'grant_type=authorization_code&client_id=%s' % self.client_id)

        # scenario 2, do not include `client_id` in the body, so it can be sent in auth.
        r2 = client.prepare_request_body(include_client_id=False)
        self.assertEqual(r2, 'grant_type=authorization_code')

        # scenario 3, Include client_id and client_secret in the body (RFC alternative solution)
        # the order of kwargs being appended is not guaranteed. for brevity, check the 2 permutations instead of sorting
        r3 = client.prepare_request_body(client_secret=self.client_secret)
        r3_params = dict(urlparse.parse_qsl(r3, keep_blank_values=True))
        self.assertEqual(len(r3_params.keys()), 3)
        self.assertEqual(r3_params['grant_type'], 'authorization_code')
        self.assertEqual(r3_params['client_id'], self.client_id)
        self.assertEqual(r3_params['client_secret'], self.client_secret)

        r3b = client.prepare_request_body(include_client_id=True, client_secret=self.client_secret)
        r3b_params = dict(urlparse.parse_qsl(r3b, keep_blank_values=True))
        self.assertEqual(len(r3b_params.keys()), 3)
        self.assertEqual(r3b_params['grant_type'], 'authorization_code')
        self.assertEqual(r3b_params['client_id'], self.client_id)
        self.assertEqual(r3b_params['client_secret'], self.client_secret)

        # scenario 4, `client_secret` is an empty string
        r4 = client.prepare_request_body(include_client_id=True, client_secret='')
        r4_params = dict(urlparse.parse_qsl(r4, keep_blank_values=True))
        self.assertEqual(len(r4_params.keys()), 3)
        self.assertEqual(r4_params['grant_type'], 'authorization_code')
        self.assertEqual(r4_params['client_id'], self.client_id)
        self.assertEqual(r4_params['client_secret'], '')

        # scenario 4b, `client_secret` is `None`
        r4b = client.prepare_request_body(include_client_id=True, client_secret=None)
        r4b_params = dict(urlparse.parse_qsl(r4b, keep_blank_values=True))
        self.assertEqual(len(r4b_params.keys()), 2)
        self.assertEqual(r4b_params['grant_type'], 'authorization_code')
        self.assertEqual(r4b_params['client_id'], self.client_id)

        # scenario Warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")  # catch all

            # warning1 - raise a DeprecationWarning if a `client_id` is submitted
            rWarnings1 = client.prepare_request_body(client_id=self.client_id)
            self.assertEqual(len(w), 1)
            self.assertIsInstance(w[0].message, DeprecationWarning)

            # testing the exact warning message in Python2&Python3 is a pain

        # scenario Exceptions
        # exception1 - raise a ValueError if the a different `client_id` is submitted
        with self.assertRaises(ValueError) as cm:
            client.prepare_request_body(client_id='different_client_id')
            # testing the exact exception message in Python2&Python3 is a pain
