# -*- coding: utf-8 -*-
import datetime

from oauthlib import common
from oauthlib.oauth2 import Client, InsecureTransportError, TokenExpiredError
from oauthlib.oauth2.rfc6749 import utils
from oauthlib.oauth2.rfc6749.clients import AUTH_HEADER, BODY, URI_QUERY

from tests.unittest import TestCase


class ClientTest(TestCase):

    client_id = "someclientid"
    uri = "https://example.com/path?query=world"
    body = "not=empty"
    headers = {}
    access_token = "token"
    mac_key = "secret"

    bearer_query = uri + "&access_token=" + access_token
    bearer_header = {
        "Authorization": "Bearer " + access_token
    }
    bearer_body = body + "&access_token=" + access_token

    mac_00_header = {
        "Authorization": 'MAC id="' + access_token + '", nonce="0:abc123",' +
                         ' bodyhash="Yqyso8r3hR5Nm1ZFv+6AvNHrxjE=",' +
                         ' mac="0X6aACoBY0G6xgGZVJ1IeE8dF9k="'
    }
    mac_01_header = {
        "Authorization": 'MAC id="' + access_token + '", ts="123456789",' +
                          ' nonce="abc123", mac="Xuk+9oqaaKyhitkgh1CD0xrI6+s="'
    }

    def test_add_bearer_token(self):
        """Test a number of bearer token placements"""

        # Invalid token type
        client = Client(self.client_id, token_type="invalid")
        self.assertRaises(ValueError, client.add_token, self.uri)

        # Case-insensitive token type
        client = Client(self.client_id, access_token=self.access_token, token_type="bEAreR")
        uri, headers, body = client.add_token(self.uri, body=self.body,
                headers=self.headers)
        self.assertURLEqual(uri, self.uri)
        self.assertFormBodyEqual(body, self.body)
        self.assertEqual(headers, self.bearer_header)

        # Non-HTTPS
        insecure_uri = 'http://example.com/path?query=world'
        client = Client(self.client_id, access_token=self.access_token, token_type="Bearer")
        self.assertRaises(InsecureTransportError, client.add_token, insecure_uri,
                body=self.body,
                headers=self.headers)

        # Missing access token
        client = Client(self.client_id)
        self.assertRaises(ValueError, client.add_token, self.uri)

        # Expired token
        expired = 523549800
        expired_token = {
                'expires_at': expired,
        }
        client = Client(self.client_id, token=expired_token, access_token=self.access_token, token_type="Bearer")
        self.assertRaises(TokenExpiredError, client.add_token, self.uri,
                body=self.body, headers=self.headers)

        # The default token placement, bearer in auth header
        client = Client(self.client_id, access_token=self.access_token)
        uri, headers, body = client.add_token(self.uri, body=self.body,
                headers=self.headers)
        self.assertURLEqual(uri, self.uri)
        self.assertFormBodyEqual(body, self.body)
        self.assertEqual(headers, self.bearer_header)

        # Setting default placements of tokens
        client = Client(self.client_id, access_token=self.access_token,
                default_token_placement=AUTH_HEADER)
        uri, headers, body = client.add_token(self.uri, body=self.body,
                headers=self.headers)
        self.assertURLEqual(uri, self.uri)
        self.assertFormBodyEqual(body, self.body)
        self.assertEqual(headers, self.bearer_header)

        client = Client(self.client_id, access_token=self.access_token,
                default_token_placement=URI_QUERY)
        uri, headers, body = client.add_token(self.uri, body=self.body,
                headers=self.headers)
        self.assertURLEqual(uri, self.bearer_query)
        self.assertFormBodyEqual(body, self.body)
        self.assertEqual(headers, self.headers)

        client = Client(self.client_id, access_token=self.access_token,
                default_token_placement=BODY)
        uri, headers, body = client.add_token(self.uri, body=self.body,
                headers=self.headers)
        self.assertURLEqual(uri, self.uri)
        self.assertFormBodyEqual(body, self.bearer_body)
        self.assertEqual(headers, self.headers)

        # Asking for specific placement in the add_token method
        client = Client(self.client_id, access_token=self.access_token)
        uri, headers, body = client.add_token(self.uri, body=self.body,
                headers=self.headers, token_placement=AUTH_HEADER)
        self.assertURLEqual(uri, self.uri)
        self.assertFormBodyEqual(body, self.body)
        self.assertEqual(headers, self.bearer_header)

        client = Client(self.client_id, access_token=self.access_token)
        uri, headers, body = client.add_token(self.uri, body=self.body,
                headers=self.headers, token_placement=URI_QUERY)
        self.assertURLEqual(uri, self.bearer_query)
        self.assertFormBodyEqual(body, self.body)
        self.assertEqual(headers, self.headers)

        client = Client(self.client_id, access_token=self.access_token)
        uri, headers, body = client.add_token(self.uri, body=self.body,
                headers=self.headers, token_placement=BODY)
        self.assertURLEqual(uri, self.uri)
        self.assertFormBodyEqual(body, self.bearer_body)
        self.assertEqual(headers, self.headers)

        # Invalid token placement
        client = Client(self.client_id, access_token=self.access_token)
        self.assertRaises(ValueError, client.add_token, self.uri, body=self.body,
                headers=self.headers, token_placement="invalid")

        client = Client(self.client_id, access_token=self.access_token,
                default_token_placement="invalid")
        self.assertRaises(ValueError, client.add_token, self.uri, body=self.body,
                headers=self.headers)

    def test_add_mac_token(self):
        # Missing access token
        client = Client(self.client_id, token_type="MAC")
        self.assertRaises(ValueError, client.add_token, self.uri)

        # Invalid hash algorithm
        client = Client(self.client_id, token_type="MAC",
                access_token=self.access_token, mac_key=self.mac_key,
                mac_algorithm="hmac-sha-2")
        self.assertRaises(ValueError, client.add_token, self.uri)

        orig_generate_timestamp = common.generate_timestamp
        orig_generate_nonce = common.generate_nonce
        orig_generate_age = utils.generate_age
        self.addCleanup(setattr, common, 'generage_timestamp', orig_generate_timestamp)
        self.addCleanup(setattr, common, 'generage_nonce', orig_generate_nonce)
        self.addCleanup(setattr, utils, 'generate_age', orig_generate_age)
        common.generate_timestamp = lambda: '123456789'
        common.generate_nonce = lambda: 'abc123'
        utils.generate_age = lambda *args: 0

        # Add the Authorization header (draft 00)
        client = Client(self.client_id, token_type="MAC",
                access_token=self.access_token, mac_key=self.mac_key,
                mac_algorithm="hmac-sha-1")
        uri, headers, body = client.add_token(self.uri, body=self.body,
                headers=self.headers, issue_time=datetime.datetime.now())
        self.assertEqual(uri, self.uri)
        self.assertEqual(body, self.body)
        self.assertEqual(headers, self.mac_00_header)
        # Non-HTTPS
        insecure_uri = 'http://example.com/path?query=world'
        self.assertRaises(InsecureTransportError, client.add_token, insecure_uri,
                body=self.body,
                headers=self.headers,
                issue_time=datetime.datetime.now())
        # Expired Token
        expired = 523549800
        expired_token = {
                'expires_at': expired,
        }
        client = Client(self.client_id, token=expired_token, token_type="MAC",
                access_token=self.access_token, mac_key=self.mac_key,
                mac_algorithm="hmac-sha-1")
        self.assertRaises(TokenExpiredError, client.add_token, self.uri,
                body=self.body,
                headers=self.headers,
                issue_time=datetime.datetime.now())

        # Add the Authorization header (draft 01)
        client = Client(self.client_id, token_type="MAC",
                access_token=self.access_token, mac_key=self.mac_key,
                mac_algorithm="hmac-sha-1")
        uri, headers, body = client.add_token(self.uri, body=self.body,
                headers=self.headers, draft=1)
        self.assertEqual(uri, self.uri)
        self.assertEqual(body, self.body)
        self.assertEqual(headers, self.mac_01_header)
        # Non-HTTPS
        insecure_uri = 'http://example.com/path?query=world'
        self.assertRaises(InsecureTransportError, client.add_token, insecure_uri,
                body=self.body,
                headers=self.headers,
                draft=1)
        # Expired Token
        expired = 523549800
        expired_token = {
                'expires_at': expired,
        }
        client = Client(self.client_id, token=expired_token, token_type="MAC",
                access_token=self.access_token, mac_key=self.mac_key,
                mac_algorithm="hmac-sha-1")
        self.assertRaises(TokenExpiredError, client.add_token, self.uri,
                body=self.body,
                headers=self.headers,
                draft=1)

    def test_revocation_request(self):
        client = Client(self.client_id)

        url = 'https://example.com/revoke'
        token = 'foobar'

        # Valid request
        u, h, b = client.prepare_token_revocation_request(url, token)
        self.assertEqual(u, url)
        self.assertEqual(h, {'Content-Type': 'application/x-www-form-urlencoded'})
        self.assertEqual(b, 'token=%s&token_type_hint=access_token' % token)

        # Non-HTTPS revocation endpoint
        self.assertRaises(InsecureTransportError,
                          client.prepare_token_revocation_request,
                          'http://example.com/revoke', token)


        u, h, b = client.prepare_token_revocation_request(
            url, token, token_type_hint='refresh_token')
        self.assertEqual(u, url)
        self.assertEqual(h, {'Content-Type': 'application/x-www-form-urlencoded'})
        self.assertEqual(b, 'token=%s&token_type_hint=refresh_token' % token)

        # JSONP
        u, h, b = client.prepare_token_revocation_request(
            url, token, callback='hello.world')
        self.assertURLEqual(u, url + '?callback=hello.world&token=%s&token_type_hint=access_token' % token)
        self.assertEqual(h, {'Content-Type': 'application/x-www-form-urlencoded'})
        self.assertEqual(b, '')

    def test_prepare_authorization_request(self):
        redirect_url = 'https://example.com/callback/'
        scopes = 'read'
        auth_url = 'https://example.com/authorize/'
        state = 'fake_state'

        client = Client(self.client_id, redirect_url=redirect_url, scope=scopes, state=state)

        # Non-HTTPS
        self.assertRaises(InsecureTransportError,
                          client.prepare_authorization_request, 'http://example.com/authorize/')

        # NotImplementedError
        self.assertRaises(NotImplementedError, client.prepare_authorization_request, auth_url)

    def test_prepare_token_request(self):
        redirect_url = 'https://example.com/callback/'
        scopes = 'read'
        token_url = 'https://example.com/token/'
        state = 'fake_state'

        client = Client(self.client_id, scope=scopes, state=state)

        # Non-HTTPS
        self.assertRaises(InsecureTransportError,
                          client.prepare_token_request, 'http://example.com/token/')

        # NotImplementedError
        self.assertRaises(NotImplementedError, client.prepare_token_request, token_url)

    def test_prepare_refresh_token_request(self):
        client = Client(self.client_id)

        url = 'https://example.com/revoke'
        token = 'foobar'
        scope = 'extra_scope'

        u, h, b = client.prepare_refresh_token_request(url, token)
        self.assertEqual(u, url)
        self.assertEqual(h, {'Content-Type': 'application/x-www-form-urlencoded'})
        self.assertFormBodyEqual(b, 'grant_type=refresh_token&refresh_token=%s' % token)

        # Non-HTTPS revocation endpoint
        self.assertRaises(InsecureTransportError,
                          client.prepare_refresh_token_request,
                          'http://example.com/revoke', token)

        # provide extra scope
        u, h, b = client.prepare_refresh_token_request(url, token, scope=scope)
        self.assertEqual(u, url)
        self.assertEqual(h, {'Content-Type': 'application/x-www-form-urlencoded'})
        self.assertFormBodyEqual(b, 'grant_type=refresh_token&scope={}&refresh_token={}'.format(scope, token))

        # provide scope while init
        client = Client(self.client_id, scope=scope)
        u, h, b = client.prepare_refresh_token_request(url, token, scope=scope)
        self.assertEqual(u, url)
        self.assertEqual(h, {'Content-Type': 'application/x-www-form-urlencoded'})
        self.assertFormBodyEqual(b, 'grant_type=refresh_token&scope={}&refresh_token={}'.format(scope, token))

    def test_parse_token_response_invalid_expires_at(self):
        token_json = ('{   "access_token":"2YotnFZFEjr1zCsicMWpAA",'
                      '    "token_type":"example",'
                      '    "expires_at":"2006-01-02T15:04:05Z",'
                      '    "scope":"/profile",'
                      '    "example_parameter":"example_value"}')
        token = {
            "access_token": "2YotnFZFEjr1zCsicMWpAA",
            "token_type": "example",
            "expires_at": "2006-01-02T15:04:05Z",
            "scope": ["/profile"],
            "example_parameter": "example_value"
        }

        client = Client(self.client_id)

        # Parse code and state
        response = client.parse_request_body_response(token_json, scope=["/profile"])
        self.assertEqual(response, token)
        self.assertEqual(None, client._expires_at)
        self.assertEqual(client.access_token, response.get("access_token"))
        self.assertEqual(client.refresh_token, response.get("refresh_token"))
        self.assertEqual(client.token_type, response.get("token_type"))


    def test_create_code_verifier_min_length(self):
        client = Client(self.client_id)
        length = 43
        code_verifier = client.create_code_verifier(length=length)
        self.assertEqual(client.code_verifier, code_verifier)

    def test_create_code_verifier_max_length(self):
        client = Client(self.client_id)
        length = 128
        code_verifier = client.create_code_verifier(length=length)
        self.assertEqual(client.code_verifier, code_verifier)

    def test_create_code_challenge_plain(self):
        client = Client(self.client_id)
        code_verifier = client.create_code_verifier(length=128)
        code_challenge_plain = client.create_code_challenge(code_verifier=code_verifier)

        # if no code_challenge_method specified, code_challenge = code_verifier
        self.assertEqual(code_challenge_plain, client.code_verifier)
        self.assertEqual(client.code_challenge_method, "plain")

    def test_create_code_challenge_s256(self):
        client = Client(self.client_id)
        code_verifier = client.create_code_verifier(length=128)
        code_challenge_s256 = client.create_code_challenge(code_verifier=code_verifier, code_challenge_method='S256')
        self.assertEqual(code_challenge_s256, client.code_challenge)
