# -*- coding: utf-8 -*-
from oauthlib.oauth2 import (
    FatalClientError, OAuth2Error, RequestValidator, Server,
)
from oauthlib.oauth2.rfc6749 import (
    BaseEndpoint, catch_errors_and_unavailability,
)

from tests.unittest import TestCase


class BaseEndpointTest(TestCase):

    def test_default_config(self):
        endpoint = BaseEndpoint()
        self.assertFalse(endpoint.catch_errors)
        self.assertTrue(endpoint.available)
        endpoint.catch_errors = True
        self.assertTrue(endpoint.catch_errors)
        endpoint.available = False
        self.assertFalse(endpoint.available)

    def test_error_catching(self):
        validator = RequestValidator()
        server = Server(validator)
        server.catch_errors = True
        h, b, s = server.create_token_response(
            'https://example.com', body='grant_type=authorization_code&code=abc'
        )
        self.assertIn("server_error", b)
        self.assertEqual(s, 500)

    def test_unavailability(self):
        validator = RequestValidator()
        server = Server(validator)
        server.available = False
        h, b, s = server.create_authorization_response('https://example.com')
        self.assertIn("temporarily_unavailable", b)
        self.assertEqual(s, 503)

    def test_wrapper(self):

        class TestServer(Server):

            @catch_errors_and_unavailability
            def throw_error(self, uri):
                raise ValueError()

            @catch_errors_and_unavailability
            def throw_oauth_error(self, uri):
                raise OAuth2Error()

            @catch_errors_and_unavailability
            def throw_fatal_oauth_error(self, uri):
                raise FatalClientError()

        validator = RequestValidator()
        server = TestServer(validator)

        server.catch_errors = True
        h, b, s = server.throw_error('a')
        self.assertIn("server_error", b)
        self.assertEqual(s, 500)

        server.available = False
        h, b, s = server.throw_error('a')
        self.assertIn("temporarily_unavailable", b)
        self.assertEqual(s, 503)

        server.available = True
        self.assertRaises(OAuth2Error, server.throw_oauth_error, 'a')
        self.assertRaises(FatalClientError, server.throw_fatal_oauth_error, 'a')
        server.catch_errors = False
        self.assertRaises(OAuth2Error, server.throw_oauth_error, 'a')
        self.assertRaises(FatalClientError, server.throw_fatal_oauth_error, 'a')
