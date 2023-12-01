# -*- coding: utf-8 -*-
from unittest import mock

from oauthlib.common import Request
from oauthlib.oauth2.rfc6749.grant_types import ImplicitGrant
from oauthlib.oauth2.rfc6749.tokens import BearerToken

from tests.unittest import TestCase


class ImplicitGrantTest(TestCase):

    def setUp(self):
        mock_client = mock.MagicMock()
        mock_client.user.return_value = 'mocked user'
        self.request = Request('http://a.b/path')
        self.request.scopes = ('hello', 'world')
        self.request.client = mock_client
        self.request.client_id = 'abcdef'
        self.request.response_type = 'token'
        self.request.state = 'xyz'
        self.request.redirect_uri = 'https://b.c/p'

        self.mock_validator = mock.MagicMock()
        self.auth = ImplicitGrant(request_validator=self.mock_validator)

    @mock.patch('oauthlib.common.generate_token')
    def test_create_token_response(self, generate_token):
        generate_token.return_value = '1234'
        bearer = BearerToken(self.mock_validator, expires_in=1800)
        h, b, s = self.auth.create_token_response(self.request, bearer)
        correct_uri = 'https://b.c/p#access_token=1234&token_type=Bearer&expires_in=1800&state=xyz&scope=hello+world'
        self.assertEqual(s, 302)
        self.assertURLEqual(h['Location'], correct_uri, parse_fragment=True)
        self.assertEqual(self.mock_validator.save_token.call_count, 1)

        correct_uri = 'https://b.c/p?access_token=1234&token_type=Bearer&expires_in=1800&state=xyz&scope=hello+world'
        self.request.response_mode = 'query'
        h, b, s = self.auth.create_token_response(self.request, bearer)
        self.assertURLEqual(h['Location'], correct_uri)

    def test_custom_validators(self):
        self.authval1, self.authval2 = mock.Mock(), mock.Mock()
        self.tknval1, self.tknval2 = mock.Mock(), mock.Mock()
        for val in (self.authval1, self.authval2):
            val.return_value = {}
        for val in (self.tknval1, self.tknval2):
            val.return_value = None
        self.auth.custom_validators.pre_token.append(self.tknval1)
        self.auth.custom_validators.post_token.append(self.tknval2)
        self.auth.custom_validators.pre_auth.append(self.authval1)
        self.auth.custom_validators.post_auth.append(self.authval2)

        bearer = BearerToken(self.mock_validator)
        self.auth.create_token_response(self.request, bearer)
        self.assertTrue(self.tknval1.called)
        self.assertTrue(self.tknval2.called)
        self.assertTrue(self.authval1.called)
        self.assertTrue(self.authval2.called)

    def test_error_response(self):
        pass
