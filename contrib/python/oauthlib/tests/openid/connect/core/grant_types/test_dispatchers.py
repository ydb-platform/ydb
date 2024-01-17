# -*- coding: utf-8 -*-
from unittest import mock

from oauthlib.common import Request
from oauthlib.oauth2.rfc6749.grant_types import (
    AuthorizationCodeGrant as OAuth2AuthorizationCodeGrant,
    ImplicitGrant as OAuth2ImplicitGrant,
)
from oauthlib.openid.connect.core.grant_types.authorization_code import (
    AuthorizationCodeGrant,
)
from oauthlib.openid.connect.core.grant_types.dispatchers import (
    AuthorizationTokenGrantDispatcher, ImplicitTokenGrantDispatcher,
)
from oauthlib.openid.connect.core.grant_types.implicit import ImplicitGrant

from tests.unittest import TestCase


class ImplicitTokenGrantDispatcherTest(TestCase):
    def setUp(self):
        self.request = Request('http://a.b/path')
        request_validator = mock.MagicMock()
        implicit_grant = OAuth2ImplicitGrant(request_validator)
        openid_connect_implicit = ImplicitGrant(request_validator)

        self.dispatcher = ImplicitTokenGrantDispatcher(
            default_grant=implicit_grant,
            oidc_grant=openid_connect_implicit
        )

    def test_create_authorization_response_openid(self):
        self.request.scopes = ('hello', 'openid')
        self.request.response_type = 'id_token'
        handler = self.dispatcher._handler_for_request(self.request)
        self.assertIsInstance(handler, ImplicitGrant)

    def test_validate_authorization_request_openid(self):
        self.request.scopes = ('hello', 'openid')
        self.request.response_type = 'id_token'
        handler = self.dispatcher._handler_for_request(self.request)
        self.assertIsInstance(handler, ImplicitGrant)

    def test_create_authorization_response_oauth(self):
        self.request.scopes = ('hello', 'world')
        handler = self.dispatcher._handler_for_request(self.request)
        self.assertIsInstance(handler, OAuth2ImplicitGrant)

    def test_validate_authorization_request_oauth(self):
        self.request.scopes = ('hello', 'world')
        handler = self.dispatcher._handler_for_request(self.request)
        self.assertIsInstance(handler, OAuth2ImplicitGrant)


class DispatcherTest(TestCase):
    def setUp(self):
        self.request = Request('http://a.b/path')
        self.request.decoded_body = (
            ("client_id", "me"),
            ("code", "code"),
            ("redirect_url", "https://a.b/cb"),
        )

        self.request_validator = mock.MagicMock()
        self.auth_grant = OAuth2AuthorizationCodeGrant(self.request_validator)
        self.openid_connect_auth = AuthorizationCodeGrant(self.request_validator)


class AuthTokenGrantDispatcherOpenIdTest(DispatcherTest):

    def setUp(self):
        super().setUp()
        self.request_validator.get_authorization_code_scopes.return_value = ('hello', 'openid')
        self.dispatcher = AuthorizationTokenGrantDispatcher(
            self.request_validator,
            default_grant=self.auth_grant,
            oidc_grant=self.openid_connect_auth
        )

    def test_create_token_response_openid(self):
        handler = self.dispatcher._handler_for_request(self.request)
        self.assertIsInstance(handler, AuthorizationCodeGrant)
        self.assertTrue(self.dispatcher.request_validator.get_authorization_code_scopes.called)


class AuthTokenGrantDispatcherOpenIdWithoutCodeTest(DispatcherTest):

    def setUp(self):
        super().setUp()
        self.request.decoded_body = (
            ("client_id", "me"),
            ("code", ""),
            ("redirect_url", "https://a.b/cb"),
        )
        self.request_validator.get_authorization_code_scopes.return_value = ('hello', 'openid')
        self.dispatcher = AuthorizationTokenGrantDispatcher(
            self.request_validator,
            default_grant=self.auth_grant,
            oidc_grant=self.openid_connect_auth
        )

    def test_create_token_response_openid_without_code(self):
        handler = self.dispatcher._handler_for_request(self.request)
        self.assertIsInstance(handler, OAuth2AuthorizationCodeGrant)
        self.assertFalse(self.dispatcher.request_validator.get_authorization_code_scopes.called)


class AuthTokenGrantDispatcherOAuthTest(DispatcherTest):

    def setUp(self):
        super().setUp()
        self.request_validator.get_authorization_code_scopes.return_value = ('hello', 'world')
        self.dispatcher = AuthorizationTokenGrantDispatcher(
            self.request_validator,
            default_grant=self.auth_grant,
            oidc_grant=self.openid_connect_auth
        )

    def test_create_token_response_oauth(self):
        handler = self.dispatcher._handler_for_request(self.request)
        self.assertIsInstance(handler, OAuth2AuthorizationCodeGrant)
        self.assertTrue(self.dispatcher.request_validator.get_authorization_code_scopes.called)
