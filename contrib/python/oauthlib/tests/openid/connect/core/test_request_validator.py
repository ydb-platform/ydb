# -*- coding: utf-8 -*-
from oauthlib.openid import RequestValidator

from tests.unittest import TestCase


class RequestValidatorTest(TestCase):

    def test_method_contracts(self):
        v = RequestValidator()
        self.assertRaises(
            NotImplementedError,
            v.get_authorization_code_scopes,
            'client_id', 'code', 'redirect_uri', 'request'
        )
        self.assertRaises(
            NotImplementedError,
            v.get_jwt_bearer_token,
            'token', 'token_handler', 'request'
        )
        self.assertRaises(
            NotImplementedError,
            v.finalize_id_token,
            'id_token', 'token', 'token_handler', 'request'
        )
        self.assertRaises(
            NotImplementedError,
            v.validate_jwt_bearer_token,
            'token', 'scopes', 'request'
        )
        self.assertRaises(
            NotImplementedError,
            v.validate_id_token,
            'token', 'scopes', 'request'
        )
        self.assertRaises(
            NotImplementedError,
            v.validate_silent_authorization,
            'request'
        )
        self.assertRaises(
            NotImplementedError,
            v.validate_silent_login,
            'request'
        )
        self.assertRaises(
            NotImplementedError,
            v.validate_user_match,
            'id_token_hint', 'scopes', 'claims', 'request'
        )
