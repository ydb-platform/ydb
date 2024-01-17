from unittest import mock

from oauthlib.openid.connect.core.tokens import JWTToken

from tests.unittest import TestCase


class JWTTokenTestCase(TestCase):

    def test_create_token_callable_expires_in(self):
        """
        Test retrieval of the expires in value by calling the callable expires_in property
        """

        expires_in_mock = mock.MagicMock()
        request_mock = mock.MagicMock()

        token = JWTToken(expires_in=expires_in_mock, request_validator=mock.MagicMock())
        token.create_token(request=request_mock)

        expires_in_mock.assert_called_once_with(request_mock)

    def test_create_token_non_callable_expires_in(self):
        """
        When a non callable expires in is set this should just be set to the request
        """

        expires_in_mock = mock.NonCallableMagicMock()
        request_mock = mock.MagicMock()

        token = JWTToken(expires_in=expires_in_mock, request_validator=mock.MagicMock())
        token.create_token(request=request_mock)

        self.assertFalse(expires_in_mock.called)
        self.assertEqual(request_mock.expires_in, expires_in_mock)

    def test_create_token_calls_get_id_token(self):
        """
        When create_token is called the call should be forwarded to the get_id_token on the token validator
        """
        request_mock = mock.MagicMock()

        with mock.patch('oauthlib.openid.RequestValidator',
                        autospec=True) as RequestValidatorMock:

            request_validator = RequestValidatorMock()

            token = JWTToken(expires_in=mock.MagicMock(), request_validator=request_validator)
            token.create_token(request=request_mock)

            request_validator.get_jwt_bearer_token.assert_called_once_with(None, None, request_mock)

    def test_validate_request_token_from_headers(self):
        """
        Bearer token get retrieved from headers.
        """

        with mock.patch('oauthlib.common.Request', autospec=True) as RequestMock, \
                mock.patch('oauthlib.openid.RequestValidator',
                           autospec=True) as RequestValidatorMock:
            request_validator_mock = RequestValidatorMock()

            token = JWTToken(request_validator=request_validator_mock)

            request = RequestMock('/uri')
            # Scopes is retrieved using the __call__ method which is not picked up correctly by mock.patch
            # with autospec=True
            request.scopes = mock.MagicMock()
            request.headers = {
                'Authorization': 'Bearer some-token-from-header'
            }

            token.validate_request(request=request)

            request_validator_mock.validate_jwt_bearer_token.assert_called_once_with('some-token-from-header',
                                                                                     request.scopes,
                                                                                     request)

    def test_validate_request_token_from_headers_basic(self):
        """
        Wrong kind of token (Basic) retrieved from headers. Confirm token is not parsed.
        """

        with mock.patch('oauthlib.common.Request', autospec=True) as RequestMock, \
                mock.patch('oauthlib.openid.RequestValidator',
                           autospec=True) as RequestValidatorMock:
            request_validator_mock = RequestValidatorMock()

            token = JWTToken(request_validator=request_validator_mock)

            request = RequestMock('/uri')
            # Scopes is retrieved using the __call__ method which is not picked up correctly by mock.patch
            # with autospec=True
            request.scopes = mock.MagicMock()
            request.headers = {
                'Authorization': 'Basic some-token-from-header'
            }

            token.validate_request(request=request)

            request_validator_mock.validate_jwt_bearer_token.assert_called_once_with(None,
                                                                                     request.scopes,
                                                                                     request)

    def test_validate_token_from_request(self):
        """
        Token get retrieved from request object.
        """

        with mock.patch('oauthlib.common.Request', autospec=True) as RequestMock, \
                mock.patch('oauthlib.openid.RequestValidator',
                           autospec=True) as RequestValidatorMock:
            request_validator_mock = RequestValidatorMock()

            token = JWTToken(request_validator=request_validator_mock)

            request = RequestMock('/uri')
            # Scopes is retrieved using the __call__ method which is not picked up correctly by mock.patch
            # with autospec=True
            request.scopes = mock.MagicMock()
            request.access_token = 'some-token-from-request-object'
            request.headers = {}

            token.validate_request(request=request)

            request_validator_mock.validate_jwt_bearer_token.assert_called_once_with('some-token-from-request-object',
                                                                                     request.scopes,
                                                                                     request)

    def test_estimate_type(self):
        """
        Estimate type results for a jwt token
        """

        def test_token(token, expected_result):
            with mock.patch('oauthlib.common.Request', autospec=True) as RequestMock:
                jwt_token = JWTToken()

                request = RequestMock('/uri')
                # Scopes is retrieved using the __call__ method which is not picked up correctly by mock.patch
                # with autospec=True
                request.headers = {
                    'Authorization': 'Bearer {}'.format(token)
                }

                result = jwt_token.estimate_type(request=request)

                self.assertEqual(result, expected_result)

        test_items = (
            ('eyfoo.foo.foo', 10),
            ('eyfoo.foo.foo.foo.foo', 10),
            ('eyfoobar', 0)
        )

        for token, expected_result in test_items:
            test_token(token, expected_result)
