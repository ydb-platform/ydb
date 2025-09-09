import json
from unittest import mock
import pytest

from oauthlib import common

from oauthlib.oauth2.rfc8628.grant_types import DeviceCodeGrant
from oauthlib.oauth2.rfc6749.tokens import BearerToken

def create_request(body: str = "") -> common.Request:
    request = common.Request("http://a.b/path", body=body or None)
    request.scopes = ("hello", "world")
    request.expires_in = 1800
    request.client = "batman"
    request.client_id = "abcdef"
    request.code = "1234"
    request.response_type = "code"
    request.grant_type = "urn:ietf:params:oauth:grant-type:device_code"
    request.redirect_uri = "https://a.b/"
    return request


def create_device_code_grant(mock_validator: mock.MagicMock) -> DeviceCodeGrant:
    return DeviceCodeGrant(request_validator=mock_validator)


def test_custom_auth_validators_unsupported():
    custom_validator = mock.Mock()
    validator = mock.MagicMock()

    expected = (
        "DeviceCodeGrant does not "
        "support authorization validators. Use token validators instead."
    )
    with pytest.raises(ValueError, match=expected):
        DeviceCodeGrant(validator, pre_auth=[custom_validator])

    with pytest.raises(ValueError, match=expected):
        DeviceCodeGrant(validator, post_auth=[custom_validator])

    expected = "'tuple' object has no attribute 'append'"
    auth = DeviceCodeGrant(validator)
    with pytest.raises(AttributeError, match=expected):
        auth.custom_validators.pre_auth.append(custom_validator)


def test_custom_pre_and_post_token_validators():
    client = mock.MagicMock()

    validator = mock.MagicMock()
    pre_token_validator = mock.Mock()
    post_token_validator = mock.Mock()

    request: common.Request = create_request()
    request.client = client

    auth = DeviceCodeGrant(validator)

    auth.custom_validators.pre_token.append(pre_token_validator)
    auth.custom_validators.post_token.append(post_token_validator)

    bearer = BearerToken(validator)
    auth.create_token_response(request, bearer)

    pre_token_validator.assert_called()
    post_token_validator.assert_called()


def test_create_token_response():
    validator = mock.MagicMock()
    request: common.Request = create_request()
    request.client = mock.Mock()

    auth = DeviceCodeGrant(validator)

    bearer = BearerToken(validator)

    headers, body, status_code = auth.create_token_response(request, bearer)
    token = json.loads(body)

    assert headers == {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
        "Pragma": "no-cache",
    }

    # when a custom token generator callable isn't used
    # the random generator is used as default for the access token
    assert token == {
        "access_token": mock.ANY,
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "hello world",
        "refresh_token": mock.ANY,
    }

    assert status_code == 200

    validator.save_token.assert_called_once()


def test_invalid_client_error():
    validator = mock.MagicMock()
    request: common.Request = create_request()
    request.client = mock.Mock()

    auth = DeviceCodeGrant(validator)
    bearer = BearerToken(validator)

    validator.authenticate_client.return_value = False

    headers, body, status_code = auth.create_token_response(request, bearer)
    body = json.loads(body)

    assert headers == {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
        "Pragma": "no-cache",
        "WWW-Authenticate": 'Bearer error="invalid_client"',
    }
    assert body == {"error": "invalid_client"}
    assert status_code == 401

    validator.save_token.assert_not_called()


def test_invalid_grant_type_error():
    validator = mock.MagicMock()
    request: common.Request = create_request()
    request.client = mock.Mock()

    request.grant_type = "not_device_code"

    auth = DeviceCodeGrant(validator)
    bearer = BearerToken(validator)

    headers, body, status_code = auth.create_token_response(request, bearer)
    body = json.loads(body)

    assert headers == {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
        "Pragma": "no-cache",
    }
    assert body == {"error": "unsupported_grant_type"}
    assert status_code == 400

    validator.save_token.assert_not_called()


def test_duplicate_params_error():
    validator = mock.MagicMock()
    request: common.Request = create_request(
        "client_id=123&scope=openid&scope=openid"
    )
    request.client = mock.Mock()

    auth = DeviceCodeGrant(validator)
    bearer = BearerToken(validator)

    headers, body, status_code = auth.create_token_response(request, bearer)
    body = json.loads(body)

    assert headers == {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
        "Pragma": "no-cache",
    }
    assert body == {"error": "invalid_request", "error_description": "Duplicate scope parameter."}
    assert status_code == 400

    validator.save_token.assert_not_called()
