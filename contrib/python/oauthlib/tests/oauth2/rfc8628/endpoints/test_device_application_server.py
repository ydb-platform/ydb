import json
from unittest import TestCase, mock

from oauthlib.common import Request, urlencode
from oauthlib.oauth2.rfc6749 import errors
from oauthlib.oauth2.rfc8628.endpoints.pre_configured import DeviceApplicationServer
from oauthlib.oauth2.rfc8628.request_validator import RequestValidator


def test_server_set_up_device_endpoint_instance_attributes_correctly():
    """
    Simple test that just instantiates DeviceApplicationServer
    and asserts the important attributes are present
    """
    validator = mock.MagicMock(spec=RequestValidator)
    validator.get_default_redirect_uri.return_value = None
    validator.get_code_challenge.return_value = None

    verification_uri = "test.com/device"
    verification_uri_complete = "test.com/device?user_code=123"
    device = DeviceApplicationServer(validator,  verification_uri=verification_uri, verification_uri_complete=verification_uri_complete)
    device_vars = vars(device)
    assert device_vars["_verification_uri_complete"] == "test.com/device?user_code=123"
    assert device_vars["_verification_uri"] == "test.com/device"
    assert device_vars["_expires_in"] == 1800
    assert device_vars["_interval"] == 5
