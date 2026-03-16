from unittest import TestCase

from webauthn.helpers.bytes_to_base64url import bytes_to_base64url


class TestWebAuthnBytesToBase64URL(TestCase):
    def test_converts_buffer_to_base64url_string(self) -> None:
        output = bytes_to_base64url(bytes([1, 2, 3, 4, 5]))

        assert output == "AQIDBAU"
