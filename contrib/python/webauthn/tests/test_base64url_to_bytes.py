from unittest import TestCase

from webauthn.helpers.base64url_to_bytes import base64url_to_bytes


class TestWebAuthnBase64URLToBytes(TestCase):
    def test_converts_base64url_string_to_buffer(self) -> None:
        output = base64url_to_bytes("AQIDBAU")

        assert output == bytes([1, 2, 3, 4, 5])
