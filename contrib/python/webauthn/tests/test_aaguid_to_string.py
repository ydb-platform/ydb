from unittest import TestCase

from webauthn.helpers import base64url_to_bytes
from webauthn.helpers.aaguid_to_string import aaguid_to_string


class TestAAGUIDToString(TestCase):
    def test_converts_bytes_to_uuid_format(self):
        aaguid_bytes = base64url_to_bytes("AAAAAAAAAAAAAAAAAAAAAA")

        output = aaguid_to_string(aaguid_bytes)

        assert output == "00000000-0000-0000-0000-000000000000"
