import json
from unittest import TestCase

from webauthn.helpers import base64url_to_bytes, bytes_to_base64url
from webauthn.helpers.exceptions import InvalidJSONStructure
from webauthn.helpers.parse_client_data_json import parse_client_data_json
from webauthn.helpers.structs import TokenBindingStatus


class TestParseClientDataJSON(TestCase):
    def test_can_parse_attestation_client_data(self):
        client_data_bytes = base64url_to_bytes(
            "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoidkZHdmt6UFZzQ0JKOXVLMWxHT0ZucGl4NDF5clB1eFBITFlMdGJRckVYNjNqSlplYXNaVlBfY3lZZkExYktmelJJbk1vcG05VUJtNkNvS2FfakZ0MWciLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9",
        )
        expected_challenge = base64url_to_bytes(
            "vFGvkzPVsCBJ9uK1lGOFnpix41yrPuxPHLYLtbQrEX63jJZeasZVP_cyYfA1bKfzRInMopm9UBm6CoKa_jFt1g"
        )

        output = parse_client_data_json(client_data_bytes)

        assert output.type == "webauthn.create"
        assert output.challenge == expected_challenge
        assert output.origin == "http://localhost:5000"
        assert output.cross_origin is False
        assert output.token_binding is None

    def test_raises_exception_on_bad_json(self):
        client_data_bytes = b"not_real-JS0N"

        with self.assertRaisesRegex(
            InvalidJSONStructure,
            "Unable to decode",
        ):
            parse_client_data_json(client_data_bytes)

    def test_requires_type(self):
        client_data_str = json.dumps(
            {
                "challenge": bytes_to_base64url(b"challenge"),
                "origin": "http://localhost:5000",
            }
        )
        client_data_bytes = client_data_str.encode("utf-8")

        with self.assertRaisesRegex(
            InvalidJSONStructure,
            'missing required property "type"',
        ):
            parse_client_data_json(client_data_bytes)

    def test_requires_challenge(self):
        client_data_str = json.dumps(
            {"type": "webauthn.create", "origin": "http://localhost:5000"}
        )
        client_data_bytes = client_data_str.encode("utf-8")

        with self.assertRaisesRegex(
            InvalidJSONStructure,
            'missing required property "challenge"',
        ):
            parse_client_data_json(client_data_bytes)

    def test_requires_origin(self):
        client_data_str = json.dumps(
            {
                "type": "webauthn.create",
                "challenge": bytes_to_base64url(b"challenge"),
            }
        )
        client_data_bytes = client_data_str.encode("utf-8")

        with self.assertRaisesRegex(
            InvalidJSONStructure,
            'missing required property "origin"',
        ):
            parse_client_data_json(client_data_bytes)

    def test_omit_cross_origin_if_not_present(self):
        client_data_str = json.dumps(
            {
                "type": "webauthn.create",
                "challenge": bytes_to_base64url(b"challenge"),
                "origin": "http://localhost:5000",
            }
        )
        client_data_bytes = client_data_str.encode("utf-8")

        output = parse_client_data_json(client_data_bytes)

        assert output.cross_origin is None

    def test_omit_token_binding_if_not_present(self):
        client_data_str = json.dumps(
            {
                "type": "webauthn.create",
                "challenge": bytes_to_base64url(b"challenge"),
                "origin": "http://localhost:5000",
            }
        )
        client_data_bytes = client_data_str.encode("utf-8")

        output = parse_client_data_json(client_data_bytes)

        assert output.token_binding is None

    def test_include_token_binding_when_present(self):
        client_data_str = json.dumps(
            {
                "type": "webauthn.create",
                "challenge": bytes_to_base64url(b"challenge"),
                "origin": "http://localhost:5000",
                "tokenBinding": {"status": "present", "id": "someidhere"},
            }
        )
        client_data_bytes = client_data_str.encode("utf-8")

        output = parse_client_data_json(client_data_bytes)

        assert output.token_binding
        assert output.token_binding.status == TokenBindingStatus.PRESENT
        assert output.token_binding.id == "someidhere"

    def test_require_status_in_token_binding_when_present(self):
        client_data_str = json.dumps(
            {
                "type": "webauthn.create",
                "challenge": bytes_to_base64url(b"challenge"),
                "origin": "http://localhost:5000",
                "tokenBinding": {"id": "someidhere"},
            }
        )
        client_data_bytes = client_data_str.encode("utf-8")

        with self.assertRaises(InvalidJSONStructure) as context:
            parse_client_data_json(client_data_bytes)

        assert 'missing required property "status"' in str(context.exception)

    def test_omit_id_when_missing_from_token_binding(self):
        client_data_str = json.dumps(
            {
                "type": "webauthn.create",
                "challenge": bytes_to_base64url(b"challenge"),
                "origin": "http://localhost:5000",
                "tokenBinding": {
                    "status": "present",
                },
            }
        )
        client_data_bytes = client_data_str.encode("utf-8")

        output = parse_client_data_json(client_data_bytes)

        assert output.token_binding
        assert output.token_binding.id is None
