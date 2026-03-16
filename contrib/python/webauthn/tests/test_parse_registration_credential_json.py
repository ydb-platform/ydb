from unittest import TestCase

from webauthn.helpers import base64url_to_bytes
from webauthn.helpers.exceptions import InvalidJSONStructure, InvalidRegistrationResponse
from webauthn.helpers.structs import AuthenticatorTransport, AuthenticatorAttachment
from webauthn.helpers.parse_registration_credential_json import parse_registration_credential_json


class TestParseRegistrationCredentialJSON(TestCase):
    def test_raises_on_non_dict_json(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "not a JSON object"):
            parse_registration_credential_json("[0]")

    def test_raises_on_missing_id(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required id"):
            parse_registration_credential_json({})

    def test_raises_on_missing_raw_id(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required rawId"):
            parse_registration_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                }
            )

    def test_raises_on_missing_response(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required response"):
            parse_registration_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                }
            )

    def test_raises_on_missing_client_data_json(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required clientDataJSON"):
            parse_registration_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {},
                }
            )

    def test_raises_on_missing_attestation_object(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required attestationObject"):
            parse_registration_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {
                        "clientDataJSON": "...",
                    },
                }
            )

    def test_validates_credential_type(self) -> None:
        parsed = parse_registration_credential_json(
            {
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "attestationObject": "...",
                    "clientDataJSON": "...",
                },
                "type": "public-key",
            }
        )

    def test_raises_on_invalid_credential_type(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "unexpected type"):
            parse_registration_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {
                        "attestationObject": "...",
                        "clientDataJSON": "...",
                    },
                    "type": "not-a-public-key",
                }
            )

    def test_parses_transports(self) -> None:
        parsed = parse_registration_credential_json(
            {
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "attestationObject": "o2NmbXRkbm9uZWdhdHRTdG10oGhhdXRoRGF0YVikdKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvBBAAAAAK3OAAI1vMYKZIsLJfHwVQMAIFH-jHEbNNz948Ezg2yqJF5T6_kME1M0b-NZf85uvdvwpQECAyYgASFYIOmR1v1KhciLZLM_DDxy67MDa3J1vsiQWyzl20P0sy6fIlggita-HvZVinRVURaWCr-GJDm9iQ-Z1f5WfRhOA3CwZcU",
                    "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiUENYcURTMDFJbkRTeFAwbDhCMnVDcWxoR1BseEw4VHJSeDdpbHpjczIwNHplTklvMlJ0U3RkbFVSMWhfaW5WQzVPYkNjOElNT1JSYl9jWWNJMDNNeFEiLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlLCJvdGhlcl9rZXlzX2Nhbl9iZV9hZGRlZF9oZXJlIjoiZG8gbm90IGNvbXBhcmUgY2xpZW50RGF0YUpTT04gYWdhaW5zdCBhIHRlbXBsYXRlLiBTZWUgaHR0cHM6Ly9nb28uZ2wveWFiUGV4In0",
                    "transports": ["internal", "hybrid"],
                },
                "type": "public-key",
            }
        )

        self.assertEqual(
            parsed.response.transports,
            [AuthenticatorTransport.INTERNAL, AuthenticatorTransport.HYBRID],
        )

    def test_handles_missing_transports(self) -> None:
        parsed = parse_registration_credential_json(
            {
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "attestationObject": "o2NmbXRkbm9uZWdhdHRTdG10oGhhdXRoRGF0YVikdKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvBBAAAAAK3OAAI1vMYKZIsLJfHwVQMAIFH-jHEbNNz948Ezg2yqJF5T6_kME1M0b-NZf85uvdvwpQECAyYgASFYIOmR1v1KhciLZLM_DDxy67MDa3J1vsiQWyzl20P0sy6fIlggita-HvZVinRVURaWCr-GJDm9iQ-Z1f5WfRhOA3CwZcU",
                    "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiUENYcURTMDFJbkRTeFAwbDhCMnVDcWxoR1BseEw4VHJSeDdpbHpjczIwNHplTklvMlJ0U3RkbFVSMWhfaW5WQzVPYkNjOElNT1JSYl9jWWNJMDNNeFEiLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlLCJvdGhlcl9rZXlzX2Nhbl9iZV9hZGRlZF9oZXJlIjoiZG8gbm90IGNvbXBhcmUgY2xpZW50RGF0YUpTT04gYWdhaW5zdCBhIHRlbXBsYXRlLiBTZWUgaHR0cHM6Ly9nb28uZ2wveWFiUGV4In0",
                },
                "type": "public-key",
            }
        )

        self.assertIsNone(parsed.response.transports)

    def test_ignores_non_list_transports(self) -> None:
        parsed = parse_registration_credential_json(
            {
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "attestationObject": "o2NmbXRkbm9uZWdhdHRTdG10oGhhdXRoRGF0YVikdKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvBBAAAAAK3OAAI1vMYKZIsLJfHwVQMAIFH-jHEbNNz948Ezg2yqJF5T6_kME1M0b-NZf85uvdvwpQECAyYgASFYIOmR1v1KhciLZLM_DDxy67MDa3J1vsiQWyzl20P0sy6fIlggita-HvZVinRVURaWCr-GJDm9iQ-Z1f5WfRhOA3CwZcU",
                    "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiUENYcURTMDFJbkRTeFAwbDhCMnVDcWxoR1BseEw4VHJSeDdpbHpjczIwNHplTklvMlJ0U3RkbFVSMWhfaW5WQzVPYkNjOElNT1JSYl9jWWNJMDNNeFEiLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlLCJvdGhlcl9rZXlzX2Nhbl9iZV9hZGRlZF9oZXJlIjoiZG8gbm90IGNvbXBhcmUgY2xpZW50RGF0YUpTT04gYWdhaW5zdCBhIHRlbXBsYXRlLiBTZWUgaHR0cHM6Ly9nb28uZ2wveWFiUGV4In0",
                    # Pretend someone got clever on the front end
                    "transports": "usb|nfc|ble",
                },
                "type": "public-key",
            }
        )

        self.assertIsNone(parsed.response.transports)

    def test_handles_authenticator_attachment(self) -> None:
        parsed = parse_registration_credential_json(
            {
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "attestationObject": "o2NmbXRkbm9uZWdhdHRTdG10oGhhdXRoRGF0YVikdKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvBBAAAAAK3OAAI1vMYKZIsLJfHwVQMAIFH-jHEbNNz948Ezg2yqJF5T6_kME1M0b-NZf85uvdvwpQECAyYgASFYIOmR1v1KhciLZLM_DDxy67MDa3J1vsiQWyzl20P0sy6fIlggita-HvZVinRVURaWCr-GJDm9iQ-Z1f5WfRhOA3CwZcU",
                    "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiUENYcURTMDFJbkRTeFAwbDhCMnVDcWxoR1BseEw4VHJSeDdpbHpjczIwNHplTklvMlJ0U3RkbFVSMWhfaW5WQzVPYkNjOElNT1JSYl9jWWNJMDNNeFEiLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlLCJvdGhlcl9rZXlzX2Nhbl9iZV9hZGRlZF9oZXJlIjoiZG8gbm90IGNvbXBhcmUgY2xpZW50RGF0YUpTT04gYWdhaW5zdCBhIHRlbXBsYXRlLiBTZWUgaHR0cHM6Ly9nb28uZ2wveWFiUGV4In0",
                },
                "authenticatorAttachment": "platform",
                "type": "public-key",
            }
        )

        self.assertEqual(parsed.authenticator_attachment, AuthenticatorAttachment.PLATFORM)

    def test_handles_bad_authenticator_attachment(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "unexpected authenticatorAttachment"):
            parse_registration_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {
                        "attestationObject": "o2NmbXRkbm9uZWdhdHRTdG10oGhhdXRoRGF0YVikdKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvBBAAAAAK3OAAI1vMYKZIsLJfHwVQMAIFH-jHEbNNz948Ezg2yqJF5T6_kME1M0b-NZf85uvdvwpQECAyYgASFYIOmR1v1KhciLZLM_DDxy67MDa3J1vsiQWyzl20P0sy6fIlggita-HvZVinRVURaWCr-GJDm9iQ-Z1f5WfRhOA3CwZcU",
                        "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiUENYcURTMDFJbkRTeFAwbDhCMnVDcWxoR1BseEw4VHJSeDdpbHpjczIwNHplTklvMlJ0U3RkbFVSMWhfaW5WQzVPYkNjOElNT1JSYl9jWWNJMDNNeFEiLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlLCJvdGhlcl9rZXlzX2Nhbl9iZV9hZGRlZF9oZXJlIjoiZG8gbm90IGNvbXBhcmUgY2xpZW50RGF0YUpTT04gYWdhaW5zdCBhIHRlbXBsYXRlLiBTZWUgaHR0cHM6Ly9nb28uZ2wveWFiUGV4In0",
                    },
                    "authenticatorAttachment": "badValue",
                    "type": "public-key",
                }
            )

    def test_raises_on_non_base64url_raw_id(self) -> None:
        with self.assertRaisesRegex(
            InvalidRegistrationResponse, "Could not parse registration credential"
        ):
            parse_registration_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "baddd",
                    "response": {
                        "attestationObject": "...",
                        "clientDataJSON": "...",
                    },
                    "type": "public-key",
                }
            )

    def test_raises_on_non_base64url_attestation_object(self) -> None:
        with self.assertRaisesRegex(
            InvalidRegistrationResponse, "Could not parse registration credential"
        ):
            parse_registration_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {
                        "attestationObject": "baddd",
                        "clientDataJSON": "...",
                    },
                    "type": "public-key",
                }
            )

    def test_raises_on_non_base64url_client_data_json(self) -> None:
        with self.assertRaisesRegex(
            InvalidRegistrationResponse, "Could not parse registration credential"
        ):
            parse_registration_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {
                        "attestationObject": "...",
                        "clientDataJSON": "baddd",
                    },
                    "type": "public-key",
                }
            )

    def test_success_from_dict(self) -> None:
        # A bit more complex than a basic response, but it should get parsed all the same
        parsed = parse_registration_credential_json(
            {
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "attestationObject": "o2NmbXRkbm9uZWdhdHRTdG10oGhhdXRoRGF0YVikdKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvBBAAAAAK3OAAI1vMYKZIsLJfHwVQMAIFH-jHEbNNz948Ezg2yqJF5T6_kME1M0b-NZf85uvdvwpQECAyYgASFYIOmR1v1KhciLZLM_DDxy67MDa3J1vsiQWyzl20P0sy6fIlggita-HvZVinRVURaWCr-GJDm9iQ-Z1f5WfRhOA3CwZcU",
                    "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiUENYcURTMDFJbkRTeFAwbDhCMnVDcWxoR1BseEw4VHJSeDdpbHpjczIwNHplTklvMlJ0U3RkbFVSMWhfaW5WQzVPYkNjOElNT1JSYl9jWWNJMDNNeFEiLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlLCJvdGhlcl9rZXlzX2Nhbl9iZV9hZGRlZF9oZXJlIjoiZG8gbm90IGNvbXBhcmUgY2xpZW50RGF0YUpTT04gYWdhaW5zdCBhIHRlbXBsYXRlLiBTZWUgaHR0cHM6Ly9nb28uZ2wveWFiUGV4In0",
                    "transports": ["internal"],
                    "publicKeyAlgorithm": -7,
                    "publicKey": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE6ZHW_UqFyItksz8MPHLrswNrcnW-yJBbLOXbQ_SzLp-K1r4e9lWKdFVRFpYKv4YkOb2JD5nV_lZ9GE4DcLBlxQ",
                    "authenticatorData": "dKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvBBAAAAAK3OAAI1vMYKZIsLJfHwVQMAIFH-jHEbNNz948Ezg2yqJF5T6_kME1M0b-NZf85uvdvwpQECAyYgASFYIOmR1v1KhciLZLM_DDxy67MDa3J1vsiQWyzl20P0sy6fIlggita-HvZVinRVURaWCr-GJDm9iQ-Z1f5WfRhOA3CwZcU",
                },
                "type": "public-key",
                "clientExtensionResults": {"credProps": {"rk": True}},
                "authenticatorAttachment": "platform",
            }
        )

        self.assertEqual(parsed.id, "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A")
        self.assertEqual(
            parsed.raw_id,
            base64url_to_bytes("Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A"),
        )
        self.assertEqual(
            parsed.response.attestation_object,
            base64url_to_bytes(
                "o2NmbXRkbm9uZWdhdHRTdG10oGhhdXRoRGF0YVikdKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvBBAAAAAK3OAAI1vMYKZIsLJfHwVQMAIFH-jHEbNNz948Ezg2yqJF5T6_kME1M0b-NZf85uvdvwpQECAyYgASFYIOmR1v1KhciLZLM_DDxy67MDa3J1vsiQWyzl20P0sy6fIlggita-HvZVinRVURaWCr-GJDm9iQ-Z1f5WfRhOA3CwZcU"
            ),
        )
        self.assertEqual(
            parsed.response.client_data_json,
            base64url_to_bytes(
                "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiUENYcURTMDFJbkRTeFAwbDhCMnVDcWxoR1BseEw4VHJSeDdpbHpjczIwNHplTklvMlJ0U3RkbFVSMWhfaW5WQzVPYkNjOElNT1JSYl9jWWNJMDNNeFEiLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlLCJvdGhlcl9rZXlzX2Nhbl9iZV9hZGRlZF9oZXJlIjoiZG8gbm90IGNvbXBhcmUgY2xpZW50RGF0YUpTT04gYWdhaW5zdCBhIHRlbXBsYXRlLiBTZWUgaHR0cHM6Ly9nb28uZ2wveWFiUGV4In0"
            ),
        )
        self.assertEqual(
            parsed.response.transports,
            [AuthenticatorTransport.INTERNAL],
        )
        self.assertEqual(parsed.type, "public-key")
        self.assertEqual(parsed.authenticator_attachment, AuthenticatorAttachment.PLATFORM)

    def test_success_from_str(self) -> None:
        # Same dict as above, just stringified
        parsed = parse_registration_credential_json(
            """{
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "attestationObject": "o2NmbXRkbm9uZWdhdHRTdG10oGhhdXRoRGF0YVikdKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvBBAAAAAK3OAAI1vMYKZIsLJfHwVQMAIFH-jHEbNNz948Ezg2yqJF5T6_kME1M0b-NZf85uvdvwpQECAyYgASFYIOmR1v1KhciLZLM_DDxy67MDa3J1vsiQWyzl20P0sy6fIlggita-HvZVinRVURaWCr-GJDm9iQ-Z1f5WfRhOA3CwZcU",
                    "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiUENYcURTMDFJbkRTeFAwbDhCMnVDcWxoR1BseEw4VHJSeDdpbHpjczIwNHplTklvMlJ0U3RkbFVSMWhfaW5WQzVPYkNjOElNT1JSYl9jWWNJMDNNeFEiLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlLCJvdGhlcl9rZXlzX2Nhbl9iZV9hZGRlZF9oZXJlIjoiZG8gbm90IGNvbXBhcmUgY2xpZW50RGF0YUpTT04gYWdhaW5zdCBhIHRlbXBsYXRlLiBTZWUgaHR0cHM6Ly9nb28uZ2wveWFiUGV4In0",
                    "transports": ["internal"],
                    "publicKeyAlgorithm": -7,
                    "publicKey": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE6ZHW_UqFyItksz8MPHLrswNrcnW-yJBbLOXbQ_SzLp-K1r4e9lWKdFVRFpYKv4YkOb2JD5nV_lZ9GE4DcLBlxQ",
                    "authenticatorData": "dKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvBBAAAAAK3OAAI1vMYKZIsLJfHwVQMAIFH-jHEbNNz948Ezg2yqJF5T6_kME1M0b-NZf85uvdvwpQECAyYgASFYIOmR1v1KhciLZLM_DDxy67MDa3J1vsiQWyzl20P0sy6fIlggita-HvZVinRVURaWCr-GJDm9iQ-Z1f5WfRhOA3CwZcU"
                },
                "type": "public-key",
                "clientExtensionResults": {"credProps": {"rk": true}},
                "authenticatorAttachment": "platform"
            }"""
        )

        self.assertEqual(parsed.id, "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A")
        self.assertEqual(
            parsed.raw_id,
            base64url_to_bytes("Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A"),
        )
        self.assertEqual(
            parsed.response.attestation_object,
            base64url_to_bytes(
                "o2NmbXRkbm9uZWdhdHRTdG10oGhhdXRoRGF0YVikdKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvBBAAAAAK3OAAI1vMYKZIsLJfHwVQMAIFH-jHEbNNz948Ezg2yqJF5T6_kME1M0b-NZf85uvdvwpQECAyYgASFYIOmR1v1KhciLZLM_DDxy67MDa3J1vsiQWyzl20P0sy6fIlggita-HvZVinRVURaWCr-GJDm9iQ-Z1f5WfRhOA3CwZcU"
            ),
        )
        self.assertEqual(
            parsed.response.client_data_json,
            base64url_to_bytes(
                "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiUENYcURTMDFJbkRTeFAwbDhCMnVDcWxoR1BseEw4VHJSeDdpbHpjczIwNHplTklvMlJ0U3RkbFVSMWhfaW5WQzVPYkNjOElNT1JSYl9jWWNJMDNNeFEiLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlLCJvdGhlcl9rZXlzX2Nhbl9iZV9hZGRlZF9oZXJlIjoiZG8gbm90IGNvbXBhcmUgY2xpZW50RGF0YUpTT04gYWdhaW5zdCBhIHRlbXBsYXRlLiBTZWUgaHR0cHM6Ly9nb28uZ2wveWFiUGV4In0"
            ),
        )
        self.assertEqual(
            parsed.response.transports,
            [AuthenticatorTransport.INTERNAL],
        )
        self.assertEqual(parsed.type, "public-key")
        self.assertEqual(parsed.authenticator_attachment, AuthenticatorAttachment.PLATFORM)
