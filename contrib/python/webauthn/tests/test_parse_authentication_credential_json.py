from unittest import TestCase

from webauthn.helpers import base64url_to_bytes
from webauthn.helpers.exceptions import InvalidJSONStructure, InvalidAuthenticationResponse
from webauthn.helpers.structs import AuthenticatorTransport, AuthenticatorAttachment
from webauthn.helpers.parse_authentication_credential_json import (
    parse_authentication_credential_json,
)


class TestParseClientDataJSON(TestCase):
    def test_raises_on_non_dict_json(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "not a JSON object"):
            parse_authentication_credential_json("[0]")

    def test_raises_on_missing_id(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required id"):
            parse_authentication_credential_json({})

    def test_raises_on_missing_raw_id(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required rawId"):
            parse_authentication_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                }
            )

    def test_raises_on_missing_response(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required response"):
            parse_authentication_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                }
            )

    def test_raises_on_missing_client_data_json(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required clientDataJSON"):
            parse_authentication_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {},
                }
            )

    def test_raises_on_missing_authenticator_data(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required authenticatorData"):
            parse_authentication_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {
                        "clientDataJSON": "...",
                    },
                }
            )

    def test_raises_on_missing_signature(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required signature"):
            parse_authentication_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {
                        "authenticatorData": "...",
                        "clientDataJSON": "...",
                    },
                }
            )

    def test_validates_credential_type(self) -> None:
        parsed = parse_authentication_credential_json(
            {
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "authenticatorData": "...",
                    "clientDataJSON": "...",
                    "signature": "...",
                },
                "type": "public-key",
            }
        )

    def test_raises_on_invalid_credential_type(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "unexpected type"):
            parse_authentication_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {
                        "authenticatorData": "...",
                        "clientDataJSON": "...",
                        "signature": "...",
                    },
                    "type": "not-a-public-key",
                }
            )

    def test_handles_authenticator_attachment(self) -> None:
        parsed = parse_authentication_credential_json(
            {
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "authenticatorData": "dKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvABAAAAAA",
                    "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiSjlyUFpWWnFWODlUSW53bzV3cU11R3dlZjdET0pZRi1OVHlMQnhHV2pjZi16amFzOFRTUTlMbXI3em4wSmpkMTQyMU1sV0ItS2JYdEs5RW5sN19JM3ciLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlfQ",
                    "signature": "MEQCIBYTvMC-3pw88hoiYwdmPCHmPdz__tuhkFrfq-E03NvSAiBzelRNe6FCgsYL6_x6xmUlWM_ULmxRi6cX5iPZPiDrxA",
                },
                "authenticatorAttachment": "platform",
                "type": "public-key",
            }
        )

        self.assertEqual(parsed.authenticator_attachment, AuthenticatorAttachment.PLATFORM)

    def test_handles_bad_authenticator_attachment(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "unexpected authenticatorAttachment"):
            parse_authentication_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {
                        "authenticatorData": "dKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvABAAAAAA",
                        "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiSjlyUFpWWnFWODlUSW53bzV3cU11R3dlZjdET0pZRi1OVHlMQnhHV2pjZi16amFzOFRTUTlMbXI3em4wSmpkMTQyMU1sV0ItS2JYdEs5RW5sN19JM3ciLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlfQ",
                        "signature": "MEQCIBYTvMC-3pw88hoiYwdmPCHmPdz__tuhkFrfq-E03NvSAiBzelRNe6FCgsYL6_x6xmUlWM_ULmxRi6cX5iPZPiDrxA",
                    },
                    "authenticatorAttachment": "badValue",
                    "type": "public-key",
                }
            )

    def test_handles_user_handle(self) -> None:
        parsed = parse_authentication_credential_json(
            {
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "authenticatorData": "...",
                    "clientDataJSON": "...",
                    "signature": "...",
                    "userHandle": "bW1pbGxlcg",
                },
                "type": "public-key",
            }
        )

        self.assertEqual(parsed.response.user_handle, base64url_to_bytes("bW1pbGxlcg"))

    def test_handles_missing_user_handle(self) -> None:
        parsed = parse_authentication_credential_json(
            {
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "authenticatorData": "...",
                    "clientDataJSON": "...",
                    "signature": "...",
                },
                "type": "public-key",
            }
        )

        self.assertIsNone(parsed.response.user_handle)

    def test_raises_on_non_base64url_raw_id(self) -> None:
        with self.assertRaisesRegex(
            InvalidAuthenticationResponse, "Could not parse authentication credential"
        ):
            parse_authentication_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "baddd",
                    "response": {
                        "authenticatorData": "...",
                        "clientDataJSON": "...",
                        "signature": "...",
                    },
                    "type": "public-key",
                }
            )

    def test_raises_on_non_base64url_authenticator_data(self) -> None:
        with self.assertRaisesRegex(
            InvalidAuthenticationResponse, "Could not parse authentication credential"
        ):
            parse_authentication_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {
                        "authenticatorData": "baddd",
                        "clientDataJSON": "...",
                        "signature": "...",
                    },
                    "type": "public-key",
                }
            )

    def test_raises_on_non_base64url_client_data_json(self) -> None:
        with self.assertRaisesRegex(
            InvalidAuthenticationResponse, "Could not parse authentication credential"
        ):
            parse_authentication_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {
                        "authenticatorData": "...",
                        "clientDataJSON": "baddd",
                        "signature": "...",
                    },
                    "type": "public-key",
                }
            )

    def test_raises_on_non_base64url_signature(self) -> None:
        with self.assertRaisesRegex(
            InvalidAuthenticationResponse, "Could not parse authentication credential"
        ):
            parse_authentication_credential_json(
                {
                    "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                    "response": {
                        "authenticatorData": "...",
                        "clientDataJSON": "...",
                        "signature": "baddd",
                    },
                    "type": "public-key",
                }
            )

    def test_success_from_dict(self) -> None:
        # A bit more complex than a basic response, but it should get parsed all the same
        parsed = parse_authentication_credential_json(
            {
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "authenticatorData": "dKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvABAAAAAA",
                    "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiSjlyUFpWWnFWODlUSW53bzV3cU11R3dlZjdET0pZRi1OVHlMQnhHV2pjZi16amFzOFRTUTlMbXI3em4wSmpkMTQyMU1sV0ItS2JYdEs5RW5sN19JM3ciLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlfQ",
                    "signature": "MEQCIBYTvMC-3pw88hoiYwdmPCHmPdz__tuhkFrfq-E03NvSAiBzelRNe6FCgsYL6_x6xmUlWM_ULmxRi6cX5iPZPiDrxA",
                    "userHandle": "bW1pbGxlcg",
                },
                "type": "public-key",
                "clientExtensionResults": {},
                "authenticatorAttachment": "platform",
            }
        )

        self.assertEqual(parsed.id, "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A")
        self.assertEqual(
            parsed.raw_id,
            base64url_to_bytes("Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A"),
        )
        self.assertEqual(
            parsed.response.authenticator_data,
            base64url_to_bytes("dKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvABAAAAAA"),
        )
        self.assertEqual(
            parsed.response.client_data_json,
            base64url_to_bytes(
                "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiSjlyUFpWWnFWODlUSW53bzV3cU11R3dlZjdET0pZRi1OVHlMQnhHV2pjZi16amFzOFRTUTlMbXI3em4wSmpkMTQyMU1sV0ItS2JYdEs5RW5sN19JM3ciLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlfQ"
            ),
        )
        self.assertEqual(parsed.response.user_handle, base64url_to_bytes("bW1pbGxlcg"))
        self.assertEqual(parsed.type, "public-key")
        self.assertEqual(parsed.authenticator_attachment, AuthenticatorAttachment.PLATFORM)

    def test_success_from_str(self) -> None:
        # Same dict as above, just stringified
        parsed = parse_authentication_credential_json(
            """{
                "id": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "rawId": "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A",
                "response": {
                    "authenticatorData": "dKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvABAAAAAA",
                    "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiSjlyUFpWWnFWODlUSW53bzV3cU11R3dlZjdET0pZRi1OVHlMQnhHV2pjZi16amFzOFRTUTlMbXI3em4wSmpkMTQyMU1sV0ItS2JYdEs5RW5sN19JM3ciLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlfQ",
                    "signature": "MEQCIBYTvMC-3pw88hoiYwdmPCHmPdz__tuhkFrfq-E03NvSAiBzelRNe6FCgsYL6_x6xmUlWM_ULmxRi6cX5iPZPiDrxA",
                    "userHandle": "bW1pbGxlcg"
                },
                "type": "public-key",
                "clientExtensionResults": {},
                "authenticatorAttachment": "platform"
            }"""
        )

        self.assertEqual(parsed.id, "Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A")
        self.assertEqual(
            parsed.raw_id,
            base64url_to_bytes("Uf6McRs03P3jwTODbKokXlPr-QwTUzRv41l_zm692_A"),
        )
        self.assertEqual(
            parsed.response.authenticator_data,
            base64url_to_bytes("dKbqkhPJnC90siSSsyDPQCYqlMGpUKA5fyklC2CEHvABAAAAAA"),
        )
        self.assertEqual(
            parsed.response.client_data_json,
            base64url_to_bytes(
                "eyJ0eXBlIjoid2ViYXV0aG4uZ2V0IiwiY2hhbGxlbmdlIjoiSjlyUFpWWnFWODlUSW53bzV3cU11R3dlZjdET0pZRi1OVHlMQnhHV2pjZi16amFzOFRTUTlMbXI3em4wSmpkMTQyMU1sV0ItS2JYdEs5RW5sN19JM3ciLCJvcmlnaW4iOiJodHRwczovL3dlYmF1dGhuLmlvIiwiY3Jvc3NPcmlnaW4iOmZhbHNlfQ"
            ),
        )
        self.assertEqual(parsed.response.user_handle, base64url_to_bytes("bW1pbGxlcg"))
        self.assertEqual(parsed.type, "public-key")
        self.assertEqual(parsed.authenticator_attachment, AuthenticatorAttachment.PLATFORM)
