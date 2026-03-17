from email.mime import base
from unittest import TestCase

from webauthn.helpers import base64url_to_bytes, options_to_json
from webauthn.helpers.exceptions import InvalidJSONStructure
from webauthn.helpers.structs import (
    AuthenticatorTransport,
    PublicKeyCredentialDescriptor,
    UserVerificationRequirement,
)
from webauthn.helpers.parse_authentication_options_json import parse_authentication_options_json
from webauthn.authentication.generate_authentication_options import generate_authentication_options


class TestParseAuthenticationOptionsJSON(TestCase):
    maxDiff = None

    def test_returns_parsed_options_simple(self) -> None:
        opts = parse_authentication_options_json(
            {
                "challenge": "skxyhJljbw-ZQn-g1i87FBWeJ8_8B78oihdtSmVYaI2mArvHxI7WyTEW3gIeIRamDPlh8PJOK-ThcQc3xPNYTQ",
                "timeout": 60000,
                "rpId": "example.com",
                "allowCredentials": [],
                "userVerification": "preferred",
            }
        )

        self.assertEqual(
            opts.challenge,
            base64url_to_bytes(
                "skxyhJljbw-ZQn-g1i87FBWeJ8_8B78oihdtSmVYaI2mArvHxI7WyTEW3gIeIRamDPlh8PJOK-ThcQc3xPNYTQ"
            ),
        )
        self.assertEqual(opts.timeout, 60000)
        self.assertEqual(opts.rp_id, "example.com")
        self.assertEqual(opts.allow_credentials, [])
        self.assertEqual(opts.user_verification, UserVerificationRequirement.PREFERRED)

    def test_returns_parsed_options_full(self) -> None:
        opts = parse_authentication_options_json(
            {
                "challenge": "MTIzNDU2Nzg5MA",
                "timeout": 12000,
                "rpId": "example.com",
                "allowCredentials": [
                    {
                        "id": "MTIzNDU2Nzg5MA",
                        "type": "public-key",
                        "transports": ["internal", "hybrid"],
                    }
                ],
                "userVerification": "required",
            }
        )

        self.assertEqual(opts.challenge, base64url_to_bytes("MTIzNDU2Nzg5MA"))
        self.assertEqual(opts.timeout, 12000)
        self.assertEqual(opts.rp_id, "example.com")
        self.assertEqual(
            opts.allow_credentials,
            [
                PublicKeyCredentialDescriptor(
                    id=base64url_to_bytes("MTIzNDU2Nzg5MA"),
                    transports=[AuthenticatorTransport.INTERNAL, AuthenticatorTransport.HYBRID],
                )
            ],
        )
        self.assertEqual(opts.user_verification, UserVerificationRequirement.REQUIRED)

    def test_supports_json_string(self) -> None:
        opts = parse_authentication_options_json(
            '{"challenge": "skxyhJljbw-ZQn-g1i87FBWeJ8_8B78oihdtSmVYaI2mArvHxI7WyTEW3gIeIRamDPlh8PJOK-ThcQc3xPNYTQ", "timeout": 60000, "rpId": "example.com", "allowCredentials": [], "userVerification": "preferred"}'
        )

        self.assertEqual(
            opts.challenge,
            base64url_to_bytes(
                "skxyhJljbw-ZQn-g1i87FBWeJ8_8B78oihdtSmVYaI2mArvHxI7WyTEW3gIeIRamDPlh8PJOK-ThcQc3xPNYTQ"
            ),
        )
        self.assertEqual(opts.timeout, 60000)
        self.assertEqual(opts.rp_id, "example.com")
        self.assertEqual(opts.allow_credentials, [])
        self.assertEqual(opts.user_verification, UserVerificationRequirement.PREFERRED)

    def test_supports_options_to_json_output(self) -> None:
        """
        Test that output from `generate_authentication_options()` that's fed directly into
        `options_to_json()` gets parsed back into the original options without any changes along
        the way.
        """
        opts = generate_authentication_options(
            rp_id="example.com",
            challenge=b"1234567890",
            timeout=12000,
            allow_credentials=[
                PublicKeyCredentialDescriptor(
                    id=b"1234567890",
                    transports=[AuthenticatorTransport.INTERNAL, AuthenticatorTransport.HYBRID],
                )
            ],
            user_verification=UserVerificationRequirement.REQUIRED,
        )

        opts_json = options_to_json(opts)

        parsed_opts_json = parse_authentication_options_json(opts_json)

        self.assertEqual(parsed_opts_json.rp_id, opts.rp_id)
        self.assertEqual(parsed_opts_json.challenge, opts.challenge)
        self.assertEqual(parsed_opts_json.allow_credentials, opts.allow_credentials)
        self.assertEqual(parsed_opts_json.timeout, opts.timeout)
        self.assertEqual(parsed_opts_json.user_verification, opts.user_verification)

    def test_raises_on_non_dict_json(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "not a JSON object"):
            parse_authentication_options_json("[0]")

    def test_raises_on_missing_challenge(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required challenge"):
            parse_authentication_options_json({})

    def test_supports_optional_timeout(self) -> None:
        opts = parse_authentication_options_json(
            {
                "challenge": "aaa",
                "userVerification": "required",
            }
        )

        self.assertIsNone(opts.timeout)

    def test_supports_optional_rp_id(self) -> None:
        opts = parse_authentication_options_json(
            {
                "challenge": "aaa",
                "userVerification": "required",
            }
        )

        self.assertIsNone(opts.rp_id)

    def test_raises_on_missing_user_verification(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required userVerification"):
            parse_authentication_options_json(
                {
                    "challenge": "aaaa",
                }
            )

    def test_raises_on_invalid_user_verification(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "userVerification was invalid"):
            parse_authentication_options_json(
                {
                    "challenge": "aaaa",
                    "userVerification": "when_inconvenient",
                }
            )

    def test_supports_optional_allow_credentials(self) -> None:
        opts = parse_authentication_options_json(
            {
                "challenge": "aaa",
                "userVerification": "required",
            }
        )

        self.assertIsNone(opts.allow_credentials)

    def test_raises_on_allow_credentials_entry_missing_id(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required id"):
            parse_authentication_options_json(
                {
                    "challenge": "aaa",
                    "userVerification": "required",
                    "allowCredentials": [{}],
                }
            )

    def test_raises_on_allow_credentials_entry_invalid_transports(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "transports was not list"):
            parse_authentication_options_json(
                {
                    "challenge": "aaa",
                    "userVerification": "required",
                    "allowCredentials": [{"id": "aaaa", "transports": ""}],
                }
            )

    def test_raises_on_allow_credentials_entry_invalid_transports_entry(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "entry transports had invalid value"):
            parse_authentication_options_json(
                {
                    "challenge": "aaa",
                    "userVerification": "required",
                    "allowCredentials": [{"id": "aaaa", "transports": ["pcie"]}],
                }
            )
