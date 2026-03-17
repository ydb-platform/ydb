from unittest import TestCase

from webauthn.helpers import base64url_to_bytes, options_to_json
from webauthn.helpers.exceptions import InvalidJSONStructure
from webauthn.helpers.structs import (
    AuthenticatorTransport,
    AuthenticatorAttachment,
    AttestationConveyancePreference,
    AuthenticatorSelectionCriteria,
    PublicKeyCredentialDescriptor,
    ResidentKeyRequirement,
    PublicKeyCredentialHint,
    PublicKeyCredentialRpEntity,
    PublicKeyCredentialUserEntity,
    UserVerificationRequirement,
    PublicKeyCredentialParameters,
)
from webauthn.helpers.cose import COSEAlgorithmIdentifier
from webauthn.helpers.parse_registration_options_json import parse_registration_options_json
from webauthn.registration.generate_registration_options import generate_registration_options


class TestParseRegistrationOptionsJSON(TestCase):
    maxDiff = None

    def test_returns_parsed_options_simple(self) -> None:
        parsed = parse_registration_options_json(
            {
                "rp": {"name": "Example Co", "id": "example.com"},
                "user": {
                    "id": "vEC5nFXSxpc_W68bX59JeD3c_-1XDJ5RblcWjY3Tx7RvfC0rkB19UWadf6wDEWG8T1ztksOYMim0sJIn6z_5tw",
                    "name": "bob",
                    "displayName": "bob",
                },
                "challenge": "scb_z5GweYijAT2ppsB0HAklsw96fPs_tOWh-myqkOeb9rcvhWBwUZ56J3t3eocgjHkS4Mf3XeXTOQc1ySvk5w",
                "pubKeyCredParams": [{"type": "public-key", "alg": -36}],
                "timeout": 60000,
                "excludeCredentials": [],
                "attestation": "none",
            }
        )

        self.assertEqual(
            parsed.rp, PublicKeyCredentialRpEntity(id="example.com", name="Example Co")
        )
        self.assertEqual(
            parsed.user,
            PublicKeyCredentialUserEntity(
                id=base64url_to_bytes(
                    "vEC5nFXSxpc_W68bX59JeD3c_-1XDJ5RblcWjY3Tx7RvfC0rkB19UWadf6wDEWG8T1ztksOYMim0sJIn6z_5tw"
                ),
                name="bob",
                display_name="bob",
            ),
        )
        self.assertEqual(parsed.attestation, AttestationConveyancePreference.NONE)
        self.assertEqual(parsed.authenticator_selection, None)
        self.assertEqual(
            parsed.challenge,
            base64url_to_bytes(
                "scb_z5GweYijAT2ppsB0HAklsw96fPs_tOWh-myqkOeb9rcvhWBwUZ56J3t3eocgjHkS4Mf3XeXTOQc1ySvk5w"
            ),
        )
        self.assertEqual(parsed.exclude_credentials, [])
        self.assertEqual(
            parsed.pub_key_cred_params,
            [
                PublicKeyCredentialParameters(
                    alg=COSEAlgorithmIdentifier.ECDSA_SHA_512,
                    type="public-key",
                )
            ],
        )
        self.assertEqual(parsed.timeout, 60000)

    def test_returns_parsed_options_full(self) -> None:
        parsed = parse_registration_options_json(
            {
                "rp": {"name": "Example Co", "id": "example.com"},
                "user": {"id": "AQIDBA", "name": "lee", "displayName": "Lee"},
                "challenge": "AQIDBAUGBwgJAA",
                "pubKeyCredParams": [
                    {"type": "public-key", "alg": -7},
                    {"type": "public-key", "alg": -8},
                    {"type": "public-key", "alg": -36},
                    {"type": "public-key", "alg": -37},
                    {"type": "public-key", "alg": -38},
                    {"type": "public-key", "alg": -39},
                    {"type": "public-key", "alg": -257},
                    {"type": "public-key", "alg": -258},
                    {"type": "public-key", "alg": -259},
                ],
                "timeout": 12000,
                "excludeCredentials": [
                    {
                        "id": "MTIzNDU2Nzg5MA",
                        "type": "public-key",
                        "transports": ["internal", "hybrid"],
                    }
                ],
                "authenticatorSelection": {
                    "authenticatorAttachment": "platform",
                    "residentKey": "required",
                    "requireResidentKey": True,
                    "userVerification": "discouraged",
                },
                "attestation": "direct",
                "hints": ["security-key", "client-device", "hybrid"],
            }
        )

        self.assertEqual(
            parsed.rp, PublicKeyCredentialRpEntity(id="example.com", name="Example Co")
        )
        self.assertEqual(
            parsed.user,
            PublicKeyCredentialUserEntity(
                id=base64url_to_bytes("AQIDBA"),
                name="lee",
                display_name="Lee",
            ),
        )
        self.assertEqual(parsed.attestation, AttestationConveyancePreference.DIRECT)
        self.assertEqual(
            parsed.authenticator_selection,
            AuthenticatorSelectionCriteria(
                authenticator_attachment=AuthenticatorAttachment.PLATFORM,
                resident_key=ResidentKeyRequirement.REQUIRED,
                require_resident_key=True,
                user_verification=UserVerificationRequirement.DISCOURAGED,
            ),
        )
        self.assertEqual(parsed.challenge, base64url_to_bytes("AQIDBAUGBwgJAA"))
        self.assertEqual(
            parsed.exclude_credentials,
            [
                PublicKeyCredentialDescriptor(
                    id=base64url_to_bytes("MTIzNDU2Nzg5MA"),
                    transports=[AuthenticatorTransport.INTERNAL, AuthenticatorTransport.HYBRID],
                )
            ],
        )
        self.assertEqual(
            parsed.pub_key_cred_params,
            [
                PublicKeyCredentialParameters(
                    alg=COSEAlgorithmIdentifier.ECDSA_SHA_256,
                    type="public-key",
                ),
                PublicKeyCredentialParameters(
                    alg=COSEAlgorithmIdentifier.EDDSA,
                    type="public-key",
                ),
                PublicKeyCredentialParameters(
                    alg=COSEAlgorithmIdentifier.ECDSA_SHA_512,
                    type="public-key",
                ),
                PublicKeyCredentialParameters(
                    alg=COSEAlgorithmIdentifier.RSASSA_PSS_SHA_256,
                    type="public-key",
                ),
                PublicKeyCredentialParameters(
                    alg=COSEAlgorithmIdentifier.RSASSA_PSS_SHA_384,
                    type="public-key",
                ),
                PublicKeyCredentialParameters(
                    alg=COSEAlgorithmIdentifier.RSASSA_PSS_SHA_512,
                    type="public-key",
                ),
                PublicKeyCredentialParameters(
                    alg=COSEAlgorithmIdentifier.RSASSA_PKCS1_v1_5_SHA_256,
                    type="public-key",
                ),
                PublicKeyCredentialParameters(
                    alg=COSEAlgorithmIdentifier.RSASSA_PKCS1_v1_5_SHA_384,
                    type="public-key",
                ),
                PublicKeyCredentialParameters(
                    alg=COSEAlgorithmIdentifier.RSASSA_PKCS1_v1_5_SHA_512,
                    type="public-key",
                ),
            ],
        )
        self.assertEqual(parsed.timeout, 12000)
        self.assertEqual(
            parsed.hints,
            [
                PublicKeyCredentialHint.SECURITY_KEY,
                PublicKeyCredentialHint.CLIENT_DEVICE,
                PublicKeyCredentialHint.HYBRID,
            ],
        )

    def test_supports_json_string(self) -> None:
        parsed = parse_registration_options_json(
            '{"rp": {"name": "Example Co", "id": "example.com"}, "user": {"id": "vEC5nFXSxpc_W68bX59JeD3c_-1XDJ5RblcWjY3Tx7RvfC0rkB19UWadf6wDEWG8T1ztksOYMim0sJIn6z_5tw", "name": "bob", "displayName": "bob"}, "challenge": "scb_z5GweYijAT2ppsB0HAklsw96fPs_tOWh-myqkOeb9rcvhWBwUZ56J3t3eocgjHkS4Mf3XeXTOQc1ySvk5w", "authenticatorSelection": {"userVerification": "required"}, "pubKeyCredParams": [{"type": "public-key", "alg": -36}], "timeout": 60000, "excludeCredentials": [], "attestation": "none"}'
        )

        self.assertEqual(
            parsed.rp, PublicKeyCredentialRpEntity(id="example.com", name="Example Co")
        )
        self.assertEqual(
            parsed.user,
            PublicKeyCredentialUserEntity(
                id=base64url_to_bytes(
                    "vEC5nFXSxpc_W68bX59JeD3c_-1XDJ5RblcWjY3Tx7RvfC0rkB19UWadf6wDEWG8T1ztksOYMim0sJIn6z_5tw"
                ),
                name="bob",
                display_name="bob",
            ),
        )
        self.assertEqual(parsed.attestation, AttestationConveyancePreference.NONE)
        self.assertEqual(
            parsed.authenticator_selection,
            AuthenticatorSelectionCriteria(user_verification=UserVerificationRequirement.REQUIRED),
        )
        self.assertEqual(
            parsed.challenge,
            base64url_to_bytes(
                "scb_z5GweYijAT2ppsB0HAklsw96fPs_tOWh-myqkOeb9rcvhWBwUZ56J3t3eocgjHkS4Mf3XeXTOQc1ySvk5w"
            ),
        )
        self.assertEqual(parsed.exclude_credentials, [])
        self.assertEqual(
            parsed.pub_key_cred_params,
            [
                PublicKeyCredentialParameters(
                    alg=COSEAlgorithmIdentifier.ECDSA_SHA_512,
                    type="public-key",
                )
            ],
        )
        self.assertEqual(parsed.timeout, 60000)

    def test_supports_options_to_json_output(self) -> None:
        """
        Test that output from `generate_registration_options()` that's fed directly into
        `options_to_json()` gets parsed back into the original options without any changes along
        the way.
        """
        opts = generate_registration_options(
            rp_id="example.com",
            rp_name="Example Co",
            user_id=bytes([1, 2, 3, 4]),
            user_name="lee",
            user_display_name="Lee",
            attestation=AttestationConveyancePreference.DIRECT,
            authenticator_selection=AuthenticatorSelectionCriteria(
                authenticator_attachment=AuthenticatorAttachment.PLATFORM,
                resident_key=ResidentKeyRequirement.REQUIRED,
                require_resident_key=True,
                user_verification=UserVerificationRequirement.DISCOURAGED,
            ),
            challenge=bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            exclude_credentials=[
                PublicKeyCredentialDescriptor(
                    id=b"1234567890",
                    transports=[AuthenticatorTransport.INTERNAL, AuthenticatorTransport.HYBRID],
                ),
            ],
            supported_pub_key_algs=[COSEAlgorithmIdentifier.ECDSA_SHA_512],
            timeout=12000,
            hints=[
                PublicKeyCredentialHint.CLIENT_DEVICE,
                PublicKeyCredentialHint.SECURITY_KEY,
                PublicKeyCredentialHint.HYBRID,
            ],
        )

        opts_json = options_to_json(opts)

        parsed_opts_json = parse_registration_options_json(opts_json)

        self.assertEqual(parsed_opts_json.rp, opts.rp)
        self.assertEqual(parsed_opts_json.user, opts.user)
        self.assertEqual(parsed_opts_json.attestation, opts.attestation)
        self.assertEqual(parsed_opts_json.authenticator_selection, opts.authenticator_selection)
        self.assertEqual(parsed_opts_json.challenge, opts.challenge)
        self.assertEqual(parsed_opts_json.exclude_credentials, opts.exclude_credentials)
        self.assertEqual(parsed_opts_json.pub_key_cred_params, opts.pub_key_cred_params)
        self.assertEqual(parsed_opts_json.timeout, opts.timeout)
        self.assertEqual(parsed_opts_json.hints, opts.hints)

    def test_raises_on_non_dict_json(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "not a JSON object"):
            parse_registration_options_json("[0]")

    def test_raises_on_missing_rp(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required rp"):
            parse_registration_options_json({})

    def test_raises_on_malformed_rp_id(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "id present but not string"):
            parse_registration_options_json(
                {
                    "rp": {"id": 0},
                }
            )

    def test_raises_on_missing_missing_rp_name(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required name"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com"},
                }
            )

    def test_raises_on_missing_user(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required user"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                }
            )

    def test_raises_on_missing_user_id(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required id"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {},
                }
            )

    def test_raises_on_missing_user_name(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required name"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa"},
                }
            )

    def test_raises_on_missing_user_display_name(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required displayName"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "Lee"},
                }
            )

    def test_raises_on_missing_attestation(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required attestation"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                }
            )

    def test_raises_on_unrecognized_attestation(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "attestation was invalid value"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "if_you_feel_like_it",
                }
            )

    def test_supports_optional_authenticator_selection(self) -> None:
        opts = parse_registration_options_json(
            {
                "rp": {"id": "example.com", "name": "Example Co"},
                "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                "attestation": "none",
                "challenge": "AAAA",
                "pubKeyCredParams": [{"type": "public-key", "alg": -7}],
            }
        )

        self.assertIsNone(opts.authenticator_selection)

    def test_raises_on_invalid_authenticator_selection_authenticator_attachment(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "attachment was invalid value"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "none",
                    "authenticatorSelection": {"authenticatorAttachment": "pcie"},
                }
            )

    def test_raises_on_invalid_authenticator_selection_resident_key(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "residentKey was invalid value"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "none",
                    "authenticatorSelection": {"residentKey": "yes_please"},
                }
            )

    def test_raises_on_invalid_authenticator_selection_require_resident_key(self) -> None:
        with self.assertRaisesRegex(
            InvalidJSONStructure, "requireResidentKey was invalid boolean"
        ):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "none",
                    "authenticatorSelection": {"requireResidentKey": "always"},
                }
            )

    def test_raises_on_invalid_authenticator_selection_user_verification(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "userVerification was invalid value"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "none",
                    "authenticatorSelection": {"userVerification": "when_inconvenient"},
                }
            )

    def test_raises_on_missing_challenge(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required challenge"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "none",
                }
            )

    def test_raises_on_missing_pub_key_cred_params(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "pubKeyCredParams was invalid value"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "none",
                    "challenge": "aaaa",
                }
            )

    def test_raises_on_pub_key_cred_params_entry_with_invalid_alg(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "entry had invalid alg"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "none",
                    "challenge": "aaaa",
                    "pubKeyCredParams": [{"alg": 0}],
                }
            )

    def test_supports_optional_exclude_credentials(self) -> None:
        opts = parse_registration_options_json(
            {
                "rp": {"id": "example.com", "name": "Example Co"},
                "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                "attestation": "none",
                "challenge": "aaaa",
                "pubKeyCredParams": [{"alg": -7}],
            }
        )

        self.assertIsNone(opts.exclude_credentials)

    def test_raises_on_exclude_credentials_entry_missing_id(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "missing required id"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "none",
                    "challenge": "aaaa",
                    "pubKeyCredParams": [{"alg": -7}],
                    "excludeCredentials": [{}],
                }
            )

    def test_raises_on_exclude_credentials_entry_invalid_transports(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "transports was not list"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "none",
                    "challenge": "aaaa",
                    "pubKeyCredParams": [{"alg": -7}],
                    "excludeCredentials": [{"id": "aaaa", "transports": ""}],
                }
            )

    def test_raises_on_exclude_credentials_entry_invalid_transports_entry(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "entry transports had invalid value"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "none",
                    "challenge": "aaaa",
                    "pubKeyCredParams": [{"alg": -7}],
                    "excludeCredentials": [{"id": "aaaa", "transports": ["pcie"]}],
                }
            )

    def test_supports_missing_timeout(self) -> None:
        opts = parse_registration_options_json(
            {
                "rp": {"id": "example.com", "name": "Example Co"},
                "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                "attestation": "none",
                "challenge": "aaaa",
                "pubKeyCredParams": [{"alg": -7}],
            }
        )

        self.assertIsNone(opts.timeout)

    def test_supports_empty_hints(self) -> None:
        opts = parse_registration_options_json(
            {
                "rp": {"id": "example.com", "name": "Example Co"},
                "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                "attestation": "none",
                "challenge": "aaaa",
                "pubKeyCredParams": [{"alg": -7}],
                "hints": [],
            }
        )

        self.assertEqual(opts.hints, [])

    def test_raises_on_invalid_hints_assignment(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "hints was invalid value"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "none",
                    "challenge": "aaaa",
                    "pubKeyCredParams": [{"alg": -7}],
                    "hints": "security-key",
                }
            )

    def test_raises_on_invalid_hints_entry(self) -> None:
        with self.assertRaisesRegex(InvalidJSONStructure, "hints had invalid value"):
            parse_registration_options_json(
                {
                    "rp": {"id": "example.com", "name": "Example Co"},
                    "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                    "attestation": "none",
                    "challenge": "aaaa",
                    "pubKeyCredParams": [{"alg": -7}],
                    "hints": ["platform"],
                }
            )

    def test_supports_optional_hints(self) -> None:
        opts = parse_registration_options_json(
            {
                "rp": {"id": "example.com", "name": "Example Co"},
                "user": {"id": "aaaa", "name": "lee", "displayName": "Lee"},
                "attestation": "none",
                "challenge": "aaaa",
                "pubKeyCredParams": [{"alg": -7}],
            }
        )

        self.assertIsNone(opts.hints)
