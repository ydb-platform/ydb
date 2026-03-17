import json
from unittest import TestCase

from webauthn.helpers.cose import COSEAlgorithmIdentifier
from webauthn.helpers.options_to_json import options_to_json
from webauthn.helpers.structs import (
    AttestationConveyancePreference,
    AuthenticatorAttachment,
    AuthenticatorSelectionCriteria,
    AuthenticatorTransport,
    PublicKeyCredentialDescriptor,
    PublicKeyCredentialHint,
    ResidentKeyRequirement,
    UserVerificationRequirement,
)
from webauthn import generate_registration_options, generate_authentication_options


class TestWebAuthnOptionsToJSON(TestCase):
    maxDiff = None

    def test_converts_registration_options_to_JSON(self) -> None:
        options = generate_registration_options(
            rp_id="example.com",
            rp_name="Example Co",
            user_id=bytes([1, 2, 3, 4]),
            user_name="lee",
            user_display_name="Lee",
            attestation=AttestationConveyancePreference.DIRECT,
            authenticator_selection=AuthenticatorSelectionCriteria(
                authenticator_attachment=AuthenticatorAttachment.PLATFORM,
                resident_key=ResidentKeyRequirement.REQUIRED,
            ),
            challenge=b"1234567890",
            exclude_credentials=[
                PublicKeyCredentialDescriptor(id=b"1234567890"),
            ],
            supported_pub_key_algs=[COSEAlgorithmIdentifier.ECDSA_SHA_512],
            timeout=120000,
            hints=[
                PublicKeyCredentialHint.SECURITY_KEY,
                PublicKeyCredentialHint.CLIENT_DEVICE,
                PublicKeyCredentialHint.HYBRID,
            ],
        )

        output = options_to_json(options)

        self.assertEqual(
            json.loads(output),
            {
                "rp": {"name": "Example Co", "id": "example.com"},
                "user": {
                    "id": "AQIDBA",
                    "name": "lee",
                    "displayName": "Lee",
                },
                "challenge": "MTIzNDU2Nzg5MA",
                "pubKeyCredParams": [{"type": "public-key", "alg": -36}],
                "timeout": 120000,
                "excludeCredentials": [{"type": "public-key", "id": "MTIzNDU2Nzg5MA"}],
                "authenticatorSelection": {
                    "authenticatorAttachment": "platform",
                    "residentKey": "required",
                    "requireResidentKey": True,
                    "userVerification": "preferred",
                },
                "attestation": "direct",
                "hints": ["security-key", "client-device", "hybrid"],
            },
        )

    def test_includes_optional_value_when_set(self) -> None:
        options = generate_registration_options(
            rp_id="example.com",
            rp_name="Example Co",
            user_name="lee",
            exclude_credentials=[
                PublicKeyCredentialDescriptor(
                    id=b"1234567890",
                    transports=[AuthenticatorTransport.USB],
                )
            ],
        )

        output = options_to_json(options)

        self.assertEqual(
            json.loads(output)["excludeCredentials"],
            [
                {
                    "id": "MTIzNDU2Nzg5MA",
                    "transports": ["usb"],
                    "type": "public-key",
                }
            ],
        )

    def test_converts_authentication_options_to_JSON(self) -> None:
        options = generate_authentication_options(
            rp_id="example.com",
            challenge=b"1234567890",
            allow_credentials=[
                PublicKeyCredentialDescriptor(id=b"1234567890"),
            ],
            timeout=120000,
            user_verification=UserVerificationRequirement.DISCOURAGED,
        )

        output = options_to_json(options)

        self.assertEqual(
            json.loads(output),
            {
                "rpId": "example.com",
                "challenge": "MTIzNDU2Nzg5MA",
                "allowCredentials": [{"type": "public-key", "id": "MTIzNDU2Nzg5MA"}],
                "timeout": 120000,
                "userVerification": "discouraged",
            },
        )

    def test_raises_on_bad_input(self) -> None:
        class FooClass:
            pass

        with self.assertRaisesRegex(TypeError, "not instance"):
            options_to_json(FooClass())  # type: ignore
