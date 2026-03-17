from unittest import TestCase
from unittest.mock import MagicMock, patch

from webauthn import generate_authentication_options
from webauthn.helpers.structs import (
    PublicKeyCredentialDescriptor,
    UserVerificationRequirement,
)


class TestWebAuthnGenerateAttestationOptions(TestCase):
    @patch("secrets.token_bytes")
    def test_generates_options_with_defaults(self, token_bytes_mock: MagicMock) -> None:
        token_bytes_mock.return_value = b"12345"

        options = generate_authentication_options(rp_id="example.com")

        assert options.challenge == b"12345"
        assert options.timeout == 60000
        assert options.rp_id == "example.com"
        assert options.allow_credentials == []
        assert options.user_verification == UserVerificationRequirement.PREFERRED

    def test_generates_options_with_custom_values(self) -> None:
        options = generate_authentication_options(
            rp_id="example.com",
            allow_credentials=[
                PublicKeyCredentialDescriptor(id=b"12345"),
            ],
            challenge=b"this_is_a_challenge",
            timeout=12000,
            user_verification=UserVerificationRequirement.REQUIRED,
        )

        assert options.challenge == b"this_is_a_challenge"
        assert options.timeout == 12000
        assert options.rp_id == "example.com"
        assert options.allow_credentials == [
            PublicKeyCredentialDescriptor(id=b"12345"),
        ]
        assert options.user_verification == UserVerificationRequirement.REQUIRED

    def test_raises_on_empty_rp_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "rp_id"):
            generate_authentication_options(
                rp_id="",
            )
