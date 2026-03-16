from unittest import TestCase
from unittest.mock import MagicMock, patch

from webauthn.helpers.cose import COSEAlgorithmIdentifier
from webauthn.helpers.structs import (
    AttestationConveyancePreference,
    AuthenticatorAttachment,
    AuthenticatorSelectionCriteria,
    PublicKeyCredentialDescriptor,
    PublicKeyCredentialParameters,
    PublicKeyCredentialRpEntity,
    PublicKeyCredentialUserEntity,
    ResidentKeyRequirement,
)
from webauthn import generate_registration_options


class TestGenerateRegistrationOptions(TestCase):
    @patch("secrets.token_bytes")
    def test_generates_options_with_defaults(self, token_bytes_mock: MagicMock) -> None:
        token_bytes_mock.return_value = b"12345"
        user_id = "ABAV6QWPBEY9WOTOA1A4".encode("utf-8")

        options = generate_registration_options(
            rp_id="example.com",
            rp_name="Example Co",
            user_id=user_id,
            user_name="lee",
        )

        assert options.rp == PublicKeyCredentialRpEntity(
            id="example.com",
            name="Example Co",
        )
        assert options.challenge == b"12345"
        assert options.user == PublicKeyCredentialUserEntity(
            id=user_id,
            name="lee",
            display_name="lee",
        )
        assert options.pub_key_cred_params[0] == PublicKeyCredentialParameters(
            type="public-key",
            alg=COSEAlgorithmIdentifier.ECDSA_SHA_256,
        )
        assert options.timeout == 60000
        assert options.exclude_credentials == []
        assert options.authenticator_selection is None
        assert options.attestation == AttestationConveyancePreference.NONE

    def test_generates_options_with_custom_values(self) -> None:
        user_id = "ABAV6QWPBEY9WOTOA1A4".encode("utf-8")

        options = generate_registration_options(
            rp_id="example.com",
            rp_name="Example Co",
            user_id=user_id,
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
        )

        assert options.rp == PublicKeyCredentialRpEntity(id="example.com", name="Example Co")
        assert options.challenge == b"1234567890"
        assert options.user == PublicKeyCredentialUserEntity(
            id=user_id,
            name="lee",
            display_name="Lee",
        )
        assert options.pub_key_cred_params[0] == PublicKeyCredentialParameters(
            type="public-key",
            alg=COSEAlgorithmIdentifier.ECDSA_SHA_512,
        )
        assert options.timeout == 120000
        assert options.exclude_credentials == [PublicKeyCredentialDescriptor(id=b"1234567890")]
        assert options.authenticator_selection == AuthenticatorSelectionCriteria(
            authenticator_attachment=AuthenticatorAttachment.PLATFORM,
            resident_key=ResidentKeyRequirement.REQUIRED,
            require_resident_key=True,
        )
        assert options.attestation == AttestationConveyancePreference.DIRECT

    def test_raises_on_empty_rp_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "rp_id"):
            generate_registration_options(
                rp_id="",
                rp_name="Example Co",
                user_name="blah",
            )

    def test_raises_on_empty_rp_name(self) -> None:
        with self.assertRaisesRegex(ValueError, "rp_name"):
            generate_registration_options(
                rp_id="example.com",
                rp_name="",
                user_name="blah",
            )

    @patch("secrets.token_bytes")
    def test_generated_random_id_on_empty_user_id(self, token_bytes_mock: MagicMock) -> None:
        token_bytes_mock.return_value = bytes([1, 2, 3, 4])

        options = generate_registration_options(
            rp_id="example.com",
            rp_name="Example Co",
            user_name="blah",
            user_id=None,
        )

        self.assertEqual(options.user.id, bytes([1, 2, 3, 4]))

    def test_raises_on_non_bytes_user_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "user_id"):
            generate_registration_options(
                rp_id="example.com",
                rp_name="Example Co",
                user_name="hello",
                user_id="hello",  # type: ignore
            )

    def test_raises_on_empty_user_name(self) -> None:
        with self.assertRaisesRegex(ValueError, "user_name"):
            generate_registration_options(
                rp_id="example.com",
                rp_name="Example Co",
                user_name="",
            )
