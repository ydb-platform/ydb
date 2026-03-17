import secrets
from typing import Optional, Tuple
from unittest import TestCase
import cbor2

from webauthn.helpers import parse_authenticator_data, bytes_to_base64url, parse_cbor
from webauthn.helpers.base64url_to_bytes import base64url_to_bytes


def _generate_auth_data(
    sign_count: int = 0,
    up: bool = True,
    uv: bool = False,
    be: bool = False,
    bs: bool = False,
    at: bool = False,
    ed: bool = False,
) -> Tuple[bytes, bytes, int, Optional[bytes], Optional[bytes], Optional[bytes]]:
    """A helper to generate auth_data

    Args:
        `sign_count`: How many times the authenticator has been used
        `up`: Whether user was present
        `uv`: Whether user was verified
        `be`: Whether credential can be backed up
        `bs`: Whether credential has been backed up
        `at`: Whether attested credential data is present
        `ed`: Whether extension data is present

    Returns:
        A `tuple` comprised of the following values:
            `bytes`: Authenticator data
            `bytes`: RP ID hash
            `int`: Sign count
            `Optional[bytes]`: AAGUID
            `Optional[bytes]`: Credential ID
            `Optional[bytes]`: Credential public key
    """
    rp_id_hash = secrets.token_bytes(32)

    flags = 0b00000000
    if up is True:
        flags = flags | 1 << 0
    if uv is True:
        flags = flags | 1 << 2
    if be is True:
        flags = flags | 1 << 3
    if bs is True:
        flags = flags | 1 << 4
    if at is True:
        flags = flags | 1 << 6
    if ed is True:
        flags = flags | 1 << 7

    bytes_to_join = [
        rp_id_hash,
        flags.to_bytes(1, byteorder="big"),
        sign_count.to_bytes(4, byteorder="big"),
    ]

    aaguid: Optional[bytes] = None
    credential_id: Optional[bytes] = None
    credential_public_key: Optional[bytes] = None
    if at is True:
        aaguid = secrets.token_bytes(16)
        credential_id = secrets.token_bytes(32)
        credential_public_key = secrets.token_bytes(32)

        attested_data = [
            aaguid,
            len(credential_id).to_bytes(2, byteorder="big"),
            credential_id,
            credential_public_key,
        ]

        bytes_to_join += attested_data

    auth_data = b"".join(bytes_to_join)

    return (
        auth_data,
        rp_id_hash,
        sign_count,
        aaguid,
        credential_id,
        credential_public_key,
    )


class TestWebAuthnParseAuthenticatorData(TestCase):
    def test_correctly_parses_simple(self) -> None:
        (auth_data, rp_id_hash, sign_count, _, _, _) = _generate_auth_data(10, up=True, uv=True)

        output = parse_authenticator_data(auth_data)

        assert output.rp_id_hash == rp_id_hash
        assert output.flags.up is True
        assert output.flags.uv is True
        assert output.flags.be is False
        assert output.flags.be is False
        assert output.flags.at is False
        assert output.flags.ed is False
        assert output.sign_count == sign_count

    def test_correctly_parses_attested_credential_data(self) -> None:
        auth_data = base64url_to_bytes(
            "SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2NBAAAAJch83ZdWwUm4niTLNjZU81AAIHa7Ksm5br3hAh3UjxP9-4rqu8BEsD-7SZ2xWe1_yHv6pAEDAzkBACBZAQDcxA7Ehs9goWB2Hbl6e9v-aUub9rvy2M7Hkvf-iCzMGE63e3sCEW5Ru33KNy4um46s9jalcBHtZgtEnyeRoQvszis-ws5o4Da0vQfuzlpBmjWT1dV6LuP-vs9wrfObW4jlA5bKEIhv63-jAxOtdXGVzo75PxBlqxrmrr5IR9n8Fw7clwRsDkjgRHaNcQVbwq_qdNwU5H3hZKu9szTwBS5NGRq01EaDF2014YSTFjwtAmZ3PU1tcO_QD2U2zg6eB5grfWDeAJtRE8cbndDWc8aLL0aeC37Q36-TVsGe6AhBgHEw6eO3I3NW5r9v_26CqMPBDwmEundeq1iGyKfMloobIUMBAAE"
        )

        output = parse_authenticator_data(auth_data)

        cred_data = output.attested_credential_data
        self.assertIsNotNone(cred_data)
        assert cred_data  # Make mypy happy
        self.assertEqual(
            cred_data.aaguid,
            base64url_to_bytes("yHzdl1bBSbieJMs2NlTzUA"),
        )
        self.assertEqual(
            cred_data.credential_id,
            base64url_to_bytes("drsqybluveECHdSPE_37iuq7wESwP7tJnbFZ7X_Ie_o"),
        )
        self.assertEqual(
            cred_data.credential_public_key,
            base64url_to_bytes(
                "pAEDAzkBACBZAQDcxA7Ehs9goWB2Hbl6e9v-aUub9rvy2M7Hkvf-iCzMGE63e3sCEW5Ru33KNy4um46s9jalcBHtZgtEnyeRoQvszis-ws5o4Da0vQfuzlpBmjWT1dV6LuP-vs9wrfObW4jlA5bKEIhv63-jAxOtdXGVzo75PxBlqxrmrr5IR9n8Fw7clwRsDkjgRHaNcQVbwq_qdNwU5H3hZKu9szTwBS5NGRq01EaDF2014YSTFjwtAmZ3PU1tcO_QD2U2zg6eB5grfWDeAJtRE8cbndDWc8aLL0aeC37Q36-TVsGe6AhBgHEw6eO3I3NW5r9v_26CqMPBDwmEundeq1iGyKfMloobIUMBAAE"
            ),
        )

    def test_parses_uv_false(self) -> None:
        auth_data = _generate_auth_data()[0]

        output = parse_authenticator_data(auth_data)

        self.assertTrue(output.flags.up)
        self.assertFalse(output.flags.uv)

    def test_parses_attested_credential_data_and_extension_data(self) -> None:
        auth_data = bytes.fromhex(
            "50569158be61d7a1ba084f80e45e938fd326e0a8dff07b37036e6c82303ae26bc1000004377b3024675546afcb92e4495c8a1e193f00dca30058b8d74f6bd74de90baeb34afb51e3578e1ac4ca9f79a7f88473d8254d5762ca82d68f3bf63f49e9b284caab4d45d6f9bb468d0c1b7f0f727378c1db8adb4802cb7c5ad9c5eb905bf0ba03f79bd1f04d63765452d49c4087acfad340516dc892eafd87d498ae9e6fd6f06a3f423108ebdc032d93e82fdd6deacc1b638fd56838a482f01232ad01e266e016a50b8121816997a167f41139900fe46094b8ef30aad14ee08cc457366a033bb4a0554dcf9c9589f9622d4f84481541014c870291c87d7a3bbe3d8b07eb02509de5721e3f728aa5eac41e9c5af02869a4010103272006215820e613b86a8d4ebae24e84a0270b6773f7bb30d1d59f5ec379910ebe7c87714274a16b6372656450726f7465637401"
        )
        output = parse_authenticator_data(auth_data)

        cred_data = output.attested_credential_data
        self.assertIsNotNone(cred_data)
        assert cred_data  # Make mypy happy
        self.assertEqual(
            bytes_to_base64url(cred_data.credential_public_key),
            "pAEBAycgBiFYIOYTuGqNTrriToSgJwtnc_e7MNHVn17DeZEOvnyHcUJ0",
        )

        extensions = output.extensions
        self.assertIsNotNone(extensions)
        assert extensions  # Make mypy happy

        parsed_extensions = parse_cbor(extensions)
        self.assertEqual(parsed_extensions, {"credProtect": 1})

    def test_parses_only_extension_data(self) -> None:
        # Pulled from Conformance Testing suite
        auth_data = base64url_to_bytes(
            "SZYN5YgOjGh0NBcPZHZgW4_krrmihjLHmVzzuoMdl2OBAAAAjaFxZXhhbXBsZS5leHRlbnNpb254dlRoaXMgaXMgYW4gZXhhbXBsZSBleHRlbnNpb24hIElmIHlvdSByZWFkIHRoaXMgbWVzc2FnZSwgeW91IHByb2JhYmx5IHN1Y2Nlc3NmdWxseSBwYXNzaW5nIGNvbmZvcm1hbmNlIHRlc3RzLiBHb29kIGpvYiE"
        )

        output = parse_authenticator_data(auth_data)

        extensions = output.extensions
        self.assertIsNotNone(extensions)
        assert extensions  # Make mypy happy
        parsed_extensions = parse_cbor(extensions)
        self.assertEqual(
            parsed_extensions,
            {
                "example.extension": "This is an example extension! If you read this message, you probably successfully passing conformance tests. Good job!",
            },
        )

    def test_parses_backup_state_flags(self) -> None:
        (auth_data, _, _, _, _, _) = _generate_auth_data(be=True, bs=True)

        output = parse_authenticator_data(auth_data)

        assert output.flags.be is True
        assert output.flags.be is True

    def test_parses_bad_eddsa_auth_data(self) -> None:
        """
        Help out particular YubiKeys that incorrectly CBOR-encode authData when they use Ed25519
        for their public key.

        See https://github.com/duo-labs/py_webauthn/issues/160
        """
        auth_data = bytearray.fromhex(
            "16B02DFBC3D4CCA37EBC2F6516659B12210DB9E1018AB9F13A9690638EA6FDA845000000012FC0579F811347EAB116BB5A8DB9202A0080E82FE6BDE300E4ECC93E0016448AD00FA6F28A011A6F87FF7B0CFCA499BEAF83344C3660B5ECABF72A3B2838A0CC7D87D3FA58292B53449CFF13AD69732D7521649D365CCBC5D0A0FA4B4E09EAE99537261F2F44093F8F4FD4CF5796E0FE58FF0615FFC5882836BBE7B99B08BE2986721C1C5A6AC7F32D3220D9B34D8DEE2FC9A301634F4B5003272067456432353531392198201618F6185918FA182E141875183A18841718521874187A18C51318D918C51883182D18ED181818EA188F182E187407185E18F41518CC18C9186D"
        )

        output = parse_authenticator_data(auth_data)

        cred_data = output.attested_credential_data
        self.assertIsNotNone(cred_data)
        assert cred_data  # Make mypy happy

        self.assertEqual(
            cred_data.credential_id.hex(),
            "e82fe6bde300e4ecc93e0016448ad00fa6f28a011a6f87ff7b0cfca499beaf83344c3660b5ecabf72a3b2838a0cc7d87d3fa58292b53449cff13ad69732d7521649d365ccbc5d0a0fa4b4e09eae99537261f2f44093f8f4fd4cf5796e0fe58ff0615ffc5882836bbe7b99b08be2986721c1c5a6ac7f32d3220d9b34d8dee2fc9",
        )
        self.assertEqual(
            cred_data.credential_public_key.hex(),
            "a401634f4b5003272067456432353531392198201618f6185918fa182e141875183a18841718521874187a18c51318d918c51883182d18ed181818ea188f182e187407185e18f41518cc18c9186d",
        )
