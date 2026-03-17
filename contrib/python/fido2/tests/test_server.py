import unittest

from fido2.server import Fido2Server, U2FFido2Server, verify_app_id
from fido2.webauthn import (
    CollectedClientData,
    PublicKeyCredentialRpEntity,
    UserVerificationRequirement,
    AttestedCredentialData,
    AuthenticatorData,
)
from fido2.utils import websafe_encode

from .test_ctap2 import _ATT_CRED_DATA, _CRED_ID
from .utils import U2FDevice


class TestAppId(unittest.TestCase):
    def test_valid_ids(self):
        self.assertTrue(
            verify_app_id("https://example.com", "https://register.example.com")
        )
        self.assertTrue(
            verify_app_id("https://example.com", "https://fido.example.com")
        )
        self.assertTrue(
            verify_app_id("https://example.com", "https://www.example.com:444")
        )

        self.assertTrue(
            verify_app_id(
                "https://companyA.hosting.example.com",
                "https://fido.companyA.hosting.example.com",
            )
        )
        self.assertTrue(
            verify_app_id(
                "https://companyA.hosting.example.com",
                "https://xyz.companyA.hosting.example.com",
            )
        )

    def test_invalid_ids(self):
        self.assertFalse(verify_app_id("https://example.com", "http://example.com"))
        self.assertFalse(verify_app_id("https://example.com", "http://www.example.com"))
        self.assertFalse(
            verify_app_id("https://example.com", "https://example-test.com")
        )

        self.assertFalse(
            verify_app_id(
                "https://companyA.hosting.example.com", "https://register.example.com"
            )
        )
        self.assertFalse(
            verify_app_id(
                "https://companyA.hosting.example.com",
                "https://companyB.hosting.example.com",
            )
        )

    def test_effective_tld_names(self):
        self.assertFalse(
            verify_app_id("https://appspot.com", "https://foo.appspot.com")
        )
        self.assertFalse(verify_app_id("https://co.uk", "https://example.co.uk"))


class TestPublicKeyCredentialRpEntity(unittest.TestCase):
    def test_id_hash(self):
        rp = PublicKeyCredentialRpEntity("Example", "example.com")
        rp_id_hash = (
            b"\xa3y\xa6\xf6\xee\xaf\xb9\xa5^7\x8c\x11\x804\xe2u\x1eh/"
            b"\xab\x9f-0\xab\x13\xd2\x12U\x86\xce\x19G"
        )
        self.assertEqual(rp.id_hash, rp_id_hash)


USER = {"id": b"user_id", "name": "A. User"}


class TestFido2Server(unittest.TestCase):
    def test_register_begin_rp(self):
        rp = PublicKeyCredentialRpEntity("Example", "example.com")
        server = Fido2Server(rp)

        request, state = server.register_begin(USER)

        self.assertEqual(
            request["publicKey"]["rp"], {"id": "example.com", "name": "Example"}
        )

    def test_register_begin_custom_challenge(self):
        rp = PublicKeyCredentialRpEntity("Example", "example.com")
        server = Fido2Server(rp)

        challenge = b"1234567890123456"
        request, state = server.register_begin(USER, challenge=challenge)

        self.assertEqual(request["publicKey"]["challenge"], websafe_encode(challenge))

    def test_register_begin_custom_challenge_too_short(self):
        rp = PublicKeyCredentialRpEntity("Example", "example.com")
        server = Fido2Server(rp)

        challenge = b"123456789012345"
        with self.assertRaises(ValueError):
            request, state = server.register_begin(USER, challenge=challenge)

    def test_authenticate_complete_invalid_signature(self):
        rp = PublicKeyCredentialRpEntity("Example", "example.com")
        server = Fido2Server(rp)

        state = {
            "challenge": "GAZPACHO!",
            "user_verification": UserVerificationRequirement.PREFERRED,
        }
        client_data = CollectedClientData.create(
            CollectedClientData.TYPE.GET,
            "GAZPACHO!",
            "https://example.com",
        )
        _AUTH_DATA = bytes.fromhex(
            "A379A6F6EEAFB9A55E378C118034E2751E682FAB9F2D30AB13D2125586CE1947010000001D"
        )
        with self.assertRaisesRegex(ValueError, "Invalid signature."):
            server.authenticate_complete(
                state,
                [AttestedCredentialData(_ATT_CRED_DATA)],
                _CRED_ID,
                client_data,
                AuthenticatorData(_AUTH_DATA),
                b"INVALID",
            )


class TestU2FFido2Server(unittest.TestCase):
    def test_u2f(self):
        rp = PublicKeyCredentialRpEntity("Example", "example.com")
        app_id = b"https://example.com"
        server = U2FFido2Server(app_id=app_id.decode("ascii"), rp=rp)

        state = {
            "challenge": "GAZPACHO!",
            "user_verification": UserVerificationRequirement.PREFERRED,
        }
        client_data = CollectedClientData.create(
            CollectedClientData.TYPE.GET,
            "GAZPACHO!",
            "https://example.com",
        )

        param = b"TOMATO GIVES "

        device = U2FDevice(param, app_id)
        auth_data = AttestedCredentialData.from_ctap1(param, device.public_key_bytes)
        authenticator_data, signature = device.sign(client_data)

        server.authenticate_complete(
            state,
            [auth_data],
            device.credential_id,
            client_data,
            authenticator_data,
            signature,
        )

    def test_u2f_facets(self):
        rp = PublicKeyCredentialRpEntity("Example", "example.com")
        app_id = b"https://www.example.com/facets.json"

        def verify_u2f_origin(origin):
            return origin in ("https://oauth.example.com", "https://admin.example.com")

        server = U2FFido2Server(
            app_id=app_id.decode("ascii"), rp=rp, verify_u2f_origin=verify_u2f_origin
        )

        state = {
            "challenge": "GAZPACHO!",
            "user_verification": UserVerificationRequirement.PREFERRED,
        }

        client_data = CollectedClientData.create(
            CollectedClientData.TYPE.GET,
            "GAZPACHO!",
            "https://oauth.example.com",
        )

        param = b"TOMATO GIVES "

        device = U2FDevice(param, app_id)
        auth_data = AttestedCredentialData.from_ctap1(param, device.public_key_bytes)
        authenticator_data, signature = device.sign(client_data)

        server.authenticate_complete(
            state,
            [auth_data],
            device.credential_id,
            client_data,
            authenticator_data,
            signature,
        )

        # Now with something not whitelisted
        client_data = CollectedClientData.create(
            CollectedClientData.TYPE.GET,
            "GAZPACHO!",
            "https://publicthingy.example.com",
        )

        authenticator_data, signature = device.sign(client_data)

        with self.assertRaisesRegex(
            ValueError, "Invalid origin in CollectedClientData."
        ):
            server.authenticate_complete(
                state,
                [auth_data],
                device.credential_id,
                client_data,
                authenticator_data,
                signature,
            )
