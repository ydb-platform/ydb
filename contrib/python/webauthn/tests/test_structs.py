from unittest import TestCase

from webauthn.helpers import parse_registration_credential_json
from webauthn.helpers.structs import (
    AuthenticatorTransport,
    AuthenticatorAttachment,
)


class TestStructsRegistrationCredential(TestCase):
    def test_parse_registration_credential_json(self):
        """
        Check that we can properly parse some values that aren't really here-or-there for response
        verification, but can still be useful to RP's to fine-tune the WebAuthn experience.
        """
        parsed = parse_registration_credential_json(
            """{
                "id": "ZoIKP1JQvKdrYj1bTUPJ2eTUsbLeFkv-X5xJQNr4k6s",
                "rawId": "ZoIKP1JQvKdrYj1bTUPJ2eTUsbLeFkv-X5xJQNr4k6s",
                "response": {
                    "attestationObject": "o2NmbXRkbm9uZWdhdHRTdG10oGhhdXRoRGF0YVkBZ0mWDeWIDoxodDQXD2R2YFuP5K65ooYyx5lc87qDHZdjRQAAAAAAAAAAAAAAAAAAAAAAAAAAACBmggo_UlC8p2tiPVtNQ8nZ5NSxst4WS_5fnElA2viTq6QBAwM5AQAgWQEA31dtHqc70D_h7XHQ6V_nBs3Tscu91kBL7FOw56_VFiaKYRH6Z4KLr4J0S12hFJ_3fBxpKfxyMfK66ZMeAVbOl_wemY4S5Xs4yHSWy21Xm_dgWhLJjZ9R1tjfV49kDPHB_ssdvP7wo3_NmoUPYMgK-edgZ_ehttp_I6hUUCnVaTvn_m76b2j9yEPReSwl-wlGsabYG6INUhTuhSOqG-UpVVQdNJVV7GmIPHCA2cQpJBDZBohT4MBGme_feUgm4sgqVCWzKk6CzIKIz5AIVnspLbu05SulAVnSTB3NxTwCLNJR_9v9oSkvphiNbmQBVQH1tV_psyi9HM1Jtj9VJVKMeyFDAQAB",
                    "clientDataJSON": "eyJ0eXBlIjoid2ViYXV0aG4uY3JlYXRlIiwiY2hhbGxlbmdlIjoiQ2VUV29nbWcwY2NodWlZdUZydjhEWFhkTVpTSVFSVlpKT2dhX3hheVZWRWNCajBDdzN5NzN5aEQ0RmtHU2UtUnJQNmhQSkpBSW0zTFZpZW40aFhFTGciLCJvcmlnaW4iOiJodHRwOi8vbG9jYWxob3N0OjUwMDAiLCJjcm9zc09yaWdpbiI6ZmFsc2V9",
                    "transports": ["internal", "hybrid"]
                },
                "type": "public-key",
                "clientExtensionResults": {},
                "authenticatorAttachment": "platform"
            }"""
        )

        self.assertEqual(
            parsed.response.transports,
            [
                AuthenticatorTransport.INTERNAL,
                AuthenticatorTransport.HYBRID,
            ],
        )
        self.assertEqual(parsed.authenticator_attachment, AuthenticatorAttachment.PLATFORM)
