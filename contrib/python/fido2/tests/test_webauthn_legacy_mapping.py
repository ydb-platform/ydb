# Copyright (c) 2019 Yubico AB
# All rights reserved.
#
#   Redistribution and use in source and binary forms, with or
#   without modification, are permitted provided that the following
#   conditions are met:
#
#    1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#    2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from fido2.webauthn import (
    PublicKeyCredentialUserEntity,
    PublicKeyCredentialCreationOptions,
    PublicKeyCredentialRequestOptions,
)

from fido2.features import webauthn_json_mapping
import unittest


class TestLegacyMapping(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        webauthn_json_mapping._enabled = False

    @classmethod
    def tearDownClass(cls):
        webauthn_json_mapping._enabled = True

    def test_user_entity(self):
        o = PublicKeyCredentialUserEntity("Example", b"user", display_name="Display")
        self.assertEqual(
            o, {"id": b"user", "name": "Example", "displayName": "Display"}
        )
        self.assertEqual(o.id, b"user")
        self.assertEqual(o.name, "Example")
        self.assertEqual(o.display_name, "Display")

    def test_creation_options(self):
        o = PublicKeyCredentialCreationOptions(
            {"id": "example.com", "name": "Example"},
            {"id": b"user_id", "name": "A. User"},
            b"request_challenge",
            [{"type": "public-key", "alg": -7}],
            10000,
            [{"type": "public-key", "id": b"credential_id"}],
            {
                "authenticatorAttachment": "platform",
                "residentKey": "required",
                "userVerification": "required",
            },
            "direct",
        )
        self.assertEqual(o.rp, {"id": "example.com", "name": "Example"})
        self.assertEqual(o.user, {"id": b"user_id", "name": "A. User"})
        self.assertIsNone(o.extensions)

        o2 = PublicKeyCredentialCreationOptions.from_dict(dict(o))
        self.assertEqual(o, o2)

        o = PublicKeyCredentialCreationOptions(
            {"id": "example.com", "name": "Example"},
            {"id": b"user_id", "name": "A. User"},
            b"request_challenge",
            [{"type": "public-key", "alg": -7}],
        )
        self.assertIsNone(o.timeout)
        self.assertIsNone(o.authenticator_selection)
        self.assertIsNone(o.attestation)

        self.assertIsNone(
            PublicKeyCredentialCreationOptions(
                {"id": "example.com", "name": "Example"},
                {"id": b"user_id", "name": "A. User"},
                b"request_challenge",
                [{"type": "public-key", "alg": -7}],
                attestation="invalid",
            ).attestation
        )

    def test_request_options(self):
        o = PublicKeyCredentialRequestOptions(
            b"request_challenge",
            10000,
            "example.com",
            [{"type": "public-key", "id": b"credential_id"}],
            "discouraged",
        )
        self.assertEqual(o.challenge, b"request_challenge")
        self.assertEqual(o.rp_id, "example.com")
        self.assertEqual(o.timeout, 10000)
        self.assertIsNone(o.extensions)

        o = PublicKeyCredentialRequestOptions(b"request_challenge")
        self.assertIsNone(o.timeout)
        self.assertIsNone(o.rp_id)
        self.assertIsNone(o.allow_credentials)
        self.assertIsNone(o.user_verification)

        self.assertIsNone(
            PublicKeyCredentialRequestOptions(
                b"request_challenge", user_verification="invalid"
            ).user_verification
        )
