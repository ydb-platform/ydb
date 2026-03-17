# Copyright 2013 Donald Stufft and individual contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import binascii
from typing import List, Tuple, Union

import pytest

from nacl.bindings import crypto_sign_PUBLICKEYBYTES, crypto_sign_SEEDBYTES
from nacl.encoding import Base64Encoder, HexEncoder
from nacl.exceptions import BadSignatureError
from nacl.signing import SignedMessage, SigningKey, VerifyKey

from .utils import (
    assert_equal,
    assert_not_equal,
    check_type_error,
    read_crypto_test_vectors,
)


def tohex(b: bytes) -> str:
    return binascii.hexlify(b).decode("ascii")


def ed25519_known_answers() -> List[Tuple[bytes, bytes, bytes, bytes, bytes]]:
    # Known answers taken from: http://ed25519.cr.yp.to/python/sign.input
    # hex-encoded fields on each input line: sk||pk, pk, msg, signature||msg
    # known answer fields: sk, pk, msg, signature, signed
    DATA = "ed25519"
    lines = read_crypto_test_vectors(DATA, delimiter=b":")
    return [
        (
            x[0][:64],  # secret key
            x[1],  # public key
            x[2],  # message
            x[3][:128],  # signature
            x[3],  # signed message
        )
        for x in lines
    ]


class TestSigningKey:
    def test_initialize_with_generate(self):
        SigningKey.generate()

    def test_wrong_length(self):
        with pytest.raises(ValueError):
            SigningKey(b"")

    def test_bytes(self):
        k = SigningKey(b"\x00" * crypto_sign_SEEDBYTES)
        assert bytes(k) == b"\x00" * crypto_sign_SEEDBYTES

    def test_equal_keys_are_equal(self):
        k1 = SigningKey(b"\x00" * crypto_sign_SEEDBYTES)
        k2 = SigningKey(b"\x00" * crypto_sign_SEEDBYTES)
        assert_equal(k1, k1)
        assert_equal(k1, k2)

    def test_equal_keys_have_equal_hashes(self):
        k1 = SigningKey(b"\x00" * crypto_sign_SEEDBYTES)
        k2 = SigningKey(b"\x00" * crypto_sign_SEEDBYTES)
        assert hash(k1) == hash(k2)
        assert id(k1) != id(k2)

    @pytest.mark.parametrize(
        "k2",
        [
            b"\x00" * crypto_sign_SEEDBYTES,
            SigningKey(b"\x01" * crypto_sign_SEEDBYTES),
            SigningKey(b"\x00" * (crypto_sign_SEEDBYTES - 1) + b"\x01"),
        ],
    )
    def test_different_keys_are_not_equal(self, k2: Union[bytes, SigningKey]):
        k1 = SigningKey(b"\x00" * crypto_sign_SEEDBYTES)
        assert_not_equal(k1, k2)

    @pytest.mark.parametrize(
        "seed",
        [b"77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a"],
    )
    def test_initialization_with_seed(self, seed: bytes):
        SigningKey(seed, encoder=HexEncoder)

    @pytest.mark.parametrize(
        ("seed", "_public_key", "message", "signature", "expected"),
        ed25519_known_answers(),
    )
    def test_message_signing(
        self,
        seed: bytes,
        _public_key: bytes,
        message: bytes,
        signature: bytes,
        expected: bytes,
    ):
        signing_key = SigningKey(
            seed,
            encoder=HexEncoder,
        )
        signed = signing_key.sign(
            binascii.unhexlify(message),
            encoder=HexEncoder,
        )

        assert signed == expected
        assert signed.message == message
        assert signed.signature == signature


class TestVerifyKey:
    def test_wrong_length(self):
        with pytest.raises(ValueError):
            VerifyKey(b"")

    def test_bytes(self):
        k = VerifyKey(b"\x00" * crypto_sign_PUBLICKEYBYTES)
        assert bytes(k) == b"\x00" * crypto_sign_PUBLICKEYBYTES

    def test_equal_keys_are_equal(self):
        k1 = VerifyKey(b"\x00" * crypto_sign_PUBLICKEYBYTES)
        k2 = VerifyKey(b"\x00" * crypto_sign_PUBLICKEYBYTES)
        assert_equal(k1, k1)
        assert_equal(k1, k2)

    def test_equal_keys_have_equal_hashes(self):
        k1 = VerifyKey(b"\x00" * crypto_sign_PUBLICKEYBYTES)
        k2 = VerifyKey(b"\x00" * crypto_sign_PUBLICKEYBYTES)
        assert hash(k1) == hash(k2)
        assert id(k1) != id(k2)

    @pytest.mark.parametrize(
        "k2",
        [
            b"\x00" * crypto_sign_PUBLICKEYBYTES,
            VerifyKey(b"\x01" * crypto_sign_PUBLICKEYBYTES),
            VerifyKey(b"\x00" * (crypto_sign_PUBLICKEYBYTES - 1) + b"\x01"),
        ],
    )
    def test_different_keys_are_not_equal(self, k2: Union[bytes, VerifyKey]):
        k1 = VerifyKey(b"\x00" * crypto_sign_PUBLICKEYBYTES)
        assert_not_equal(k1, k2)

    @pytest.mark.parametrize(
        ("_seed", "public_key", "message", "signature", "signed"),
        ed25519_known_answers(),
    )
    def test_valid_signed_message(
        self,
        _seed: bytes,
        public_key: bytes,
        message: bytes,
        signature: bytes,
        signed: bytes,
    ):
        key = VerifyKey(
            public_key,
            encoder=HexEncoder,
        )

        assert (
            binascii.hexlify(
                key.verify(signed, encoder=HexEncoder),
            )
            == message
        )
        assert (
            binascii.hexlify(
                key.verify(
                    message, HexEncoder.decode(signature), encoder=HexEncoder
                ),
            )
            == message
        )

    def test_invalid_signed_message(self):
        skey = SigningKey.generate()
        smessage = skey.sign(b"A Test Message!")
        signature, message = smessage.signature, b"A Forged Test Message!"

        # Small sanity check
        assert skey.verify_key.verify(smessage)

        with pytest.raises(BadSignatureError):
            skey.verify_key.verify(message, signature)

        with pytest.raises(BadSignatureError):
            forged = SignedMessage(signature + message)
            skey.verify_key.verify(forged)

    def test_invalid_signature_length(self):
        skey = SigningKey.generate()
        message = b"hello"
        signature = skey.sign(message).signature

        # Sanity checks
        assert skey.verify_key.verify(message, signature)
        assert skey.verify_key.verify(signature + message)

        with pytest.raises(ValueError):
            skey.verify_key.verify(message, b"")

        with pytest.raises(ValueError):
            skey.verify_key.verify(message, signature * 2)

        with pytest.raises(ValueError):
            skey.verify_key.verify(signature + message, b"")

    def test_base64_smessage_with_detached_sig_matches_with_attached_sig(self):
        sk = SigningKey.generate()
        vk = sk.verify_key

        smsg = sk.sign(b"Hello World in base64", encoder=Base64Encoder)

        msg = smsg.message
        b64sig = smsg.signature

        sig = Base64Encoder.decode(b64sig)

        assert vk.verify(msg, sig, encoder=Base64Encoder) == vk.verify(
            smsg, encoder=Base64Encoder
        )

        assert Base64Encoder.decode(msg) == b"Hello World in base64"

    def test_hex_smessage_with_detached_sig_matches_with_attached_sig(self):
        sk = SigningKey.generate()
        vk = sk.verify_key

        smsg = sk.sign(b"Hello World in hex", encoder=HexEncoder)

        msg = smsg.message
        hexsig = smsg.signature

        sig = HexEncoder.decode(hexsig)

        assert vk.verify(msg, sig, encoder=HexEncoder) == vk.verify(
            smsg, encoder=HexEncoder
        )

        assert HexEncoder.decode(msg) == b"Hello World in hex"

    def test_key_conversion(self):
        keypair_seed = (
            b"421151a459faeade3d247115f94aedae"
            b"42318124095afabe4d1451a559faedee"
        )
        signing_key = SigningKey(binascii.unhexlify(keypair_seed))
        verify_key = signing_key.verify_key

        private_key = bytes(signing_key.to_curve25519_private_key())
        public_key = bytes(verify_key.to_curve25519_public_key())

        assert tohex(private_key) == (
            "8052030376d47112be7f73ed7a019293"
            "dd12ad910b654455798b4667d73de166"
        )

        assert tohex(public_key) == (
            "f1814f0e8ff1043d8a44d25babff3ced"
            "cae6c22c3edaa48f857ae70de2baae50"
        )


def test_wrong_types():
    sk = SigningKey.generate()

    check_type_error(
        "SigningKey must be created from a 32 byte seed", SigningKey, 12
    )
    check_type_error(
        "SigningKey must be created from a 32 byte seed", SigningKey, sk
    )
    check_type_error(
        "SigningKey must be created from a 32 byte seed",
        SigningKey,
        sk.verify_key,
    )

    check_type_error("VerifyKey must be created from 32 bytes", VerifyKey, 13)
    check_type_error("VerifyKey must be created from 32 bytes", VerifyKey, sk)
    check_type_error(
        "VerifyKey must be created from 32 bytes", VerifyKey, sk.verify_key
    )

    def verify_detached_signature(x: bytes) -> None:
        sk.verify_key.verify(b"", x)

    check_type_error(
        "Verification signature must be created from 64 bytes",
        verify_detached_signature,
        13,
    )
    check_type_error(
        "Verification signature must be created from 64 bytes",
        verify_detached_signature,
        sk,
    )
    check_type_error(
        "Verification signature must be created from 64 bytes",
        verify_detached_signature,
        sk.verify_key,
    )
