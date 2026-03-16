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

import pytest

from nacl.encoding import HexEncoder
from nacl.exceptions import CryptoError
from nacl.public import Box, PrivateKey, PublicKey
from nacl.utils import random

from .test_bindings import _box_from_seed_vectors
from .utils import check_type_error

VECTORS = [
    # privalice, pubalice, privbob, pubbob, nonce, plaintext, ciphertext
    (
        b"77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a",
        b"8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a",
        b"5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb",
        b"de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f",
        b"69696ee955b62b73cd62bda875fc73d68219e0036b7a0b37",
        (
            b"be075fc53c81f2d5cf141316ebeb0c7b5228c52a4c62cbd44b66849b64244ffce5e"
            b"cbaaf33bd751a1ac728d45e6c61296cdc3c01233561f41db66cce314adb310e3be8"
            b"250c46f06dceea3a7fa1348057e2f6556ad6b1318a024a838f21af1fde048977eb4"
            b"8f59ffd4924ca1c60902e52f0a089bc76897040e082f937763848645e0705"
        ),
        (
            b"f3ffc7703f9400e52a7dfb4b3d3305d98e993b9f48681273c29650ba32fc76ce483"
            b"32ea7164d96a4476fb8c531a1186ac0dfc17c98dce87b4da7f011ec48c97271d2c2"
            b"0f9b928fe2270d6fb863d51738b48eeee314a7cc8ab932164548e526ae902243685"
            b"17acfeabd6bb3732bc0e9da99832b61ca01b6de56244a9e88d5f9b37973f622a43d"
            b"14a6599b1f654cb45a74e355a5"
        ),
    ),
]


def test_generate_private_key():
    PrivateKey.generate()


def test_generate_private_key_from_random_seed():
    PrivateKey.from_seed(random(PrivateKey.SEED_SIZE))


@pytest.mark.parametrize(
    ("seed", "public_key", "secret_key"), _box_from_seed_vectors()
)
def test_generate_private_key_from_seed(
    seed: bytes, public_key: bytes, secret_key: bytes
):
    prvt = PrivateKey.from_seed(seed, encoder=HexEncoder)
    sk = binascii.unhexlify(secret_key)
    pk = binascii.unhexlify(public_key)
    assert bytes(prvt) == sk
    assert bytes(prvt.public_key) == pk


def test_box_creation():
    pub = PublicKey(
        b"ec2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )
    priv = PrivateKey(
        b"5c2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )
    Box(priv, pub)


def test_box_decode():
    pub = PublicKey(
        b"ec2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )
    priv = PrivateKey(
        b"5c2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )
    b1 = Box(priv, pub)
    b2 = Box.decode(b1._shared_key)
    assert b1._shared_key == b2._shared_key


def test_box_bytes():
    pub = PublicKey(
        b"ec2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )
    priv = PrivateKey(
        b"5c2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )
    b = Box(priv, pub)
    assert bytes(b) == b._shared_key


@pytest.mark.parametrize(
    (
        "privalice",
        "pubalice",
        "privbob",
        "pubbob",
        "nonce",
        "plaintext",
        "ciphertext",
    ),
    VECTORS,
)
def test_box_encryption(
    privalice: bytes,
    pubalice: bytes,
    privbob: bytes,
    pubbob: bytes,
    nonce: bytes,
    plaintext: bytes,
    ciphertext: bytes,
):
    pubalice_decoded = PublicKey(pubalice, encoder=HexEncoder)
    privbob_decoded = PrivateKey(privbob, encoder=HexEncoder)

    box = Box(privbob_decoded, pubalice_decoded)
    encrypted = box.encrypt(
        binascii.unhexlify(plaintext),
        binascii.unhexlify(nonce),
        encoder=HexEncoder,
    )

    expected = binascii.hexlify(
        binascii.unhexlify(nonce) + binascii.unhexlify(ciphertext),
    )

    assert encrypted == expected
    assert encrypted.nonce == nonce
    assert encrypted.ciphertext == ciphertext


@pytest.mark.parametrize(
    (
        "privalice",
        "pubalice",
        "privbob",
        "pubbob",
        "nonce",
        "plaintext",
        "ciphertext",
    ),
    VECTORS,
)
def test_box_decryption(
    privalice: bytes,
    pubalice: bytes,
    privbob: bytes,
    pubbob: bytes,
    nonce: bytes,
    plaintext: bytes,
    ciphertext: bytes,
):
    pubbob_decoded = PublicKey(pubbob, encoder=HexEncoder)
    privalice_decoded = PrivateKey(privalice, encoder=HexEncoder)

    box = Box(privalice_decoded, pubbob_decoded)

    nonce = binascii.unhexlify(nonce)
    decrypted = binascii.hexlify(
        box.decrypt(ciphertext, nonce, encoder=HexEncoder),
    )

    assert decrypted == plaintext


@pytest.mark.parametrize(
    (
        "privalice",
        "pubalice",
        "privbob",
        "pubbob",
        "nonce",
        "plaintext",
        "ciphertext",
    ),
    VECTORS,
)
def test_box_decryption_combined(
    privalice: bytes,
    pubalice: bytes,
    privbob: bytes,
    pubbob: bytes,
    nonce: bytes,
    plaintext: bytes,
    ciphertext: bytes,
):
    pubbob_decoded = PublicKey(pubbob, encoder=HexEncoder)
    privalice_decoded = PrivateKey(privalice, encoder=HexEncoder)

    box = Box(privalice_decoded, pubbob_decoded)

    combined = binascii.hexlify(
        binascii.unhexlify(nonce) + binascii.unhexlify(ciphertext),
    )
    decrypted = binascii.hexlify(box.decrypt(combined, encoder=HexEncoder))

    assert decrypted == plaintext


@pytest.mark.parametrize(
    (
        "privalice",
        "pubalice",
        "privbob",
        "pubbob",
        "nonce",
        "plaintext",
        "ciphertext",
    ),
    VECTORS,
)
def test_box_optional_nonce(
    privalice: bytes,
    pubalice: bytes,
    privbob: bytes,
    pubbob: bytes,
    nonce: bytes,
    plaintext: bytes,
    ciphertext: bytes,
):
    pubbob_decoded = PublicKey(pubbob, encoder=HexEncoder)
    privalice_decoded = PrivateKey(privalice, encoder=HexEncoder)

    box = Box(privalice_decoded, pubbob_decoded)

    encrypted = box.encrypt(binascii.unhexlify(plaintext), encoder=HexEncoder)

    decrypted = binascii.hexlify(box.decrypt(encrypted, encoder=HexEncoder))

    assert decrypted == plaintext


@pytest.mark.parametrize(
    (
        "privalice",
        "pubalice",
        "privbob",
        "pubbob",
        "nonce",
        "plaintext",
        "ciphertext",
    ),
    VECTORS,
)
def test_box_encryption_generates_different_nonces(
    privalice: bytes,
    pubalice: bytes,
    privbob: bytes,
    pubbob: bytes,
    nonce: bytes,
    plaintext: bytes,
    ciphertext: bytes,
):
    pubbob_decoded = PublicKey(pubbob, encoder=HexEncoder)
    privalice_decoded = PrivateKey(privalice, encoder=HexEncoder)

    box = Box(privalice_decoded, pubbob_decoded)

    nonce_0 = box.encrypt(
        binascii.unhexlify(plaintext), encoder=HexEncoder
    ).nonce

    nonce_1 = box.encrypt(
        binascii.unhexlify(plaintext), encoder=HexEncoder
    ).nonce

    assert nonce_0 != nonce_1


@pytest.mark.parametrize(
    (
        "privalice",
        "pubalice",
        "privbob",
        "pubbob",
        "nonce",
        "plaintext",
        "ciphertext",
    ),
    VECTORS,
)
def test_box_failed_decryption(
    privalice: bytes,
    pubalice: bytes,
    privbob: bytes,
    pubbob: bytes,
    nonce: bytes,
    plaintext: bytes,
    ciphertext: bytes,
):
    pubbob_decoded = PublicKey(pubbob, encoder=HexEncoder)
    privbob_decoded = PrivateKey(privbob, encoder=HexEncoder)

    # this cannot decrypt the ciphertext! the ciphertext must be decrypted by
    # (privalice, pubbob) or (privbob, pubalice)
    box = Box(privbob_decoded, pubbob_decoded)

    with pytest.raises(CryptoError):
        box.decrypt(ciphertext, binascii.unhexlify(nonce), encoder=HexEncoder)


def test_box_wrong_length():
    with pytest.raises(ValueError):
        PublicKey(b"")
    # TODO: should the below raise a ValueError?
    with pytest.raises(TypeError):
        PrivateKey(b"")

    pub = PublicKey(
        b"ec2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )
    priv = PrivateKey(
        b"5c2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )
    b = Box(priv, pub)
    with pytest.raises(ValueError):
        b.encrypt(b"", b"")
    with pytest.raises(ValueError):
        b.decrypt(b"", b"")


def test_wrong_types():
    priv = PrivateKey.generate()

    check_type_error(
        ("PrivateKey must be created from a 32 bytes long raw secret key"),
        PrivateKey,
        12,
    )
    check_type_error(
        ("PrivateKey must be created from a 32 bytes long raw secret key"),
        PrivateKey,
        priv,
    )
    check_type_error(
        ("PrivateKey must be created from a 32 bytes long raw secret key"),
        PrivateKey,
        priv.public_key,
    )

    check_type_error("PublicKey must be created from 32 bytes", PublicKey, 13)
    check_type_error(
        "PublicKey must be created from 32 bytes", PublicKey, priv
    )
    check_type_error(
        "PublicKey must be created from 32 bytes", PublicKey, priv.public_key
    )

    check_type_error(
        "Box must be created from a PrivateKey and a PublicKey",
        Box,
        priv,
        "not a public key",
    )
    check_type_error(
        "Box must be created from a PrivateKey and a PublicKey",
        Box,
        priv.encode(),
        priv.public_key.encode(),
    )
    check_type_error(
        "Box must be created from a PrivateKey and a PublicKey",
        Box,
        priv,
        priv.public_key.encode(),
    )
    check_type_error(
        "Box must be created from a PrivateKey and a PublicKey",
        Box,
        priv.encode(),
        priv.public_key,
    )

    check_type_error(
        "seed must be a 32 bytes long", PrivateKey.from_seed, b"1"
    )
