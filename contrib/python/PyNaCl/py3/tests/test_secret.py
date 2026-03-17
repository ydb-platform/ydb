# Copyright 2013-2018 Donald Stufft and individual contributors
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
import re
from typing import Dict, Type, TypeVar

from hypothesis import given, strategies as st

import pytest

from nacl.encoding import HexEncoder
from nacl.exceptions import CryptoError
from nacl.secret import Aead, SecretBox

from .test_aead import xchacha20poly1305_ietf_vectors
from .utils import flip_byte


VECTORS = [
    # Key, Nonce, Plaintext, Ciphertext
    (
        b"1b27556473e985d462cd51197a9a46c76009549eac6474f206c4ee0844f68389",
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
_BoxType = TypeVar("_BoxType", Aead, SecretBox)


def hex_keys(m: Type[_BoxType]) -> st.SearchStrategy[bytes]:
    return st.binary(min_size=m.KEY_SIZE, max_size=m.KEY_SIZE).map(
        binascii.hexlify
    )


def boxes(m: Type[_BoxType]) -> st.SearchStrategy[_BoxType]:
    return st.binary(min_size=m.KEY_SIZE, max_size=m.KEY_SIZE).map(m)


@given(k=hex_keys(Aead))
def test_aead_creation(k: bytes):
    Aead(k, encoder=HexEncoder)


@given(k=hex_keys(Aead))
def test_aead_bytes(k: bytes):
    s = Aead(k, encoder=HexEncoder)
    assert bytes(s) == s._key == binascii.unhexlify(k)


@given(box=boxes(Aead), plaintext=st.binary(), aad=st.binary())
def test_aead_roundtrip(box: Aead, plaintext: bytes, aad: bytes):
    assert plaintext == box.decrypt(box.encrypt(plaintext, aad), aad)


@given(k=hex_keys(SecretBox))
def test_secret_box_creation(k: bytes):
    SecretBox(k, encoder=HexEncoder)


@given(k=hex_keys(SecretBox))
def test_secret_box_bytes(k: bytes):
    s = SecretBox(k, encoder=HexEncoder)
    assert bytes(s) == s._key == binascii.unhexlify(k)


AEAD_VECTORS = [
    {k: binascii.unhexlify(v) for (k, v) in d.items() if k != "AEAD"}
    for d in xchacha20poly1305_ietf_vectors()
]


@pytest.mark.parametrize("kv", AEAD_VECTORS, ids=range(len(AEAD_VECTORS)))
def test_aead_vectors(kv: Dict[str, bytes]):
    box = Aead(kv["KEY"])
    combined = kv["CT"] + kv["TAG"]
    aad, nonce, plaintext = kv["AD"], kv["NONCE"], kv["IN"]

    assert box.encrypt(plaintext, aad, nonce) == nonce + combined
    assert box.decrypt(combined, aad, nonce) == plaintext
    assert box.decrypt(nonce + combined, aad) == plaintext


@pytest.mark.parametrize(
    ("key", "nonce", "plaintext", "ciphertext"),
    VECTORS,
    ids=range(len(VECTORS)),
)
def test_secret_box_encryption(
    key: bytes, nonce: bytes, plaintext: bytes, ciphertext: bytes
):
    box = SecretBox(key, encoder=HexEncoder)
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
    ("key", "nonce", "plaintext", "ciphertext"),
    VECTORS,
    ids=range(len(VECTORS)),
)
def test_secret_box_decryption(
    key: bytes, nonce: bytes, plaintext: bytes, ciphertext: bytes
):
    box = SecretBox(key, encoder=HexEncoder)

    nonce = binascii.unhexlify(nonce)
    decrypted = binascii.hexlify(
        box.decrypt(ciphertext, nonce, encoder=HexEncoder),
    )

    assert decrypted == plaintext


@pytest.mark.parametrize(
    ("key", "nonce", "plaintext", "ciphertext"),
    VECTORS,
    ids=range(len(VECTORS)),
)
def test_secret_box_decryption_combined(
    key: bytes, nonce: bytes, plaintext: bytes, ciphertext: bytes
):
    box = SecretBox(key, encoder=HexEncoder)

    combined = binascii.hexlify(
        binascii.unhexlify(nonce) + binascii.unhexlify(ciphertext),
    )
    decrypted = binascii.hexlify(box.decrypt(combined, encoder=HexEncoder))

    assert decrypted == plaintext


@pytest.mark.parametrize(
    ("key", "nonce", "plaintext", "ciphertext"),
    VECTORS,
    ids=range(len(VECTORS)),
)
def test_secret_box_optional_nonce(
    key: bytes, nonce: bytes, plaintext: bytes, ciphertext: bytes
):
    box = SecretBox(key, encoder=HexEncoder)

    encrypted = box.encrypt(binascii.unhexlify(plaintext), encoder=HexEncoder)

    decrypted = binascii.hexlify(box.decrypt(encrypted, encoder=HexEncoder))

    assert decrypted == plaintext


@pytest.mark.parametrize(
    ("key", "nonce", "plaintext", "ciphertext"),
    VECTORS,
    ids=range(len(VECTORS)),
)
def test_secret_box_encryption_generates_different_nonces(
    key: bytes, nonce: bytes, plaintext: bytes, ciphertext: bytes
):
    box = SecretBox(key, encoder=HexEncoder)

    nonce_0 = box.encrypt(
        binascii.unhexlify(plaintext), encoder=HexEncoder
    ).nonce

    nonce_1 = box.encrypt(
        binascii.unhexlify(plaintext), encoder=HexEncoder
    ).nonce

    assert nonce_0 != nonce_1


def wrong_length(length: int) -> st.SearchStrategy[bytes]:
    return st.binary().filter(lambda s: len(s) != length)


@given(key=wrong_length(Aead.KEY_SIZE))
def test_aead_wrong_key_length(key: bytes):
    with pytest.raises(
        ValueError, match=r"key must be exactly \d+ bytes long"
    ):
        Aead(key)


@given(box=boxes(Aead), nonce=wrong_length(Aead.NONCE_SIZE))
def test_aead_wrong_nonce_length(box: Aead, nonce: bytes):
    with pytest.raises(
        ValueError, match=r"nonce must be exactly \d+ bytes long"
    ):
        box.encrypt(b"", aad=b"", nonce=nonce)
    with pytest.raises(
        ValueError, match=r"nonce must be exactly \d+ bytes long"
    ):
        box.decrypt(b"", aad=b"", nonce=nonce)


@given(key=wrong_length(SecretBox.KEY_SIZE))
def test_secret_box_wrong_key_length(key: bytes):
    with pytest.raises(
        ValueError, match=r"key must be exactly \d+ bytes long"
    ):
        SecretBox(key)


@given(box=boxes(SecretBox), nonce=wrong_length(SecretBox.NONCE_SIZE))
def test_secret_box_wrong_nonce_length(box: SecretBox, nonce: bytes):
    with pytest.raises(
        ValueError, match=r"nonce must be exactly \d+ bytes long"
    ):
        box.encrypt(b"", nonce)
    with pytest.raises(
        ValueError, match=r"nonce must be exactly \d+ bytes long"
    ):
        box.decrypt(b"", nonce)


@pytest.mark.parametrize("cls", (SecretBox, Aead))
def test_wrong_types(cls: Type[_BoxType]):
    expected = re.compile(
        cls.__name__ + " must be created from 32 bytes", re.IGNORECASE
    )
    # Type saftey: we're checking these type errors are detected at runtime.
    with pytest.raises(TypeError, match=expected):
        cls(12)  # type: ignore[arg-type]

    box = SecretBox(b"11" * 32, encoder=HexEncoder)
    with pytest.raises(TypeError, match=expected):
        cls(box)  # type: ignore[arg-type]


def test_aead_bad_decryption():
    box = Aead(b"\x11" * Aead.KEY_SIZE)
    aad = b"some data"
    ciphertext = box.encrypt(b"hello")

    with pytest.raises(CryptoError):
        # changes the nonce
        box.decrypt(flip_byte(ciphertext, 0), aad)
    with pytest.raises(CryptoError):
        # changes ciphertext
        box.decrypt(flip_byte(ciphertext, 24), aad)
    with pytest.raises(CryptoError):
        # changes MAC tag
        box.decrypt(flip_byte(ciphertext, len(ciphertext) - 1), aad)

    with pytest.raises(CryptoError):
        # completely changes ciphertext and tag
        box.decrypt(ciphertext + b"\x00", aad)
    with pytest.raises(CryptoError):
        # completely changes everything
        box.decrypt(b"\x00" + ciphertext, aad)

    with pytest.raises(CryptoError):
        # changes the AAD
        box.decrypt(ciphertext, flip_byte(aad, 0))


def test_secret_box_bad_decryption():
    box = SecretBox(b"\x11" * 32)
    ciphertext = box.encrypt(b"hello")

    with pytest.raises(CryptoError):
        # changes the nonce
        box.decrypt(flip_byte(ciphertext, 0))
    with pytest.raises(CryptoError):
        # changes ciphertext
        box.decrypt(flip_byte(ciphertext, 24))
    with pytest.raises(CryptoError):
        # changes MAC tag
        box.decrypt(flip_byte(ciphertext, len(ciphertext) - 1))

    with pytest.raises(CryptoError):
        # completely changes ciphertext and tag
        box.decrypt(ciphertext + b"\x00")
    with pytest.raises(CryptoError):
        # completely changes everything
        box.decrypt(b"\x00" + ciphertext)
