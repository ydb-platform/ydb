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

from __future__ import absolute_import, division, print_function

import binascii

import pytest

from .utils import flip_byte

from nacl.encoding import HexEncoder
from nacl.exceptions import CryptoError
from nacl.secret import SecretBox


VECTORS = [
    # Key, Nonce, Plaintext, Ciphertext
    (
        b"1b27556473e985d462cd51197a9a46c76009549eac6474f206c4ee0844f68389",
        b"69696ee955b62b73cd62bda875fc73d68219e0036b7a0b37",
        (b"be075fc53c81f2d5cf141316ebeb0c7b5228c52a4c62cbd44b66849b64244ffce5e"
         b"cbaaf33bd751a1ac728d45e6c61296cdc3c01233561f41db66cce314adb310e3be8"
         b"250c46f06dceea3a7fa1348057e2f6556ad6b1318a024a838f21af1fde048977eb4"
         b"8f59ffd4924ca1c60902e52f0a089bc76897040e082f937763848645e0705"),
        (b"f3ffc7703f9400e52a7dfb4b3d3305d98e993b9f48681273c29650ba32fc76ce483"
         b"32ea7164d96a4476fb8c531a1186ac0dfc17c98dce87b4da7f011ec48c97271d2c2"
         b"0f9b928fe2270d6fb863d51738b48eeee314a7cc8ab932164548e526ae902243685"
         b"17acfeabd6bb3732bc0e9da99832b61ca01b6de56244a9e88d5f9b37973f622a43d"
         b"14a6599b1f654cb45a74e355a5"),
    ),
]


def test_secret_box_creation():
    SecretBox(
        b"ec2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )


def test_secret_box_bytes():
    s = SecretBox(
        b"ec2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )
    assert bytes(s) == s._key


@pytest.mark.parametrize(("key", "nonce", "plaintext", "ciphertext"), VECTORS)
def test_secret_box_encryption(key, nonce, plaintext, ciphertext):
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


@pytest.mark.parametrize(("key", "nonce", "plaintext", "ciphertext"), VECTORS)
def test_secret_box_decryption(key, nonce, plaintext, ciphertext):
    box = SecretBox(key, encoder=HexEncoder)

    nonce = binascii.unhexlify(nonce)
    decrypted = binascii.hexlify(
        box.decrypt(ciphertext, nonce, encoder=HexEncoder),
    )

    assert decrypted == plaintext


@pytest.mark.parametrize(("key", "nonce", "plaintext", "ciphertext"), VECTORS)
def test_secret_box_decryption_combined(key, nonce, plaintext, ciphertext):
    box = SecretBox(key, encoder=HexEncoder)

    combined = binascii.hexlify(
        binascii.unhexlify(nonce) + binascii.unhexlify(ciphertext),
    )
    decrypted = binascii.hexlify(box.decrypt(combined, encoder=HexEncoder))

    assert decrypted == plaintext


@pytest.mark.parametrize(("key", "nonce", "plaintext", "ciphertext"), VECTORS)
def test_secret_box_optional_nonce(key, nonce, plaintext, ciphertext):
    box = SecretBox(key, encoder=HexEncoder)

    encrypted = box.encrypt(binascii.unhexlify(plaintext), encoder=HexEncoder)

    decrypted = binascii.hexlify(box.decrypt(encrypted, encoder=HexEncoder))

    assert decrypted == plaintext


@pytest.mark.parametrize(("key", "nonce", "plaintext", "ciphertext"), VECTORS)
def test_secret_box_encryption_generates_different_nonces(
        key, nonce, plaintext, ciphertext):
    box = SecretBox(key, encoder=HexEncoder)

    nonce_0 = box.encrypt(binascii.unhexlify(plaintext),
                          encoder=HexEncoder).nonce

    nonce_1 = box.encrypt(binascii.unhexlify(plaintext),
                          encoder=HexEncoder).nonce

    assert nonce_0 != nonce_1


def test_secret_box_wrong_lengths():
    with pytest.raises(ValueError):
        SecretBox(b"")

    box = SecretBox(
        b"ec2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )
    with pytest.raises(ValueError):
        box.encrypt(b"", b"")
    with pytest.raises(ValueError):
        box.decrypt(b"", b"")


def check_type_error(expected, f, *args):
    with pytest.raises(TypeError) as e:
        f(*args)
    assert expected in str(e.value)


def test_wrong_types():
    box = SecretBox(b"11" * 32, encoder=HexEncoder)

    check_type_error("SecretBox must be created from 32 bytes",
                     SecretBox, 12)
    check_type_error("SecretBox must be created from 32 bytes",
                     SecretBox, box)


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
