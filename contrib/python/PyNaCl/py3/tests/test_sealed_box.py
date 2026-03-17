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
from typing import List, Tuple

import pytest

from nacl.encoding import HexEncoder
from nacl.exceptions import CryptoError
from nacl.public import PrivateKey, PublicKey, SealedBox

from .utils import check_type_error, read_crypto_test_vectors


def sealbox_vectors() -> List[Tuple[bytes, bytes, bytes, bytes]]:
    # Fmt: <recipient sk><tab><recipient pk><tab><pt_len>:<plaintext>
    # <tab><cr_len>:<ciphertext>[<tab> ...]

    def splitlen(x: bytes) -> bytes:
        ln, dta = x.split(b":")
        assert len(dta) == 2 * int(ln)
        return dta

    DATA = "sealed_box_ref.txt"
    return [
        (x[0], x[1], splitlen(x[2]), splitlen(x[3]))
        for x in read_crypto_test_vectors(DATA, maxels=4, delimiter=b"\t")
    ]


def test_generate_private_key():
    PrivateKey.generate()


def test_sealed_box_creation():
    pub = PublicKey(
        b"ec2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )
    priv = PrivateKey(
        b"5c2bee2d5be613ca82e377c96a0bf2220d823ce980cdff6279473edc52862798",
        encoder=HexEncoder,
    )
    SealedBox(priv)
    SealedBox(pub)


@pytest.mark.parametrize(
    ("privalice", "pubalice", "plaintext", "_encrypted"), sealbox_vectors()
)
def test_sealed_box_encryption(
    privalice: bytes, pubalice: bytes, plaintext: bytes, _encrypted: bytes
):
    pubalice_decoded = PublicKey(pubalice, encoder=HexEncoder)
    privalice_decoded = PrivateKey(privalice, encoder=HexEncoder)

    box = SealedBox(pubalice_decoded)
    encrypted = box.encrypt(
        binascii.unhexlify(plaintext),
        encoder=HexEncoder,
    )

    assert encrypted != _encrypted
    # since SealedBox.encrypt uses an ephemeral sender's keypair

    box2 = SealedBox(privalice_decoded)
    decrypted = box2.decrypt(
        encrypted,
        encoder=HexEncoder,
    )
    assert binascii.hexlify(decrypted) == plaintext
    assert bytes(box) == bytes(box2)


@pytest.mark.parametrize(
    ("privalice", "_pubalice", "plaintext", "encrypted"), sealbox_vectors()
)
def test_sealed_box_decryption(
    privalice: bytes, _pubalice: bytes, plaintext: bytes, encrypted: bytes
):
    privalice_decoded = PrivateKey(privalice, encoder=HexEncoder)

    box = SealedBox(privalice_decoded)
    decrypted = box.decrypt(
        encrypted,
        encoder=HexEncoder,
    )
    assert binascii.hexlify(decrypted) == plaintext


def test_wrong_types():
    priv = PrivateKey.generate()

    check_type_error(
        ("SealedBox must be created from a PublicKey or a PrivateKey"),
        SealedBox,
        priv.encode(),
    )
    check_type_error(
        ("SealedBox must be created from a PublicKey or a PrivateKey"),
        SealedBox,
        priv.public_key.encode(),
    )
    with pytest.raises(TypeError):
        # Type safety: we want to check this error is detected at runtime.
        SealedBox(priv, priv.public_key)  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ("_privalice", "pubalice", "_plaintext", "encrypted"), sealbox_vectors()
)
def test_sealed_box_public_key_cannot_decrypt(
    _privalice: bytes, pubalice: bytes, _plaintext: bytes, encrypted: bytes
):
    pubalice_decoded = PublicKey(pubalice, encoder=HexEncoder)

    box = SealedBox(pubalice_decoded)
    with pytest.raises(TypeError):
        # Type safety: mypy spots that you can't decrypt with a public key, but we
        # want to detect this at runtime too.
        box.decrypt(  # type: ignore[misc]
            encrypted,
            encoder=HexEncoder,
        )


def test_sealed_box_zero_length_plaintext():
    empty_plaintext = b""
    k = PrivateKey.generate()
    enc_box = SealedBox(k.public_key)
    dec_box = SealedBox(k)

    msg = enc_box.encrypt(empty_plaintext)
    decoded = dec_box.decrypt(msg)

    assert decoded == empty_plaintext


def test_sealed_box_too_short_msg():
    empty_plaintext = b""
    k = PrivateKey.generate()
    enc_box = SealedBox(k.public_key)
    dec_box = SealedBox(k)

    msg = enc_box.encrypt(empty_plaintext)
    with pytest.raises(CryptoError):
        dec_box.decrypt(msg[:-1])
