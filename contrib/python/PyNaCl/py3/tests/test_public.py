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
from typing import Tuple, Union

import pytest

from nacl.bindings import crypto_box_PUBLICKEYBYTES, crypto_box_SECRETKEYBYTES
from nacl.public import Box, PrivateKey, PublicKey
from nacl.utils import random

from .utils import assert_equal, assert_not_equal


class TestPublicKey:
    def test_equal_keys_have_equal_hashes(self):
        k1 = PublicKey(b"\x00" * crypto_box_PUBLICKEYBYTES)
        k2 = PublicKey(b"\x00" * crypto_box_PUBLICKEYBYTES)
        assert hash(k1) == hash(k2)
        assert id(k1) != id(k2)

    def test_equal_keys_are_equal(self):
        k1 = PublicKey(b"\x00" * crypto_box_PUBLICKEYBYTES)
        k2 = PublicKey(b"\x00" * crypto_box_PUBLICKEYBYTES)
        assert_equal(k1, k1)
        assert_equal(k1, k2)

    @pytest.mark.parametrize(
        "k2",
        [
            b"\x00" * crypto_box_PUBLICKEYBYTES,
            PublicKey(b"\x01" * crypto_box_PUBLICKEYBYTES),
            PublicKey(b"\x00" * (crypto_box_PUBLICKEYBYTES - 1) + b"\x01"),
        ],
    )
    def test_different_keys_are_not_equal(self, k2: Union[bytes, PublicKey]):
        k1 = PublicKey(b"\x00" * crypto_box_PUBLICKEYBYTES)
        assert_not_equal(k1, k2)


class TestPrivateKey:
    def test_equal_keys_have_equal_hashes(self):
        k1 = PrivateKey(b"\x00" * crypto_box_SECRETKEYBYTES)
        k2 = PrivateKey(b"\x00" * crypto_box_SECRETKEYBYTES)
        assert hash(k1) == hash(k2)
        assert id(k1) != id(k2)

    def test_equal_keys_are_equal(self):
        k1 = PrivateKey(b"\x00" * crypto_box_SECRETKEYBYTES)
        k2 = PrivateKey(b"\x00" * crypto_box_SECRETKEYBYTES)
        assert_equal(k1, k1)
        assert_equal(k1, k2)

    def _gen_equivalent_raw_keys_couple(self) -> Tuple[PrivateKey, PrivateKey]:
        rwk1 = bytearray(random(crypto_box_SECRETKEYBYTES))
        rwk2 = bytearray(rwk1)
        # mask rwk1 bits
        rwk1[0] &= 248
        rwk1[31] &= 127
        rwk1[31] |= 64
        # set rwk2 bits
        rwk2[0] |= 7
        rwk2[31] |= 128
        rwk2[31] &= 191
        sk1 = PrivateKey(bytes(rwk1))
        sk2 = PrivateKey(bytes(rwk2))
        return sk1, sk2

    def test_equivalent_keys_have_equal_hashes(self):
        k1, k2 = self._gen_equivalent_raw_keys_couple()
        assert bytes(k1) != bytes(k2)
        assert hash(k1) == hash(k2)

    def test_equivalent_keys_compare_as_equal(self):
        k1, k2 = self._gen_equivalent_raw_keys_couple()
        assert bytes(k1) != bytes(k2)
        assert k1 == k2

    def test_sk_and_pk_hashes_are_different(self):
        sk = PrivateKey(random(crypto_box_SECRETKEYBYTES))
        assert hash(sk) != hash(sk.public_key)

    @pytest.mark.parametrize(
        "k2",
        [
            b"\x00" * crypto_box_SECRETKEYBYTES,
            PrivateKey(b"\x01" * crypto_box_SECRETKEYBYTES),
            PrivateKey(b"\x00" * (crypto_box_SECRETKEYBYTES - 1) + b"\x01"),
        ],
    )
    def test_different_keys_are_not_equal(self, k2: Union[bytes, PrivateKey]):
        k1 = PrivateKey(b"\x00" * crypto_box_SECRETKEYBYTES)
        assert_not_equal(k1, k2)

    def test_shared_key_getter(self):
        """
        RFC 7748 "Elliptic Curves for Security" gives a set of test
        parameters for the Diffie-Hellman key exchange on Curve25519:

        6.1.  [Diffie-Hellman on] Curve25519
            [ . . . ]
        Alice's private key, a:
          77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a
        Alice's public key, X25519(a, 9):
          8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a
        Bob's private key, b:
          5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb
        Bob's public key, X25519(b, 9):
          de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f

        Since libNaCl/libsodium shared key generation adds an HSalsa20
        key derivation pass on the raw shared Diffie-Hellman key, which
        is not exposed by itself, we just check the shared key for equality.
        """
        prv_A = (
            b"77076d0a7318a57d3c16c17251b26645"
            b"df4c2f87ebc0992ab177fba51db92c2a"
        )
        pub_A = (
            b"8520f0098930a754748b7ddcb43ef75a"
            b"0dbf3a0d26381af4eba4a98eaa9b4e6a"
        )
        prv_B = (
            b"5dab087e624a8a4b79e17f8b83800ee6"
            b"6f3bb1292618b6fd1c2f8b27ff88e0eb"
        )
        pub_B = (
            b"de9edb7d7b7dc1b4d35b61c2ece43537"
            b"3f8343c85b78674dadfc7e146f882b4f"
        )

        alices = PrivateKey(binascii.unhexlify(prv_A))
        bobs = PrivateKey(binascii.unhexlify(prv_B))
        alicesP = alices.public_key
        bobsP = bobs.public_key

        assert binascii.unhexlify(pub_A) == bytes(alicesP)
        assert binascii.unhexlify(pub_B) == bytes(bobsP)

        box_AB = Box(alices, bobsP)
        box_BA = Box(bobs, alicesP)

        assert box_AB.shared_key() == box_BA.shared_key()

    def test_equivalent_keys_shared_key_getter(self):
        alices = PrivateKey.generate()
        alicesP = alices.public_key
        bobs, bobsprime = self._gen_equivalent_raw_keys_couple()
        bobsP, bobsprimeP = bobs.public_key, bobsprime.public_key

        assert bobsP == bobsprimeP

        box_AB = Box(alices, bobsP)

        box_BA = Box(bobs, alicesP)
        box_BprimeA = Box(bobsprime, alicesP)

        assert box_AB.shared_key() == box_BA.shared_key()
        assert box_BprimeA.shared_key() == box_BA.shared_key()
