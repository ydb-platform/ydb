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


import pytest

import nacl.secret
import nacl.utils


def test_random_bytes_produces():
    assert len(nacl.utils.random(16)) == 16


def test_random_bytes_produces_different_bytes():
    assert nacl.utils.random(16) != nacl.utils.random(16)


def test_string_fixer():
    assert str(nacl.secret.SecretBox(b"\x00" * 32)) == str(b"\x00" * 32)


def test_deterministic_random_bytes():
    expected = (
        b"0d8e6cc68715648926732e7ea73250cfaf2d58422083904c841a8ba"
        b"33b986111f346ba50723a68ae283524a6bded09f83be6b80595856f"
        b"72e25b86918e8b114bafb94bc8abedd73daab454576b7c5833eb0bf"
        b"982a1bb4587a5c970ff0810ca3b791d7e12"
    )
    seed = (
        b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d"
        b"\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b"
        b"\x1c\x1d\x1e\x1f"
    )
    assert (
        nacl.utils.randombytes_deterministic(
            100, seed, encoder=nacl.utils.encoding.HexEncoder
        )
        == expected
    )


def test_deterministic_random_bytes_invalid_seed_length():
    expected = "Deterministic random bytes must be generated from 32 bytes"
    seed = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a"
    with pytest.raises(TypeError) as e:
        nacl.utils.randombytes_deterministic(100, seed)
    assert expected in str(e.value)
