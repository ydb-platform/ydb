# Copyright 2016 Donald Stufft and individual contributors
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
import copy
import json
import os
from typing import AnyStr, Dict, List, Tuple, Union

import pytest

import nacl.encoding
import nacl.exceptions as exc
import nacl.hash
import nacl.hashlib

from .utils import read_crypto_test_vectors


OVERLONG_PARAMS_VECTORS = [
    (b"key", 65 * b"\xaa", 16 * b"\xaa", 16 * b"\x55", 64, b"will raise"),
    (b"salt", b"key", 17 * b"\xaa", 16 * b"\x55", 64, b"will raise"),
    (b"personal", b"key", 16 * b"\xaa", 17 * b"\x55", 64, b"will raise"),
    (b"digest_size", b"key", 16 * b"\xaa", 16 * b"\x55", 65, b"will raise"),
]


def generichash_vectors() -> List[Tuple[bytes, bytes, bytes, bytes]]:
    # Format: <message> <tab> <key> <tab> <output length> <tab> <output>
    DATA = "crypto-test-vectors-blake2-nosalt-nopersonalization.txt"
    # Type safety: read_crypto_test_vectors returns an arbitrary length tuple, but we
    # know this file's test entries contain exactly four fields.
    return read_crypto_test_vectors(DATA, delimiter=b"\t")  # type: ignore[return-value]


def blake2_salt_pers_vectors() -> List[
    Tuple[bytes, bytes, bytes, bytes, bytes, bytes]
]:
    # Format: <message> <tab> <key> <tab> <salt> <tab>
    # <personalization> <tab> <output length> <tab> <output>
    DATA = "crypto-test-vectors-blake2-salt-personalization.txt"
    # Type safety: read_crypto_test_vectors returns an arbitrary length tuple, but we
    # know this file's test entries contain exactly six fields.
    return read_crypto_test_vectors(DATA, delimiter=b"\t")  # type: ignore[return-value]


def blake2_reference_vectors() -> List[Tuple[str, str, int, str]]:
    DATA = "blake2-kat.json"
    path = os.path.join(__import__('yatest').common.source_path(os.path.dirname(__file__)), "data", DATA)
    jvectors: List[Dict[str, str]] = json.load(open(path))
    vectors = [
        (x["in"], x["key"], len(x["out"]) // 2, x["out"])
        for x in jvectors
        if x["hash"] == "blake2b"
    ]
    return vectors


@pytest.mark.parametrize(
    ["message", "key", "outlen", "output"], generichash_vectors()
)
def test_generichash(
    message: AnyStr, key: AnyStr, outlen: Union[AnyStr, int], output: AnyStr
):
    msg = binascii.unhexlify(message)
    output_bytes = binascii.hexlify(binascii.unhexlify(output))
    k = binascii.unhexlify(key)
    outlen_parsed = int(outlen)
    out = nacl.hash.generichash(msg, digest_size=outlen_parsed, key=k)
    assert out == output_bytes


@pytest.mark.parametrize(
    ["message", "key", "salt", "person", "outlen", "output"],
    OVERLONG_PARAMS_VECTORS,
)
def test_overlong_blake2b_oneshot_params(
    message: bytes,
    key: bytes,
    salt: bytes,
    person: bytes,
    outlen: int,
    output: bytes,
):
    with pytest.raises(exc.ValueError):
        nacl.hash.blake2b(
            message, digest_size=outlen, key=key, salt=salt, person=person
        )


@pytest.mark.parametrize(
    ["message", "key", "outlen", "output"], blake2_reference_vectors()
)
def test_generichash_blake2_ref(
    message: str, key: str, outlen: int, output: str
):
    test_generichash(message, key, outlen, output)


@pytest.mark.parametrize(
    ["message", "key", "salt", "person", "outlen", "output"],
    blake2_salt_pers_vectors(),
)
def test_hash_blake2b(
    message: bytes,
    key: bytes,
    salt: bytes,
    person: bytes,
    outlen: bytes,
    output: bytes,
):
    msg = binascii.unhexlify(message)
    output = binascii.hexlify(binascii.unhexlify(output))
    k = binascii.unhexlify(key)
    slt = binascii.unhexlify(salt)
    pers = binascii.unhexlify(person)
    outlen_parsed = int(outlen)
    out = nacl.hash.blake2b(
        msg, digest_size=outlen_parsed, key=k, salt=slt, person=pers
    )
    assert out == output


def test_expected_hashlib_level_pickle_and_copy_failures():
    h = nacl.hashlib.blake2b()
    with pytest.raises(TypeError):
        copy.deepcopy(h)
    with pytest.raises(TypeError):
        copy.copy(h)


def test_expected_bindings_level_pickle_and_copy_failures():
    from nacl.bindings.crypto_generichash import (
        Blake2State,
        crypto_generichash_BYTES,
    )

    st = Blake2State(crypto_generichash_BYTES)
    with pytest.raises(TypeError):
        copy.deepcopy(st)
    with pytest.raises(TypeError):
        copy.copy(st)


@pytest.mark.parametrize(
    ["message", "key", "outlen", "output"], blake2_reference_vectors()
)
def test_hashlib_blake2_ref_vectors(
    message: str, key: str, outlen: int, output: str
):
    msg = binascii.unhexlify(message)
    k = binascii.unhexlify(key)
    outlen = int(outlen)
    out = binascii.unhexlify(output)
    h = nacl.hashlib.blake2b(msg, digest_size=outlen, key=k)
    dgst = h.digest()
    assert out == dgst


@pytest.mark.parametrize(
    ["message", "key", "outlen", "output"], blake2_reference_vectors()
)
def test_hashlib_blake2_iuf_ref_vectors(
    message: str, key: str, outlen: int, output: str
):
    msg = binascii.unhexlify(message)
    k = binascii.unhexlify(key)
    outlen = int(outlen)
    out = binascii.unhexlify(output)
    h = nacl.hashlib.blake2b(digest_size=outlen, key=k)
    for _pos in range(len(msg)):
        _end = _pos + 1
        h.update(bytes(msg[_pos:_end]))
    dgst = h.digest()
    hdgst = h.hexdigest()
    assert hdgst == output
    assert out == dgst


@pytest.mark.parametrize(
    ["message", "key", "outlen", "output"], blake2_reference_vectors()
)
def test_hashlib_blake2_iuf_cp_ref_vectors(
    message: str, key: str, outlen: int, output: str
):
    msg = binascii.unhexlify(message)
    msglen = len(msg)
    if msglen < 2:
        pytest.skip("Message too short for splitting")
    k = binascii.unhexlify(key)
    outlen = int(outlen)
    out = binascii.unhexlify(output)
    h = nacl.hashlib.blake2b(digest_size=outlen, key=k)
    for _pos in range(len(msg)):
        _end = _pos + 1
        h.update(bytes(msg[_pos:_end]))
        if _end == msglen // 2:
            h2 = h.copy()
    dgst = h.digest()
    d2 = h2.digest()
    assert out == dgst
    assert d2 != dgst


@pytest.mark.parametrize(
    ["message", "key", "salt", "person", "outlen", "output"],
    OVERLONG_PARAMS_VECTORS,
)
def test_overlong_blake2b_iuf_params(
    message: bytes,
    key: bytes,
    salt: bytes,
    person: bytes,
    outlen: int,
    output: bytes,
):
    with pytest.raises(exc.ValueError):
        nacl.hashlib.blake2b(
            message, digest_size=outlen, key=key, salt=salt, person=person
        )


def test_blake2_descriptors_presence():
    h = nacl.hashlib.blake2b()
    assert h.name == "blake2b"
    assert h.block_size == 128
    assert h.digest_size == 32  # this is the default digest_size


def test_blake2_digest_size_descriptor_coherence():
    h = nacl.hashlib.blake2b(digest_size=64)
    assert h.name == "blake2b"
    assert h.block_size == 128
    assert h.digest_size == 64
