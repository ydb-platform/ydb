# Copyright 2018 Donald Stufft and individual contributors
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


from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import binary

import pytest

import nacl.bindings as b
import nacl.exceptions as exc


def test_crypto_kx_keypair():
    public_key, secret_key = b.crypto_kx_keypair()
    public_key_2, secret_key_2 = b.crypto_kx_keypair()
    assert public_key != public_key_2
    assert secret_key != secret_key_2


@given(
    binary(min_size=32, max_size=32),
    binary(min_size=32, max_size=32),
)
@settings(max_examples=100)
def test_crypto_kx_seed_keypair(seed1: bytes, seed2: bytes):
    seeded = b.crypto_kx_seed_keypair(seed1)
    seeded_other = b.crypto_kx_seed_keypair(seed2)
    if seed1 != seed2:
        assert seeded != seeded_other
    else:
        assert seeded == seeded_other


@given(
    binary(min_size=33, max_size=128),
)
@settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
def test_crypto_kx_seed_keypair_seed_too_large(seed: bytes):
    with pytest.raises(exc.TypeError):
        b.crypto_kx_seed_keypair(seed)


@given(
    binary(min_size=0, max_size=31),
)
@settings(max_examples=20)
def test_crypto_kx_seed_keypair_seed_too_small(seed: bytes):
    with pytest.raises(exc.TypeError):
        b.crypto_kx_seed_keypair(seed)


@given(
    binary(min_size=32, max_size=32),
    binary(min_size=32, max_size=32),
)
@settings(max_examples=100)
def test_crypto_kx_session_keys(seed1: bytes, seed2: bytes):
    s_keys = b.crypto_kx_seed_keypair(seed1)
    c_keys = b.crypto_kx_seed_keypair(seed2)

    server_rx_key, server_tx_key = b.crypto_kx_server_session_keys(
        s_keys[0], s_keys[1], c_keys[0]
    )
    client_rx_key, client_tx_key = b.crypto_kx_client_session_keys(
        c_keys[0], c_keys[1], s_keys[0]
    )

    assert client_rx_key == server_tx_key
    assert server_rx_key == client_tx_key


def test_crypto_kx_session_wrong_key_lengths():
    s_keys = b.crypto_kx_keypair()
    c_keys = b.crypto_kx_keypair()

    # TODO: should invalid argument lengths (but correct types) raise ValueError?
    with pytest.raises(exc.TypeError):
        b.crypto_kx_server_session_keys(s_keys[0][:-1], s_keys[1], c_keys[0])

    with pytest.raises(exc.TypeError):
        b.crypto_kx_client_session_keys(c_keys[0][:-1], c_keys[1], s_keys[0])

    with pytest.raises(exc.TypeError):
        b.crypto_kx_server_session_keys(s_keys[0], s_keys[1][:-1], c_keys[0])

    with pytest.raises(exc.TypeError):
        b.crypto_kx_client_session_keys(c_keys[0], c_keys[1][:-1], s_keys[0])

    with pytest.raises(exc.TypeError):
        b.crypto_kx_server_session_keys(s_keys[0], s_keys[1], c_keys[0][:-1])

    with pytest.raises(exc.TypeError):
        b.crypto_kx_client_session_keys(c_keys[0], c_keys[1], s_keys[0][:-1])
