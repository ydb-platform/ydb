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

from __future__ import absolute_import, division, print_function

import binascii
import sys
from collections import namedtuple

from hypothesis import given, settings
from hypothesis.strategies import binary, sampled_from

import pytest

from .utils import read_kv_test_vectors

import nacl.bindings as b
import nacl.exceptions as exc


def chacha20poly1305_agl_vectors():
    # NIST vectors derived format
    DATA = "chacha20-poly1305-agl_ref.txt"
    return read_kv_test_vectors(DATA, delimiter=b':', newrecord=b'AEAD')


def chacha20poly1305_ietf_vectors():
    # NIST vectors derived format
    DATA = "chacha20-poly1305-ietf_ref.txt"
    return read_kv_test_vectors(DATA, delimiter=b':', newrecord=b'AEAD')


def xchacha20poly1305_ietf_vectors():
    # NIST vectors derived format
    DATA = "xchacha20-poly1305-ietf_ref.txt"
    return read_kv_test_vectors(DATA, delimiter=b':', newrecord=b'AEAD')


Construction = namedtuple('Construction', 'encrypt, decrypt, NPUB, KEYBYTES')


def _getconstruction(construction):
    if construction == b'chacha20-poly1305-old':
        encrypt = b.crypto_aead_chacha20poly1305_encrypt
        decrypt = b.crypto_aead_chacha20poly1305_decrypt
        NPUB = b.crypto_aead_chacha20poly1305_NPUBBYTES
        KEYBYTES = b.crypto_aead_chacha20poly1305_KEYBYTES
    elif construction == b'chacha20-poly1305':
        encrypt = b.crypto_aead_chacha20poly1305_ietf_encrypt
        decrypt = b.crypto_aead_chacha20poly1305_ietf_decrypt
        NPUB = b.crypto_aead_chacha20poly1305_ietf_NPUBBYTES
        KEYBYTES = b.crypto_aead_chacha20poly1305_ietf_KEYBYTES
    else:
        encrypt = b.crypto_aead_xchacha20poly1305_ietf_encrypt
        decrypt = b.crypto_aead_xchacha20poly1305_ietf_decrypt
        NPUB = b.crypto_aead_xchacha20poly1305_ietf_NPUBBYTES
        KEYBYTES = b.crypto_aead_xchacha20poly1305_ietf_KEYBYTES

    return Construction(encrypt, decrypt, NPUB, KEYBYTES)


@pytest.mark.parametrize("kv",
                         chacha20poly1305_agl_vectors() +
                         chacha20poly1305_ietf_vectors() +
                         xchacha20poly1305_ietf_vectors()
                         )
def test_chacha20poly1305_variants_kat(kv):
    msg = binascii.unhexlify(kv['IN'])
    ad = binascii.unhexlify(kv['AD'])
    nonce = binascii.unhexlify(kv['NONCE'])
    k = binascii.unhexlify(kv['KEY'])
    c = _getconstruction(kv['AEAD'])
    _tag = kv.get('TAG', b'')
    exp = binascii.unhexlify(kv['CT']) + binascii.unhexlify(_tag)
    out = c.encrypt(msg, ad, nonce, k)
    assert (out == exp)


@given(sampled_from((b'chacha20-poly1305-old',
                     b'chacha20-poly1305',
                     b'xchacha20-poly1305'
                     )),
       binary(min_size=0, max_size=100),
       binary(min_size=0, max_size=50),
       binary(min_size=b.crypto_aead_xchacha20poly1305_ietf_NPUBBYTES,
              max_size=b.crypto_aead_xchacha20poly1305_ietf_NPUBBYTES),
       binary(min_size=b.crypto_aead_chacha20poly1305_KEYBYTES,
              max_size=b.crypto_aead_chacha20poly1305_KEYBYTES))
@settings(deadline=None, max_examples=20)
def test_chacha20poly1305_variants_roundtrip(construction,
                                             message,
                                             aad,
                                             nonce,
                                             key):

    c = _getconstruction(construction)
    unonce = nonce[:c.NPUB]

    ct = c.encrypt(message, aad, unonce, key)
    pt = c.decrypt(ct, aad, unonce, key)

    assert pt == message
    with pytest.raises(exc.CryptoError):
        ct1 = bytearray(ct)
        ct1[0] = ct1[0] ^ 0xff
        c.decrypt(ct1, aad, unonce, key)


@pytest.mark.parametrize("construction",
                         [b'chacha20-poly1305-old',
                          b'chacha20-poly1305',
                          b'xchacha20-poly1305']
                         )
def test_chacha20poly1305_variants_wrong_params(construction):
    c = _getconstruction(construction)
    nonce = b'\x00' * c.NPUB
    key = b'\x00' * c.KEYBYTES
    aad = None
    c.encrypt(b'', aad, nonce, key)
    with pytest.raises(exc.TypeError):
        c.encrypt(b'', aad, nonce[:-1], key)
    with pytest.raises(exc.TypeError):
        c.encrypt(b'', aad, nonce, key[:-1])
    with pytest.raises(exc.TypeError):
        c.encrypt(b'', aad, nonce.decode('utf-8'), key)
    with pytest.raises(exc.TypeError):
        c.encrypt(b'', aad, nonce, key.decode('utf-8'))


@pytest.mark.skipif(sys.version_info < (3,),
                    reason="Python 2 doesn't distinguish str() from bytes()")
@pytest.mark.parametrize("construction",
                         [b'chacha20-poly1305-old',
                          b'chacha20-poly1305',
                          b'xchacha20-poly1305']
                         )
def test_chacha20poly1305_variants_str_msg(construction):
    c = _getconstruction(construction)
    nonce = b'\x00' * c.NPUB
    key = b'\x00' * c.KEYBYTES
    aad = None
    with pytest.raises(exc.TypeError):
        c.encrypt('', aad, nonce, key)
