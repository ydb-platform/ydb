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


import hashlib
from binascii import hexlify, unhexlify
from typing import List, Tuple

from hypothesis import given, settings
from hypothesis.strategies import binary, integers

import pytest

from nacl import bindings as c
from nacl.exceptions import BadSignatureError, CryptoError, UnavailableError

from .test_signing import ed25519_known_answers
from .utils import flip_byte, read_crypto_test_vectors


def tohex(b: bytes) -> str:
    return hexlify(b).decode("ascii")


def test_hash():
    msg = b"message"
    h1 = c.crypto_hash(msg)
    assert len(h1) == c.crypto_hash_BYTES
    assert tohex(h1) == (
        "f8daf57a3347cc4d6b9d575b31fe6077"
        "e2cb487f60a96233c08cb479dbf31538"
        "cc915ec6d48bdbaa96ddc1a16db4f4f9"
        "6f37276cfcb3510b8246241770d5952c"
    )
    assert tohex(h1) == hashlib.sha512(msg).hexdigest()

    h2 = c.crypto_hash_sha512(msg)
    assert len(h2) == c.crypto_hash_sha512_BYTES
    assert tohex(h2) == tohex(h1)

    h3 = c.crypto_hash_sha256(msg)
    assert len(h3) == c.crypto_hash_sha256_BYTES
    assert tohex(h3) == (
        "ab530a13e45914982b79f9b7e3fba994cfd1f3fb22f71cea1afbf02b460c6d1d"
    )
    assert tohex(h3) == hashlib.sha256(msg).hexdigest()


def test_secretbox():
    key = b"\x00" * c.crypto_secretbox_KEYBYTES
    msg = b"message"
    nonce = b"\x01" * c.crypto_secretbox_NONCEBYTES
    ct = c.crypto_secretbox(msg, nonce, key)
    assert len(ct) == len(msg) + c.crypto_secretbox_BOXZEROBYTES
    assert tohex(ct) == "3ae84dfb89728737bd6e2c8cacbaf8af3d34cc1666533a"
    msg2 = c.crypto_secretbox_open(ct, nonce, key)
    assert msg2 == msg

    with pytest.raises(CryptoError):
        c.crypto_secretbox_open(
            msg + b"!",
            nonce,
            key,
        )


def test_secretbox_wrong_length():
    with pytest.raises(ValueError):
        c.crypto_secretbox(b"", b"", b"")
    with pytest.raises(ValueError):
        c.crypto_secretbox(b"", b"", b"\x00" * c.crypto_secretbox_KEYBYTES)
    with pytest.raises(ValueError):
        c.crypto_secretbox_open(b"", b"", b"")
    with pytest.raises(ValueError):
        c.crypto_secretbox_open(
            b"", b"", b"\x00" * c.crypto_secretbox_KEYBYTES
        )


def test_box():
    A_pubkey, A_secretkey = c.crypto_box_keypair()
    assert len(A_secretkey) == c.crypto_box_SECRETKEYBYTES
    assert len(A_pubkey) == c.crypto_box_PUBLICKEYBYTES
    B_pubkey, B_secretkey = c.crypto_box_keypair()

    k1 = c.crypto_box_beforenm(B_pubkey, A_secretkey)
    assert len(k1) == c.crypto_box_BEFORENMBYTES
    k2 = c.crypto_box_beforenm(A_pubkey, B_secretkey)
    assert tohex(k1) == tohex(k2)

    message = b"message"
    nonce = b"\x01" * c.crypto_box_NONCEBYTES
    ct1 = c.crypto_box_afternm(message, nonce, k1)
    assert len(ct1) == len(message) + c.crypto_box_BOXZEROBYTES

    ct2 = c.crypto_box(message, nonce, B_pubkey, A_secretkey)
    assert tohex(ct2) == tohex(ct1)

    m1 = c.crypto_box_open(ct1, nonce, A_pubkey, B_secretkey)
    assert m1 == message

    m2 = c.crypto_box_open_afternm(ct1, nonce, k1)
    assert m2 == message

    with pytest.raises(CryptoError):
        c.crypto_box_open(message + b"!", nonce, A_pubkey, A_secretkey)


def test_box_wrong_lengths():
    A_pubkey, A_secretkey = c.crypto_box_keypair()
    with pytest.raises(ValueError):
        c.crypto_box(b"abc", b"\x00", A_pubkey, A_secretkey)
    with pytest.raises(ValueError):
        c.crypto_box(
            b"abc", b"\x00" * c.crypto_box_NONCEBYTES, b"", A_secretkey
        )
    with pytest.raises(ValueError):
        c.crypto_box(b"abc", b"\x00" * c.crypto_box_NONCEBYTES, A_pubkey, b"")

    with pytest.raises(ValueError):
        c.crypto_box_open(b"", b"", b"", b"")
    with pytest.raises(ValueError):
        c.crypto_box_open(b"", b"\x00" * c.crypto_box_NONCEBYTES, b"", b"")
    with pytest.raises(ValueError):
        c.crypto_box_open(
            b"", b"\x00" * c.crypto_box_NONCEBYTES, A_pubkey, b""
        )

    with pytest.raises(ValueError):
        c.crypto_box_beforenm(b"", b"")
    with pytest.raises(ValueError):
        c.crypto_box_beforenm(A_pubkey, b"")

    with pytest.raises(ValueError):
        c.crypto_box_afternm(b"", b"", b"")
    with pytest.raises(ValueError):
        c.crypto_box_afternm(b"", b"\x00" * c.crypto_box_NONCEBYTES, b"")

    with pytest.raises(ValueError):
        c.crypto_box_open_afternm(b"", b"", b"")
    with pytest.raises(ValueError):
        c.crypto_box_open_afternm(b"", b"\x00" * c.crypto_box_NONCEBYTES, b"")


def test_sign():
    seed = b"\x00" * c.crypto_sign_SEEDBYTES
    pubkey, secretkey = c.crypto_sign_seed_keypair(seed)
    assert len(pubkey) == c.crypto_sign_PUBLICKEYBYTES
    assert len(secretkey) == c.crypto_sign_SECRETKEYBYTES

    pubkey, secretkey = c.crypto_sign_keypair()
    assert len(pubkey) == c.crypto_sign_PUBLICKEYBYTES
    assert len(secretkey) == c.crypto_sign_SECRETKEYBYTES

    msg = b"message"
    sigmsg = c.crypto_sign(msg, secretkey)
    assert len(sigmsg) == len(msg) + c.crypto_sign_BYTES

    msg2 = c.crypto_sign_open(sigmsg, pubkey)
    assert msg2 == msg


def test_sign_wrong_lengths():
    with pytest.raises(ValueError):
        c.crypto_sign_seed_keypair(b"")


def secret_scalar() -> Tuple[bytes, bytes]:
    pubkey, secretkey = c.crypto_box_keypair()
    assert len(secretkey) == c.crypto_box_SECRETKEYBYTES
    assert c.crypto_box_SECRETKEYBYTES == c.crypto_scalarmult_BYTES
    return secretkey, pubkey


def test_scalarmult():
    x, xpub = secret_scalar()
    assert len(x) == 32
    y, ypub = secret_scalar()
    # the Curve25519 base point (generator)
    base = unhexlify(b"09" + b"00" * 31)

    bx1 = c.crypto_scalarmult_base(x)
    bx2 = c.crypto_scalarmult(x, base)
    assert tohex(bx1) == tohex(bx2)
    assert tohex(bx1) == tohex(xpub)

    xby = c.crypto_scalarmult(x, c.crypto_scalarmult_base(y))
    ybx = c.crypto_scalarmult(y, c.crypto_scalarmult_base(x))
    assert tohex(xby) == tohex(ybx)

    z = unhexlify(b"10" * 32)
    bz1 = c.crypto_scalarmult_base(z)
    assert tohex(bz1) == (
        "781faab908430150daccdd6f9d6c5086e34f73a93ebbaa271765e5036edfc519"
    )
    bz2 = c.crypto_scalarmult(z, base)
    assert tohex(bz1) == tohex(bz2)


def test_sign_test_key_conversion():
    """
    Taken from test vectors in libsodium
    """
    keypair_seed = unhexlify(
        b"421151a459faeade3d247115f94aedae42318124095afabe4d1451a559faedee"
    )
    ed25519_pk, ed25519_sk = c.crypto_sign_seed_keypair(keypair_seed)

    assert c.crypto_sign_ed25519_sk_to_pk(ed25519_sk) == ed25519_pk
    with pytest.raises(ValueError):
        c.crypto_sign_ed25519_sk_to_pk(unhexlify(b"12"))

    assert c.crypto_sign_ed25519_sk_to_seed(ed25519_sk) == keypair_seed
    with pytest.raises(ValueError):
        c.crypto_sign_ed25519_sk_to_seed(unhexlify(b"12"))

    curve25519_pk = c.crypto_sign_ed25519_pk_to_curve25519(ed25519_pk)

    with pytest.raises(ValueError):
        c.crypto_sign_ed25519_pk_to_curve25519(unhexlify(b"12"))
    with pytest.raises(ValueError):
        c.crypto_sign_ed25519_sk_to_curve25519(unhexlify(b"12"))

    curve25519_sk = c.crypto_sign_ed25519_sk_to_curve25519(ed25519_sk)

    assert tohex(curve25519_pk) == (
        "f1814f0e8ff1043d8a44d25babff3cedcae6c22c3edaa48f857ae70de2baae50"
    )
    assert tohex(curve25519_sk) == (
        "8052030376d47112be7f73ed7a019293dd12ad910b654455798b4667d73de166"
    )


def test_box_seal_empty():
    A_pubkey, A_secretkey = c.crypto_box_keypair()
    empty = b""
    msg = c.crypto_box_seal(empty, A_pubkey)
    decoded = c.crypto_box_seal_open(msg, A_pubkey, A_secretkey)
    assert decoded == empty


def test_box_seal_empty_is_verified():
    A_pubkey, A_secretkey = c.crypto_box_keypair()
    empty = b""
    amsg = bytearray(c.crypto_box_seal(empty, A_pubkey))
    amsg[-1] ^= 1
    msg = bytes(amsg)
    with pytest.raises(CryptoError):
        c.crypto_box_seal_open(msg, A_pubkey, A_secretkey)


def test_box_seal_wrong_lengths():
    A_pubkey, A_secretkey = c.crypto_box_keypair()
    with pytest.raises(ValueError):
        c.crypto_box_seal(b"abc", A_pubkey[:-1])
    with pytest.raises(ValueError):
        c.crypto_box_seal_open(b"abc", b"", A_secretkey)
    with pytest.raises(ValueError):
        c.crypto_box_seal_open(b"abc", A_pubkey, A_secretkey[:-1])
    msg = c.crypto_box_seal(b"", A_pubkey)
    with pytest.raises(CryptoError):
        c.crypto_box_seal_open(msg[:-1], A_pubkey, A_secretkey)


def test_box_seal_wrong_types():
    A_pubkey, A_secretkey = c.crypto_box_keypair()
    # type safety: mypy can spot these errors, but we want to spot them at runtime too.
    with pytest.raises(TypeError):
        c.crypto_box_seal(b"abc", dict())  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        c.crypto_box_seal_open(b"abc", None, A_secretkey)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        c.crypto_box_seal_open(b"abc", A_pubkey, None)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        c.crypto_box_seal_open(None, A_pubkey, A_secretkey)  # type: ignore[arg-type]


def _box_from_seed_vectors() -> List[Tuple[bytes, bytes, bytes]]:
    # Fmt: <seed> <tab> <public_key> || <secret_key>
    DATA = "box_from_seed.txt"
    lines = read_crypto_test_vectors(DATA, maxels=2, delimiter=b"\t")
    return [
        (
            x[0],  # seed
            x[1][:64],  # derived public key
            x[1][64:],  # derived secret key
        )
        for x in lines
    ]


@pytest.mark.parametrize(
    ("seed", "public_key", "secret_key"), _box_from_seed_vectors()
)
def test_box_seed_keypair_reference(
    seed: bytes, public_key: bytes, secret_key: bytes
):
    seed = unhexlify(seed)
    pk, sk = c.crypto_box_seed_keypair(seed)
    assert pk == unhexlify(public_key)
    assert sk == unhexlify(secret_key)


def test_box_seed_keypair_random():
    seed = c.randombytes(c.crypto_box_SEEDBYTES)
    pk, sk = c.crypto_box_seed_keypair(seed)
    ppk = c.crypto_scalarmult_base(sk)
    assert pk == ppk


def test_box_seed_keypair_short_seed():
    seed = c.randombytes(c.crypto_box_SEEDBYTES - 1)
    with pytest.raises(ValueError):
        c.crypto_box_seed_keypair(seed)
    with pytest.raises(CryptoError):
        c.crypto_box_seed_keypair(seed)


@given(integers(min_value=-2, max_value=0))
def test_pad_wrong_blocksize(bl_sz):
    with pytest.raises(ValueError):
        c.sodium_pad(b"x", bl_sz)


def test_unpad_not_padded():
    with pytest.raises(CryptoError):
        c.sodium_unpad(b"x", 8)


@given(
    binary(min_size=0, max_size=2049), integers(min_value=16, max_value=256)
)
@settings(max_examples=20)
def test_pad_sizes(msg: bytes, bl_sz: int):
    padded = c.sodium_pad(msg, bl_sz)
    assert len(padded) > len(msg)
    assert len(padded) >= bl_sz
    assert len(padded) % bl_sz == 0


@given(
    binary(min_size=0, max_size=2049), integers(min_value=16, max_value=256)
)
@settings(max_examples=20)
def test_pad_roundtrip(msg: bytes, bl_sz: int):
    padded = c.sodium_pad(msg, bl_sz)
    assert len(padded) > len(msg)
    assert len(padded) >= bl_sz
    assert len(padded) % bl_sz == 0
    unpadded = c.sodium_unpad(padded, bl_sz)
    assert len(unpadded) == len(msg)
    assert unpadded == msg


def test_sodium_increment():
    maxint = 32 * b"\xff"
    zero = 32 * b"\x00"
    one = b"\x01" + 31 * b"\x00"
    two = b"\x02" + 31 * b"\x00"

    res = c.sodium_increment(maxint)
    assert res == zero

    res = c.sodium_increment(res)
    assert res == one

    res = c.sodium_increment(res)
    assert res == two


def test_sodium_add():
    maxint = 32 * b"\xff"
    zero = 32 * b"\x00"
    one = b"\x01" + 31 * b"\x00"
    short_one = b"\x01" + 15 * b"\x00"
    two = b"\x02" + 31 * b"\x00"
    three = b"\x03" + 31 * b"\x00"
    four = b"\x04" + 31 * b"\x00"

    res = c.sodium_add(one, two)
    assert res == three

    res = c.sodium_add(maxint, four)
    assert res == three

    res = c.sodium_add(one, maxint)
    assert res == zero

    with pytest.raises(TypeError):
        res = c.sodium_add(short_one, two)


def test_sign_ed25519ph_rfc8032():
    # sk, pk, msg, exp_sig
    # taken from RFC 8032 section 7.3.  Test Vectors for Ed25519ph
    sk = unhexlify(
        b"833fe62409237b9d62ec77587520911e9a759cec1d19755b7da901b96dca3d42"
    )
    pk = unhexlify(
        b"ec172b93ad5e563bf4932c70e1245034c35467ef2efd4d64ebf819683467e2bf"
    )
    msg = b"abc"
    exp_sig = unhexlify(
        b"98a70222f0b8121aa9d30f813d683f80"
        b"9e462b469c7ff87639499bb94e6dae41"
        b"31f85042463c2a355a2003d062adf5aa"
        b"a10b8c61e636062aaad11c2a26083406"
    )
    c_sk = sk + pk

    edph = c.crypto_sign_ed25519ph_state()
    c.crypto_sign_ed25519ph_update(edph, msg)
    sig = c.crypto_sign_ed25519ph_final_create(edph, c_sk)

    assert sig == exp_sig

    edph_v = c.crypto_sign_ed25519ph_state()
    c.crypto_sign_ed25519ph_update(edph_v, msg)

    assert c.crypto_sign_ed25519ph_final_verify(edph_v, exp_sig, pk) is True

    c.crypto_sign_ed25519ph_update(edph_v, msg)

    with pytest.raises(BadSignatureError):
        c.crypto_sign_ed25519ph_final_verify(edph_v, exp_sig, pk)


def test_sign_ed25519ph_libsodium():
    #
    _hsk, _hpk, hmsg, _hsig, _hsigmsg = ed25519_known_answers()[-1]

    msg = unhexlify(hmsg)

    seed = unhexlify(
        b"421151a459faeade3d247115f94aedae42318124095afabe4d1451a559faedee"
    )

    pk, sk = c.crypto_sign_seed_keypair(seed)

    exp_sig = unhexlify(
        b"10c5411e40bd10170fb890d4dfdb6d33"
        b"8c8cb11d2764a216ee54df10977dcdef"
        b"d8ff755b1eeb3f16fce80e40e7aafc99"
        b"083dbff43d5031baf04157b48423960d"
    )

    edph = c.crypto_sign_ed25519ph_state()
    c.crypto_sign_ed25519ph_update(edph, msg)
    sig = c.crypto_sign_ed25519ph_final_create(edph, sk)

    assert sig == exp_sig

    edph_incr = c.crypto_sign_ed25519ph_state()
    c.crypto_sign_ed25519ph_update(edph_incr, b"")
    c.crypto_sign_ed25519ph_update(edph_incr, msg[0 : len(msg) // 2])
    c.crypto_sign_ed25519ph_update(edph_incr, msg[len(msg) // 2 :])

    assert c.crypto_sign_ed25519ph_final_verify(edph_incr, exp_sig, pk) is True

    with pytest.raises(BadSignatureError):
        wrng_sig = flip_byte(exp_sig, 0)
        c.crypto_sign_ed25519ph_final_verify(edph_incr, wrng_sig, pk)

    with pytest.raises(BadSignatureError):
        wrng_mesg = flip_byte(msg, 1022)
        edph_wrng = c.crypto_sign_ed25519ph_state()
        c.crypto_sign_ed25519ph_update(edph_wrng, wrng_mesg)
        c.crypto_sign_ed25519ph_final_verify(edph_wrng, exp_sig, pk)


@pytest.mark.skipif(
    not c.has_crypto_core_ed25519, reason="Requires full build of libsodium"
)
def test_ed25519_is_valid_point():
    """
    Verify crypto_core_ed25519_is_valid_point correctly rejects
    the all-zeros "point"
    """
    zero = c.crypto_core_ed25519_BYTES * b"\x00"
    res = c.crypto_core_ed25519_is_valid_point(zero)
    assert res is False


@pytest.mark.skipif(
    not c.has_crypto_core_ed25519, reason="Requires full build of libsodium"
)
def test_ed25519_add_and_sub():
    # the public component of a ed25519 keypair
    # is a point on the ed25519 curve
    p1, _s1 = c.crypto_sign_keypair()
    p2, _s2 = c.crypto_sign_keypair()

    p3 = c.crypto_core_ed25519_add(p1, p2)

    assert c.crypto_core_ed25519_is_valid_point(p3) is True
    assert c.crypto_core_ed25519_sub(p3, p1) == p2
    assert c.crypto_core_ed25519_sub(p3, p2) == p1


@pytest.mark.skipif(
    not c.has_crypto_core_ed25519 or not c.has_crypto_scalarmult_ed25519,
    reason="Requires full build of libsodium",
)
def test_scalarmult_ed25519():
    SCALARBYTES = c.crypto_scalarmult_ed25519_SCALARBYTES

    # the minimum ed25519 scalar is represented by a 8 value in the
    # first octet, a 64 value in the last octet, and all zeros
    # in between:
    MINSC = bytes(bytearray([8] + (SCALARBYTES - 2) * [0] + [64]))

    # the scalar multiplication formula for ed25519
    # "clamps" the scalar by setting the most significant bit
    # of the last octet to zero, therefore scalar multiplication
    # by CLMPD is equivalent to scalar multiplication by MINSC
    CLMPD = bytes(bytearray([8] + (SCALARBYTES - 2) * [0] + [192]))

    MIN_P1 = bytes(bytearray([9] + (SCALARBYTES - 2) * [0] + [64]))
    MIN_P7 = bytes(bytearray([15] + (SCALARBYTES - 2) * [0] + [64]))
    MIN_P8 = bytes(bytearray([16] + (SCALARBYTES - 2) * [0] + [64]))

    p, _s = c.crypto_sign_keypair()
    _p = p

    for i in range(254):
        # double _p
        _p = c.crypto_core_ed25519_add(_p, _p)

    for i in range(8):
        _p = c.crypto_core_ed25519_add(_p, p)

    # at this point _p is (2^254+8) times p

    assert c.crypto_scalarmult_ed25519(MINSC, p) == _p
    assert c.crypto_scalarmult_ed25519(CLMPD, p) == _p

    # ed25519 scalar multiplication sets the least three significant
    # bits of the first octet to zero; therefore:
    assert c.crypto_scalarmult_ed25519(MIN_P1, p) == _p
    assert c.crypto_scalarmult_ed25519(MIN_P7, p) == _p

    _p8 = _p
    for i in range(8):
        _p8 = c.crypto_core_ed25519_add(_p8, p)

    # at this point _p is (2^254 + 16) times p

    assert c.crypto_scalarmult_ed25519(MIN_P8, p) == _p8


@pytest.mark.skipif(
    not c.has_crypto_scalarmult_ed25519,
    reason="Requires full build of libsodium",
)
def test_scalarmult_ed25519_base():
    """
    Verify scalarmult_ed25519_base is congruent to
    scalarmult_ed25519 on the ed25519 base point
    """

    BASEPOINT = bytes(
        bytearray(
            [
                0x58,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
            ]
        )
    )

    sclr = c.randombytes(c.crypto_scalarmult_ed25519_SCALARBYTES)

    p = c.crypto_scalarmult_ed25519_base(sclr)
    p2 = c.crypto_scalarmult_ed25519(sclr, BASEPOINT)

    assert p2 == p


@pytest.mark.skipif(
    not c.has_crypto_scalarmult_ed25519,
    reason="Requires full build of libsodium",
)
def test_scalarmult_ed25519_noclamp():
    # An arbitrary scalar which is known to differ once clamped
    scalar = 32 * b"\x01"
    BASEPOINT = bytes(
        bytearray(
            [
                0x58,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
                0x66,
            ]
        )
    )

    p = c.crypto_scalarmult_ed25519_noclamp(scalar, BASEPOINT)
    pb = c.crypto_scalarmult_ed25519_base_noclamp(scalar)
    pc = c.crypto_scalarmult_ed25519_base(scalar)
    assert p == pb
    assert pb != pc

    # clamp manually
    ba = bytearray(scalar)
    ba0 = bytes(bytearray([ba[0] & 248]))
    ba31 = bytes(bytearray([(ba[31] & 127) | 64]))
    scalar_clamped = ba0 + bytes(ba[1:31]) + ba31

    p1 = c.crypto_scalarmult_ed25519_noclamp(scalar_clamped, BASEPOINT)
    p2 = c.crypto_scalarmult_ed25519(scalar, BASEPOINT)
    assert p1 == p2


@pytest.mark.skipif(
    not c.has_crypto_core_ed25519, reason="Requires full build of libsodium"
)
def test_ed25519_scalar_add_and_sub():
    zero = 32 * b"\x00"
    one = b"\x01" + 31 * b"\x00"
    two = b"\x02" + 31 * b"\x00"
    # the max integer over l, the order of the main subgroup
    # 2^252+27742317777372353535851937790883648493 - 1
    max = bytes(
        bytearray(
            [
                0xEC,
                0xD3,
                0xF5,
                0x5C,
                0x1A,
                0x63,
                0x12,
                0x58,
                0xD6,
                0x9C,
                0xF7,
                0xA2,
                0xDE,
                0xF9,
                0xDE,
                0x14,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x10,
            ]
        )
    )

    p1 = c.crypto_core_ed25519_scalar_add(two, max)
    assert p1 == one

    p2 = c.crypto_core_ed25519_scalar_sub(p1, p1)
    assert p2 == zero

    p3 = c.crypto_core_ed25519_scalar_sub(p2, one)
    assert p3 == max


@pytest.mark.skipif(
    not c.has_crypto_core_ed25519, reason="Requires full build of libsodium"
)
def test_ed25519_scalar_mul():
    zero = 32 * b"\x00"
    three = b"\x03" + 31 * b"\x00"

    # random scalar modulo l
    sclr = c.randombytes(c.crypto_core_ed25519_SCALARBYTES)
    p = c.crypto_core_ed25519_scalar_add(sclr, zero)

    p3 = c.crypto_core_ed25519_scalar_mul(p, three)
    p2 = c.crypto_core_ed25519_scalar_add(p, p)
    p1 = c.crypto_core_ed25519_scalar_sub(p3, p2)

    assert p1 == p


@pytest.mark.skipif(
    not c.has_crypto_core_ed25519, reason="Requires full build of libsodium"
)
def test_ed25519_scalar_invert_negate_complement():
    zero = 32 * b"\x00"
    one = b"\x01" + 31 * b"\x00"

    # random scalar modulo l
    sclr = c.randombytes(c.crypto_core_ed25519_SCALARBYTES)
    sclr = c.crypto_core_ed25519_scalar_add(sclr, zero)

    i = c.crypto_core_ed25519_scalar_invert(sclr)
    assert c.crypto_core_ed25519_scalar_mul(sclr, i) == one

    n = c.crypto_core_ed25519_scalar_negate(sclr)
    assert c.crypto_core_ed25519_scalar_add(sclr, n) == zero

    cp = c.crypto_core_ed25519_scalar_complement(sclr)
    assert c.crypto_core_ed25519_scalar_add(sclr, cp) == one


@pytest.mark.skipif(
    not c.has_crypto_core_ed25519, reason="Requires full build of libsodium"
)
def test_ed25519_scalar_reduce():
    zero = 32 * b"\x00"
    # 65536 times the order of the main subgroup (which is bigger
    # than 32 bytes), padded to 64 bytes
    # 2^252+27742317777372353535851937790883648493
    l65536 = (
        bytes(2 * b"\x00")
        + bytes(
            bytearray(
                [
                    0xED,
                    0xD3,
                    0xF5,
                    0x5C,
                    0x1A,
                    0x63,
                    0x12,
                    0x58,
                    0xD6,
                    0x9C,
                    0xF7,
                    0xA2,
                    0xDE,
                    0xF9,
                    0xDE,
                    0x14,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x10,
                ]
            )
        )
        + bytes(30 * b"\x00")
    )

    # random scalar modulo l
    sclr = c.randombytes(c.crypto_core_ed25519_SCALARBYTES)
    p = c.crypto_core_ed25519_scalar_add(sclr, zero)

    # l65536 + p is bigger than 32 bytes
    big = c.sodium_add(l65536, p + bytes(32 * b"\x00"))

    r = c.crypto_core_ed25519_scalar_reduce(big)
    assert r == p


@pytest.mark.skipif(
    c.has_crypto_core_ed25519, reason="Requires minimal build of libsodium"
)
def test_ed25519_unavailable():
    zero = 32 * b"\x00"

    with pytest.raises(UnavailableError):
        c.crypto_core_ed25519_is_valid_point(zero)
    with pytest.raises(UnavailableError):
        c.crypto_core_ed25519_add(zero, zero)
    with pytest.raises(UnavailableError):
        c.crypto_core_ed25519_sub(zero, zero)

    with pytest.raises(UnavailableError):
        c.crypto_core_ed25519_scalar_invert(zero)
    with pytest.raises(UnavailableError):
        c.crypto_core_ed25519_scalar_negate(zero)
    with pytest.raises(UnavailableError):
        c.crypto_core_ed25519_scalar_complement(zero)
    with pytest.raises(UnavailableError):
        c.crypto_core_ed25519_scalar_add(zero, zero)
    with pytest.raises(UnavailableError):
        c.crypto_core_ed25519_scalar_sub(zero, zero)
    with pytest.raises(UnavailableError):
        c.crypto_core_ed25519_scalar_mul(zero, zero)
    with pytest.raises(UnavailableError):
        c.crypto_core_ed25519_scalar_reduce(zero)


@pytest.mark.skipif(
    c.has_crypto_scalarmult_ed25519,
    reason="Requires minimal build of libsodium",
)
def test_scalarmult_ed25519_unavailable():
    zero = 32 * b"\x00"

    with pytest.raises(UnavailableError):
        c.crypto_scalarmult_ed25519_base(zero)
    with pytest.raises(UnavailableError):
        c.crypto_scalarmult_ed25519_base_noclamp(zero)
    with pytest.raises(UnavailableError):
        c.crypto_scalarmult_ed25519(zero, zero)
    with pytest.raises(UnavailableError):
        c.crypto_scalarmult_ed25519_noclamp(zero, zero)
