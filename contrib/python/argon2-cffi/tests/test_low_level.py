# SPDX-License-Identifier: MIT

import binascii
import os

import pytest

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from argon2.exceptions import (
    HashingError,
    VerificationError,
    VerifyMismatchError,
)
from argon2.low_level import (
    ARGON2_VERSION,
    Type,
    core,
    ffi,
    hash_secret,
    hash_secret_raw,
    lib,
    verify_secret,
)


# Example data obtained using the official Argon2 CLI client:
#
# $ echo -n "password" | ./argon2 somesalt -t 2 -m 16 -p 4
# Type:		Argon2i
# Iterations:	2
# Memory:		65536 KiB
# Parallelism:	4
# Hash:		20c8adf6a90550b08c03f5628b32f9edc9d32ce6b90e254cf5e330a40bcfc2be
# Encoded:	$argon2i$v=19$m=65536,t=2,p=4$
#           c29tZXNhbHQ$IMit9qkFULCMA/ViizL57cnTLOa5DiVM9eMwpAvPwr4
# 0.120 seconds
# Verification ok
#
# $ echo -n "password" | ./argon2 somesalt -t 2 -m 16 -p 4 -d
# Type:		Argon2d
# Iterations:	2
# Memory:		65536 KiB
# Parallelism:	4
# Hash:		7199f977eac587e65fb91866da21941a072b5b960b78ceaaecbdef06c766140d
# Encoded:	$argon2d$v=19$m=65536,t=2,p=4$
#           c29tZXNhbHQ$cZn5d+rFh+ZfuRhm2iGUGgcrW5YLeM6q7L3vBsdmFA0
# 0.119 seconds
# Verification ok
#
# Type:		Argon2id
# Iterations:	2
# Memory:		65536 KiB
# Parallelism:	4
# Hash:		1a9677b0afe81fda7b548895e7a1bfeb8668ffc19a530e37e088a668fab1c02a
# Encoded:	$argon2id$v=19$m=65536,t=2,p=4$
#           c29tZXNhbHQ$GpZ3sK/oH9p7VIiV56G/64Zo/8GaUw434IimaPqxwCo
# 0.154 seconds
# Verification ok

TEST_HASH_I_OLD = (
    b"$argon2i$m=65536,t=2,p=4"
    b"$c29tZXNhbHQAAAAAAAAAAA"
    b"$QWLzI4TY9HkL2ZTLc8g6SinwdhZewYrzz9zxCo0bkGY"
)  # a v1.2 hash without a version tag
TEST_HASH_I = (
    b"$argon2i$v=19$m=65536,t=2,p=4$"
    b"c29tZXNhbHQ$IMit9qkFULCMA/ViizL57cnTLOa5DiVM9eMwpAvPwr4"
)
TEST_HASH_D = (
    b"$argon2d$v=19$m=65536,t=2,p=4$"
    b"c29tZXNhbHQ$cZn5d+rFh+ZfuRhm2iGUGgcrW5YLeM6q7L3vBsdmFA0"
)
TEST_HASH_ID = (
    b"$argon2id$v=19$m=65536,t=2,p=4$"
    b"c29tZXNhbHQ$GpZ3sK/oH9p7VIiV56G/64Zo/8GaUw434IimaPqxwCo"
)
TEST_RAW_I = binascii.unhexlify(
    b"20c8adf6a90550b08c03f5628b32f9edc9d32ce6b90e254cf5e330a40bcfc2be"
)
TEST_RAW_D = binascii.unhexlify(
    b"7199f977eac587e65fb91866da21941a072b5b960b78ceaaecbdef06c766140d"
)
TEST_RAW_ID = binascii.unhexlify(
    b"1a9677b0afe81fda7b548895e7a1bfeb8668ffc19a530e37e088a668fab1c02a"
)

TEST_PASSWORD = b"password"
TEST_SALT_LEN = 16
TEST_SALT = b"somesalt"
TEST_TIME = 2
TEST_MEMORY = 65536
TEST_PARALLELISM = 4
TEST_HASH_LEN = 32

i_and_d_encoded = pytest.mark.parametrize(
    ("type", "hash"),
    [(Type.I, TEST_HASH_I), (Type.D, TEST_HASH_D), (Type.ID, TEST_HASH_ID)],
)
i_and_d_raw = pytest.mark.parametrize(
    ("type", "hash"),
    [(Type.I, TEST_RAW_I), (Type.D, TEST_RAW_D), (Type.ID, TEST_RAW_ID)],
)

both_hash_funcs = pytest.mark.parametrize(
    "func", [hash_secret, hash_secret_raw]
)


class TestHash:
    @i_and_d_encoded
    def test_hash_secret(self, type, hash):
        """
        Creates the same encoded hash as the Argon2 CLI client.
        """
        rv = hash_secret(
            TEST_PASSWORD,
            TEST_SALT,
            TEST_TIME,
            TEST_MEMORY,
            TEST_PARALLELISM,
            TEST_HASH_LEN,
            type,
        )

        assert hash == rv
        assert isinstance(rv, bytes)

    @i_and_d_raw
    def test_hash_secret_raw(self, type, hash):
        """
        Creates the same raw hash as the Argon2 CLI client.
        """
        rv = hash_secret_raw(
            TEST_PASSWORD,
            TEST_SALT,
            TEST_TIME,
            TEST_MEMORY,
            TEST_PARALLELISM,
            TEST_HASH_LEN,
            type,
        )

        assert hash == rv
        assert isinstance(rv, bytes)

    def test_hash_nul_bytes(self):
        """
        Hashing secrets with NUL bytes works as expected.
        """
        params = (
            TEST_SALT,
            TEST_TIME,
            TEST_MEMORY,
            TEST_PARALLELISM,
            TEST_HASH_LEN,
            Type.I,
        )
        rv = hash_secret_raw(b"abc\x00", *params)

        assert rv != hash_secret_raw(b"abc", *params)

    @both_hash_funcs
    def test_hash_wrong_arg_type(self, func):
        """
        Passing an argument of wrong type raises TypeError.
        """
        with pytest.raises(TypeError):
            func("oh no, unicode!")

    @both_hash_funcs
    def test_illegal_argon2_parameter(self, func):
        """
        Raises HashingError if hashing fails.
        """
        with pytest.raises(HashingError):
            func(
                TEST_PASSWORD,
                TEST_SALT,
                TEST_TIME,
                1,
                TEST_PARALLELISM,
                TEST_HASH_LEN,
                Type.I,
            )

    @given(
        st.sampled_from((hash_secret, hash_secret_raw)),
        st.binary(max_size=128),
    )
    def test_hash_fast(self, func, secret):
        """
        Hash various secrets as cheaply as possible.
        """
        func(
            secret,
            salt=b"12345678",
            time_cost=1,
            memory_cost=8,
            parallelism=1,
            hash_len=8,
            type=Type.I,
        )


class TestVerify:
    @i_and_d_encoded
    def test_success(self, type, hash):
        """
        Given a valid hash and secret and correct type, we succeed.
        """
        assert True is verify_secret(hash, TEST_PASSWORD, type)

    @i_and_d_encoded
    def test_fail(self, type, hash):
        """
        Wrong password fails.
        """
        with pytest.raises(VerifyMismatchError):
            verify_secret(hash, bytes(reversed(TEST_PASSWORD)), type)

    def test_fail_wrong_argon2_type(self):
        """
        Given a valid hash and secret and wrong type, we fail.
        """
        verify_secret(TEST_HASH_D, TEST_PASSWORD, Type.D)
        verify_secret(TEST_HASH_I, TEST_PASSWORD, Type.I)
        with pytest.raises(VerificationError):
            verify_secret(TEST_HASH_I, TEST_PASSWORD, Type.D)

    def test_wrong_arg_type(self):
        """
        Passing an argument of wrong type raises TypeError.
        """
        with pytest.raises(TypeError) as e:
            verify_secret(TEST_HASH_I, TEST_PASSWORD.decode("ascii"), Type.I)

        assert e.value.args[0].startswith(
            "initializer for ctype 'uint8_t[]' must be a"
        )

    def test_old_hash(self):
        """
        Hashes without a version tag are recognized and verified correctly.
        """
        assert True is verify_secret(TEST_HASH_I_OLD, TEST_PASSWORD, Type.I)


@given(
    password=st.binary(min_size=lib.ARGON2_MIN_PWD_LENGTH, max_size=65),
    time_cost=st.integers(lib.ARGON2_MIN_TIME, 3),
    parallelism=st.integers(lib.ARGON2_MIN_LANES, 5),
    memory_cost=st.integers(0, 1025),
    hash_len=st.integers(lib.ARGON2_MIN_OUTLEN, 513),
    salt_len=st.integers(lib.ARGON2_MIN_SALT_LENGTH, 513),
)
@settings(deadline=None)
def test_argument_ranges(
    password, time_cost, parallelism, memory_cost, hash_len, salt_len
):
    """
    Ensure that both hashing and verifying works for most combinations of legal
    values.

    Limits are intentionally chosen to be *not* on 2^x boundaries.

    This test is rather slow.
    """
    assume(parallelism * 8 <= memory_cost)
    hash = hash_secret(
        secret=password,
        salt=os.urandom(salt_len),
        time_cost=time_cost,
        parallelism=parallelism,
        memory_cost=memory_cost,
        hash_len=hash_len,
        type=Type.I,
    )
    assert verify_secret(hash, password, Type.I)


def test_core():
    """
    If called with equal parameters, core() will return the same as
    hash_secret().
    """
    pwd = b"secret"
    salt = b"12345678"
    hash_len = 8

    # Keep FFI objects alive throughout the function.
    cout = ffi.new("uint8_t[]", hash_len)
    cpwd = ffi.new("uint8_t[]", pwd)
    csalt = ffi.new("uint8_t[]", salt)

    ctx = ffi.new(
        "argon2_context *",
        {
            "out": cout,
            "outlen": hash_len,
            "version": ARGON2_VERSION,
            "pwd": cpwd,
            "pwdlen": len(pwd),
            "salt": csalt,
            "saltlen": len(salt),
            "secret": ffi.NULL,
            "secretlen": 0,
            "ad": ffi.NULL,
            "adlen": 0,
            "t_cost": 1,
            "m_cost": 8,
            "lanes": 1,
            "threads": 1,
            "allocate_cbk": ffi.NULL,
            "free_cbk": ffi.NULL,
            "flags": lib.ARGON2_DEFAULT_FLAGS,
        },
    )

    rv = core(ctx, Type.D.value)

    assert 0 == rv
    assert hash_secret_raw(
        pwd,
        salt=salt,
        time_cost=1,
        memory_cost=8,
        parallelism=1,
        hash_len=hash_len,
        type=Type.D,
    ) == bytes(ffi.buffer(ctx.out, ctx.outlen))
