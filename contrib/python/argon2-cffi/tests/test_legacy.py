# SPDX-License-Identifier: MIT

import pytest

from hypothesis import given
from hypothesis import strategies as st

from argon2 import (
    DEFAULT_RANDOM_SALT_LENGTH,
    Type,
    hash_password,
    hash_password_raw,
    verify_password,
)
from argon2.exceptions import HashingError, VerificationError

from .test_low_level import (
    TEST_HASH_I,
    TEST_HASH_LEN,
    TEST_MEMORY,
    TEST_PARALLELISM,
    TEST_PASSWORD,
    TEST_SALT,
    TEST_TIME,
    i_and_d_encoded,
    i_and_d_raw,
)


class TestHash:
    def test_hash_defaults(self):
        """
        Calling without arguments works.
        """
        with pytest.deprecated_call(
            match="argon2.hash_password is deprecated"
        ) as dc:
            hash_password(b"secret")

        assert dc.pop().filename.endswith("test_legacy.py")

    def test_raw_defaults(self):
        """
        Calling without arguments works.
        """
        with pytest.deprecated_call(
            match="argon2.hash_password_raw is deprecated"
        ) as dc:
            hash_password_raw(b"secret")

        assert dc.pop().filename.endswith("test_legacy.py")

    @i_and_d_encoded
    def test_hash_password(self, type, hash):
        """
        Creates the same encoded hash as the Argon2 CLI client.
        """
        with pytest.deprecated_call(
            match="argon2.hash_password is deprecated"
        ):
            rv = hash_password(
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
    def test_hash_password_raw(self, type, hash):
        """
        Creates the same raw hash as the Argon2 CLI client.
        """
        with pytest.deprecated_call(
            match="argon2.hash_password_raw is deprecated"
        ):
            rv = hash_password_raw(
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
        Hashing passwords with NUL bytes works as expected.
        """
        with pytest.deprecated_call(
            match="argon2.hash_password_raw is deprecated"
        ):
            rv = hash_password_raw(b"abc\x00", TEST_SALT)

        with pytest.deprecated_call(
            match="argon2.hash_password_raw is deprecated"
        ):
            assert rv != hash_password_raw(b"abc", TEST_SALT)

    def test_random_salt(self):
        """
        Omitting a salt, creates a random one.
        """
        with pytest.deprecated_call(
            match="argon2.hash_password is deprecated"
        ):
            rv = hash_password(b"secret")
        salt = rv.split(b",")[-1].split(b"$")[1]

        assert (
            # -1 for not NUL byte
            int((DEFAULT_RANDOM_SALT_LENGTH << 2) / 3 + 2) - 1 == len(salt)
        )

    def test_hash_wrong_arg_type(self):
        """
        Passing an argument of wrong type raises TypeError.
        """
        with pytest.deprecated_call(
            match="argon2.hash_password is deprecated"
        ), pytest.raises(TypeError):
            hash_password("oh no, unicode!")

    def test_illegal_argon2_parameter(self):
        """
        Raises HashingError if hashing fails.
        """
        with pytest.deprecated_call(
            match="argon2.hash_password is deprecated"
        ), pytest.raises(HashingError):
            hash_password(TEST_PASSWORD, memory_cost=1)

    @given(st.binary(max_size=128))
    def test_hash_fast(self, password):
        """
        Hash various passwords as cheaply as possible.
        """
        with pytest.deprecated_call(
            match="argon2.hash_password is deprecated"
        ):
            hash_password(
                password,
                salt=b"12345678",
                time_cost=1,
                memory_cost=8,
                parallelism=1,
                hash_len=8,
            )


class TestVerify:
    @i_and_d_encoded
    def test_success(self, type, hash):
        """
        Given a valid hash and password and correct type, we succeed.
        """
        with pytest.deprecated_call(
            match="argon2.verify_password is deprecated"
        ) as dc:
            assert True is verify_password(hash, TEST_PASSWORD, type)

        assert dc.pop().filename.endswith("test_legacy.py")

    def test_fail_wrong_argon2_type(self):
        """
        Given a valid hash and password and wrong type, we fail.
        """
        with pytest.deprecated_call(
            match="argon2.verify_password is deprecated"
        ), pytest.raises(VerificationError):
            verify_password(TEST_HASH_I, TEST_PASSWORD, Type.D)

    def test_wrong_arg_type(self):
        """
        Passing an argument of wrong type raises TypeError.
        """
        with pytest.deprecated_call(
            match="argon2.verify_password is deprecated"
        ), pytest.raises(TypeError):
            verify_password(TEST_HASH_I, TEST_PASSWORD.decode("ascii"))
