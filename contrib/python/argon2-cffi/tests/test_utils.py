# SPDX-License-Identifier: MIT

from base64 import b64encode
from dataclasses import replace

import pytest

from hypothesis import given
from hypothesis import strategies as st

from argon2 import Parameters, Type, extract_parameters
from argon2._utils import NoneType, _check_types, _decoded_str_len
from argon2.exceptions import InvalidHashError


class TestCheckTypes:
    def test_success(self):
        """
        Returns None if all types are okay.
        """
        assert None is _check_types(
            bytes=(b"bytes", bytes),
            tuple=((1, 2), tuple),
            str_or_None=(None, (str, NoneType)),
        )

    def test_fail(self):
        """
        Returns summary of failures.
        """
        rv = _check_types(
            bytes=("not bytes", bytes), str_or_None=(42, (str, NoneType))
        )

        assert "." == rv[-1]  # proper grammar FTW
        assert "'str_or_None' must be a str, or NoneType (got int)" in rv

        assert "'bytes' must be a bytes (got str)" in rv


@given(st.binary())
def test_decoded_str_len(bs):
    """
    _decoded_str_len computes the resulting length.
    """
    assert len(bs) == _decoded_str_len(len(b64encode(bs).rstrip(b"=")))


VALID_HASH = (
    "$argon2id$v=19$m=65536,t=2,p=4$"
    "c29tZXNhbHQ$GpZ3sK/oH9p7VIiV56G/64Zo/8GaUw434IimaPqxwCo"
)
VALID_PARAMETERS = Parameters(
    type=Type.ID,
    salt_len=8,
    hash_len=32,
    version=19,
    memory_cost=65536,
    time_cost=2,
    parallelism=4,
)

VALID_HASH_V18 = (
    "$argon2i$m=8,t=1,p=1$c29tZXNhbHQ$gwQOXSNhxiOxPOA0+PY10P9QFO"
    "4NAYysnqRt1GSQLE55m+2GYDt9FEjPMHhP2Cuf0nOEXXMocVrsJAtNSsKyfg"
)
VALID_PARAMETERS_V18 = Parameters(
    type=Type.I,
    salt_len=8,
    hash_len=64,
    version=18,
    memory_cost=8,
    time_cost=1,
    parallelism=1,
)


class TestExtractParameters:
    def test_valid_hash(self):
        """
        A valid hash is parsed.
        """
        parsed = extract_parameters(VALID_HASH)

        assert VALID_PARAMETERS == parsed

    def test_valid_hash_v18(self):
        """
        A valid Argon v1.2 hash is parsed.
        """

        parsed = extract_parameters(VALID_HASH_V18)

        assert VALID_PARAMETERS_V18 == parsed

    @pytest.mark.parametrize(
        "hash",
        [
            "",
            "abc" + VALID_HASH,
            VALID_HASH.replace("p=4", "p=four"),
            VALID_HASH.replace(",p=4", ""),
        ],
    )
    def test_invalid_hash(self, hash):
        """
        Invalid hashes of various types raise an InvalidHash error.
        """
        with pytest.raises(InvalidHashError):
            extract_parameters(hash)


class TestParameters:
    def test_eq(self):
        """
        Parameters are equal iff every attribute is equal.
        """
        assert VALID_PARAMETERS == VALID_PARAMETERS  # noqa: PLR0124
        assert VALID_PARAMETERS != replace(VALID_PARAMETERS, salt_len=9)

    def test_eq_wrong_type(self):
        """
        Parameters are only compared if they have the same type.
        """
        assert VALID_PARAMETERS != "foo"
        assert VALID_PARAMETERS != object()

    def test_repr(self):
        """
        __repr__ returns s ensible string.
        """
        assert repr(
            Parameters(
                type=Type.ID,
                salt_len=8,
                hash_len=32,
                version=19,
                memory_cost=65536,
                time_cost=2,
                parallelism=4,
            )
        ) == (
            "Parameters(type=<Type.ID: 2>, version=19, salt_len=8, "
            "hash_len=32, time_cost=2, memory_cost=65536, parallelism=4)"
        )
