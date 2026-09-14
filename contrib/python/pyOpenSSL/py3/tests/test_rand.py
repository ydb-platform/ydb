# Copyright (c) Frederick Dean
# See LICENSE for details.

"""
Unit tests for `OpenSSL.rand`.
"""

from __future__ import annotations

import pytest

from OpenSSL import rand


class TestRand:
    @pytest.mark.parametrize("args", [(b"foo", None), (None, 3)])
    def test_add_wrong_args(self, args: tuple[object, object]) -> None:
        """
        `OpenSSL.rand.add` raises `TypeError` if called with arguments not of
        type `str` and `int`.
        """
        with pytest.raises(TypeError):
            rand.add(*args)  # type: ignore[arg-type]

    def test_add(self) -> None:
        """
        `OpenSSL.rand.add` adds entropy to the PRNG.
        """
        rand.add(b"hamburger", 3)

    def test_status(self) -> None:
        """
        `OpenSSL.rand.status` returns `1` if the PRNG has sufficient entropy,
        `0` otherwise.
        """
        assert rand.status() == 1
