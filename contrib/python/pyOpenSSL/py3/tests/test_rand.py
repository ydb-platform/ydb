# Copyright (c) Frederick Dean
# See LICENSE for details.

"""
Unit tests for `OpenSSL.rand`.
"""

import pytest

from OpenSSL import rand


class TestRand(object):
    @pytest.mark.parametrize("args", [(b"foo", None), (None, 3)])
    def test_add_wrong_args(self, args):
        """
        `OpenSSL.rand.add` raises `TypeError` if called with arguments not of
        type `str` and `int`.
        """
        with pytest.raises(TypeError):
            rand.add(*args)

    def test_add(self):
        """
        `OpenSSL.rand.add` adds entropy to the PRNG.
        """
        rand.add(b"hamburger", 3)

    def test_status(self):
        """
        `OpenSSL.rand.status` returns `1` if the PRNG has sufficient entropy,
        `0` otherwise.
        """
        assert rand.status() == 1
