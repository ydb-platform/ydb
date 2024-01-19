#!/usr/bin/env python

from __future__ import division

import pytest
import six

from cyson import UInt


if six.PY3:
    long = int


def equals_with_type(data, etalon):
    return type(data) is type(etalon) and data == etalon


def equals_as_uint(data, etalon):
    return type(data) is UInt and data == etalon


N = long(12)
UN = UInt(N)


def test_uint64_initialization():
    assert UInt(2**63 - 1) == 2**63 - 1
    assert UInt() == UInt(0) == 0
    assert UInt(1) == 1
    assert UInt(2) == 2
    assert UInt(long(78)) == 78
    assert UInt(23.57) == 23
    assert UInt('111') == 111

    with pytest.raises(OverflowError):
        UInt(-10)


def test_add():
    assert equals_as_uint(UN + 1, N + 1)
    assert equals_as_uint(UN + long(1), N + 1)
    assert equals_as_uint(UN + UInt(1), N + 1)
    assert equals_as_uint(1 + UN, 1 + N)
    assert equals_as_uint(long(1) + UN, long(1) + N)
    assert equals_as_uint(UInt(1) + UN, 1 + N)
    assert equals_with_type(UN + 1.1, N + 1.1)
    assert equals_with_type(1.1 + UN, 1.1 + N)
    assert equals_with_type(UN + int(-N - 1), N + int(-N - 1))
    assert equals_with_type(UN + long(-N - 1), N + long(-N - 1))
    assert equals_with_type(int(-N - 1) + UN, int(-N - 1) + N)
    assert equals_with_type(long(-N - 1) + UN, long(-N - 1) + N)


def test_sub():
    assert equals_as_uint(UN - 1, N - 1)
    assert equals_as_uint(UN - long(1), N - long(1))
    assert equals_as_uint(UN - UInt(1), N - 1)
    assert equals_as_uint(13 - UN, 13 - UN)
    assert equals_as_uint(UInt(13) - UN, long(13) - N)
    assert equals_as_uint(long(13) - UN, long(13) - UN)
    assert equals_with_type(UN - 0.1, N - 0.1)
    assert equals_with_type(13.1 - UN, 13.1 - N)
    assert equals_with_type(1 - UN, long(1) - N)
    assert equals_with_type(long(1) - UN, long(1) - N)
    assert equals_with_type(UInt(1) - UN, long(1) - N)
    assert equals_with_type(UN - int(UN + 1), N - int(UN + 1))
    assert equals_with_type(UN - long(UN + 1), N - long(UN + 1))
    assert equals_with_type(UN - UInt(UN + 1), N - long(UN + 1))


def test_mul():
    assert equals_as_uint(UN * 2, N * 2)
    assert equals_as_uint(UN * long(2), N * long(2))
    assert equals_as_uint(UN * UInt(2), N * long(2))
    assert equals_as_uint(2 * UN, 2 * N)
    assert equals_as_uint(long(2) * UN, long(2) * UN)
    assert equals_as_uint(UInt(2) * UN, long(2) * UN)
    assert equals_with_type(-3 * UN, -3 * N)
    assert equals_with_type(long(-3) * UN, long(-3) * N)
    assert equals_with_type(UN * -3, N * -3)
    assert equals_with_type(UN * long(-3), N * long(-3))
    assert equals_with_type(UN * 1.1, N * 1.1)
    assert equals_with_type(1.1 * UN, 1.1 * N)


def test_truediv():
    assert equals_with_type(UN / 1, N / long(1))
    assert equals_with_type(UN / UInt(1), N / long(1))
    assert equals_with_type(1 / UN, long(1) / N)
    assert equals_with_type(UInt(1) / UN, long(1) / N)
    assert equals_with_type(UN / N, N / long(N))
    assert equals_with_type(UN / UInt(N), N / long(N))
    assert equals_with_type(UN / -1, N / long(-1))
    assert equals_with_type(-1 / UN, long(-1) / N)
    assert equals_with_type(UN / 1.1, N / 1.1)
    assert equals_with_type(1.1 / UN, 1.1 / N)


def test_floordiv():
    # floor division (__floordiv__)
    assert equals_as_uint(UN // 1, N // 1)
    assert equals_as_uint(UN // long(1), N // long(1))
    assert equals_as_uint(UN // UInt(1), N // long(1))
    assert equals_as_uint(1 // UN, 1 // N)
    assert equals_as_uint(long(1) // UN, long(1) // N)
    assert equals_as_uint(UInt(1) // UN, long(1) // N)
    assert equals_as_uint(UN // N, N // N)
    assert equals_as_uint(UN // UN, N // N)
    assert equals_with_type(UN // -1, N // long(-1))
    assert equals_with_type(UN // long(-1), N // long(-1))
    assert equals_with_type(-1 // UN, -long(1) // N)
    assert equals_with_type(long(-1) // UN, long(-1) // N)
    assert equals_with_type(UN // 1.1, N // 1.1)
    assert equals_with_type(1.1 // UN, 1.1 // N)


def test_mod():
    assert equals_as_uint(UN % 7, N % 7)
    assert equals_as_uint(UN % long(7), N % long(7))
    assert equals_as_uint(UN % UInt(7), N % long(7))
    assert equals_as_uint(23 % UN, 23 % N)
    assert equals_as_uint(long(23) % UN, long(23) % N)
    assert equals_as_uint(UInt(23) % UN, long(23) % N)
    assert equals_as_uint(-23 % UN, -23 % N)
    assert equals_as_uint(long(-23) % UN, long(-23) % N)
    assert equals_with_type(UN % -11, N % long(-11))
    assert equals_with_type(UN % long(-11), N % long(-11))


def test_pow():
    assert equals_as_uint(UN ** 2, N ** 2)
    assert equals_as_uint(UN ** long(2), N ** long(2))
    assert equals_as_uint(UN ** UInt(2), N ** long(2))
    assert equals_as_uint(2 ** UN, 2 ** N)
    assert equals_as_uint(long(2) ** UN, long(2) ** N)
    assert equals_as_uint(UInt(2) ** UN, long(2) ** N)
    assert equals_with_type(UN ** -1, N ** long(-1))
    assert equals_with_type(UN ** long(-1), N ** -long(1))
    assert equals_with_type(UN ** 1.1, N ** 1.1)
    assert equals_with_type(UN ** -1.1, N ** -1.1)
    assert equals_with_type(1.1 ** UN, 1.1 ** N)
    assert equals_with_type(UN ** 0.5, N ** 0.5)
    assert equals_with_type(0.5 ** UN, 0.5 ** N)


def test_neg():
    assert equals_with_type(-UN, -N)
    assert equals_with_type(-UInt(0), long(0))


def test_pos():
    assert equals_as_uint(+UN, N)
    assert equals_as_uint(+UInt(0), 0)


def test_abs():
    assert equals_as_uint(abs(UN), N)
    assert abs(UN) is UN


def test_invert():
    assert equals_with_type(~UN, ~N)
    assert equals_with_type(~UInt(0), ~long(0))


def test_lshift():
    assert equals_as_uint(1 << UN, 1 << N)
    assert equals_as_uint(long(1) << UN, long(1) << N)
    assert equals_as_uint(UInt(1) << UN, long(1) << N)
    assert equals_as_uint(UN << 2, N << 2)
    assert equals_as_uint(UN << long(2), N << 2)
    assert equals_as_uint(UN << UInt(2), N << 2)
    assert equals_with_type(-1 << UN, -1 << N)
    assert equals_with_type(long(-1) << UN, -long(1) << N)

    with pytest.raises(TypeError):
        UN << 1.1
    with pytest.raises(TypeError):
        1.1 << UN
    with pytest.raises(ValueError):
        UN << -1


def test_rshift():
    assert equals_as_uint(10000 >> UN, 10000 >> N)
    assert equals_as_uint(long(10000) >> UN, long(10000) >> N)
    assert equals_as_uint(UInt(10000) >> UN, long(10000) >> N)
    assert equals_as_uint(UN >> 2, N >> 2)
    assert equals_as_uint(UN >> long(2), N >> long(2))
    assert equals_as_uint(UN >> UInt(2), N >> long(2))
    assert equals_with_type(-10000 >> UN, -10000 >> N)
    assert equals_with_type(long(-10000) >> UN, long(-10000) >> N)

    with pytest.raises(TypeError):
        UN >> 1.1
    with pytest.raises(TypeError):
        1.1 >> UN
    with pytest.raises(ValueError):
        UN >> -1


def test_and():
    assert equals_as_uint(UN & 15, N & 15)
    assert equals_as_uint(UN & long(15), N & long(15))

    with pytest.raises(TypeError):
        UN & 1.1


def test_or():
    assert equals_as_uint(UN | 15, N | 15)
    assert equals_as_uint(UN | long(15), N | long(15))

    with pytest.raises(TypeError):
        UN | 1.1


def test_xor():
    assert equals_as_uint(UN ^ 9, N ^ 9)
    assert equals_as_uint(UN ^ long(9), N ^ long(9))

    with pytest.raises(TypeError):
        UN ^ 1.1
