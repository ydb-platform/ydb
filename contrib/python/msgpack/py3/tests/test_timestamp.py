import datetime

import pytest

import msgpack
from msgpack.ext import Timestamp


def test_timestamp():
    # timestamp32
    ts = Timestamp(2**32 - 1)
    assert ts.to_bytes() == b"\xff\xff\xff\xff"
    packed = msgpack.packb(ts)
    assert packed == b"\xd6\xff" + ts.to_bytes()
    unpacked = msgpack.unpackb(packed)
    assert ts == unpacked
    assert ts.seconds == 2**32 - 1 and ts.nanoseconds == 0

    # timestamp64
    ts = Timestamp(2**34 - 1, 999999999)
    assert ts.to_bytes() == b"\xee\x6b\x27\xff\xff\xff\xff\xff"
    packed = msgpack.packb(ts)
    assert packed == b"\xd7\xff" + ts.to_bytes()
    unpacked = msgpack.unpackb(packed)
    assert ts == unpacked
    assert ts.seconds == 2**34 - 1 and ts.nanoseconds == 999999999

    # timestamp96
    ts = Timestamp(2**63 - 1, 999999999)
    assert ts.to_bytes() == b"\x3b\x9a\xc9\xff\x7f\xff\xff\xff\xff\xff\xff\xff"
    packed = msgpack.packb(ts)
    assert packed == b"\xc7\x0c\xff" + ts.to_bytes()
    unpacked = msgpack.unpackb(packed)
    assert ts == unpacked
    assert ts.seconds == 2**63 - 1 and ts.nanoseconds == 999999999

    # negative fractional
    ts = Timestamp.from_unix(-2.3)  # s: -3, ns: 700000000
    assert ts.seconds == -3 and ts.nanoseconds == 700000000
    assert ts.to_bytes() == b"\x29\xb9\x27\x00\xff\xff\xff\xff\xff\xff\xff\xfd"
    packed = msgpack.packb(ts)
    assert packed == b"\xc7\x0c\xff" + ts.to_bytes()
    unpacked = msgpack.unpackb(packed)
    assert ts == unpacked


def test_unpack_timestamp():
    # timestamp 32
    assert msgpack.unpackb(b"\xd6\xff\x00\x00\x00\x00") == Timestamp(0)

    # timestamp 64
    assert msgpack.unpackb(b"\xd7\xff" + b"\x00" * 8) == Timestamp(0)
    with pytest.raises(ValueError):
        msgpack.unpackb(b"\xd7\xff" + b"\xff" * 8)

    # timestamp 96
    assert msgpack.unpackb(b"\xc7\x0c\xff" + b"\x00" * 12) == Timestamp(0)
    with pytest.raises(ValueError):
        msgpack.unpackb(b"\xc7\x0c\xff" + b"\xff" * 12) == Timestamp(0)

    # Undefined
    with pytest.raises(ValueError):
        msgpack.unpackb(b"\xd4\xff\x00")  # fixext 1
    with pytest.raises(ValueError):
        msgpack.unpackb(b"\xd5\xff\x00\x00")  # fixext 2
    with pytest.raises(ValueError):
        msgpack.unpackb(b"\xc7\x00\xff")  # ext8 (len=0)
    with pytest.raises(ValueError):
        msgpack.unpackb(b"\xc7\x03\xff\0\0\0")  # ext8 (len=3)
    with pytest.raises(ValueError):
        msgpack.unpackb(b"\xc7\x05\xff\0\0\0\0\0")  # ext8 (len=5)


def test_timestamp_from():
    t = Timestamp(42, 14000)
    assert Timestamp.from_unix(42.000014) == t
    assert Timestamp.from_unix_nano(42000014000) == t


def test_timestamp_to():
    t = Timestamp(42, 14000)
    assert t.to_unix() == 42.000014
    assert t.to_unix_nano() == 42000014000


def test_timestamp_datetime():
    t = Timestamp(42, 14)
    utc = datetime.timezone.utc
    assert t.to_datetime() == datetime.datetime(1970, 1, 1, 0, 0, 42, 0, tzinfo=utc)

    ts = datetime.datetime(2024, 4, 16, 8, 43, 9, 420317, tzinfo=utc)
    ts2 = datetime.datetime(2024, 4, 16, 8, 43, 9, 420318, tzinfo=utc)

    assert (
        Timestamp.from_datetime(ts2).nanoseconds - Timestamp.from_datetime(ts).nanoseconds == 1000
    )

    ts3 = datetime.datetime(2024, 4, 16, 8, 43, 9, 4256)
    ts4 = datetime.datetime(2024, 4, 16, 8, 43, 9, 4257)
    assert (
        Timestamp.from_datetime(ts4).nanoseconds - Timestamp.from_datetime(ts3).nanoseconds == 1000
    )

    assert Timestamp.from_datetime(ts).to_datetime() == ts


def test_unpack_datetime():
    t = Timestamp(42, 14)
    utc = datetime.timezone.utc
    packed = msgpack.packb(t)
    unpacked = msgpack.unpackb(packed, timestamp=3)
    assert unpacked == datetime.datetime(1970, 1, 1, 0, 0, 42, 0, tzinfo=utc)


def test_pack_unpack_before_epoch():
    utc = datetime.timezone.utc
    t_in = datetime.datetime(1960, 1, 1, tzinfo=utc)
    packed = msgpack.packb(t_in, datetime=True)
    unpacked = msgpack.unpackb(packed, timestamp=3)
    assert unpacked == t_in


def test_pack_datetime():
    t = Timestamp(42, 14000)
    dt = t.to_datetime()
    utc = datetime.timezone.utc
    assert dt == datetime.datetime(1970, 1, 1, 0, 0, 42, 14, tzinfo=utc)

    packed = msgpack.packb(dt, datetime=True)
    packed2 = msgpack.packb(t)
    assert packed == packed2

    unpacked = msgpack.unpackb(packed)
    print(packed, unpacked)
    assert unpacked == t

    unpacked = msgpack.unpackb(packed, timestamp=3)
    assert unpacked == dt

    x = []
    packed = msgpack.packb(dt, datetime=False, default=x.append)
    assert x
    assert x[0] == dt
    assert msgpack.unpackb(packed) is None


def test_issue451():
    # https://github.com/msgpack/msgpack-python/issues/451
    utc = datetime.timezone.utc
    dt = datetime.datetime(2100, 1, 1, 1, 1, tzinfo=utc)
    packed = msgpack.packb(dt, datetime=True)
    assert packed == b"\xd6\xff\xf4\x86eL"

    unpacked = msgpack.unpackb(packed, timestamp=3)
    assert dt == unpacked


def test_pack_datetime_without_tzinfo():
    dt = datetime.datetime(1970, 1, 1, 0, 0, 42, 14)
    with pytest.raises(ValueError, match="where tzinfo=None"):
        packed = msgpack.packb(dt, datetime=True)

    dt = datetime.datetime(1970, 1, 1, 0, 0, 42, 14)
    packed = msgpack.packb(dt, datetime=True, default=lambda x: None)
    assert packed == msgpack.packb(None)

    utc = datetime.timezone.utc
    dt = datetime.datetime(1970, 1, 1, 0, 0, 42, 14, tzinfo=utc)
    packed = msgpack.packb(dt, datetime=True)
    unpacked = msgpack.unpackb(packed, timestamp=3)
    assert unpacked == dt
