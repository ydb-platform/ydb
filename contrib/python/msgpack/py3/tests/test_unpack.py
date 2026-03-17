import sys
from io import BytesIO

from pytest import mark, raises

from msgpack import ExtType, OutOfData, Unpacker, packb


def test_unpack_array_header_from_file():
    f = BytesIO(packb([1, 2, 3, 4]))
    unpacker = Unpacker(f)
    assert unpacker.read_array_header() == 4
    assert unpacker.unpack() == 1
    assert unpacker.unpack() == 2
    assert unpacker.unpack() == 3
    assert unpacker.unpack() == 4
    with raises(OutOfData):
        unpacker.unpack()


@mark.skipif(
    "not hasattr(sys, 'getrefcount') == True",
    reason="sys.getrefcount() is needed to pass this test",
)
def test_unpacker_hook_refcnt():
    result = []

    def hook(x):
        result.append(x)
        return x

    basecnt = sys.getrefcount(hook)

    up = Unpacker(object_hook=hook, list_hook=hook)

    assert sys.getrefcount(hook) >= basecnt + 2

    up.feed(packb([{}]))
    up.feed(packb([{}]))
    assert up.unpack() == [{}]
    assert up.unpack() == [{}]
    assert result == [{}, [{}], {}, [{}]]

    del up

    assert sys.getrefcount(hook) == basecnt


def test_unpacker_ext_hook():
    class MyUnpacker(Unpacker):
        def __init__(self):
            super().__init__(ext_hook=self._hook, raw=False)

        def _hook(self, code, data):
            if code == 1:
                return int(data)
            else:
                return ExtType(code, data)

    unpacker = MyUnpacker()
    unpacker.feed(packb({"a": 1}))
    assert unpacker.unpack() == {"a": 1}
    unpacker.feed(packb({"a": ExtType(1, b"123")}))
    assert unpacker.unpack() == {"a": 123}
    unpacker.feed(packb({"a": ExtType(2, b"321")}))
    assert unpacker.unpack() == {"a": ExtType(2, b"321")}


def test_unpacker_tell():
    objects = 1, 2, "abc", "def", "ghi"
    packed = b"\x01\x02\xa3abc\xa3def\xa3ghi"
    positions = 1, 2, 6, 10, 14
    unpacker = Unpacker(BytesIO(packed))
    for obj, unp, pos in zip(objects, unpacker, positions):
        assert obj == unp
        assert pos == unpacker.tell()


def test_unpacker_tell_read_bytes():
    objects = 1, "abc", "ghi"
    packed = b"\x01\x02\xa3abc\xa3def\xa3ghi"
    raw_data = b"\x02", b"\xa3def", b""
    lenghts = 1, 4, 999
    positions = 1, 6, 14
    unpacker = Unpacker(BytesIO(packed))
    for obj, unp, pos, n, raw in zip(objects, unpacker, positions, lenghts, raw_data):
        assert obj == unp
        assert pos == unpacker.tell()
        assert unpacker.read_bytes(n) == raw
