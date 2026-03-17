import io
import pytest
from srsly.msgpack import Unpacker, BufferFull
from srsly.msgpack import pack
from srsly.msgpack.exceptions import OutOfData


def test_partialdata():
    unpacker = Unpacker()
    unpacker.feed(b"\xa5")
    with pytest.raises(StopIteration):
        next(iter(unpacker))
    unpacker.feed(b"h")
    with pytest.raises(StopIteration):
        next(iter(unpacker))
    unpacker.feed(b"a")
    with pytest.raises(StopIteration):
        next(iter(unpacker))
    unpacker.feed(b"l")
    with pytest.raises(StopIteration):
        next(iter(unpacker))
    unpacker.feed(b"l")
    with pytest.raises(StopIteration):
        next(iter(unpacker))
    unpacker.feed(b"o")
    assert next(iter(unpacker)) == b"hallo"


def test_foobar():
    unpacker = Unpacker(read_size=3, use_list=1)
    unpacker.feed(b"foobar")
    assert unpacker.unpack() == ord(b"f")
    assert unpacker.unpack() == ord(b"o")
    assert unpacker.unpack() == ord(b"o")
    assert unpacker.unpack() == ord(b"b")
    assert unpacker.unpack() == ord(b"a")
    assert unpacker.unpack() == ord(b"r")
    with pytest.raises(OutOfData):
        unpacker.unpack()

    unpacker.feed(b"foo")
    unpacker.feed(b"bar")

    k = 0
    for o, e in zip(unpacker, "foobarbaz"):
        assert o == ord(e)
        k += 1
    assert k == len(b"foobar")


def test_foobar_skip():
    unpacker = Unpacker(read_size=3, use_list=1)
    unpacker.feed(b"foobar")
    assert unpacker.unpack() == ord(b"f")
    unpacker.skip()
    assert unpacker.unpack() == ord(b"o")
    unpacker.skip()
    assert unpacker.unpack() == ord(b"a")
    unpacker.skip()
    with pytest.raises(OutOfData):
        unpacker.unpack()


def test_maxbuffersize():
    with pytest.raises(ValueError):
        Unpacker(read_size=5, max_buffer_size=3)
    unpacker = Unpacker(read_size=3, max_buffer_size=3, use_list=1)
    unpacker.feed(b"fo")
    with pytest.raises(BufferFull):
        unpacker.feed(b"ob")
    unpacker.feed(b"o")
    assert ord("f") == next(unpacker)
    unpacker.feed(b"b")
    assert ord("o") == next(unpacker)
    assert ord("o") == next(unpacker)
    assert ord("b") == next(unpacker)


def test_readbytes():
    unpacker = Unpacker(read_size=3)
    unpacker.feed(b"foobar")
    assert unpacker.unpack() == ord(b"f")
    assert unpacker.read_bytes(3) == b"oob"
    assert unpacker.unpack() == ord(b"a")
    assert unpacker.unpack() == ord(b"r")

    # Test buffer refill
    unpacker = Unpacker(io.BytesIO(b"foobar"), read_size=3)
    assert unpacker.unpack() == ord(b"f")
    assert unpacker.read_bytes(3) == b"oob"
    assert unpacker.unpack() == ord(b"a")
    assert unpacker.unpack() == ord(b"r")


def test_issue124():
    unpacker = Unpacker()
    unpacker.feed(b"\xa1?\xa1!")
    assert tuple(unpacker) == (b"?", b"!")
    assert tuple(unpacker) == ()
    unpacker.feed(b"\xa1?\xa1")
    assert tuple(unpacker) == (b"?",)
    assert tuple(unpacker) == ()
    unpacker.feed(b"!")
    assert tuple(unpacker) == (b"!",)
    assert tuple(unpacker) == ()


def test_unpack_tell():
    stream = io.BytesIO()
    messages = [2 ** i - 1 for i in range(65)]
    messages += [-(2 ** i) for i in range(1, 64)]
    messages += [
        b"hello",
        b"hello" * 1000,
        list(range(20)),
        {i: bytes(i) * i for i in range(10)},
        {i: bytes(i) * i for i in range(32)},
    ]
    offsets = []
    for m in messages:
        pack(m, stream)
        offsets.append(stream.tell())
    stream.seek(0)
    unpacker = Unpacker(stream)
    for m, o in zip(messages, offsets):
        m2 = next(unpacker)
        assert m == m2
        assert o == unpacker.tell()
