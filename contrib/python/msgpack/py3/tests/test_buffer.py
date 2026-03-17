from pytest import raises

from msgpack import Packer, packb, unpackb


def test_unpack_buffer():
    from array import array

    buf = array("b")
    buf.frombytes(packb((b"foo", b"bar")))
    obj = unpackb(buf, use_list=1)
    assert [b"foo", b"bar"] == obj


def test_unpack_bytearray():
    buf = bytearray(packb((b"foo", b"bar")))
    obj = unpackb(buf, use_list=1)
    assert [b"foo", b"bar"] == obj
    expected_type = bytes
    assert all(type(s) is expected_type for s in obj)


def test_unpack_memoryview():
    buf = bytearray(packb((b"foo", b"bar")))
    view = memoryview(buf)
    obj = unpackb(view, use_list=1)
    assert [b"foo", b"bar"] == obj
    expected_type = bytes
    assert all(type(s) is expected_type for s in obj)


def test_packer_getbuffer():
    packer = Packer(autoreset=False)
    packer.pack_array_header(2)
    packer.pack(42)
    packer.pack("hello")
    buffer = packer.getbuffer()
    assert isinstance(buffer, memoryview)
    assert bytes(buffer) == b"\x92*\xa5hello"

    if Packer.__module__ == "msgpack._cmsgpack":  # only for Cython
        # cython Packer supports buffer protocol directly
        assert bytes(packer) == b"\x92*\xa5hello"

        with raises(BufferError):
            packer.pack(42)
        buffer.release()
        packer.pack(42)
        assert bytes(packer) == b"\x92*\xa5hello*"
