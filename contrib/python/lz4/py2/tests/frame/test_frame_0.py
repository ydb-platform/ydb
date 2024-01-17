import lz4.frame as lz4frame
import lz4
import re


def test_library_version_number():
    v = lz4.library_version_number()
    assert isinstance(v, int)
    assert v > 10000


def test_library_version_string():
    v = lz4.library_version_string()
    assert isinstance(v, str)
    assert v.count('.') == 2
    r = re.compile(r'^[0-9]*\.[0-9]*\.[0-9]*$')
    assert r.match(v) is not None


def test_create_compression_context():
    context = lz4frame.create_compression_context()
    assert context is not None


def test_create_decompression_context():
    context = lz4frame.create_decompression_context()
    assert context is not None


def test_reset_decompression_context_1():
    if lz4.library_version_number() >= 10800:
        context = lz4frame.create_decompression_context()
        r = lz4frame.reset_decompression_context(context)
        assert r is None
    else:
        pass


def test_reset_decompression_context_2():
    if lz4.library_version_number() >= 10800:
        c = lz4frame.compress(b'1234', return_bytearray=False)
        context = lz4frame.create_decompression_context()
        try:
            # Simulate an error by passing junk to decompress
            d = lz4frame.decompress_chunk(context, c[4:])
        except RuntimeError:
            pass
        r = lz4frame.reset_decompression_context(context)
        assert r is None
        # And confirm we can use the context after reset
        d, bytes_read, eof = lz4frame.decompress_chunk(context, c)
        assert d == b'1234'
        assert bytes_read == len(c)
        assert eof is True
    else:
        pass


def test_compress_return_type_1():
    r = lz4frame.compress(b'', return_bytearray=False)
    assert isinstance(r, bytes)


def test_compress_return_type_2():
    r = lz4frame.compress(b'', return_bytearray=True)
    assert isinstance(r, bytearray)


def test_decompress_return_type_1():
    c = lz4frame.compress(b'', return_bytearray=False)
    r = lz4frame.decompress(
        c,
        return_bytearray=False,
        return_bytes_read=False
    )
    assert isinstance(r, bytes)


def test_decompress_return_type_2():
    c = lz4frame.compress(b'', return_bytearray=False)
    r = lz4frame.decompress(
        c,
        return_bytearray=True,
        return_bytes_read=False
    )
    assert isinstance(r, bytearray)


def test_decompress_return_type_3():
    c = lz4frame.compress(b'', return_bytearray=False)
    r = lz4frame.decompress(
        c,
        return_bytearray=False,
        return_bytes_read=True
    )
    assert isinstance(r, tuple)
    assert isinstance(r[0], bytes)
    assert isinstance(r[1], int)


def test_decompress_return_type_4():
    c = lz4frame.compress(b'', return_bytearray=False)
    r = lz4frame.decompress(
        c,
        return_bytearray=True,
        return_bytes_read=True
    )
    assert isinstance(r, tuple)
    assert isinstance(r[0], bytearray)
    assert isinstance(r[1], int)


def test_decompress_chunk_return_type_1():
    c = lz4frame.compress(b'', return_bytearray=False)
    d = lz4frame.create_decompression_context()
    r, b, e = lz4frame.decompress_chunk(
        d,
        c,
        return_bytearray=False,
    )
    assert isinstance(r, bytes)
    assert isinstance(b, int)
    assert isinstance(e, bool)


def test_decompress_chunk_return_type_2():
    c = lz4frame.compress(b'', return_bytearray=False)
    d = lz4frame.create_decompression_context()
    r, b, e = lz4frame.decompress_chunk(
        d,
        c,
        return_bytearray=True,
    )
    assert isinstance(r, bytearray)
    assert isinstance(b, int)
    assert isinstance(e, bool)


def test_decompress_chunk_return_type_3():
    c = lz4frame.compress(b'', return_bytearray=False)
    d = lz4frame.create_decompression_context()
    r = lz4frame.decompress_chunk(
        d,
        c,
        return_bytearray=False,
    )
    assert isinstance(r, tuple)
    assert isinstance(r[0], bytes)
    assert isinstance(r[1], int)
    assert isinstance(r[2], bool)


def test_decompress_chunk_return_type_4():
    c = lz4frame.compress(b'', return_bytearray=False)
    d = lz4frame.create_decompression_context()
    r = lz4frame.decompress_chunk(
        d,
        c,
        return_bytearray=True,
    )
    assert isinstance(r, tuple)
    assert isinstance(r[0], bytearray)
    assert isinstance(r[1], int)
    assert isinstance(r[2], bool)


def test_block_size_constants():
    assert lz4frame.BLOCKSIZE_DEFAULT == 0
    assert lz4frame.BLOCKSIZE_MAX64KB == 4
    assert lz4frame.BLOCKSIZE_MAX256KB == 5
    assert lz4frame.BLOCKSIZE_MAX1MB == 6
    assert lz4frame.BLOCKSIZE_MAX4MB == 7
