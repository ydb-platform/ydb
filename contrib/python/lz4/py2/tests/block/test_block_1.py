import lz4.block
import pytest
import sys
import os


def test_decompress_ui32_overflow():
    data = lz4.block.compress(b'A' * 64)
    with pytest.raises(OverflowError):
        lz4.block.decompress(data[4:], uncompressed_size=((1 << 32) + 64))


def test_decompress_without_leak():
    # Verify that hand-crafted packet does not leak uninitialized(?) memory.
    data = lz4.block.compress(b'A' * 64)
    message = r'^Decompressor wrote 64 bytes, but 79 bytes expected from header$'
    with pytest.raises(lz4.block.LZ4BlockError, match=message):
        lz4.block.decompress(b'\x4f' + data[1:])


def test_decompress_with_small_buffer():
    data = lz4.block.compress(b'A' * 64, store_size=False)
    message = r'^Decompression failed: corrupt input or insufficient space in destination buffer. Error code: \d+$'
    with pytest.raises(lz4.block.LZ4BlockError, match=message):
        lz4.block.decompress(data[4:], uncompressed_size=64)
    with pytest.raises(lz4.block.LZ4BlockError, match=message):
        lz4.block.decompress(data, uncompressed_size=60)


def test_decompress_truncated():
    input_data = b"2099023098234882923049823094823094898239230982349081231290381209380981203981209381238901283098908123109238098123" * 24
    compressed = lz4.block.compress(input_data)
    # for i in range(len(compressed)):
    #     try:
    #         lz4.block.decompress(compressed[:i])
    #     except:
    #         print(i, sys.exc_info()[0], sys.exc_info()[1])
    with pytest.raises(ValueError, match='Input source data size too small'):
        lz4.block.decompress(compressed[:0])
    for n in [0, 1]:
        with pytest.raises(ValueError, match='Input source data size too small'):
            lz4.block.decompress(compressed[:n])
    for n in [24, 25, -2, 27, 67, 85]:
        with pytest.raises(lz4.block.LZ4BlockError):
            lz4.block.decompress(compressed[:n])


def test_decompress_with_trailer():
    data = b'A' * 64
    comp = lz4.block.compress(data)
    message = r'^Decompression failed: corrupt input or insufficient space in destination buffer. Error code: \d+$'
    with pytest.raises(lz4.block.LZ4BlockError, match=message):
        lz4.block.decompress(comp + b'A')
    with pytest.raises(lz4.block.LZ4BlockError, match=message):
        lz4.block.decompress(comp + comp)
    with pytest.raises(lz4.block.LZ4BlockError, match=message):
        lz4.block.decompress(comp + comp[4:])


def test_unicode():
    if sys.version_info < (3,):
        return  # skip
    DATA = b'x'
    with pytest.raises(TypeError):
        lz4.block.compress(DATA.decode('latin1'))
        lz4.block.decompress(lz4.block.compress(DATA).decode('latin1'))

# These next two are probably redundant given test_1 above but we'll keep them
# for now


def test_return_bytearray():
    if sys.version_info < (3,):
        return  # skip
    data = os.urandom(128 * 1024)  # Read 128kb
    compressed = lz4.block.compress(data)
    b = lz4.block.compress(data, return_bytearray=True)
    assert isinstance(b, bytearray)
    assert bytes(b) == compressed
    b = lz4.block.decompress(compressed, return_bytearray=True)
    assert isinstance(b, bytearray)
    assert bytes(b) == data


def test_memoryview():
    if sys.version_info < (2, 7):
        return  # skip
    data = os.urandom(128 * 1024)  # Read 128kb
    compressed = lz4.block.compress(data)
    assert lz4.block.compress(memoryview(data)) == compressed
    assert lz4.block.decompress(memoryview(compressed)) == data


def test_with_dict_none():
    input_data = b"2099023098234882923049823094823094898239230982349081231290381209380981203981209381238901283098908123109238098123" * 24
    for mode in ['default', 'high_compression']:
        assert lz4.block.decompress(lz4.block.compress(
            input_data, mode=mode, dict=None)) == input_data
        assert lz4.block.decompress(lz4.block.compress(
            input_data, mode=mode), dict=None) == input_data
        assert lz4.block.decompress(lz4.block.compress(
            input_data, mode=mode, dict=b'')) == input_data
        assert lz4.block.decompress(lz4.block.compress(
            input_data, mode=mode), dict=b'') == input_data
        assert lz4.block.decompress(lz4.block.compress(
            input_data, mode=mode, dict='')) == input_data
        assert lz4.block.decompress(lz4.block.compress(
            input_data, mode=mode), dict='') == input_data


def test_with_dict():
    input_data = b"2099023098234882923049823094823094898239230982349081231290381209380981203981209381238901283098908123109238098123" * 24
    dict1 = input_data[10:30]
    dict2 = input_data[20:40]
    message = r'^Decompression failed: corrupt input or insufficient space in destination buffer. Error code: \d+$'
    for mode in ['default', 'high_compression']:
        compressed = lz4.block.compress(input_data, mode=mode, dict=dict1)
        with pytest.raises(lz4.block.LZ4BlockError, match=message):
            lz4.block.decompress(compressed)
        with pytest.raises(lz4.block.LZ4BlockError, match=message):
            lz4.block.decompress(compressed, dict=dict1[:2])
        assert lz4.block.decompress(compressed, dict=dict2) != input_data
        assert lz4.block.decompress(compressed, dict=dict1) == input_data
    assert lz4.block.decompress(lz4.block.compress(
        input_data), dict=dict1) == input_data


def test_known_decompress_1():
    input = b'\x00\x00\x00\x00\x00'
    output = b''
    assert lz4.block.decompress(input) == output


def test_known_decompress_2():
    input = b'\x01\x00\x00\x00\x10 '
    output = b' '
    assert lz4.block.decompress(input) == output


def test_known_decompress_3():
    input = b'h\x00\x00\x00\xff\x0bLorem ipsum dolor sit amet\x1a\x006P amet'
    output = b'Lorem ipsum dolor sit amet' * 4
    assert lz4.block.decompress(input) == output


def test_known_decompress_4():
    input = b'\xb0\xb3\x00\x00\xff\x1fExcepteur sint occaecat cupidatat non proident.\x00' + (b'\xff' * 180) + b'\x1ePident'
    output = b'Excepteur sint occaecat cupidatat non proident' * 1000
    assert lz4.block.decompress(input) == output
