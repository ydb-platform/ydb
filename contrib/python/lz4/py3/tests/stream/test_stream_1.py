import lz4.stream
import pytest
import sys
import os


if sys.version_info < (3, ):
    from struct import pack, unpack

    def _get_format(length, byteorder, signed):
        _order = {'l': '<', 'b': '>'}
        _fmt = {1: 'b', 2: 'h', 4: 'i', 8: 'q'}
        _sign = {True: lambda x: x.lower(), False: lambda x: x.upper()}
        return _sign[signed](_order[byteorder[0].lower()] + _fmt[length])

    def int_to_bytes(value, length=4, byteorder='little', signed=False):
        return bytearray(pack(_get_format(length, byteorder, signed), value))

    def int_from_bytes(bytes, byteorder='little', signed=False):
        return unpack(_get_format(len(bytes), byteorder, signed), bytes)[0]

else:
    def int_to_bytes(value, length=4, byteorder='little', signed=False):
        return value.to_bytes(length, byteorder, signed=signed)

    def int_from_bytes(bytes, byteorder='little', signed=False):
        return int.from_bytes(bytes, byteorder, signed=signed)


# This test requires allocating a big lump of memory. In order to
# avoid a massive memory allocation during byte compilation, we have
# to declare a variable for the size of the buffer we're going to
# create outside the scope of the function below. See:
# https://bugs.python.org/issue21074
_4GB = 0x100000000  # 4GB


def compress(x, c_kwargs, return_block_offset=False, check_block_type=False):
    o = [0, ]
    if c_kwargs.get('return_bytearray', False):
        c = bytearray()
    else:
        c = bytes()
    with lz4.stream.LZ4StreamCompressor(**c_kwargs) as proc:
        for start in range(0, len(x), c_kwargs['buffer_size']):
            chunk = x[start:start + c_kwargs['buffer_size']]
            block = proc.compress(chunk)
            c += block
            if return_block_offset:
                o.append(len(c))
            if check_block_type:
                assert isinstance(block, c.__class__)
    if return_block_offset:
        return c, o
    else:
        return c


def decompress(x, d_kwargs, check_chunk_type=False):
    if d_kwargs.get('return_bytearray', False):
        d = bytearray()
    else:
        d = bytes()
    with lz4.stream.LZ4StreamDecompressor(**d_kwargs) as proc:
        start = 0
        while start < len(x):
            block = proc.get_block(x[start:])
            chunk = proc.decompress(block)
            d += chunk
            start += d_kwargs['store_comp_size'] + len(block)
            if check_chunk_type:
                assert isinstance(chunk, d.__class__)
    return d


def test_invalid_config_c_1():
    c_kwargs = {}
    c_kwargs['strategy'] = "ring_buffer"
    c_kwargs['buffer_size'] = 1024

    with pytest.raises(NotImplementedError):
        lz4.stream.LZ4StreamCompressor(**c_kwargs)


def test_invalid_config_d_1():
    d_kwargs = {}
    d_kwargs['strategy'] = "ring_buffer"
    d_kwargs['buffer_size'] = 1024

    with pytest.raises(NotImplementedError):
        lz4.stream.LZ4StreamDecompressor(**d_kwargs)


def test_invalid_config_c_2():
    c_kwargs = {}
    c_kwargs['strategy'] = "foo"
    c_kwargs['buffer_size'] = 1024

    with pytest.raises(ValueError):
        lz4.stream.LZ4StreamCompressor(**c_kwargs)


def test_invalid_config_d_2():
    d_kwargs = {}
    d_kwargs['strategy'] = "foo"
    d_kwargs['buffer_size'] = 1024

    with pytest.raises(ValueError):
        lz4.stream.LZ4StreamDecompressor(**d_kwargs)


def test_invalid_config_c_3(store_comp_size):
    c_kwargs = {}
    c_kwargs['strategy'] = "double_buffer"
    c_kwargs['buffer_size'] = 1024
    c_kwargs['store_comp_size'] = store_comp_size['store_comp_size'] + 5

    with pytest.raises(ValueError):
        lz4.stream.LZ4StreamCompressor(**c_kwargs)


def test_invalid_config_d_3(store_comp_size):
    d_kwargs = {}
    d_kwargs['strategy'] = "double_buffer"
    d_kwargs['buffer_size'] = 1024
    d_kwargs['store_comp_size'] = store_comp_size['store_comp_size'] + 5

    with pytest.raises(ValueError):
        lz4.stream.LZ4StreamDecompressor(**d_kwargs)


def test_invalid_config_c_4(store_comp_size):
    c_kwargs = {}
    c_kwargs['strategy'] = "double_buffer"
    c_kwargs['buffer_size'] = 1 << (8 * store_comp_size['store_comp_size'])
    c_kwargs.update(store_comp_size)

    if store_comp_size['store_comp_size'] >= 4:
        # No need for skiping this test case, since arguments check is
        # expecting to raise an error.

        # Make sure the page size is larger than what the input bound will be,
        # but still fit in 4 bytes
        c_kwargs['buffer_size'] -= 1

    if c_kwargs['buffer_size'] > lz4.stream.LZ4_MAX_INPUT_SIZE:
        message = r"^Invalid buffer_size argument: \d+. Cannot define output buffer size. Must be lesser or equal to 2113929216$"  # noqa
        err_class = ValueError
    else:
        message = r"^Inconsistent buffer_size/store_comp_size values. Maximal compressed length \(\d+\) cannot fit in a \d+ byte-long integer$"  # noqa
        err_class = lz4.stream.LZ4StreamError

    with pytest.raises(err_class, match=message):
        lz4.stream.LZ4StreamCompressor(**c_kwargs)


def test_invalid_config_d_4(store_comp_size):
    d_kwargs = {}
    d_kwargs['strategy'] = "double_buffer"
    d_kwargs['buffer_size'] = 1 << (8 * store_comp_size['store_comp_size'])
    d_kwargs.update(store_comp_size)

    if store_comp_size['store_comp_size'] >= 4:

        if sys.maxsize < 0xffffffff:
            pytest.skip('Py_ssize_t too small for this test')

        # Make sure the page size is larger than what the input bound will be,
        # but still fit in 4 bytes
        d_kwargs['buffer_size'] -= 1

    # No failure expected during instanciation/initialization
    lz4.stream.LZ4StreamDecompressor(**d_kwargs)


def test_invalid_config_c_5():
    c_kwargs = {}
    c_kwargs['strategy'] = "double_buffer"
    c_kwargs['buffer_size'] = lz4.stream.LZ4_MAX_INPUT_SIZE

    if sys.maxsize < 0xffffffff:
        pytest.skip('Py_ssize_t too small for this test')

    # No failure expected
    lz4.stream.LZ4StreamCompressor(**c_kwargs)

    c_kwargs['buffer_size'] = lz4.stream.LZ4_MAX_INPUT_SIZE + 1
    with pytest.raises(ValueError):
        lz4.stream.LZ4StreamCompressor(**c_kwargs)

    # Make sure the page size is larger than what the input bound will be,
    # but still fit in 4 bytes
    c_kwargs['buffer_size'] = _4GB - 1  # 4GB - 1 (to fit in 4 bytes)
    with pytest.raises(ValueError):
        lz4.stream.LZ4StreamCompressor(**c_kwargs)


def test_invalid_config_d_5():
    d_kwargs = {}
    d_kwargs['strategy'] = "double_buffer"

    # No failure expected during instanciation/initialization
    d_kwargs['buffer_size'] = lz4.stream.LZ4_MAX_INPUT_SIZE

    if sys.maxsize < 0xffffffff:
        pytest.skip('Py_ssize_t too small for this test')

    lz4.stream.LZ4StreamDecompressor(**d_kwargs)

    # No failure expected during instanciation/initialization
    d_kwargs['buffer_size'] = lz4.stream.LZ4_MAX_INPUT_SIZE + 1

    if sys.maxsize < 0xffffffff:
        pytest.skip('Py_ssize_t too small for this test')

    lz4.stream.LZ4StreamDecompressor(**d_kwargs)

    # No failure expected during instanciation/initialization
    d_kwargs['buffer_size'] = _4GB - 1  # 4GB - 1 (to fit in 4 bytes)

    if sys.maxsize < 0xffffffff:
        pytest.skip('Py_ssize_t too small for this test')

    lz4.stream.LZ4StreamDecompressor(**d_kwargs)


def test_decompress_corrupted_input_1():
    c_kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    d_kwargs = {}
    d_kwargs.update(c_kwargs)

    data = compress(b'A' * 512, c_kwargs)
    decompress(data, d_kwargs)

    message = r"^Requested input size \(\d+\) larger than source size \(\d+\)$"

    with pytest.raises(lz4.stream.LZ4StreamError, match=message):
        decompress(data[4:], d_kwargs)


def test_decompress_corrupted_input_2():
    c_kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    d_kwargs = {}
    d_kwargs.update(c_kwargs)

    data = compress(b'A' * 512, c_kwargs)
    decompress(data, d_kwargs)

    message = r"^Decompression failed. error: \d+$"

    # Block size corruption in the first block

    # Block size longer than actual:
    data = int_to_bytes(int_from_bytes(data[:4], 'little') + 1, 4, 'little') + data[4:]
    with pytest.raises(lz4.stream.LZ4StreamError, match=message):
        decompress(data, d_kwargs)

    # Block size shorter than actual:
    data = int_to_bytes(int_from_bytes(data[:4], 'little') - 2, 4, 'little') + data[4:]
    with pytest.raises(lz4.stream.LZ4StreamError, match=message):
        decompress(data, d_kwargs)


def test_decompress_corrupted_input_3():
    c_kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    d_kwargs = {}
    d_kwargs.update(c_kwargs)

    data = compress(b'A' * 512, c_kwargs)
    decompress(data, d_kwargs)

    message = r"^Decompression failed. error: \d+$"

    # Block size corruption in a block in the middle of the stream
    offset = 4 + int_from_bytes(data[:4], 'little')

    # Block size longer than actual:
    block_len = int_from_bytes(data[offset:offset + 4], 'little') + 1
    data = data[:offset] + int_to_bytes(block_len, 4, 'little') + data[offset + 4:]

    with pytest.raises(lz4.stream.LZ4StreamError, match=message):
        decompress(data, d_kwargs)

    # Block size shorter than actual:
    block_len = int_from_bytes(data[offset:offset + 4], 'little') - 2
    data = data[:offset] + int_to_bytes(block_len, 4, 'little') + data[offset + 4:]

    with pytest.raises(lz4.stream.LZ4StreamError, match=message):
        decompress(data, d_kwargs)


def test_decompress_corrupted_input_4():
    c_kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    d_kwargs = {}
    d_kwargs.update(c_kwargs)

    data = compress(b'A' * 256, c_kwargs)
    decompress(data, d_kwargs)

    # Block size corruption in the last block of the stream
    offset = 4 + int_from_bytes(data[:4], 'little')

    # Block size longer than actual:
    block_len = int_from_bytes(data[offset:offset + 4], 'little') + 1
    data = data[:offset] + int_to_bytes(block_len, 4, 'little') + data[offset + 4:]

    message = r"^Requested input size \(\d+\) larger than source size \(\d+\)$"

    with pytest.raises(lz4.stream.LZ4StreamError, match=message):
        decompress(data, d_kwargs)

    # Block size shorter than actual:
    block_len = int_from_bytes(data[offset:offset + 4], 'little') - 2
    data = data[:offset] + int_to_bytes(block_len, 4, 'little') + data[offset + 4:]

    message = r"^Decompression failed. error: \d+$"

    with pytest.raises(lz4.stream.LZ4StreamError, match=message):
        decompress(data, d_kwargs)


def test_decompress_truncated():
    c_kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    d_kwargs = {}
    d_kwargs.update(c_kwargs)

    input_data = b"2099023098234882923049823094823094898239230982349081231290381209380981203981209381238901283098908123109238098123" * 24
    compressed, block_offsets = compress(input_data, c_kwargs, return_block_offset=True)

    last_block_offset = 0
    for n in range(len(compressed)):
        if n in block_offsets:
            # end of input matches end of block, so decompression must succeed
            last_block_offset = n
            decompress(compressed[:n], d_kwargs)

        else:
            # end of input does not match end of block, so decompression failure is expected
            if n - last_block_offset < c_kwargs['store_comp_size']:
                message = "^Invalid source, too small for holding any block$"
            else:
                message = r"^Requested input size \(\d+\) larger than source size \(\d+\)$"

            with pytest.raises(lz4.stream.LZ4StreamError, match=message):
                decompress(compressed[:n], d_kwargs)


# This next test is probably redundant given test_decompress_truncated above
# since the trailing bytes will be considered as the truncated last block, but
# we will keep them for now


def test_decompress_with_trailer():
    c_kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    d_kwargs = {}
    d_kwargs.update(c_kwargs)

    data = b'A' * 64
    comp = compress(data, c_kwargs)

    message = "^Invalid source, too small for holding any block$"
    with pytest.raises(lz4.stream.LZ4StreamError, match=message):
        decompress(comp + b'A', d_kwargs)

    message = r"^Requested input size \(\d+\) larger than source size \(\d+\)$"
    with pytest.raises(lz4.stream.LZ4StreamError, match=message):
        decompress(comp + b'A' * 10, d_kwargs)

    for n in range(1, 10):
        if n < d_kwargs['store_comp_size']:
            message = "^Invalid source, too small for holding any block$"
        else:
            message = r"^Decompression failed. error: \d+$"
        with pytest.raises(lz4.stream.LZ4StreamError, match=message):
            decompress(comp + b'\x00' * n, d_kwargs)


def test_unicode():
    if sys.version_info < (3,):
        return  # skip

    c_kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    d_kwargs = {}
    d_kwargs.update(c_kwargs)

    DATA = b'x'
    with pytest.raises(TypeError):
        compress(DATA.decode('latin1'), c_kwargs)
        decompress(compress(DATA, c_kwargs).decode('latin1'), d_kwargs)


# These next two are probably redundant given test_1 above but we'll keep them
# for now


def test_return_bytearray():
    if sys.version_info < (3,):
        return  # skip

    c_kwargs_r = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}
    c_kwargs = {'return_bytearray': True}
    c_kwargs.update(c_kwargs_r)

    d_kwargs = {}
    d_kwargs.update(c_kwargs)

    data = os.urandom(128 * 1024)  # Read 128kb
    compressed = compress(data, c_kwargs_r, check_block_type=True)
    b = compress(data, c_kwargs, check_block_type=True)
    assert isinstance(b, bytearray)
    assert bytes(b) == compressed
    b = decompress(compressed, d_kwargs, check_chunk_type=True)
    assert isinstance(b, bytearray)
    assert bytes(b) == data


def test_memoryview():
    if sys.version_info < (2, 7):
        return  # skip

    c_kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    d_kwargs = {}
    d_kwargs.update(c_kwargs)

    data = os.urandom(128 * 1024)  # Read 128kb
    compressed = compress(data, c_kwargs)
    assert compress(memoryview(data), c_kwargs) == compressed
    assert decompress(memoryview(compressed), d_kwargs) == data


def test_with_dict_none():
    kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    input_data = b"2099023098234882923049823094823094898239230982349081231290381209380981203981209381238901283098908123109238098123" * 24
    for mode in ['default', 'high_compression']:
        c_kwargs = {'mode': mode, 'dictionary': None}
        c_kwargs.update(kwargs)
        d_kwargs = {}
        d_kwargs.update(kwargs)
        assert decompress(compress(input_data, c_kwargs), d_kwargs) == input_data

        c_kwargs = {'mode': mode}
        c_kwargs.update(kwargs)
        d_kwargs = {'dictionary': None}
        d_kwargs.update(kwargs)
        assert decompress(compress(input_data, c_kwargs), d_kwargs) == input_data

        c_kwargs = {'mode': mode, 'dictionary': b''}
        c_kwargs.update(kwargs)
        d_kwargs = {}
        d_kwargs.update(kwargs)
        assert decompress(compress(input_data, c_kwargs), d_kwargs) == input_data

        c_kwargs = {'mode': mode}
        c_kwargs.update(kwargs)
        d_kwargs = {'dictionary': b''}
        d_kwargs.update(kwargs)
        assert decompress(compress(input_data, c_kwargs), d_kwargs) == input_data

        c_kwargs = {'mode': mode, 'dictionary': ''}
        c_kwargs.update(kwargs)
        d_kwargs = {}
        d_kwargs.update(kwargs)
        assert decompress(compress(input_data, c_kwargs), d_kwargs) == input_data

        c_kwargs = {'mode': mode}
        c_kwargs.update(kwargs)
        d_kwargs = {'dictionary': ''}
        d_kwargs.update(kwargs)
        assert decompress(compress(input_data, c_kwargs), d_kwargs) == input_data


def test_with_dict():
    kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    input_data = b"2099023098234882923049823094823094898239230982349081231290381209380981203981209381238901283098908123109238098123" * 24
    dict1 = input_data[10:30]
    dict2 = input_data[20:40]
    message = r"^Decompression failed. error: \d+$"

    for mode in ['default', 'high_compression']:
        c_kwargs = {'mode': mode, 'dictionary': dict1}
        c_kwargs.update(kwargs)
        compressed = compress(input_data, c_kwargs)

        d_kwargs = {}
        d_kwargs.update(kwargs)
        with pytest.raises(lz4.stream.LZ4StreamError, match=message):
            decompress(compressed, d_kwargs)

        d_kwargs = {'dictionary': dict1[:2]}
        d_kwargs.update(kwargs)
        with pytest.raises(lz4.stream.LZ4StreamError, match=message):
            decompress(compressed, d_kwargs)

        d_kwargs = {'dictionary': dict2}
        d_kwargs.update(kwargs)
        assert decompress(compressed, d_kwargs) != input_data

        d_kwargs = {'dictionary': dict1}
        d_kwargs.update(kwargs)
        assert decompress(compressed, d_kwargs) == input_data

    c_kwargs = {}
    c_kwargs.update(kwargs)
    d_kwargs = {'dictionary': dict1}
    d_kwargs.update(kwargs)
    assert decompress(compress(input_data, c_kwargs), d_kwargs) == input_data


def test_known_decompress_1():
    d_kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    output = b''

    input = b'\x00\x00\x00\x00'
    message = "^Decompression failed. error: 1$"
    with pytest.raises(lz4.stream.LZ4StreamError, match=message):
        decompress(input, d_kwargs)

    input = b'\x01\x00\x00\x00\x00'
    assert decompress(input, d_kwargs) == output


def test_known_decompress_2():
    d_kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    input = b'\x02\x00\x00\x00\x10 '
    output = b' '
    assert decompress(input, d_kwargs) == output


def test_known_decompress_3():
    d_kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    # uncompressed data size smaller than buffer_size
    input = b'%\x00\x00\x00\xff\x0bLorem ipsum dolor sit amet\x1a\x006P amet'
    output = b'Lorem ipsum dolor sit amet' * 4
    assert decompress(input, d_kwargs) == output


def test_known_decompress_4():
    d_kwargs = {'strategy': "double_buffer", 'buffer_size': 128, 'store_comp_size': 4}

    input = b'%\x00\x00\x00\xff\x0bLorem ipsum dolor sit amet\x1a\x00NPit am\n\x00\x00\x00\x0fh\x00hP sit \x05\x00\x00\x00@amet'
    output = b'Lorem ipsum dolor sit amet' * 10
    assert decompress(input, d_kwargs) == output
