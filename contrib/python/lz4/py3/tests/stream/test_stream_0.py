import lz4.stream
import sys
import pytest
if sys.version_info <= (3, 2):
    import struct


def get_stored_size(buff, block_length_size):
    if sys.version_info > (2, 7):
        if isinstance(buff, memoryview):
            b = buff.tobytes()
        else:
            b = bytes(buff)
    else:
        b = bytes(buff)

    if len(b) < block_length_size:
        return None

    if sys.version_info > (3, 2):
        return int.from_bytes(b[:block_length_size], 'little')
    else:
        # This would not work on a memoryview object, hence buff.tobytes call
        # above
        fmt = {1: 'B', 2: 'H', 4: 'I', }
        return struct.unpack('<' + fmt[block_length_size], b[:block_length_size])[0]


def roundtrip(x, c_kwargs, d_kwargs, dictionary):
    if dictionary:
        if isinstance(dictionary, tuple):
            dict_ = x[dictionary[0]:dictionary[1]]
        else:
            dict_ = dictionary
        c_kwargs['dictionary'] = dict_
        d_kwargs['dictionary'] = dict_

    c = bytes()
    with lz4.stream.LZ4StreamCompressor(**c_kwargs) as proc:
        for start in range(0, len(x), c_kwargs['buffer_size']):
            chunk = x[start:start + c_kwargs['buffer_size']]
            assert len(chunk) <= c_kwargs['buffer_size']
            block = proc.compress(chunk)
            if c_kwargs.get('return_bytearray'):
                assert isinstance(block, bytearray)
            if start == 0:
                c = block
            else:
                c += block
            assert get_stored_size(block, c_kwargs['store_comp_size']) == \
                (len(block) - c_kwargs['store_comp_size'])

    d = bytes()
    with lz4.stream.LZ4StreamDecompressor(**d_kwargs) as proc:
        start = 0
        while start < len(c):
            block = proc.get_block(c[start:])
            chunk = proc.decompress(block)
            if d_kwargs.get('return_bytearray'):
                assert isinstance(chunk, bytearray)
            if start == 0:
                d = chunk
            else:
                d += chunk
            start += d_kwargs['store_comp_size'] + len(block)

    return d


def setup_kwargs(strategy, mode, buffer_size, store_comp_size,
                 c_return_bytearray=None, d_return_bytearray=None):
    c_kwargs = {}

    if mode[0] is not None:
        c_kwargs['mode'] = mode[0]
    if mode[1] is not None:
        c_kwargs.update(mode[1])

    c_kwargs['strategy'] = strategy
    c_kwargs['buffer_size'] = buffer_size
    c_kwargs.update(store_comp_size)

    if c_return_bytearray:
        c_kwargs.update(c_return_bytearray)

    d_kwargs = {}

    if d_return_bytearray:
        d_kwargs.update(d_return_bytearray)

    d_kwargs['strategy'] = strategy
    d_kwargs['buffer_size'] = buffer_size
    d_kwargs.update(store_comp_size)

    return (c_kwargs, d_kwargs)


# Test single threaded usage with all valid variations of input
def test_1(data, strategy, mode, buffer_size, store_comp_size,
           c_return_bytearray, d_return_bytearray, dictionary):
    if buffer_size >= (1 << (8 * store_comp_size['store_comp_size'])):
        pytest.skip("Invalid case: buffer_size too large for the block length area")

    (c_kwargs, d_kwargs) = setup_kwargs(
        strategy, mode, buffer_size, store_comp_size, c_return_bytearray, d_return_bytearray)

    d = roundtrip(data, c_kwargs, d_kwargs, dictionary)

    assert d == data


# Test multi threaded:
#   Not relevant in the lz4.stream case (the process is highly sequential,
#   and re-use/share the same context from one input chunk to the next one).
def test_2(data, strategy, mode, buffer_size, store_comp_size, dictionary): # noqa
    pass
