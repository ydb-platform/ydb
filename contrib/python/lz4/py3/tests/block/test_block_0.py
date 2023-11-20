import lz4.block
from multiprocessing.pool import ThreadPool
import sys
from functools import partial
if sys.version_info <= (3, 2):
    import struct


def get_stored_size(buff):
    if sys.version_info > (2, 7):
        if isinstance(buff, memoryview):
            b = buff.tobytes()
        else:
            b = bytes(buff)
    else:
        b = bytes(buff)

    if len(b) < 4:
        return None

    if sys.version_info > (3, 2):
        return int.from_bytes(b[:4], 'little')
    else:
        # This would not work on a memoryview object, hence buff.tobytes call
        # above
        return struct.unpack('<I', b[:4])[0]


def roundtrip(x, c_kwargs, d_kwargs, dictionary):
    if dictionary:
        if isinstance(dictionary, tuple):
            d = x[dictionary[0]:dictionary[1]]
        else:
            d = dictionary
        c_kwargs['dict'] = d
        d_kwargs['dict'] = d

    c = lz4.block.compress(x, **c_kwargs)

    if c_kwargs['store_size']:
        assert get_stored_size(c) == len(x)
    else:
        d_kwargs['uncompressed_size'] = len(x)

    return lz4.block.decompress(c, **d_kwargs)


def setup_kwargs(mode, store_size, c_return_bytearray=None, d_return_bytearray=None):
    c_kwargs = {}

    if mode[0] is not None:
        c_kwargs['mode'] = mode[0]
    if mode[1] is not None:
        c_kwargs.update(mode[1])

    c_kwargs.update(store_size)

    if c_return_bytearray:
        c_kwargs.update(c_return_bytearray)

    d_kwargs = {}

    if d_return_bytearray:
        d_kwargs.update(d_return_bytearray)

    return (c_kwargs, d_kwargs)


# Test single threaded usage with all valid variations of input
def test_1(data, mode, store_size, c_return_bytearray, d_return_bytearray, dictionary):
    (c_kwargs, d_kwargs) = setup_kwargs(
        mode, store_size, c_return_bytearray, d_return_bytearray)

    d = roundtrip(data, c_kwargs, d_kwargs, dictionary)

    assert d == data
    if d_return_bytearray['return_bytearray']:
        assert isinstance(d, bytearray)


# Test multi threaded usage with all valid variations of input
def test_2(data, mode, store_size, dictionary):
    (c_kwargs, d_kwargs) = setup_kwargs(mode, store_size)

    data_in = [data for i in range(32)]

    pool = ThreadPool(2)
    rt = partial(roundtrip, c_kwargs=c_kwargs,
                 d_kwargs=d_kwargs, dictionary=dictionary)
    data_out = pool.map(rt, data_in)
    pool.close()
    assert data_in == data_out
