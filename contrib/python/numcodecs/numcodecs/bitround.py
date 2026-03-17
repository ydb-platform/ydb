import numpy as np

from .abc import Codec
from .compat import ensure_ndarray_like, ndarray_copy

# The size in bits of the mantissa/significand for the various floating types
# You cannot keep more bits of data than you have available
# https://en.wikipedia.org/wiki/IEEE_754
max_bits = {
    "float16": 10,
    "float32": 23,
    "float64": 52,
}


class BitRound(Codec):
    """Floating-point bit rounding codec

    Drops a specified number of bits from the floating point mantissa,
    leaving an array more amenable to compression. The number of bits to keep should
    be determined by an information analysis of the data to be compressed.
    The approach is based on the paper by Klöwer et al. 2021
    (https://www.nature.com/articles/s43588-021-00156-2). See
    https://github.com/zarr-developers/numcodecs/issues/298 for discussion
    and the original implementation in Julia referred to at
    https://github.com/milankl/BitInformation.jl

    Parameters
    ----------

    keepbits: int
        The number of bits of the mantissa to keep. The range allowed
        depends on the dtype input data. If keepbits is
        equal to the maximum allowed for the data type, this is equivalent
        to no transform.
    """

    codec_id = 'bitround'

    def __init__(self, keepbits: int):
        if keepbits < 0:
            raise ValueError("keepbits must be zero or positive")
        self.keepbits = keepbits

    def encode(self, buf):
        """Create int array by rounding floating-point data

        The itemsize will be preserved, but the output should be much more
        compressible.
        """
        a = ensure_ndarray_like(buf)
        if not a.dtype.kind == "f" or a.dtype.itemsize > 8:
            raise TypeError("Only float arrays (16-64bit) can be bit-rounded")
        bits = max_bits[str(a.dtype)]
        # cast float to int type of same width (preserve endianness)
        a_int_dtype = np.dtype(a.dtype.str.replace("f", "i"))
        all_set = np.array(-1, dtype=a_int_dtype)
        if self.keepbits == bits:
            return a
        if self.keepbits > bits:
            raise ValueError("Keepbits too large for given dtype")
        b = a.copy()
        b = b.view(a_int_dtype)
        maskbits = bits - self.keepbits
        mask = (all_set >> maskbits) << maskbits
        half_quantum1 = (1 << (maskbits - 1)) - 1
        b += ((b >> maskbits) & 1) + half_quantum1
        b &= mask
        return b

    def decode(self, buf, out=None):
        """Remake floats from ints

        As with ``encode``, preserves itemsize.
        """
        buf = ensure_ndarray_like(buf)
        # Cast back from `int` to `float` type (noop if a `float`ing type buffer is provided)
        dt = np.dtype(buf.dtype.str.replace("i", "f"))
        data = buf.view(dt)
        return ndarray_copy(data, out)
