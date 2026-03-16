import zlib as _zlib

from .abc import Codec
from .compat import ensure_contiguous_ndarray, ndarray_copy


class Zlib(Codec):
    """Codec providing compression using zlib via the Python standard library.

    Parameters
    ----------
    level : int
        Compression level.

    """

    codec_id = 'zlib'

    def __init__(self, level=1):
        self.level = level

    def encode(self, buf):
        # normalise inputs
        buf = ensure_contiguous_ndarray(buf)

        # do compression
        return _zlib.compress(buf, self.level)

    # noinspection PyMethodMayBeStatic
    def decode(self, buf, out=None):
        # normalise inputs
        buf = ensure_contiguous_ndarray(buf)
        if out is not None:
            out = ensure_contiguous_ndarray(out)

        # do decompression
        dec = _zlib.decompress(buf)

        # handle destination - Python standard library zlib module does not
        # support direct decompression into buffer, so we have to copy into
        # out if given
        return ndarray_copy(dec, out)
