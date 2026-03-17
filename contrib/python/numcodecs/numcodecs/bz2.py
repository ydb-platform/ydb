import bz2 as _bz2

from numcodecs.abc import Codec
from numcodecs.compat import ensure_contiguous_ndarray, ndarray_copy


class BZ2(Codec):
    """Codec providing compression using bzip2 via the Python standard library.

    Parameters
    ----------
    level : int
        Compression level.

    """

    codec_id = 'bz2'

    def __init__(self, level=1):
        self.level = level

    def encode(self, buf):
        # normalise input
        buf = ensure_contiguous_ndarray(buf)

        # do compression
        return _bz2.compress(buf, self.level)

    # noinspection PyMethodMayBeStatic
    def decode(self, buf, out=None):
        # normalise inputs
        buf = ensure_contiguous_ndarray(buf)
        if out is not None:
            out = ensure_contiguous_ndarray(out)

        # N.B., bz2 cannot handle ndarray directly because of truth testing issues
        buf = memoryview(buf)

        # do decompression
        dec = _bz2.decompress(buf)

        # handle destination - Python standard library bz2 module does not
        # support direct decompression into buffer, so we have to copy into
        # out if given
        return ndarray_copy(dec, out)
