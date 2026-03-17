import numpy as np

from .abc import Codec
from .compat import ensure_ndarray, ndarray_copy


class AsType(Codec):
    """Filter to convert data between different types.

    Parameters
    ----------
    encode_dtype : dtype
        Data type to use for encoded data.
    decode_dtype : dtype, optional
        Data type to use for decoded data.

    Notes
    -----
    If `encode_dtype` is of lower precision than `decode_dtype`, please be
    aware that data loss can occur by writing data to disk using this filter.
    No checks are made to ensure the casting will work in that direction and
    data corruption will occur.

    Examples
    --------
    >>> import numcodecs
    >>> import numpy as np
    >>> x = np.arange(100, 120, 2, dtype=np.int8)
    >>> x
    array([100, 102, 104, 106, 108, 110, 112, 114, 116, 118], dtype=int8)
    >>> f = numcodecs.AsType(encode_dtype=x.dtype, decode_dtype=np.int16)
    >>> y = f.decode(x)
    >>> y
    array([100, 102, 104, 106, 108, 110, 112, 114, 116, 118], dtype=int16)
    >>> z = f.encode(y)
    >>> z
    array([100, 102, 104, 106, 108, 110, 112, 114, 116, 118], dtype=int8)

    """

    codec_id = 'astype'

    def __init__(self, encode_dtype, decode_dtype):
        self.encode_dtype = np.dtype(encode_dtype)
        self.decode_dtype = np.dtype(decode_dtype)

    def encode(self, buf):
        # normalise input
        arr = ensure_ndarray(buf).view(self.decode_dtype)

        # convert and copy
        return arr.astype(self.encode_dtype)

    def decode(self, buf, out=None):
        # normalise input
        enc = ensure_ndarray(buf).view(self.encode_dtype)

        # convert and copy
        dec = enc.astype(self.decode_dtype)

        # handle output
        return ndarray_copy(dec, out)

    def get_config(self):
        return {
            'id': self.codec_id,
            'encode_dtype': self.encode_dtype.str,
            'decode_dtype': self.decode_dtype.str,
        }

    def __repr__(self):
        return f'{type(self).__name__}(encode_dtype={self.encode_dtype.str!r}, decode_dtype={self.decode_dtype.str!r})'
