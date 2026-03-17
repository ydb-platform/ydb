import numpy as np

from .abc import Codec
from .compat import ensure_ndarray, ndarray_copy


class Delta(Codec):
    """Codec to encode data as the difference between adjacent values.

    Parameters
    ----------
    dtype : dtype
        Data type to use for decoded data.
    astype : dtype, optional
        Data type to use for encoded data.

    Notes
    -----
    If `astype` is an integer data type, please ensure that it is
    sufficiently large to store encoded values. No checks are made and data
    may become corrupted due to integer overflow if `astype` is too small.
    Note also that the encoded data for each chunk includes the absolute
    value of the first element in the chunk, and so the encoded data type in
    general needs to be large enough to store absolute values from the array.

    Examples
    --------
    >>> import numcodecs
    >>> import numpy as np
    >>> x = np.arange(100, 120, 2, dtype='i2')
    >>> codec = numcodecs.Delta(dtype='i2', astype='i1')
    >>> y = codec.encode(x)
    >>> y
    array([100,   2,   2,   2,   2,   2,   2,   2,   2,   2], dtype=int8)
    >>> z = codec.decode(y)
    >>> z
    array([100, 102, 104, 106, 108, 110, 112, 114, 116, 118], dtype=int16)

    """

    codec_id = 'delta'

    def __init__(self, dtype, astype=None):
        self.dtype = np.dtype(dtype)
        if astype is None:
            self.astype = self.dtype
        else:
            self.astype = np.dtype(astype)
        if self.dtype == np.dtype(object) or self.astype == np.dtype(object):
            raise ValueError('object arrays are not supported')

    def encode(self, buf):
        # normalise input
        arr = ensure_ndarray(buf).view(self.dtype)

        # flatten to simplify implementation
        arr = arr.reshape(-1, order='A')

        # setup encoded output
        enc = np.empty_like(arr, dtype=self.astype)

        # set first element
        enc[0] = arr[0]

        # compute differences
        enc[1:] = np.diff(arr)
        return enc

    def decode(self, buf, out=None):
        # normalise input
        enc = ensure_ndarray(buf).view(self.astype)

        # flatten to simplify implementation
        enc = enc.reshape(-1, order='A')

        # setup decoded output
        dec = np.empty_like(enc, dtype=self.dtype)

        # decode differences
        np.cumsum(enc, out=dec)

        # handle output
        return ndarray_copy(dec, out)

    def get_config(self):
        # override to handle encoding dtypes
        return {'id': self.codec_id, 'dtype': self.dtype.str, 'astype': self.astype.str}

    def __repr__(self):
        r = f'{type(self).__name__}(dtype={self.dtype.str!r}'
        if self.astype != self.dtype:
            r += f', astype={self.astype.str!r}'
        r += ')'
        return r
