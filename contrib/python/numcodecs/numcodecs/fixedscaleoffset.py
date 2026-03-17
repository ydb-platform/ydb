import numpy as np

from .abc import Codec
from .compat import ensure_ndarray, ndarray_copy


class FixedScaleOffset(Codec):
    """Simplified version of the scale-offset filter available in HDF5.
    Applies the transformation `(x - offset) * scale` to all chunks. Results
    are rounded to the nearest integer but are not packed according to the
    minimum number of bits.

    Parameters
    ----------
    offset : float
        Value to subtract from data.
    scale : float
        Value to multiply by data.
    dtype : dtype
        Data type to use for decoded data.
    astype : dtype, optional
        Data type to use for encoded data.

    Notes
    -----
    If `astype` is an integer data type, please ensure that it is
    sufficiently large to store encoded values. No checks are made and data
    may become corrupted due to integer overflow if `astype` is too small.

    Examples
    --------
    >>> import numcodecs
    >>> import numpy as np
    >>> x = np.linspace(1000, 1001, 10, dtype='f8')
    >>> x
    array([1000.        , 1000.11111111, 1000.22222222, 1000.33333333,
           1000.44444444, 1000.55555556, 1000.66666667, 1000.77777778,
           1000.88888889, 1001.        ])
    >>> codec = numcodecs.FixedScaleOffset(offset=1000, scale=10, dtype='f8', astype='u1')
    >>> y1 = codec.encode(x)
    >>> y1
    array([ 0,  1,  2,  3,  4,  6,  7,  8,  9, 10], dtype=uint8)
    >>> z1 = codec.decode(y1)
    >>> z1
    array([1000. , 1000.1, 1000.2, 1000.3, 1000.4, 1000.6, 1000.7,
           1000.8, 1000.9, 1001. ])
    >>> codec = numcodecs.FixedScaleOffset(offset=1000, scale=10**2, dtype='f8', astype='u1')
    >>> y2 = codec.encode(x)
    >>> y2
    array([ 0,  11,  22,  33,  44,  56,  67,  78,  89, 100], dtype=uint8)
    >>> z2 = codec.decode(y2)
    >>> z2
    array([1000.  , 1000.11, 1000.22, 1000.33, 1000.44, 1000.56,
           1000.67, 1000.78, 1000.89, 1001.  ])
    >>> codec = numcodecs.FixedScaleOffset(offset=1000, scale=10**3, dtype='f8', astype='u2')
    >>> y3 = codec.encode(x)
    >>> y3
    array([ 0,  111,  222,  333,  444,  556,  667,  778,  889, 1000], dtype=uint16)
    >>> z3 = codec.decode(y3)
    >>> z3
    array([1000.   , 1000.111, 1000.222, 1000.333, 1000.444, 1000.556,
           1000.667, 1000.778, 1000.889, 1001.   ])

    See Also
    --------
    numcodecs.quantize.Quantize

    """

    codec_id = 'fixedscaleoffset'

    def __init__(self, offset, scale, dtype, astype=None):
        self.offset = offset
        self.scale = scale
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

        # compute scale offset
        enc = (arr - self.offset) * self.scale

        # round to nearest integer
        enc = np.around(enc)

        # convert dtype
        return enc.astype(self.astype, copy=False)

    def decode(self, buf, out=None):
        # interpret buffer as numpy array
        enc = ensure_ndarray(buf).view(self.astype)

        # flatten to simplify implementation
        enc = enc.reshape(-1, order='A')

        # decode scale offset
        dec = (enc / self.scale) + self.offset

        # convert dtype
        dec = dec.astype(self.dtype, copy=False)

        # handle output
        return ndarray_copy(dec, out)

    def get_config(self):
        # override to handle encoding dtypes
        return {
            'id': self.codec_id,
            'scale': self.scale,
            'offset': self.offset,
            'dtype': self.dtype.str,
            'astype': self.astype.str,
        }

    def __repr__(self):
        r = f'{type(self).__name__}(scale={self.scale}, offset={self.offset}, dtype={self.dtype.str!r}'
        if self.astype != self.dtype:
            r += f', astype={self.astype.str!r}'
        r += ')'
        return r
