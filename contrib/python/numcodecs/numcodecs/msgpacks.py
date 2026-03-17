import msgpack
import numpy as np

from .abc import Codec
from .compat import ensure_contiguous_ndarray


class MsgPack(Codec):
    """Codec to encode data as msgpacked bytes. Useful for encoding an array of Python
    objects.

    .. versionchanged:: 0.6
        The encoding format has been changed to include the array shape in the encoded
        data, which ensures that all object arrays can be correctly encoded and decoded.

    Parameters
    ----------
    use_single_float : bool, optional
        Use single precision float type for float.
    use_bin_type : bool, optional
        Use bin type introduced in msgpack spec 2.0 for bytes. It also enables str8 type
        for unicode.
    raw : bool, optional
        If true, unpack msgpack raw to Python bytes. Otherwise, unpack to Python str
        by decoding with UTF-8 encoding.

    Examples
    --------
    >>> import numcodecs
    >>> import numpy as np
    >>> x = np.array(['foo', 'bar', 'baz'], dtype='object')
    >>> codec = numcodecs.MsgPack()
    >>> codec.decode(codec.encode(x))
    array(['foo', 'bar', 'baz'], dtype=object)

    See Also
    --------
    numcodecs.pickles.Pickle, numcodecs.json.JSON, numcodecs.vlen.VLenUTF8

    Notes
    -----
    Requires `msgpack <https://pypi.org/project/msgpack/>`_ to be installed.

    """

    codec_id = 'msgpack2'

    def __init__(self, use_single_float=False, use_bin_type=True, raw=False):
        self.use_single_float = use_single_float
        self.use_bin_type = use_bin_type
        self.raw = raw

    def encode(self, buf):
        try:
            buf = np.asarray(buf)
        except ValueError:
            buf = np.asarray(buf, dtype=object)
        items = buf.tolist()
        items.extend((buf.dtype.str, buf.shape))
        return msgpack.packb(
            items,
            use_bin_type=self.use_bin_type,
            use_single_float=self.use_single_float,
        )

    def decode(self, buf, out=None):
        buf = ensure_contiguous_ndarray(buf)
        items = msgpack.unpackb(buf, raw=self.raw)
        dec = np.empty(items[-1], dtype=items[-2])
        dec[:] = items[:-2]
        if out is not None:
            np.copyto(out, dec)
            return out
        else:
            return dec

    def get_config(self):
        return {
            'id': self.codec_id,
            'raw': self.raw,
            'use_single_float': self.use_single_float,
            'use_bin_type': self.use_bin_type,
        }

    def __repr__(self):
        return f'MsgPack(raw={self.raw!r}, use_bin_type={self.use_bin_type!r}, use_single_float={self.use_single_float!r})'
