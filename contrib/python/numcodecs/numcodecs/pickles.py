import pickle

import numpy as np

from .abc import Codec
from .compat import ensure_contiguous_ndarray


class Pickle(Codec):
    """Codec to encode data as as pickled bytes. Useful for encoding an array of Python string
    objects.

    Parameters
    ----------
    protocol : int, defaults to pickle.HIGHEST_PROTOCOL
        The protocol used to pickle data.

    Examples
    --------
    >>> import numcodecs as codecs
    >>> import numpy as np
    >>> x = np.array(['foo', 'bar', 'baz'], dtype='object')
    >>> f = codecs.Pickle()
    >>> f.decode(f.encode(x))
    array(['foo', 'bar', 'baz'], dtype=object)

    See Also
    --------
    numcodecs.msgpacks.MsgPack

    """

    codec_id = 'pickle'

    def __init__(self, protocol=pickle.HIGHEST_PROTOCOL):
        self.protocol = protocol

    def encode(self, buf):
        return pickle.dumps(buf, protocol=self.protocol)

    def decode(self, buf, out=None):
        buf = ensure_contiguous_ndarray(buf)
        dec = pickle.loads(buf)

        if out is not None:
            np.copyto(out, dec)
            return out
        else:
            return dec

    def get_config(self):
        return {'id': self.codec_id, 'protocol': self.protocol}

    def __repr__(self):
        return f'Pickle(protocol={self.protocol})'
