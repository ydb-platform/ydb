import base64 as _base64

from .abc import Codec
from .compat import ensure_contiguous_ndarray, ndarray_copy


class Base64(Codec):
    """Codec providing base64 compression via the Python standard library."""

    codec_id = "base64"

    def encode(self, buf):
        # normalise inputs
        buf = ensure_contiguous_ndarray(buf)
        # do compression
        return _base64.standard_b64encode(buf)

    def decode(self, buf, out=None):
        # normalise inputs
        buf = ensure_contiguous_ndarray(buf)
        if out is not None:
            out = ensure_contiguous_ndarray(out)
        # do decompression
        decompressed = _base64.standard_b64decode(buf)
        # handle destination
        return ndarray_copy(decompressed, out)
