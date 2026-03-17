import gzip as _gzip
import io

from .abc import Codec
from .compat import ensure_bytes, ensure_contiguous_ndarray


class GZip(Codec):
    """Codec providing gzip compression using zlib via the Python standard library.

    Parameters
    ----------
    level : int
        Compression level.

    """

    codec_id = 'gzip'

    def __init__(self, level=1):
        self.level = level

    def encode(self, buf):
        # normalise inputs
        buf = ensure_contiguous_ndarray(buf)

        # do compression
        compressed = io.BytesIO()
        with _gzip.GzipFile(fileobj=compressed, mode='wb', compresslevel=self.level) as compressor:
            compressor.write(buf)
        return compressed.getvalue()

    # noinspection PyMethodMayBeStatic
    def decode(self, buf, out=None):
        # normalise inputs
        # BytesIO only copies if the data is not of `bytes` type.
        # This allows `bytes` objects to pass through without copying.
        buf = io.BytesIO(ensure_bytes(buf))

        # do decompression
        with _gzip.GzipFile(fileobj=buf, mode='rb') as decompressor:
            if out is not None:
                out_view = ensure_contiguous_ndarray(out)
                decompressor.readinto(out_view)
                if decompressor.read(1) != b'':
                    raise ValueError("Unable to fit data into `out`")
            else:
                out = decompressor.read()

        return out
