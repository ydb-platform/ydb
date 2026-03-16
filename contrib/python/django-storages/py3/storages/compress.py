import io
import zlib
from gzip import GzipFile
from typing import Optional

from storages.utils import to_bytes


class GzipCompressionWrapper(io.RawIOBase):
    """Wrapper for compressing file contents on the fly."""

    def __init__(self, raw, level=zlib.Z_BEST_COMPRESSION):
        super().__init__()
        self.raw = raw
        self.compress = zlib.compressobj(level=level, wbits=31)
        self.leftover = bytearray()

    @staticmethod
    def readable():
        return True

    def readinto(self, buf: bytearray) -> Optional[int]:
        size = len(buf)
        while len(self.leftover) < size:
            chunk = to_bytes(self.raw.read(size))
            if not chunk:
                if self.compress:
                    self.leftover += self.compress.flush(zlib.Z_FINISH)
                    self.compress = None
                break
            self.leftover += self.compress.compress(chunk)
        if len(self.leftover) == 0:
            return 0
        output = self.leftover[:size]
        size = len(output)
        buf[:size] = output
        self.leftover = self.leftover[size:]
        return size


class CompressStorageMixin:
    def _compress_content(self, content):
        """Gzip a given string content."""
        return GzipCompressionWrapper(content)


class CompressedFileMixin:
    def _decompress_file(self, mode, file, mtime=0.0):
        return GzipFile(mode=mode, fileobj=file, mtime=mtime)
