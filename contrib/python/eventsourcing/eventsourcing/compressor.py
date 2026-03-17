from __future__ import annotations

import zlib

from eventsourcing.persistence import Compressor


class ZlibCompressor(Compressor):
    def compress(self, data: bytes) -> bytes:
        """Compress bytes using zlib."""
        return zlib.compress(data)

    def decompress(self, data: bytes) -> bytes:
        """Decompress bytes using zlib."""
        return zlib.decompress(data)
