import zstandard as zstd

from .base import BaseCompressor, BaseDecompressor
from ..protocol import CompressionMethod, CompressionMethodByte


class Compressor(BaseCompressor):
    method = CompressionMethod.ZSTD
    method_byte = CompressionMethodByte.ZSTD

    def compress_data(self, data):
        return zstd.compress(data)


class Decompressor(BaseDecompressor):
    method = CompressionMethod.ZSTD
    method_byte = CompressionMethodByte.ZSTD

    def decompress_data(self, data, uncompressed_size):
        return zstd.decompress(data)
