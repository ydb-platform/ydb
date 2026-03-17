from lz4 import block

from .base import BaseCompressor, BaseDecompressor
from ..protocol import CompressionMethod, CompressionMethodByte


class Compressor(BaseCompressor):
    method = CompressionMethod.LZ4
    method_byte = CompressionMethodByte.LZ4
    mode = 'default'

    def compress_data(self, data):
        return block.compress(data, store_size=False, mode=self.mode)


class Decompressor(BaseDecompressor):
    method = CompressionMethod.LZ4
    method_byte = CompressionMethodByte.LZ4

    def decompress_data(self, data, uncompressed_size):
        return block.decompress(data, uncompressed_size=uncompressed_size)
