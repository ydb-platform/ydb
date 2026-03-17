from __future__ import absolute_import
from io import BytesIO

from lz4 import block

from .base import BaseCompressor, BaseDecompressor
from ..protocol import CompressionMethod, CompressionMethodByte
from ..reader import read_binary_uint32
from ..writer import write_binary_uint32, write_binary_uint8


class Compressor(BaseCompressor):
    method = CompressionMethod.LZ4
    method_byte = CompressionMethodByte.LZ4
    mode = 'default'

    def get_compressed_data(self, extra_header_size):
        rv = BytesIO()

        data = self.get_value()
        compressed = block.compress(data, store_size=False, mode=self.mode)

        header_size = extra_header_size + 4 + 4  # sizes

        write_binary_uint32(header_size + len(compressed), rv)
        write_binary_uint32(len(data), rv)
        rv.write(compressed)

        return rv.getvalue()


class Decompressor(BaseDecompressor):
    method = CompressionMethod.LZ4
    method_byte = CompressionMethodByte.LZ4

    def get_decompressed_data(self, method_byte, compressed_hash,
                              extra_header_size):
        size_with_header = read_binary_uint32(self.stream)
        compressed_size = size_with_header - extra_header_size - 4

        compressed = BytesIO(self.stream.read(compressed_size))

        block_check = BytesIO()
        write_binary_uint8(method_byte, block_check)
        write_binary_uint32(size_with_header, block_check)
        block_check.write(compressed.getvalue())

        self.check_hash(block_check.getvalue(), compressed_hash)

        uncompressed_size = read_binary_uint32(compressed)

        compressed = compressed.read(compressed_size - 4)

        return block.decompress(compressed,
                                uncompressed_size=uncompressed_size)
