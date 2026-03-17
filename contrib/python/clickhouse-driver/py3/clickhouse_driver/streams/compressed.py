from io import BytesIO

try:
    from clickhouse_cityhash.cityhash import CityHash128
except ImportError:
    raise RuntimeError(
        'Package clickhouse-cityhash is required to use compression'
    )

from .native import BlockOutputStream, BlockInputStream
from ..bufferedreader import CompressedBufferedReader
from ..bufferedwriter import CompressedBufferedWriter
from ..compression import get_decompressor_cls
from ..defines import BUFFER_SIZE
from ..reader import read_binary_uint8, read_binary_uint128
from ..writer import write_binary_uint8, write_binary_uint128


class CompressedBlockOutputStream(BlockOutputStream):
    def __init__(self, compressor_cls, compress_block_size, fout, context):
        self.compressor_cls = compressor_cls
        self.compress_block_size = compress_block_size
        self.raw_fout = fout

        self.compressor = self.compressor_cls()
        self.fout = CompressedBufferedWriter(self.compressor, BUFFER_SIZE)
        super(CompressedBlockOutputStream, self).__init__(self.fout, context)

    def get_compressed_hash(self, data):
        return CityHash128(data)

    def finalize(self):
        self.fout.flush()

        compressed = self.get_compressed()
        compressed_size = len(compressed)

        compressed_hash = self.get_compressed_hash(compressed)
        write_binary_uint128(compressed_hash, self.raw_fout)

        block_size = self.compress_block_size

        i = 0
        while i < compressed_size:
            self.raw_fout.write(compressed[i:i + block_size])
            i += block_size

        self.raw_fout.flush()

    def get_compressed(self):
        compressed = BytesIO()

        if self.compressor.method_byte is not None:
            write_binary_uint8(self.compressor.method_byte, compressed)
            extra_header_size = 1  # method
        else:
            extra_header_size = 0

        data = self.compressor.get_compressed_data(extra_header_size)
        compressed.write(data)

        return compressed.getvalue()


class CompressedBlockInputStream(BlockInputStream):
    def __init__(self, fin, context):
        self.raw_fin = fin
        fin = CompressedBufferedReader(self.read_block, BUFFER_SIZE)
        super(CompressedBlockInputStream, self).__init__(fin, context)

    def get_compressed_hash(self, data):
        return CityHash128(data)

    def read_block(self):
        compressed_hash = read_binary_uint128(self.raw_fin)
        method_byte = read_binary_uint8(self.raw_fin)

        decompressor_cls = get_decompressor_cls(method_byte)
        decompressor = decompressor_cls(self.raw_fin)

        if decompressor.method_byte is not None:
            extra_header_size = 1  # method
        else:
            extra_header_size = 0

        return decompressor.get_decompressed_data(
            method_byte, compressed_hash, extra_header_size
        )
