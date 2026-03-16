from .lz4 import Compressor as BaseCompressor, Decompressor as BaseDecompressor


class Compressor(BaseCompressor):
    mode = 'high_compression'


class Decompressor(BaseDecompressor):
    pass
