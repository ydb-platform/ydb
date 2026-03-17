import zlib

try:
    import bz2
except ImportError:
    pass


class BaseCompressor(object):

    def __init__(self, **kwargs):
        super(BaseCompressor, self).__init__()

    def compress(self, value):
        raise NotImplementedError

    def decompress(self, value):
        raise NotImplementedError


class NoopCompressor(BaseCompressor):

    def compress(self, value):
        return value

    def decompress(self, value):
        return value


class ZLibCompressor(BaseCompressor):

    def __init__(self, level=6):
        self.level = level
        super(ZLibCompressor, self).__init__()

    def compress(self, value):
        return zlib.compress(value, self.level)

    def decompress(self, value):
        return zlib.decompress(value)


class BZip2Compressor(BaseCompressor):

    def __init__(self, compresslevel=9):
        self.compresslevel = compresslevel
        super(BZip2Compressor, self).__init__()

    def compress(self, value):
        return bz2.compress(value, compresslevel=self.compresslevel)

    def decompress(self, value):
        return bz2.decompress(value)
