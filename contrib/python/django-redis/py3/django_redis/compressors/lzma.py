import lzma

from django_redis.compressors.base import BaseCompressor
from django_redis.exceptions import CompressorError


class LzmaCompressor(BaseCompressor):
    min_length = 100
    preset = 4

    def compress(self, value: bytes) -> bytes:
        if len(value) > self.min_length:
            return lzma.compress(value, preset=self.preset)
        return value

    def decompress(self, value: bytes) -> bytes:
        try:
            return lzma.decompress(value)
        except lzma.LZMAError as e:
            raise CompressorError from e
