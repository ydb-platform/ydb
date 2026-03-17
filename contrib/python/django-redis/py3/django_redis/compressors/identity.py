from django_redis.compressors.base import BaseCompressor


class IdentityCompressor(BaseCompressor):
    def compress(self, value: bytes) -> bytes:
        return value

    def decompress(self, value: bytes) -> bytes:
        return value
