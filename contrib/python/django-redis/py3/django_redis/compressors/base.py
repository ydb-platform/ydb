class BaseCompressor:
    def __init__(self, options):
        self._options = options

    def compress(self, value: bytes) -> bytes:
        raise NotImplementedError

    def decompress(self, value: bytes) -> bytes:
        raise NotImplementedError
