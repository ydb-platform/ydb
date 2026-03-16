from io import BytesIO

try:
    from clickhouse_cityhash.cityhash import CityHash128
except ImportError:
    raise RuntimeError(
        'Package clickhouse-cityhash is required to use compression'
    )

from .. import errors


class BaseCompressor(object):
    """
    Partial file-like object with write method.
    """
    method = None
    method_byte = None

    def __init__(self):
        self.data = BytesIO()

        super(BaseCompressor, self).__init__()

    def get_value(self):
        value = self.data.getvalue()
        self.data.seek(0)
        self.data.truncate()
        return value

    def write(self, p_str):
        self.data.write(p_str)

    def get_compressed_data(self, extra_header_size):
        raise NotImplementedError


class BaseDecompressor(object):
    method = None
    method_byte = None

    def __init__(self, real_stream):
        self.stream = real_stream
        super(BaseDecompressor, self).__init__()

    def check_hash(self, compressed_data, compressed_hash):
        if CityHash128(compressed_data) != compressed_hash:
            raise errors.ChecksumDoesntMatchError()

    def get_decompressed_data(self, method_byte, compressed_hash,
                              extra_header_size):
        raise NotImplementedError
