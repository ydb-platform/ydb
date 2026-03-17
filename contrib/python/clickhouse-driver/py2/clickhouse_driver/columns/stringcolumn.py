
from .. import defines
from ..util import compat
from .base import Column


class String(Column):
    ch_type = 'String'
    py_types = compat.string_types
    null_value = ''

    default_encoding = defines.STRINGS_ENCODING

    def __init__(self, encoding=default_encoding, **kwargs):
        self.encoding = encoding
        super(String, self).__init__(**kwargs)

    def write_items(self, items, buf):
        buf.write_strings(items, encoding=self.encoding)

    def read_items(self, n_items, buf):
        return buf.read_strings(n_items, encoding=self.encoding)


class ByteString(String):
    py_types = (bytes, )
    null_value = b''

    def write_items(self, items, buf):
        buf.write_strings(items)

    def read_items(self, n_items, buf):
        return buf.read_strings(n_items)


class FixedString(String):
    ch_type = 'FixedString'

    def __init__(self, length, **kwargs):
        self.length = length
        super(FixedString, self).__init__(**kwargs)

    def read_items(self, n_items, buf):
        return buf.read_fixed_strings(
            n_items, self.length, encoding=self.encoding
        )

    def write_items(self, items, buf):
        buf.write_fixed_strings(items, self.length, encoding=self.encoding)


class ByteFixedString(FixedString):
    py_types = (bytearray, bytes)
    null_value = b''

    def read_items(self, n_items, buf):
        return buf.read_fixed_strings(n_items, self.length)

    def write_items(self, items, buf):
        buf.write_fixed_strings(items, self.length)


def create_string_column(spec, column_options):
    client_settings = column_options['context'].client_settings
    strings_as_bytes = client_settings['strings_as_bytes']
    encoding = client_settings.get('strings_encoding', String.default_encoding)

    if spec == 'String':
        cls = ByteString if strings_as_bytes else String
        return cls(encoding=encoding, **column_options)
    else:
        length = int(spec[12:-1])
        cls = ByteFixedString if strings_as_bytes else FixedString
        return cls(length, encoding=encoding, **column_options)
