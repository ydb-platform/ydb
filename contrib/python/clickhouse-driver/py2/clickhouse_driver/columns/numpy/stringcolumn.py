import numpy as np

from ... import defines
from .base import NumpyColumn


class NumpyStringColumn(NumpyColumn):
    dtype = np.dtype('object')

    default_encoding = defines.STRINGS_ENCODING

    def __init__(self, encoding=default_encoding, **kwargs):
        self.encoding = encoding
        super(NumpyStringColumn, self).__init__(**kwargs)

    def read_items(self, n_items, buf):
        return np.array(
            buf.read_strings(n_items, encoding=self.encoding), dtype=self.dtype
        )

    def write_items(self, items, buf):
        return buf.write_strings(items.tolist(), encoding=self.encoding)


class NumpyByteStringColumn(NumpyColumn):
    def read_items(self, n_items, buf):
        return np.array(buf.read_strings(n_items), dtype=self.dtype)

    def write_items(self, items, buf):
        return buf.write_strings(items.tolist())


class NumpyFixedString(NumpyStringColumn):
    def __init__(self, length, **kwargs):
        self.length = length
        super(NumpyFixedString, self).__init__(**kwargs)

    def read_items(self, n_items, buf):
        return np.array(buf.read_fixed_strings(
            n_items, self.length, encoding=self.encoding
        ), dtype=self.dtype)

    def write_items(self, items, buf):
        return buf.write_fixed_strings(
            items.tolist(), self.length, encoding=self.encoding
        )


class NumpyByteFixedString(NumpyByteStringColumn):
    def __init__(self, length, **kwargs):
        self.length = length
        super(NumpyByteFixedString, self).__init__(**kwargs)

    def read_items(self, n_items, buf):
        return np.array(
            buf.read_fixed_strings(n_items, self.length), dtype=self.dtype
        )

    def write_items(self, items, buf):
        return buf.write_fixed_strings(items.tolist(), self.length)


def create_string_column(spec, column_options):
    client_settings = column_options['context'].client_settings
    strings_as_bytes = client_settings['strings_as_bytes']
    encoding = client_settings.get(
        'strings_encoding', NumpyStringColumn.default_encoding
    )

    if spec == 'String':
        cls = NumpyByteStringColumn if strings_as_bytes else NumpyStringColumn
        return cls(encoding=encoding, **column_options)
    else:
        length = int(spec[12:-1])
        cls = NumpyByteFixedString if strings_as_bytes else NumpyFixedString
        return cls(length, encoding=encoding, **column_options)
