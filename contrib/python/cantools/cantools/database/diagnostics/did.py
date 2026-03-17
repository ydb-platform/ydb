# A DID.

import binascii

from ..utils import create_encode_decode_formats, decode_data, encode_data


class Did:
    """A DID with identifier and other information.

    """

    def __init__(self,
                 identifier,
                 name,
                 length,
                 datas):
        self._identifier = identifier
        self._name = name
        self._length = length
        self._datas = datas
        self._codec = None
        self.refresh()

    @property
    def identifier(self):
        """The did identifier as an integer.

        """

        return self._identifier

    @identifier.setter
    def identifier(self, value):
        self._identifier = value

    @property
    def name(self):
        """The did name as a string.

        """

        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def length(self):
        """The did name as a string.

        """

        return self._length

    @length.setter
    def length(self, value):
        self._length = value

    @property
    def datas(self):
        """The did datas as a string.

        """

        return self._datas

    @datas.setter
    def datas(self, value):
        self._datas = value

    def get_data_by_name(self, name):
        for data in self._datas:
            if data.name == name:
                return data

        raise KeyError(name)

    def encode(self, data, scaling=True):
        """Encode given data as a DID of this type.

        If `scaling` is ``False`` no scaling of datas is performed.

        >>> foo = db.get_did_by_name('Foo')
        >>> foo.encode({'Bar': 1, 'Fum': 5.0})
        b'\\x01\\x45\\x23\\x00\\x11'

        """

        encoded = encode_data(data,
                              self._codec['datas'],
                              self._codec['formats'],
                              scaling)
        encoded |= (0x80 << (8 * self._length))
        encoded = hex(encoded)[4:].rstrip('L')

        return binascii.unhexlify(encoded)[:self._length]

    def decode(self,
               data,
               decode_choices=True,
               scaling=True,
               allow_truncated=False,
               allow_excess=True):
        """Decode given data as a DID of this type.

        If `decode_choices` is ``False`` scaled values are not
        converted to choice strings (if available).

        If `scaling` is ``False`` no scaling of datas is performed.

        >>> foo = db.get_did_by_name('Foo')
        >>> foo.decode(b'\\x01\\x45\\x23\\x00\\x11')
        {'Bar': 1, 'Fum': 5.0}

        """

        return decode_data(data[:self._length],
                           self.length,
                           self._codec['datas'],
                           self._codec['formats'],
                           decode_choices,
                           scaling,
                           allow_truncated,
                           allow_excess)

    def refresh(self):
        """Refresh the internal DID state.

        """

        self._codec = {
            'datas': self._datas,
            'formats': create_encode_decode_formats(self._datas,
                                                    self._length)
        }

    def __repr__(self):
        return f"did('{self._name}', 0x{self._identifier:04x})"
