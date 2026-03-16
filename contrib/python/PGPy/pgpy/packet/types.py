""" types.py
"""
from __future__ import division

import abc
import copy

from ..constants import PacketTag

from ..decorators import sdproperty

from ..types import Dispatchable
from ..types import Field
from ..types import Header as _Header

__all__ = ['Header',
           'VersionedHeader',
           'Packet',
           'VersionedPacket',
           'Opaque',
           'Key',
           'Public',
           'Private',
           'Primary',
           'Sub',
           'MPI',
           'MPIs', ]


class Header(_Header):
    @sdproperty
    def tag(self):
        return self._tag

    @tag.register(int)
    @tag.register(PacketTag)
    def tag_int(self, val):
        _tag = (val & 0x3F) if self._lenfmt else ((val & 0x3C) >> 2)
        try:
            self._tag = PacketTag(_tag)

        except ValueError:  # pragma: no cover
            self._tag = _tag

    @property
    def typeid(self):
        return self.tag

    def __init__(self):
        super(Header, self).__init__()
        self.tag = 0x00

    def __bytearray__(self):
        tag = 0x80 | (self._lenfmt << 6)
        tag |= (self.tag) if self._lenfmt else ((self.tag << 2) | {1: 0, 2: 1, 4: 2, 0: 3}[self.llen])

        _bytes = bytearray(self.int_to_bytes(tag))
        _bytes += self.encode_length(self.length, self._lenfmt, self.llen)
        return _bytes

    def __len__(self):
        return 1 + self.llen

    def parse(self, packet):
        """
        There are two formats for headers

        old style
        ---------

        Old style headers can be 1, 2, 3, or 6 octets long and are composed of a Tag and a Length.
        If the header length is 1 octet (length_type == 3), then there is no Length field.

        new style
        ---------

        New style headers can be 2, 3, or 6 octets long and are also composed of a Tag and a Length.


        Packet Tag
        ----------

        The packet tag is the first byte, comprising the following fields:

        +-------------+----------+---------------+---+---+---+---+----------+----------+
        | byte        | 1                                                              |
        +-------------+----------+---------------+---+---+---+---+----------+----------+
        | bit         | 7        | 6             | 5 | 4 | 3 | 2 | 1        | 0        |
        +-------------+----------+---------------+---+---+---+---+----------+----------+
        | old-style   | always 1 | packet format | packet tag    | length type         |
        | description |          | 0 = old-style |               | 0 = 1 octet         |
        |             |          | 1 = new-style |               | 1 = 2 octets        |
        |             |          |               |               | 2 = 5 octets        |
        |             |          |               |               | 3 = no length field |
        +-------------+          +               +---------------+---------------------+
        | new-style   |          |               | packet tag                          |
        | description |          |               |                                     |
        +-------------+----------+---------------+-------------------------------------+

        :param packet: raw packet bytes
        """
        self._lenfmt = ((packet[0] & 0x40) >> 6)
        self.tag = packet[0]
        if self._lenfmt == 0:
            self.llen = (packet[0] & 0x03)
        del packet[0]

        if (self._lenfmt == 0 and self.llen > 0) or self._lenfmt == 1:
            self.length = packet

        else:
            # indeterminate packet length
            self.length = len(packet)


class VersionedHeader(Header):
    @sdproperty
    def version(self):
        return self._version

    @version.register(int)
    def version_int(self, val):
        self._version = val

    def __init__(self):
        super(VersionedHeader, self).__init__()
        self.version = 0

    def __bytearray__(self):
        _bytes = bytearray(super(VersionedHeader, self).__bytearray__())
        _bytes += bytearray([self.version])
        return _bytes

    def parse(self, packet):  # pragma: no cover
        if self.tag == 0:
            super(VersionedHeader, self).parse(packet)

        if self.version == 0:
            self.version = packet[0]
            del packet[0]


class Packet(Dispatchable):
    __typeid__ = -1
    __headercls__ = Header

    def __init__(self, _=None):
        super(Packet, self).__init__()
        self.header = self.__headercls__()
        if isinstance(self.__typeid__, int):
            self.header.tag = self.__typeid__

    @abc.abstractmethod
    def __bytearray__(self):
        return self.header.__bytearray__()

    def __len__(self):
        return len(self.header) + self.header.length

    def __repr__(self):
        return "<{cls:s} [tag {tag:02d}] at 0x{id:x}>".format(cls=self.__class__.__name__, tag=self.header.tag, id=id(self))

    def update_hlen(self):
        self.header.length = len(self.__bytearray__()) - len(self.header)

    @abc.abstractmethod
    def parse(self, packet):
        if self.header.tag == 0:
            self.header.parse(packet)


class VersionedPacket(Packet):
    __headercls__ = VersionedHeader

    def __init__(self):
        super(VersionedPacket, self).__init__()
        if isinstance(self.__ver__, int):
            self.header.version = self.__ver__

    def __repr__(self):
        return "<{cls:s} [tag {tag:02d}][v{ver:d}] at 0x{id:x}>".format(cls=self.__class__.__name__, tag=self.header.tag,
                                                                        ver=self.header.version, id=id(self))


class Opaque(Packet):
    __typeid__ = None

    @sdproperty
    def payload(self):
        return self._payload

    @payload.register(bytearray)
    @payload.register(bytes)
    def payload_bin(self, val):
        self._payload = val

    def __init__(self):
        super(Opaque, self).__init__()
        self.payload = b''

    def __bytearray__(self):
        _bytes = super(Opaque, self).__bytearray__()
        _bytes += self.payload
        return _bytes

    def parse(self, packet):  # pragma: no cover
        super(Opaque, self).parse(packet)
        pend = self.header.length
        if hasattr(self.header, 'version'):
            pend -= 1

        self.payload = packet[:pend]
        del packet[:pend]


# key marker classes for convenience
class Key(object):
    pass


class Public(Key):
    pass


class Private(Key):
    pass


class Primary(Key):
    pass


class Sub(Key):
    pass


# This is required for class MPI to work in both Python 2 and 3
long = int


class MPI(long):
    def __new__(cls, num):
        mpi = num

        if isinstance(num, (bytes, bytearray)):
            if isinstance(num, bytes):  # pragma: no cover
                num = bytearray(num)

            fl = ((MPIs.bytes_to_int(num[:2]) + 7) // 8)
            del num[:2]

            mpi = MPIs.bytes_to_int(num[:fl])
            del num[:fl]

        return super(MPI, cls).__new__(cls, mpi)

    def byte_length(self):
        return ((self.bit_length() + 7) // 8)

    def to_mpibytes(self):
        return MPIs.int_to_bytes(self.bit_length(), 2) + MPIs.int_to_bytes(self, self.byte_length())

    def __len__(self):
        return self.byte_length() + 2


class MPIs(Field):
    # this differs from MPI in that it's subclasses hold/parse several MPI fields
    # and, in the case of v4 private keys, also a String2Key specifier/information.
    __mpis__ = ()

    def __len__(self):
        return sum(len(i) for i in self)

    def __iter__(self):
        """yield all components of an MPI so it can be iterated over"""
        for i in self.__mpis__:
            yield getattr(self, i)

    def __copy__(self):
        pk = self.__class__()
        for m in self.__mpis__:
            setattr(pk, m, copy.copy(getattr(self, m)))

        return pk
