""" subpacket.py
"""
import abc

from ..types import VersionedHeader

from ...decorators import sdproperty

from ...types import Dispatchable
from ...types import Header as _Header

__all__ = ['Header',
           'EmbeddedSignatureHeader',
           'SubPacket',
           'Signature',
           'UserAttribute',
           'Opaque']


class Header(_Header):
    @sdproperty
    def critical(self):
        return self._critical

    @critical.register(bool)
    def critical_bool(self, val):
        self._critical = val

    @sdproperty
    def typeid(self):
        return self._typeid

    @typeid.register(int)
    def typeid_int(self, val):
        self._typeid = val & 0x7f

    @typeid.register(bytes)
    @typeid.register(bytearray)
    def typeid_bin(self, val):
        v = self.bytes_to_int(val)
        self.typeid = v
        self.critical = bool(v & 0x80)

    def __init__(self):
        super(Header, self).__init__()
        self._typeid = -1
        self.critical = False

    def parse(self, packet):
        self.length = packet

        self.typeid = packet[:1]
        del packet[:1]

    def __len__(self):
        return self.llen + 1

    def __bytearray__(self):
        _bytes = bytearray(self.encode_length(self.length))
        _bytes += self.int_to_bytes((int(self.critical) << 7) + self.typeid)
        return _bytes


class EmbeddedSignatureHeader(VersionedHeader):
    def __bytearray__(self):
        return bytearray([self.version])

    def parse(self, packet):
        self.tag = 2
        super(EmbeddedSignatureHeader, self).parse(packet)


class SubPacket(Dispatchable):
    __headercls__ = Header

    def __init__(self):
        super(SubPacket, self).__init__()
        self.header = Header()

        if (
            self.header.typeid == -1
            and (not hasattr(self.__typeid__, '__abstractmethod__'))
            and (self.__typeid__ not in {-1, None})
        ):
            self.header.typeid = self.__typeid__

    def __bytearray__(self):
        return self.header.__bytearray__()

    def __len__(self):
        return (self.header.llen + self.header.length)

    def __repr__(self):
        return "<{} [0x{:02x}] at 0x{:x}>".format(self.__class__.__name__, self.header.typeid, id(self))

    def update_hlen(self):
        self.header.length = (len(self.__bytearray__()) - len(self.header)) + 1

    @abc.abstractmethod
    def parse(self, packet):  # pragma: no cover
        if self.header._typeid == -1:
            self.header.parse(packet)


class Signature(SubPacket):
    __typeid__ = -1


class UserAttribute(SubPacket):
    __typeid__ = -1


class Opaque(Signature, UserAttribute):
    __typeid__ = None

    @sdproperty
    def payload(self):
        return self._payload

    @payload.register(bytes)
    @payload.register(bytearray)
    def payload_bin(self, val):
        self._payload = bytearray(val)

    def __init__(self):
        super(Opaque, self).__init__()
        self.payload = b''

    def __bytearray__(self):
        _bytes = super(Opaque, self).__bytearray__()
        _bytes += self.payload
        return _bytes

    def parse(self, packet):
        super(Opaque, self).parse(packet)
        self.payload = packet[:(self.header.length - 1)]
        del packet[:(self.header.length - 1)]
