# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import enum
import struct
import typing


def _pack_value(addr_type: typing.Optional["AddressType"], b: typing.Optional[bytes]) -> bytes:
    """Packs an type/data entry into the byte structure required."""
    if not b:
        b = b""

    return (struct.pack("<I", addr_type) if addr_type is not None else b"") + struct.pack("<I", len(b)) + b


def _unpack_value(b_mem: memoryview, offset: int) -> typing.Tuple[bytes, int]:
    """Unpacks a raw C struct value to a byte string."""
    length = struct.unpack("<I", b_mem[offset : offset + 4].tobytes())[0]
    new_offset = offset + length + 4

    data = b""
    if length:
        data = b_mem[offset + 4 : offset + 4 + length].tobytes()

    return data, new_offset


class AddressType(enum.IntEnum):
    unspecified = 0  # GSS_C_AF_UNSPEC
    local = 1  # GSS_C_AF_LOCAL
    inet = 2  # GSS_C_AF_INET
    implink = 3  # GSS_C_AF_IMPLINK
    pup = 4  # GSS_C_AF_PUP
    chaos = 5  # GSS_C_AF_CHAOS
    ns = 6  # GSS_C_AF_NS
    nbs = 8  # GSS_C_AF_NBS
    ecma = 8  # GSS_C_AF_ECMA
    datakit = 9  # GSS_C_AF_DATAKIT
    ccitt = 10  # GSS_C_AF_CCITT
    sna = 11  # GSS_C_AF_SNA
    decnet = 12  # GSS_C_AF_DECnet
    dli = 13  # GSS_C_AF_DLI
    lat = 14  # GSS_C_AF_LAT
    hylink = 15  # GSS_C_AF_HYLINK
    appletalk = 16  # GSS_C_AF_APPLETALK
    bsc = 17  # GSS_C_AF_BSC
    dss = 18  # GSS_C_AF_DSS
    osi = 19  # GSS_C_AF_OSI
    x25 = 21  # GSS_C_AF_X25
    inet6 = 24  # GSS_C_AF_INET6
    nulladdr = 255  # GSS_C_AF_NULLADDR


class GssChannelBindings:
    """Python representation of a GSSAPI Channel Binding data structure.

    A common representation for a GSSAPI Channel Binding data structure that can be passed into a context to bind
    against that security context. Channel bindings are tags that identify the particular data channel that is used.
    Because these tags are specific to the originator and recipient applications, they offer more proof of a valid
    identity. Most HTTPS based authentications just set the application data to b'tls-server-end-point:<cert hash>'.

    Args:
        initiator_addrtype: The address type of the initiator address.
        initiator_address: The initiator's address.
        acceptor_addrtype: The address type of the acceptor address.
        acceptor_address: The acceptor's address.
        application_data: Any extra application data to set on the bindings struct.
    """

    def __init__(
        self,
        initiator_addrtype: AddressType = AddressType.unspecified,
        initiator_address: typing.Optional[bytes] = None,
        acceptor_addrtype: AddressType = AddressType.unspecified,
        acceptor_address: typing.Optional[bytes] = None,
        application_data: typing.Optional[bytes] = None,
    ) -> None:
        self.initiator_addrtype = AddressType(initiator_addrtype)
        self.initiator_address = initiator_address
        self.acceptor_addrtype = AddressType(acceptor_addrtype)
        self.acceptor_address = acceptor_address
        self.application_data = application_data

    def __repr__(self) -> str:
        return (
            "{0}.{1} initiator_addrtype={2}|initiator_address={3}|acceptor_addrtype={4}|acceptor_address={5}|"
            "application_data={6}".format(
                type(self).__module__,
                type(self).__name__,
                repr(self.initiator_addrtype),
                repr(self.initiator_address),
                repr(self.acceptor_addrtype),
                repr(self.acceptor_address),
                repr(self.application_data),
            )
        )

    def __str__(self) -> str:
        return "{0} initiator_addr({1}.{2}|{3!r}) | acceptor_addr({4}.{5}|{6!r}) | application_data({7!r})".format(
            type(self).__name__,
            type(self.initiator_addrtype).__name__,
            self.initiator_addrtype.name,
            self.initiator_address,
            type(self.acceptor_addrtype).__name__,
            self.acceptor_addrtype.name,
            self.acceptor_address,
            self.application_data,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, (bytes, GssChannelBindings)):
            return False

        if isinstance(other, GssChannelBindings):
            other = other.pack()

        return self.pack() == other

    def pack(self) -> bytes:
        """Pack struct into a byte string."""
        return b"".join(
            [
                _pack_value(self.initiator_addrtype, self.initiator_address),
                _pack_value(self.acceptor_addrtype, self.acceptor_address),
                _pack_value(None, self.application_data),
            ]
        )

    @staticmethod
    def unpack(b_data: bytes) -> "GssChannelBindings":
        b_mem = memoryview(b_data)

        initiator_addrtype = struct.unpack("<I", b_mem[:4].tobytes())[0]
        initiator_address, offset = _unpack_value(b_mem, 4)

        acceptor_addrtype = struct.unpack("<I", b_mem[offset : offset + 4].tobytes())[0]
        acceptor_address, offset = _unpack_value(b_mem, offset + 4)

        application_data = _unpack_value(b_mem, offset)[0]

        return GssChannelBindings(
            initiator_addrtype=initiator_addrtype,
            initiator_address=initiator_address,
            acceptor_addrtype=acceptor_addrtype,
            acceptor_address=acceptor_address,
            application_data=application_data,
        )
