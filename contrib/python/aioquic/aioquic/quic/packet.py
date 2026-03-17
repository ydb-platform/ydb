import binascii
import ipaddress
import os
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from ..buffer import Buffer
from .rangeset import RangeSet

PACKET_LONG_HEADER = 0x80
PACKET_FIXED_BIT = 0x40
PACKET_SPIN_BIT = 0x20

CONNECTION_ID_MAX_SIZE = 20
PACKET_NUMBER_MAX_SIZE = 4
RETRY_AEAD_KEY_VERSION_1 = binascii.unhexlify("be0c690b9f66575a1d766b54e368c84e")
RETRY_AEAD_KEY_VERSION_2 = binascii.unhexlify("8fb4b01b56ac48e260fbcbcead7ccc92")
RETRY_AEAD_NONCE_VERSION_1 = binascii.unhexlify("461599d35d632bf2239825bb")
RETRY_AEAD_NONCE_VERSION_2 = binascii.unhexlify("d86969bc2d7c6d9990efb04a")
RETRY_INTEGRITY_TAG_SIZE = 16
STATELESS_RESET_TOKEN_SIZE = 16


class QuicErrorCode(IntEnum):
    NO_ERROR = 0x0
    INTERNAL_ERROR = 0x1
    CONNECTION_REFUSED = 0x2
    FLOW_CONTROL_ERROR = 0x3
    STREAM_LIMIT_ERROR = 0x4
    STREAM_STATE_ERROR = 0x5
    FINAL_SIZE_ERROR = 0x6
    FRAME_ENCODING_ERROR = 0x7
    TRANSPORT_PARAMETER_ERROR = 0x8
    CONNECTION_ID_LIMIT_ERROR = 0x9
    PROTOCOL_VIOLATION = 0xA
    INVALID_TOKEN = 0xB
    APPLICATION_ERROR = 0xC
    CRYPTO_BUFFER_EXCEEDED = 0xD
    KEY_UPDATE_ERROR = 0xE
    AEAD_LIMIT_REACHED = 0xF
    VERSION_NEGOTIATION_ERROR = 0x11
    CRYPTO_ERROR = 0x100


class QuicPacketType(Enum):
    INITIAL = 0
    ZERO_RTT = 1
    HANDSHAKE = 2
    RETRY = 3
    VERSION_NEGOTIATION = 4
    ONE_RTT = 5


# For backwards compatibility only, use `QuicPacketType` in new code.
PACKET_TYPE_INITIAL = QuicPacketType.INITIAL

# QUIC version 1
# https://datatracker.ietf.org/doc/html/rfc9000#section-17.2
PACKET_LONG_TYPE_ENCODE_VERSION_1 = {
    QuicPacketType.INITIAL: 0,
    QuicPacketType.ZERO_RTT: 1,
    QuicPacketType.HANDSHAKE: 2,
    QuicPacketType.RETRY: 3,
}
PACKET_LONG_TYPE_DECODE_VERSION_1 = dict(
    (v, i) for (i, v) in PACKET_LONG_TYPE_ENCODE_VERSION_1.items()
)

# QUIC version 2
# https://datatracker.ietf.org/doc/html/rfc9369#section-3.2
PACKET_LONG_TYPE_ENCODE_VERSION_2 = {
    QuicPacketType.INITIAL: 1,
    QuicPacketType.ZERO_RTT: 2,
    QuicPacketType.HANDSHAKE: 3,
    QuicPacketType.RETRY: 0,
}
PACKET_LONG_TYPE_DECODE_VERSION_2 = dict(
    (v, i) for (i, v) in PACKET_LONG_TYPE_ENCODE_VERSION_2.items()
)


class QuicProtocolVersion(IntEnum):
    NEGOTIATION = 0
    VERSION_1 = 0x00000001
    VERSION_2 = 0x6B3343CF


@dataclass
class QuicHeader:
    version: Optional[int]
    "The protocol version. Only present in long header packets."

    packet_type: QuicPacketType
    "The type of the packet."

    packet_length: int
    "The total length of the packet, in bytes."

    destination_cid: bytes
    "The destination connection ID."

    source_cid: bytes
    "The destination connection ID."

    token: bytes
    "The address verification token. Only present in `INITIAL` and `RETRY` packets."

    integrity_tag: bytes
    "The retry integrity tag. Only present in `RETRY` packets."

    supported_versions: list[int]
    "Supported protocol versions. Only present in `VERSION_NEGOTIATION` packets."


def decode_packet_number(truncated: int, num_bits: int, expected: int) -> int:
    """
    Recover a packet number from a truncated packet number.

    See: Appendix A - Sample Packet Number Decoding Algorithm
    """
    window = 1 << num_bits
    half_window = window // 2
    candidate = (expected & ~(window - 1)) | truncated
    if candidate <= expected - half_window and candidate < (1 << 62) - window:
        return candidate + window
    elif candidate > expected + half_window and candidate >= window:
        return candidate - window
    else:
        return candidate


def get_retry_integrity_tag(
    packet_without_tag: bytes, original_destination_cid: bytes, version: int
) -> bytes:
    """
    Calculate the integrity tag for a RETRY packet.
    """
    # build Retry pseudo packet
    buf = Buffer(capacity=1 + len(original_destination_cid) + len(packet_without_tag))
    buf.push_uint8(len(original_destination_cid))
    buf.push_bytes(original_destination_cid)
    buf.push_bytes(packet_without_tag)
    assert buf.eof()

    if version == QuicProtocolVersion.VERSION_2:
        aead_key = RETRY_AEAD_KEY_VERSION_2
        aead_nonce = RETRY_AEAD_NONCE_VERSION_2
    else:
        aead_key = RETRY_AEAD_KEY_VERSION_1
        aead_nonce = RETRY_AEAD_NONCE_VERSION_1

    # run AES-128-GCM
    aead = AESGCM(aead_key)
    integrity_tag = aead.encrypt(aead_nonce, b"", buf.data)
    assert len(integrity_tag) == RETRY_INTEGRITY_TAG_SIZE
    return integrity_tag


def get_spin_bit(first_byte: int) -> bool:
    return bool(first_byte & PACKET_SPIN_BIT)


def is_long_header(first_byte: int) -> bool:
    return bool(first_byte & PACKET_LONG_HEADER)


def pretty_protocol_version(version: int) -> str:
    """
    Return a user-friendly representation of a protocol version.
    """
    try:
        version_name = QuicProtocolVersion(version).name
    except ValueError:
        version_name = "UNKNOWN"
    return f"0x{version:08x} ({version_name})"


def pull_quic_header(buf: Buffer, host_cid_length: Optional[int] = None) -> QuicHeader:
    packet_start = buf.tell()

    version = None
    integrity_tag = b""
    supported_versions = []
    token = b""

    first_byte = buf.pull_uint8()
    if is_long_header(first_byte):
        # Long Header Packets.
        # https://datatracker.ietf.org/doc/html/rfc9000#section-17.2
        version = buf.pull_uint32()

        destination_cid_length = buf.pull_uint8()
        if destination_cid_length > CONNECTION_ID_MAX_SIZE:
            raise ValueError(
                "Destination CID is too long (%d bytes)" % destination_cid_length
            )
        destination_cid = buf.pull_bytes(destination_cid_length)

        source_cid_length = buf.pull_uint8()
        if source_cid_length > CONNECTION_ID_MAX_SIZE:
            raise ValueError("Source CID is too long (%d bytes)" % source_cid_length)
        source_cid = buf.pull_bytes(source_cid_length)

        if version == QuicProtocolVersion.NEGOTIATION:
            # Version Negotiation Packet.
            # https://datatracker.ietf.org/doc/html/rfc9000#section-17.2.1
            packet_type = QuicPacketType.VERSION_NEGOTIATION
            while not buf.eof():
                supported_versions.append(buf.pull_uint32())
            packet_end = buf.tell()
        else:
            if not (first_byte & PACKET_FIXED_BIT):
                raise ValueError("Packet fixed bit is zero")

            if version == QuicProtocolVersion.VERSION_2:
                packet_type = PACKET_LONG_TYPE_DECODE_VERSION_2[
                    (first_byte & 0x30) >> 4
                ]
            else:
                packet_type = PACKET_LONG_TYPE_DECODE_VERSION_1[
                    (first_byte & 0x30) >> 4
                ]

            if packet_type == QuicPacketType.INITIAL:
                token_length = buf.pull_uint_var()
                token = buf.pull_bytes(token_length)
                rest_length = buf.pull_uint_var()
            elif packet_type == QuicPacketType.ZERO_RTT:
                rest_length = buf.pull_uint_var()
            elif packet_type == QuicPacketType.HANDSHAKE:
                rest_length = buf.pull_uint_var()
            else:
                token_length = buf.capacity - buf.tell() - RETRY_INTEGRITY_TAG_SIZE
                token = buf.pull_bytes(token_length)
                integrity_tag = buf.pull_bytes(RETRY_INTEGRITY_TAG_SIZE)
                rest_length = 0

            # Check remainder length.
            packet_end = buf.tell() + rest_length
            if packet_end > buf.capacity:
                raise ValueError("Packet payload is truncated")

    else:
        # Short Header Packets.
        # https://datatracker.ietf.org/doc/html/rfc9000#section-17.3
        if not (first_byte & PACKET_FIXED_BIT):
            raise ValueError("Packet fixed bit is zero")

        version = None
        packet_type = QuicPacketType.ONE_RTT
        destination_cid = buf.pull_bytes(host_cid_length)
        source_cid = b""
        packet_end = buf.capacity

    return QuicHeader(
        version=version,
        packet_type=packet_type,
        packet_length=packet_end - packet_start,
        destination_cid=destination_cid,
        source_cid=source_cid,
        token=token,
        integrity_tag=integrity_tag,
        supported_versions=supported_versions,
    )


def encode_long_header_first_byte(
    version: int, packet_type: QuicPacketType, bits: int
) -> int:
    """
    Encode the first byte of a long header packet.
    """
    if version == QuicProtocolVersion.VERSION_2:
        long_type_encode = PACKET_LONG_TYPE_ENCODE_VERSION_2
    else:
        long_type_encode = PACKET_LONG_TYPE_ENCODE_VERSION_1
    return (
        PACKET_LONG_HEADER
        | PACKET_FIXED_BIT
        | long_type_encode[packet_type] << 4
        | bits
    )


def encode_quic_retry(
    version: int,
    source_cid: bytes,
    destination_cid: bytes,
    original_destination_cid: bytes,
    retry_token: bytes,
    unused: int = 0,
) -> bytes:
    buf = Buffer(
        capacity=7
        + len(destination_cid)
        + len(source_cid)
        + len(retry_token)
        + RETRY_INTEGRITY_TAG_SIZE
    )
    buf.push_uint8(encode_long_header_first_byte(version, QuicPacketType.RETRY, unused))
    buf.push_uint32(version)
    buf.push_uint8(len(destination_cid))
    buf.push_bytes(destination_cid)
    buf.push_uint8(len(source_cid))
    buf.push_bytes(source_cid)
    buf.push_bytes(retry_token)
    buf.push_bytes(
        get_retry_integrity_tag(buf.data, original_destination_cid, version=version)
    )
    assert buf.eof()
    return buf.data


def encode_quic_version_negotiation(
    source_cid: bytes, destination_cid: bytes, supported_versions: list[int]
) -> bytes:
    buf = Buffer(
        capacity=7
        + len(destination_cid)
        + len(source_cid)
        + 4 * len(supported_versions)
    )
    buf.push_uint8(os.urandom(1)[0] | PACKET_LONG_HEADER)
    buf.push_uint32(QuicProtocolVersion.NEGOTIATION)
    buf.push_uint8(len(destination_cid))
    buf.push_bytes(destination_cid)
    buf.push_uint8(len(source_cid))
    buf.push_bytes(source_cid)
    for version in supported_versions:
        buf.push_uint32(version)
    return buf.data


# TLS EXTENSION


@dataclass
class QuicPreferredAddress:
    ipv4_address: Optional[tuple[str, int]]
    ipv6_address: Optional[tuple[str, int]]
    connection_id: bytes
    stateless_reset_token: bytes


@dataclass
class QuicVersionInformation:
    chosen_version: int
    available_versions: list[int]


@dataclass
class QuicTransportParameters:
    original_destination_connection_id: Optional[bytes] = None
    max_idle_timeout: Optional[int] = None
    stateless_reset_token: Optional[bytes] = None
    max_udp_payload_size: Optional[int] = None
    initial_max_data: Optional[int] = None
    initial_max_stream_data_bidi_local: Optional[int] = None
    initial_max_stream_data_bidi_remote: Optional[int] = None
    initial_max_stream_data_uni: Optional[int] = None
    initial_max_streams_bidi: Optional[int] = None
    initial_max_streams_uni: Optional[int] = None
    ack_delay_exponent: Optional[int] = None
    max_ack_delay: Optional[int] = None
    disable_active_migration: Optional[bool] = False
    preferred_address: Optional[QuicPreferredAddress] = None
    active_connection_id_limit: Optional[int] = None
    initial_source_connection_id: Optional[bytes] = None
    retry_source_connection_id: Optional[bytes] = None
    version_information: Optional[QuicVersionInformation] = None
    max_datagram_frame_size: Optional[int] = None
    quantum_readiness: Optional[bytes] = None


PARAMS = {
    0x00: ("original_destination_connection_id", bytes),
    0x01: ("max_idle_timeout", int),
    0x02: ("stateless_reset_token", bytes),
    0x03: ("max_udp_payload_size", int),
    0x04: ("initial_max_data", int),
    0x05: ("initial_max_stream_data_bidi_local", int),
    0x06: ("initial_max_stream_data_bidi_remote", int),
    0x07: ("initial_max_stream_data_uni", int),
    0x08: ("initial_max_streams_bidi", int),
    0x09: ("initial_max_streams_uni", int),
    0x0A: ("ack_delay_exponent", int),
    0x0B: ("max_ack_delay", int),
    0x0C: ("disable_active_migration", bool),
    0x0D: ("preferred_address", QuicPreferredAddress),
    0x0E: ("active_connection_id_limit", int),
    0x0F: ("initial_source_connection_id", bytes),
    0x10: ("retry_source_connection_id", bytes),
    # https://datatracker.ietf.org/doc/html/rfc9368#section-3
    0x11: ("version_information", QuicVersionInformation),
    # extensions
    0x0020: ("max_datagram_frame_size", int),
    0x0C37: ("quantum_readiness", bytes),
}


def pull_quic_preferred_address(buf: Buffer) -> QuicPreferredAddress:
    ipv4_address = None
    ipv4_host = buf.pull_bytes(4)
    ipv4_port = buf.pull_uint16()
    if ipv4_host != bytes(4):
        ipv4_address = (str(ipaddress.IPv4Address(ipv4_host)), ipv4_port)

    ipv6_address = None
    ipv6_host = buf.pull_bytes(16)
    ipv6_port = buf.pull_uint16()
    if ipv6_host != bytes(16):
        ipv6_address = (str(ipaddress.IPv6Address(ipv6_host)), ipv6_port)

    connection_id_length = buf.pull_uint8()
    connection_id = buf.pull_bytes(connection_id_length)
    stateless_reset_token = buf.pull_bytes(16)

    return QuicPreferredAddress(
        ipv4_address=ipv4_address,
        ipv6_address=ipv6_address,
        connection_id=connection_id,
        stateless_reset_token=stateless_reset_token,
    )


def push_quic_preferred_address(
    buf: Buffer, preferred_address: QuicPreferredAddress
) -> None:
    if preferred_address.ipv4_address is not None:
        buf.push_bytes(ipaddress.IPv4Address(preferred_address.ipv4_address[0]).packed)
        buf.push_uint16(preferred_address.ipv4_address[1])
    else:
        buf.push_bytes(bytes(6))

    if preferred_address.ipv6_address is not None:
        buf.push_bytes(ipaddress.IPv6Address(preferred_address.ipv6_address[0]).packed)
        buf.push_uint16(preferred_address.ipv6_address[1])
    else:
        buf.push_bytes(bytes(18))

    buf.push_uint8(len(preferred_address.connection_id))
    buf.push_bytes(preferred_address.connection_id)
    buf.push_bytes(preferred_address.stateless_reset_token)


def pull_quic_version_information(buf: Buffer, length: int) -> QuicVersionInformation:
    chosen_version = buf.pull_uint32()
    available_versions = []
    for i in range(length // 4 - 1):
        available_versions.append(buf.pull_uint32())

    # If an endpoint receives a Chosen Version equal to zero, or any Available Version
    # equal to zero, it MUST treat it as a parsing failure.
    #
    # https://datatracker.ietf.org/doc/html/rfc9368#section-4
    if chosen_version == 0 or 0 in available_versions:
        raise ValueError("Version Information must not contain version 0")

    return QuicVersionInformation(
        chosen_version=chosen_version,
        available_versions=available_versions,
    )


def push_quic_version_information(
    buf: Buffer, version_information: QuicVersionInformation
) -> None:
    buf.push_uint32(version_information.chosen_version)
    for version in version_information.available_versions:
        buf.push_uint32(version)


def pull_quic_transport_parameters(buf: Buffer) -> QuicTransportParameters:
    params = QuicTransportParameters()
    while not buf.eof():
        param_id = buf.pull_uint_var()
        param_len = buf.pull_uint_var()
        param_start = buf.tell()
        if param_id in PARAMS:
            # Parse known parameter.
            param_name, param_type = PARAMS[param_id]
            if param_type is int:
                setattr(params, param_name, buf.pull_uint_var())
            elif param_type is bytes:
                setattr(params, param_name, buf.pull_bytes(param_len))
            elif param_type is QuicPreferredAddress:
                setattr(params, param_name, pull_quic_preferred_address(buf))
            elif param_type is QuicVersionInformation:
                setattr(
                    params,
                    param_name,
                    pull_quic_version_information(buf, param_len),
                )
            else:
                setattr(params, param_name, True)
        else:
            # Skip unknown parameter.
            buf.pull_bytes(param_len)

        if buf.tell() != param_start + param_len:
            raise ValueError("Transport parameter length does not match")

    return params


def push_quic_transport_parameters(
    buf: Buffer, params: QuicTransportParameters
) -> None:
    for param_id, (param_name, param_type) in PARAMS.items():
        param_value = getattr(params, param_name)
        if param_value is not None and param_value is not False:
            param_buf = Buffer(capacity=65536)
            if param_type is int:
                param_buf.push_uint_var(param_value)
            elif param_type is bytes:
                param_buf.push_bytes(param_value)
            elif param_type is QuicPreferredAddress:
                push_quic_preferred_address(param_buf, param_value)
            elif param_type is QuicVersionInformation:
                push_quic_version_information(param_buf, param_value)
            buf.push_uint_var(param_id)
            buf.push_uint_var(param_buf.tell())
            buf.push_bytes(param_buf.data)


# FRAMES


class QuicFrameType(IntEnum):
    PADDING = 0x00
    PING = 0x01
    ACK = 0x02
    ACK_ECN = 0x03
    RESET_STREAM = 0x04
    STOP_SENDING = 0x05
    CRYPTO = 0x06
    NEW_TOKEN = 0x07
    STREAM_BASE = 0x08
    MAX_DATA = 0x10
    MAX_STREAM_DATA = 0x11
    MAX_STREAMS_BIDI = 0x12
    MAX_STREAMS_UNI = 0x13
    DATA_BLOCKED = 0x14
    STREAM_DATA_BLOCKED = 0x15
    STREAMS_BLOCKED_BIDI = 0x16
    STREAMS_BLOCKED_UNI = 0x17
    NEW_CONNECTION_ID = 0x18
    RETIRE_CONNECTION_ID = 0x19
    PATH_CHALLENGE = 0x1A
    PATH_RESPONSE = 0x1B
    TRANSPORT_CLOSE = 0x1C
    APPLICATION_CLOSE = 0x1D
    HANDSHAKE_DONE = 0x1E
    DATAGRAM = 0x30
    DATAGRAM_WITH_LENGTH = 0x31


NON_ACK_ELICITING_FRAME_TYPES = frozenset(
    [
        QuicFrameType.ACK,
        QuicFrameType.ACK_ECN,
        QuicFrameType.PADDING,
        QuicFrameType.TRANSPORT_CLOSE,
        QuicFrameType.APPLICATION_CLOSE,
    ]
)
NON_IN_FLIGHT_FRAME_TYPES = frozenset(
    [
        QuicFrameType.ACK,
        QuicFrameType.ACK_ECN,
        QuicFrameType.TRANSPORT_CLOSE,
        QuicFrameType.APPLICATION_CLOSE,
    ]
)

PROBING_FRAME_TYPES = frozenset(
    [
        QuicFrameType.PATH_CHALLENGE,
        QuicFrameType.PATH_RESPONSE,
        QuicFrameType.PADDING,
        QuicFrameType.NEW_CONNECTION_ID,
    ]
)


@dataclass
class QuicResetStreamFrame:
    error_code: int
    final_size: int
    stream_id: int


@dataclass
class QuicStopSendingFrame:
    error_code: int
    stream_id: int


@dataclass
class QuicStreamFrame:
    data: bytes = b""
    fin: bool = False
    offset: int = 0


def pull_ack_frame(buf: Buffer) -> tuple[RangeSet, int]:
    rangeset = RangeSet()
    end = buf.pull_uint_var()  # largest acknowledged
    delay = buf.pull_uint_var()
    ack_range_count = buf.pull_uint_var()
    ack_count = buf.pull_uint_var()  # first ack range
    rangeset.add(end - ack_count, end + 1)
    end -= ack_count
    for _ in range(ack_range_count):
        end -= buf.pull_uint_var() + 2
        ack_count = buf.pull_uint_var()
        rangeset.add(end - ack_count, end + 1)
        end -= ack_count
    return rangeset, delay


def push_ack_frame(buf: Buffer, rangeset: RangeSet, delay: int) -> int:
    ranges = len(rangeset)
    index = ranges - 1
    r = rangeset[index]
    buf.push_uint_var(r.stop - 1)
    buf.push_uint_var(delay)
    buf.push_uint_var(index)
    buf.push_uint_var(r.stop - 1 - r.start)
    start = r.start
    while index > 0:
        index -= 1
        r = rangeset[index]
        buf.push_uint_var(start - r.stop - 1)
        buf.push_uint_var(r.stop - r.start - 1)
        start = r.start
    return ranges
