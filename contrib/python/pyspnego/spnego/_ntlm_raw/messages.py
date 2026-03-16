# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import collections
import datetime
import enum
import io
import re
import struct
import typing

from spnego._text import to_text
from spnego._version import __version__ as pyspnego_version


class NegotiateFlags(enum.IntFlag):
    """NTLM Negotiation flags.

    Used during NTLM negotiation to negotiate the capabilities between the client and server.

    .. _NEGOTIATE:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/99d90ff4-957f-4c8a-80e4-5bfe5a9a9832
    """

    key_56 = 0x80000000
    key_exch = 0x40000000
    key_128 = 0x20000000
    r1 = 0x10000000
    r2 = 0x08000000
    r3 = 0x04000000
    version = 0x02000000
    r4 = 0x01000000
    target_info = 0x00800000
    non_nt_session_key = 0x00400000
    r5 = 0x00200000
    identity = 0x00100000
    extended_session_security = 0x00080000
    target_type_share = 0x00040000  # Not documented in MS-NLMP
    target_type_server = 0x00020000
    target_type_domain = 0x00010000
    always_sign = 0x00008000
    local_call = 0x00004000  # Not documented in MS-NLMP
    oem_workstation_supplied = 0x00002000
    oem_domain_name_supplied = 0x00001000
    anonymous = 0x00000800
    r8 = 0x00000400
    ntlm = 0x00000200
    r9 = 0x00000100
    lm_key = 0x00000080
    datagram = 0x00000040
    seal = 0x00000020
    sign = 0x00000010
    netware = 0x00000008  # Not documented in MS-NLMP
    request_target = 0x00000004
    oem = 0x00000002
    unicode = 0x00000001

    @classmethod
    def native_labels(cls) -> typing.Dict["NegotiateFlags", str]:
        return {
            NegotiateFlags.key_56: "NTLMSSP_NEGOTIATE_56",
            NegotiateFlags.key_exch: "NTLMSSP_NEGOTIATE_KEY_EXCH",
            NegotiateFlags.key_128: "NTLMSSP_NEGOTIATE_128",
            NegotiateFlags.r1: "NTLMSSP_RESERVED_R1",
            NegotiateFlags.r2: "NTLMSSP_RESERVED_R2",
            NegotiateFlags.r3: "NTLMSSP_RESERVED_R3",
            NegotiateFlags.version: "NTLMSSP_NEGOTIATE_VERSION",
            NegotiateFlags.r4: "NTLMSSP_RESERVED_R4",
            NegotiateFlags.target_info: "NTLMSSP_NEGOTIATE_TARGET_INFO",
            NegotiateFlags.non_nt_session_key: "NTLMSSP_REQUEST_NON_NT_SESSION_KEY",
            NegotiateFlags.r5: "NTLMSSP_RESERVED_R5",
            NegotiateFlags.identity: "NTLMSSP_NEGOTIATE_IDENTITY",
            NegotiateFlags.extended_session_security: "NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY",
            NegotiateFlags.target_type_share: "NTLMSSP_TARGET_TYPE_SHARE - R6",
            NegotiateFlags.target_type_server: "NTLMSSP_TARGET_TYPE_SERVER",
            NegotiateFlags.target_type_domain: "NTLMSSP_TARGET_TYPE_DOMAIN",
            NegotiateFlags.always_sign: "NTLMSSP_NEGOTIATE_ALWAYS_SIGN",
            NegotiateFlags.local_call: "NTLMSSP_NEGOTIATE_LOCAL_CALL - R7",
            NegotiateFlags.oem_workstation_supplied: "NTLMSSP_NEGOTIATE_OEM_WORKSTATION_SUPPLIED",
            NegotiateFlags.oem_domain_name_supplied: "NTLMSSP_NEGOTIATE_OEM_DOMAIN_SUPPLIED",
            NegotiateFlags.anonymous: "NTLMSSP_ANOYNMOUS",
            NegotiateFlags.r8: "NTLMSSP_RESERVED_R8",
            NegotiateFlags.ntlm: "NTLMSSP_NEGOTIATE_NTLM",
            NegotiateFlags.r9: "NTLMSSP_RESERVED_R9",
            NegotiateFlags.lm_key: "NTLMSSP_NEGOTIATE_LM_KEY",
            NegotiateFlags.datagram: "NTLMSSP_NEGOTIATE_DATAGRAM",
            NegotiateFlags.seal: "NTLMSSP_NEGOTIATE_SEAL",
            NegotiateFlags.sign: "NTLMSSP_NEGOTIATE_SIGN",
            NegotiateFlags.netware: "NTLMSSP_NEGOTIATE_NETWARE - R10",
            NegotiateFlags.request_target: "NTLMSSP_REQUEST_TARGET",
            NegotiateFlags.oem: "NTLMSSP_NEGOTIATE_OEM",
            NegotiateFlags.unicode: "NTLMSSP_NEGOTIATE_UNICODE",
        }


class AvId(enum.IntFlag):
    """ID for an NTLM AV_PAIR.

    These are the IDs that can be set as the `AvId` on an `AV_PAIR`_.

    .. _AV_PAIR:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/83f5e789-660d-4781-8491-5f8c6641f75e
    """

    eol = 0x0000
    nb_computer_name = 0x0001
    nb_domain_name = 0x0002
    dns_computer_name = 0x0003
    dns_domain_name = 0x0004
    dns_tree_name = 0x0005
    flags = 0x0006
    timestamp = 0x0007
    single_host = 0x0008
    target_name = 0x0009
    channel_bindings = 0x000A

    @classmethod
    def native_labels(cls) -> typing.Dict["AvId", str]:
        return {
            AvId.eol: "MSV_AV_EOL",
            AvId.nb_computer_name: "MSV_AV_NB_COMPUTER_NAME",
            AvId.nb_domain_name: "MSV_AV_NB_DOMAIN_NAME",
            AvId.dns_computer_name: "MSV_AV_DNS_COMPUTER_NAME",
            AvId.dns_domain_name: "MSV_AV_DNS_DOMAIN_NAME",
            AvId.dns_tree_name: "MSV_AV_DNS_TREE_NAME",
            AvId.flags: "MSV_AV_FLAGS",
            AvId.timestamp: "MSV_AV_TIMESTAMP",
            AvId.single_host: "MSV_AV_SINGLE_HOST",
            AvId.target_name: "MSV_AV_TARGET_NAME",
            AvId.channel_bindings: "MSV_AV_CHANNEL_BINDINGS",
        }


class AvFlags(enum.IntFlag):
    """MsvAvFlags for an AV_PAIR.

    These are the flags that can be set on the MsvAvFlags entry of an NTLM `AV_PAIR`_.

    .. _AV_PAIR:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/83f5e789-660d-4781-8491-5f8c6641f75e
    """

    none = 0x00000000
    constrained = 0x00000001
    mic = 0x00000002
    untrusted_spn = 0x00000004

    @classmethod
    def native_labels(cls) -> typing.Dict["AvFlags", str]:
        return {
            AvFlags.constrained: "AUTHENTICATION_CONSTRAINED",
            AvFlags.mic: "MIC_PROVIDED",
            AvFlags.untrusted_spn: "UNTRUSTED_SPN_SOURCE",
        }


class MessageType(enum.IntEnum):
    negotiate = 1
    challenge = 2
    authenticate = 3

    @classmethod
    def native_labels(cls) -> typing.Dict["MessageType", str]:
        return {
            MessageType.negotiate: "NEGOTIATE_MESSAGE",
            MessageType.challenge: "CHALLENGE_MESSAGE",
            MessageType.authenticate: "AUTHENTICATE_MESSAGE",
        }


def _get_payload_offset(b_data: memoryview, field_offsets: typing.List[int]) -> int:
    payload_offset = None

    for field_offset in field_offsets:
        offset = struct.unpack("<I", b_data[field_offset + 4 : field_offset + 8].tobytes())[0]
        if not payload_offset or (offset and offset < payload_offset):
            payload_offset = offset

    return payload_offset or len(b_data)


def _pack_payload(
    data: typing.Any,
    b_payload: bytearray,
    payload_offset: int,
    pack_func: typing.Optional[typing.Callable[[typing.Any], bytes]] = None,
) -> typing.Tuple[bytes, int]:
    if data:
        b_data = pack_func(data) if pack_func else data
    else:
        b_data = b""

    b_payload.extend(b_data)
    length = len(b_data)

    b_field = (struct.pack("<H", length) * 2) + struct.pack("<I", payload_offset)
    payload_offset += length

    return b_field, payload_offset


def _unpack_payload(
    b_data: memoryview,
    field_offset: int,
    unpack_func: typing.Optional[typing.Callable[[bytes], typing.Any]] = None,
) -> typing.Any:
    field_len = struct.unpack("<H", b_data[field_offset : field_offset + 2].tobytes())[0]
    if field_len:
        field_offset = struct.unpack("<I", b_data[field_offset + 4 : field_offset + 8].tobytes())[0]
        b_value = b_data[field_offset : field_offset + field_len].tobytes()

        return unpack_func(b_value) if unpack_func else b_value


class _NTLMMessageMeta(type):
    __registry: typing.Dict[int, typing.Type] = {}

    def __init__(
        cls,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> None:
        cls.__registry[getattr(cls, "MESSAGE_TYPE", 0)] = cls

    def __call__(
        cls,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> "_NTLMMessageMeta":
        if "_b_data" in kwargs:
            message_type = struct.unpack("<I", kwargs["_b_data"][8:12])[0]
            new_cls = cls.__registry[message_type]

        else:
            new_cls = cls

        return super(_NTLMMessageMeta, new_cls).__call__(*args, **kwargs)


class NTLMMessage(metaclass=_NTLMMessageMeta):
    """Base NTLM message class that defines the pack and unpack functions."""

    MESSAGE_TYPE = 0
    MINIMUM_LENGTH = 0

    def __init__(self, encoding: typing.Optional[str] = None, _b_data: typing.Optional[bytes] = None) -> None:
        self.signature = b"NTLMSSP\x00" + struct.pack("<I", self.MESSAGE_TYPE)
        self._encoding = encoding or "windows-1252"

        if _b_data:
            if len(_b_data) < self.MINIMUM_LENGTH:
                raise ValueError("Invalid NTLM %s raw byte length" % self.__class__.__name__)

            self._data = memoryview(bytearray(_b_data))

        else:
            self._data = memoryview(b"")

    def pack(self) -> bytes:
        """Packs the structure to bytes."""
        return self._data.tobytes()

    @staticmethod
    def unpack(b_data: bytes, encoding: typing.Optional[str] = None) -> "NTLMMessage":
        """Unpacks the structure from bytes."""
        return NTLMMessage(encoding=encoding, _b_data=b_data)


class Negotiate(NTLMMessage):
    """NTLM Negotiate Message

    This structure represents an NTLM `NEGOTIATE_MESSAGE`_ that can be serialized and deserialized to and from
    bytes.

    Args:
        flags: The `NegotiateFlags` that the client has negotiated.
        domain_name: The `DomainName` of the client authentication domain.
        workstation: The `Workstation` of the client.
        version: The `Version` of the client.
        encoding: The OEM encoding to use for text fields.
        _b_data: The raw bytes of the message to unpack from.

    .. _NEGOTIATE_MESSAGE:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/b34032e5-3aae-4bc6-84c3-c6d80eadf7f2
    """

    MESSAGE_TYPE = MessageType.negotiate
    MINIMUM_LENGTH = 32

    def __init__(
        self,
        flags: int = 0,
        domain_name: typing.Optional[str] = None,
        workstation: typing.Optional[str] = None,
        version: typing.Optional["Version"] = None,
        encoding: typing.Optional[str] = None,
        _b_data: typing.Optional[bytes] = None,
    ) -> None:
        super(Negotiate, self).__init__(encoding=encoding, _b_data=_b_data)

        if not _b_data:
            b_payload = bytearray()

            payload_offset = 32

            b_version = b""
            if version:
                flags |= NegotiateFlags.version
                b_version = version.pack()
            payload_offset = _pack_payload(b_version, b_payload, payload_offset)[1]

            b_domain_name = b""
            if domain_name:
                flags |= NegotiateFlags.oem_domain_name_supplied
                b_domain_name = domain_name.encode(self._encoding)
            b_domain_name_fields, payload_offset = _pack_payload(b_domain_name, b_payload, payload_offset)

            b_workstation = b""
            if workstation:
                flags |= NegotiateFlags.oem_workstation_supplied
                b_workstation = workstation.encode(self._encoding)
            b_workstation_fields = _pack_payload(b_workstation, b_payload, payload_offset)[0]

            b_data = bytearray(self.signature)
            b_data.extend(struct.pack("<I", flags))
            b_data.extend(b_domain_name_fields)
            b_data.extend(b_workstation_fields)
            b_data.extend(b_payload)

            self._data = memoryview(b_data)

    @property
    def flags(self) -> int:
        """The negotiate flags for the Negotiate message."""
        return struct.unpack("<I", self._data[12:16].tobytes())[0]

    @flags.setter
    def flags(self, value: int) -> None:
        self._data[12:16] = struct.pack("<I", value)

    @property
    def domain_name(self) -> typing.Optional[str]:
        """The name of the client authentication domain."""
        return to_text(_unpack_payload(self._data, 16), encoding=self._encoding, errors="replace", nonstring="passthru")

    @property
    def workstation(self) -> typing.Optional[str]:
        """The name of the client machine."""
        return to_text(_unpack_payload(self._data, 24), encoding=self._encoding, errors="replace", nonstring="passthru")

    @property
    def version(self) -> typing.Optional["Version"]:
        """The client NTLM version."""
        payload_offset = self._payload_offset

        # If the payload offset is at 40 or more then the Version, or at least empty bytes, is in the payload.
        if payload_offset >= 40:
            return Version.unpack(self._data[32:40].tobytes())

        else:
            return None

    @property
    def _payload_offset(self) -> int:
        """Gets the offset of the first payload value."""
        return _get_payload_offset(self._data, [16, 24])

    @staticmethod
    def unpack(b_data: bytes, encoding: typing.Optional[str] = None) -> "Negotiate":
        msg = NTLMMessage.unpack(b_data, encoding=encoding)
        if not isinstance(msg, Negotiate):
            raise ValueError("Input message was not a NTLM Negotiate message")

        return msg


class Challenge(NTLMMessage):
    """NTLM Challenge Message

    This structure represents an NTLM `CHALLENGE_MESSAGE`_ that can be serialized and deserialized to and from
    bytes.

    Args:
        flags: The `NegotiateFlags` that the client has negotiated.
        server_challenge: The random 64-bit `ServerChallenge` nonce.
        target_name: The name of the acceptor server.
        target_info: The variable length `TargetInfo` information.
        version: The `Version` of the server.
        encoding: The OEM encoding to use for text fields if `NTLMSSP_NEGOTIATE_UNICODE` was not supported.
        _b_data: The raw NTLM Challenge bytes to unpack from.

    .. _CHALLENGE_MESSAGE:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/801a4681-8809-4be9-ab0d-61dcfe762786
    """

    MESSAGE_TYPE = MessageType.challenge
    MINIMUM_LENGTH = 48

    def __init__(
        self,
        flags: int = 0,
        server_challenge: typing.Optional[bytes] = None,
        target_name: typing.Optional[str] = None,
        target_info: typing.Optional["TargetInfo"] = None,
        version: typing.Optional["Version"] = None,
        encoding: typing.Optional[str] = None,
        _b_data: typing.Optional[bytes] = None,
    ):
        super(Challenge, self).__init__(encoding=encoding, _b_data=_b_data)

        if _b_data:
            self._encoding = "utf-16-le" if self.flags & NegotiateFlags.unicode else self._encoding

        else:
            self._encoding = "utf-16-le" if flags & NegotiateFlags.unicode else self._encoding

            b_payload = bytearray()
            payload_offset = 48

            b_version = b""
            if version:
                flags |= NegotiateFlags.version
                b_version = version.pack()
            payload_offset = _pack_payload(b_version, b_payload, payload_offset)[1]

            b_target_name = b""
            if target_name:
                flags |= NegotiateFlags.request_target
                b_target_name = target_name.encode(self._encoding)
            b_target_name_fields, payload_offset = _pack_payload(b_target_name, b_payload, payload_offset)

            b_target_info = b""
            if target_info:
                flags |= NegotiateFlags.target_info
                b_target_info = target_info.pack()
            b_target_info_fields = _pack_payload(b_target_info, b_payload, payload_offset)[0]

            b_data = bytearray(self.signature)
            b_data.extend(b_target_name_fields)
            b_data.extend(struct.pack("<I", flags))
            b_data.extend(b"\x00" * 8)  # ServerChallenge, set after self._data is initialised.
            b_data.extend(b"\x00" * 8)  # Reserved
            b_data.extend(b_target_info_fields)
            b_data.extend(b_payload)

            self._data = memoryview(b_data)

            if server_challenge:
                self.server_challenge = server_challenge

    @property
    def target_name(self) -> typing.Optional[str]:
        """The name of the server authentication realm."""
        return to_text(_unpack_payload(self._data, 12), encoding=self._encoding, nonstring="passthru")

    @property
    def flags(self) -> int:
        """The negotiate flags supported by the server."""
        return struct.unpack("<I", self._data[20:24].tobytes())[0]

    @flags.setter
    def flags(self, value: int) -> None:
        self._data[20:24] = struct.pack("<I", value)

    @property
    def server_challenge(self) -> bytes:
        """The server's 8 byte nonce challenge."""
        return self._data[24:32].tobytes()

    @server_challenge.setter
    def server_challenge(self, value: bytes) -> None:
        if not value or len(value) != 8:
            raise ValueError("NTLM Challenge ServerChallenge must be 8 bytes long")

        self._data[24:32] = value

    @property
    def target_info(self) -> typing.Optional["TargetInfo"]:
        """The AV_PAIR structures generated by the server."""
        return _unpack_payload(self._data, 40, lambda d: TargetInfo.unpack(d))

    @property
    def version(self) -> typing.Optional["Version"]:
        """The server NTLM version."""
        payload_offset = self._payload_offset

        # If the payload offset is at 56 or more then the Version, or at least empty bytes, is in the payload.
        if payload_offset >= 56:
            return Version.unpack(self._data[48:56].tobytes())

        else:
            return None

    @property
    def _payload_offset(self) -> int:
        """Gets the offset of the first payload value."""
        return _get_payload_offset(self._data, [12, 40])

    @staticmethod
    def unpack(b_data: bytes, encoding: typing.Optional[str] = None) -> "Challenge":
        msg = NTLMMessage.unpack(b_data, encoding=encoding)
        if not isinstance(msg, Challenge):
            raise ValueError("Input message was not a NTLM Challenge message")

        return msg


class Authenticate(NTLMMessage):
    """NTLM Authentication Message

    This structure represents an NTLM `AUTHENTICATION_MESSAGE`_ that can be serialized and deserialized to and from
    bytes.

    Args:
        flags: The `NegotiateFlags` that the client has negotiated.
        lm_challenge_response: The `LmChallengeResponse` for the client's secret.
        nt_challenge_response: The `NtChallengeResponse` for the client's secret.
        domain_name: The `DomainName` for the client.
        username: The `UserName` for the cleint.
        workstation: The `Workstation` for the client.
        encrypted_session_key: The `EncryptedRandomSessionKey` for the set up context.
        version: The `Version` of the client.
        mic: The `MIC` for the authentication exchange.
        encoding: The OEM encoding to use for text fields if `NTLMSSP_NEGOTIATE_UNICODE` was not supported.
        _b_data: The raw NTLM Authenticate bytes to unpack from.

    .. _AUTHENTICATION_MESSAGE:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/033d32cc-88f9-4483-9bf2-b273055038ce
    """

    MESSAGE_TYPE = MessageType.authenticate
    MINIMUM_LENGTH = 64

    def __init__(
        self,
        flags: int = 0,
        lm_challenge_response: typing.Optional[bytes] = None,
        nt_challenge_response: typing.Optional[bytes] = None,
        domain_name: typing.Optional[str] = None,
        username: typing.Optional[str] = None,
        workstation: typing.Optional[str] = None,
        encrypted_session_key: typing.Optional[bytes] = None,
        version: typing.Optional["Version"] = None,
        mic: typing.Optional[bytes] = None,
        encoding: typing.Optional[str] = None,
        _b_data: typing.Optional[bytes] = None,
    ) -> None:
        super(Authenticate, self).__init__(encoding=encoding, _b_data=_b_data)

        if _b_data:
            self._encoding = "utf-16-le" if self.flags & NegotiateFlags.unicode else self._encoding

        else:
            self._encoding = "utf-16-le" if flags & NegotiateFlags.unicode else self._encoding

            b_payload = bytearray()

            payload_offset = 64

            # While MS server accept a blank version field, other implementations aren't so kind. No need to be strict
            # about it and only add the version bytes if it's present.
            b_version = b""
            if version:
                flags |= NegotiateFlags.version
                b_version = version.pack()
            payload_offset = _pack_payload(b_version, b_payload, payload_offset)[1]

            # MIC
            payload_offset = _pack_payload(b"\x00" * 16, b_payload, payload_offset)[1]

            b_lm_response_fields, payload_offset = _pack_payload(lm_challenge_response, b_payload, payload_offset)
            b_nt_response_fields, payload_offset = _pack_payload(nt_challenge_response, b_payload, payload_offset)
            b_domain_fields, payload_offset = _pack_payload(
                domain_name, b_payload, payload_offset, lambda d: d.encode(self._encoding)
            )
            b_username_fields, payload_offset = _pack_payload(
                username, b_payload, payload_offset, lambda d: d.encode(self._encoding)
            )
            b_workstation_fields, payload_offset = _pack_payload(
                workstation, b_payload, payload_offset, lambda d: d.encode(self._encoding)
            )
            if encrypted_session_key:
                flags |= NegotiateFlags.key_exch
            b_session_key_fields = _pack_payload(encrypted_session_key, b_payload, payload_offset)[0]

            b_data = bytearray(self.signature)
            b_data.extend(b_lm_response_fields)
            b_data.extend(b_nt_response_fields)
            b_data.extend(b_domain_fields)
            b_data.extend(b_username_fields)
            b_data.extend(b_workstation_fields)
            b_data.extend(b_session_key_fields)
            b_data.extend(struct.pack("<I", flags))
            b_data.extend(b_payload)

            self._data = memoryview(b_data)

            if mic:
                self.mic = mic

    @property
    def lm_challenge_response(self) -> typing.Optional[bytes]:
        """The LmChallengeResponse or None if not set."""
        return _unpack_payload(self._data, 12)

    @property
    def nt_challenge_response(self) -> typing.Optional[bytes]:
        """The NtChallengeResponse or None if not set."""
        return _unpack_payload(self._data, 20)

    @property
    def domain_name(self) -> typing.Optional[str]:
        """The domain or computer name hosting the user account."""
        return to_text(_unpack_payload(self._data, 28), encoding=self._encoding, nonstring="passthru")

    @property
    def user_name(self) -> typing.Optional[str]:
        """The name of the user to be authenticated."""
        return to_text(_unpack_payload(self._data, 36), encoding=self._encoding, nonstring="passthru")

    @property
    def workstation(self) -> typing.Optional[str]:
        """The name of the computer to which the user is logged on."""
        return to_text(_unpack_payload(self._data, 44), encoding=self._encoding, nonstring="passthru")

    @property
    def encrypted_random_session_key(self) -> typing.Optional[bytes]:
        """The client's encrypted random session key."""
        return _unpack_payload(self._data, 52)

    @property
    def flags(self) -> int:
        """The negotiate flags supported by the client and server."""
        return struct.unpack("<I", self._data[60:64].tobytes())[0]

    @flags.setter
    def flags(self, value: int) -> None:
        self._data[60:64] = struct.pack("<I", value)

    @property
    def version(self) -> typing.Optional["Version"]:
        """The client NTLM version."""
        payload_offset = self._payload_offset

        # If the payload offset is at 64 (no MIC or Version) or 80 (only MIC) then no version is present.
        if payload_offset not in [64, 80] and payload_offset >= 72:
            return Version.unpack(self._data[64:72].tobytes())

        else:
            return None

    @property
    def mic(self) -> typing.Optional[bytes]:
        """The MIC for the Authenticate message."""
        mic_offset = self._get_mic_offset()
        if mic_offset:
            return self._data.tobytes()[mic_offset : mic_offset + 16]

        else:
            return None

    @mic.setter
    def mic(self, value: bytes) -> None:
        if len(value) != 16:
            raise ValueError("NTLM Authenticate MIC must be 16 bytes long")

        mic_offset = self._get_mic_offset()
        if mic_offset:
            self._data[mic_offset : mic_offset + 16] = value
        else:
            raise ValueError("Cannot set MIC on an Authenticate message with no MIC present")

    @property
    def _payload_offset(self) -> int:
        """Gets the offset of the first payload value."""
        return _get_payload_offset(self._data, [12, 20, 28, 36, 44, 52])

    def _get_mic_offset(self) -> int:
        """Gets the offset of the MIC structure if present."""
        payload_offset = self._payload_offset

        # If the payload offset is 88 or more then we must have the Version (8 bytes) and the MIC (16 bytes) plus
        # any random data after that.
        if payload_offset >= 88:
            return 72

        # If the payload offset is between 80 and 88, then we should have just the MIC and no Version.
        elif payload_offset >= 80:
            return 64

        # Not enough room for a MIC between the minimum size and the payload offset.
        else:
            return 0

    @staticmethod
    def unpack(b_data: bytes, encoding: typing.Optional[str] = None) -> "Authenticate":
        msg = NTLMMessage.unpack(b_data, encoding=encoding)
        if not isinstance(msg, Authenticate):
            raise ValueError("Input message was not a NTLM Authenticate message")

        return msg


class FileTime(datetime.datetime):
    """Windows FILETIME structure.

    FILETIME structure representing number of 100-nanosecond intervals that have elapsed since January 1, 1601 UTC.
    This subclasses the datetime object to provide a similar interface but with the `nanosecond` attribute.

    Attrs:
        nanosecond (int): The number of nanoseconds (< 1000) in the FileTime. Note this only has a precision of up to
            100 nanoseconds.

    .. _FILETIME:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/2c57429b-fdd4-488f-b5fc-9e4cf020fcdf
    """

    _EPOCH_FILETIME = 116444736000000000  # 1970-01-01 as FILETIME.

    def __new__(cls, *args: typing.Any, **kwargs: typing.Any) -> "FileTime":
        ns = 0
        if "nanosecond" in kwargs:
            ns = kwargs.pop("nanosecond")

        dt = super(FileTime, cls).__new__(cls, *args, **kwargs)
        dt.nanosecond = ns

        return dt

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        super().__init__()
        self.nanosecond = getattr(self, "nanosecond", None) or 0

    @classmethod
    def now(cls, tz: typing.Optional[datetime.tzinfo] = None) -> "FileTime":
        """Construct a FileTime from the current time and optional time zone info."""
        return FileTime.from_datetime(datetime.datetime.now(tz=tz))

    @classmethod
    def from_datetime(cls, dt: datetime.datetime, ns: int = 0) -> "FileTime":
        """Creates a FileTime object from a datetime object."""
        return FileTime(
            year=dt.year,
            month=dt.month,
            day=dt.day,
            hour=dt.hour,
            minute=dt.minute,
            second=dt.second,
            microsecond=dt.microsecond,
            tzinfo=dt.tzinfo,
            nanosecond=ns,
        )

    def __str__(self) -> str:
        """Displays the datetime in ISO 8601 including the 100th nanosecond internal like .NET does."""
        fraction_seconds = ""

        if self.microsecond or self.nanosecond:
            fraction_seconds = self.strftime(".%f")

            if self.nanosecond:
                fraction_seconds += str(self.nanosecond // 100)

        timezone = "Z"
        if self.tzinfo:
            utc_offset = self.strftime("%z")
            timezone = "%s:%s" % (utc_offset[:3], utc_offset[3:])

        # strftime doesn't support dates < 1900 on Python 2.7
        return "{0}-{1:02d}-{2:02d}T{3:02d}:{4:02d}:{5:02d}{6}{7}".format(
            self.year, self.month, self.day, self.hour, self.minute, self.second, fraction_seconds, timezone
        )

    def pack(self) -> bytes:
        """Packs the structure to bytes."""
        # Make sure we are dealing with a timezone aware datetime
        utc_tz = datetime.timezone.utc
        utc_dt = typing.cast(datetime.datetime, self.replace(tzinfo=self.tzinfo if self.tzinfo else utc_tz))

        # Get the time since UTC EPOCH in microseconds
        td = utc_dt.astimezone(utc_tz) - datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=utc_tz)
        epoch_time_ms = td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6

        # Add the EPOCH_FILETIME to the microseconds since EPOCH and finally the nanoseconds part.
        ns100 = FileTime._EPOCH_FILETIME + (epoch_time_ms * 10) + (self.nanosecond // 100)

        return struct.pack("<Q", ns100)

    @staticmethod
    def unpack(b_data: bytes) -> "FileTime":
        """Unpacks the structure from bytes."""
        filetime = struct.unpack("<Q", b_data)[0]  # 100 nanosecond intervals since 1601-01-01.

        # Create a datetime object based on the filetime microseconds
        epoch_time_ms = (filetime - FileTime._EPOCH_FILETIME) // 10
        dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=epoch_time_ms)

        # Create the FileTime object from the datetime object and add the nanoseconds.
        ns = int(filetime % 10) * 100

        return FileTime.from_datetime(dt, ns=ns)


class NTClientChallengeV2:
    """NTLMv2 Client Challenge

    The `NTLMv2_CLIENT_CHALLENGE`_ structure defines the client challenge in the AUTHENTICATE_MESSAGE. This structure
    is only used when NTLMv2 authentication is configured and is transported in the NT Challenge Response.

    Args:
        time_stamp: The timestamp, defaults to the current time.
        client_challenge: The 8 byte nonce generated by the client.
        av_pairs: The TargetInfo AV_PAIRS for the client challenge.
        _b_data: Raw byte string for the NTClientChallengeV2, when set the other args are ignored.

    .. _NTLMv2_CLIENT_CHALLENGE:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/aee311d6-21a7-4470-92a5-c4ecb022a87b
    """

    def __init__(
        self,
        time_stamp: typing.Optional["FileTime"] = None,
        client_challenge: typing.Optional[bytes] = None,
        av_pairs: typing.Optional["TargetInfo"] = None,
        _b_data: typing.Optional[bytes] = None,
    ) -> None:
        if _b_data:
            if len(_b_data) < 32:
                raise ValueError("Invalid NTClientChallengeV2 raw byte length")

            self._data = memoryview(bytearray(_b_data))

        else:
            time_stamp = time_stamp or FileTime.now()
            av_pairs = av_pairs or TargetInfo()

            b_data = bytearray(b"\x01\x01\x00\x00\x00\x00\x00\x00")
            b_data.extend(time_stamp.pack())
            b_data.extend(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
            b_data.extend(av_pairs.pack())

            self._data = memoryview(b_data)

            client_challenge = client_challenge or b"\x00" * 8
            self.challenge_from_client = client_challenge

    @property
    def resp_type(self) -> int:
        """The current response type version, must be set to 1."""
        return struct.unpack("B", self._data[:1].tobytes())[0]

    @resp_type.setter
    def resp_type(self, value: int) -> None:
        self._data[:1] = struct.pack("B", value)

    @property
    def hi_resp_type(self) -> int:
        """The maximum response type supported, must be set to 1."""
        return struct.unpack("B", self._data[1:2].tobytes())[0]

    @hi_resp_type.setter
    def hi_resp_type(self, value: int) -> None:
        self._data[1:2] = struct.pack("B", value)

    @property
    def time_stamp(self) -> "FileTime":
        """The current system time."""
        return FileTime.unpack(self._data[8:16].tobytes())

    @time_stamp.setter
    def time_stamp(self, value: "FileTime") -> None:
        self._data[8:16] = value.pack()

    @property
    def challenge_from_client(self) -> bytes:
        """8 byte client challenge."""
        return self._data[16:24].tobytes()

    @challenge_from_client.setter
    def challenge_from_client(self, value: bytes) -> None:
        if len(value) != 8:
            raise ValueError("NTClientChallengeV2 ChallengeFromClient must be 8 bytes long")
        self._data[16:24] = value

    @property
    def av_pairs(self) -> "TargetInfo":
        """The target info AV_PAIR structures."""
        return TargetInfo.unpack(self._data[28:].tobytes())

    def pack(self) -> bytes:
        """Packs the NTClientChallengeV2 to bytes."""
        return self._data.tobytes()

    @staticmethod
    def unpack(b_data: bytes) -> "NTClientChallengeV2":
        """Unpacks the raw bytes to the NTClientChallengeV2 structure."""
        return NTClientChallengeV2(_b_data=b_data)


class TargetInfo(collections.OrderedDict):
    """A collection of AV_PAIR structures for the TargetInfo field.

    The `AV_PAIR`_ structure defines an attribute/value pair and sequences of these pairs are using in the
    :class:`Challenge` and :class:`Authenticate` messages. The value for each pair depends on the AvId specified.
    Each value can be get/set/del like a normal dictionary where the key is the AvId of the value.

    .. _AV_PAIR:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/83f5e789-660d-4781-8491-5f8c6641f75e
    """

    _FIELD_TYPES = {
        "text": (
            AvId.nb_computer_name,
            AvId.nb_domain_name,
            AvId.dns_computer_name,
            AvId.dns_domain_name,
            AvId.dns_tree_name,
            AvId.target_name,
        ),
        "int32": (AvId.flags,),
        "struct": (AvId.timestamp, AvId.single_host),
    }

    def __setitem__(self, key: AvId, value: typing.Any) -> None:
        if isinstance(value, bytes):
            if key == AvId.timestamp:
                value = FileTime.unpack(value)
            elif key == AvId.single_host:
                value = SingleHost.unpack(value)

        super(TargetInfo, self).__setitem__(key, value)

    def pack(self) -> bytes:
        """Packs the structure to bytes."""
        b_data = io.BytesIO()

        for av_id, value in self.items():
            # MsvAvEOL should only be set at the end, will just ignore these entries.
            if av_id == AvId.eol:
                continue

            if av_id in self._FIELD_TYPES["text"]:
                b_value = value.encode("utf-16-le")
            elif av_id in self._FIELD_TYPES["int32"]:
                b_value = struct.pack("<I", value)
            elif av_id in self._FIELD_TYPES["struct"]:
                b_value = value.pack()
            else:
                b_value = value

            b_data.write(struct.pack("<HH", av_id, len(b_value)) + b_value)

        b_data.write(b"\x00\x00\x00\x00")  # MsvAvEOL
        return b_data.getvalue()

    @staticmethod
    def unpack(b_data: bytes) -> "TargetInfo":
        """Unpacks the structure from bytes."""
        target_info = TargetInfo()
        b_io = io.BytesIO(b_data)

        b_av_id = b_io.read(2)

        while b_av_id:
            av_id = struct.unpack("<H", b_av_id)[0]
            length = struct.unpack("<H", b_io.read(2))[0]
            b_value = b_io.read(length)

            value: typing.Any
            if av_id in TargetInfo._FIELD_TYPES["text"]:
                # All AV_PAIRS are UNICODE encoded.
                value = b_value.decode("utf-16-le")

            elif av_id in TargetInfo._FIELD_TYPES["int32"]:
                value = AvFlags(struct.unpack("<I", b_value)[0])

            elif av_id == AvId.timestamp:
                value = FileTime.unpack(b_value)

            elif av_id == AvId.single_host:
                value = SingleHost.unpack(b_value)

            else:
                value = b_value

            target_info[AvId(av_id)] = value
            b_av_id = b_io.read(2)

        return target_info


class SingleHost:
    """Single_Host_Data structure for NTLM TargetInfo entry.

    `Single_Host_Data`_ structure allows a client to send machine-specific information within an authentication
    exchange to services on the same machine. If the server and client platforms are different or if they are on
    different hosts, then the information MUST be ignores.

    Args:
        size: A 32-bit unsigned int that defines size of the structure.
        z4: A 32-bit integer value, currently set to 0.
        custom_data: An 8-byte platform-specific blob containing info only relevant when the client and server are on
            the same host.
        machine_id: A 32-byte random number created at computer startup to identify the calling machine.
        _b_data: Create a SingleHost object from the raw data byte string.

    Attributes:
        size: See args.
        z4: See args.
        custom_data: See args.
        machine_id: See args.

    .. _Single_Host_Data:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/f221c061-cc40-4471-95da-d2ff71c85c5b
    """

    def __init__(
        self,
        size: int = 0,
        z4: int = 0,
        custom_data: typing.Optional[bytes] = None,
        machine_id: typing.Optional[bytes] = None,
        _b_data: typing.Optional[bytes] = None,
    ) -> None:
        if _b_data:
            if len(_b_data) != 48:
                raise ValueError("SingleHost bytes must have a length of 48")
            self._data = memoryview(_b_data)

        else:
            self._data = memoryview(bytearray(48))
            self.size = size
            self.z4 = z4
            self.custom_data = custom_data or b"\x00" * 8
            self.machine_id = machine_id or b"\x00" * 32

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, (bytes, SingleHost)):
            return False

        if isinstance(other, SingleHost):
            other = other.pack()

        return self.pack() == other

    @property
    def size(self) -> int:
        return struct.unpack("<I", self._data[:4].tobytes())[0]

    @size.setter
    def size(self, value: int) -> None:
        self._data[:4] = struct.pack("<I", value)

    @property
    def z4(self) -> int:
        return struct.unpack("<I", self._data[4:8].tobytes())[0]

    @z4.setter
    def z4(self, value: int) -> None:
        self._data[4:8] = struct.pack("<I", value)

    @property
    def custom_data(self) -> bytes:
        return self._data[8:16].tobytes()

    @custom_data.setter
    def custom_data(self, value: bytes) -> None:
        if len(value) != 8:
            raise ValueError("custom_data length must be 8 bytes long")

        self._data[8:16] = value

    @property
    def machine_id(self) -> bytes:
        return self._data[16:48].tobytes()

    @machine_id.setter
    def machine_id(self, value: bytes) -> None:
        if len(value) != 32:
            raise ValueError("machine_id length must be 32 bytes long")

        self._data[16:48] = value

    def pack(self) -> bytes:
        """Packs the structure to bytes."""
        return self._data.tobytes()

    @staticmethod
    def unpack(b_data: bytes) -> "SingleHost":
        """Creates a SignleHost object from raw bytes."""
        return SingleHost(_b_data=b_data)


class Version:
    """A structure contains the OS information.

    The `VERSION`_ structure contains operating system version information that SHOULD be ignored. This structure is
    used for debugging purposes only and its value does not affect NTLM message processing. It is populated in the NTLM
    messages only if `NTLMSSP_NEGOTIATE_VERSION` (`NegotiateFlags.version`) is negotiated.

    Args:
        major: See args. The 8-bit unsigned int for the version major part.
        minor: The 8-bit unsigned int for the version minor part.
        build: The 16-bit unsigned int for the version build part.
        revision: An 8-bit unsigned integer for the current NTLMSSP revision. This field SHOULD be `0x0F`.
        _b_data: Create a Version object from the raw data byte string.

    Attrs:
        major: See args.
        minor: See args.
        build: See args.
        reserved: A reserved 3-byte field that isn't used in the NTLM spec.
        revision: See args.

    .. _VERSION:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/b1a6ceb2-f8ad-462b-b5af-f18527c48175
    """

    def __init__(
        self,
        major: int = 0,
        minor: int = 0,
        build: int = 0,
        revision: int = 0x0F,
        _b_data: typing.Optional[bytes] = None,
    ) -> None:
        if _b_data:
            if len(_b_data) != 8:
                raise ValueError("Version bytes must have a length of 8")

            self._data = memoryview(_b_data)

        else:
            self._data = memoryview(bytearray(8))
            self.major = major
            self.minor = minor
            self.build = build
            self.revision = revision

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, (bytes, Version)):
            return False

        if isinstance(other, Version):
            other = other.pack()

        return self.pack() == other

    def __len__(self) -> int:
        return 8

    @property
    def major(self) -> int:
        return struct.unpack("B", self._data[0:1].tobytes())[0]

    @major.setter
    def major(self, value: int) -> None:
        self._data[0:1] = struct.pack("B", value)

    @property
    def minor(self) -> int:
        return struct.unpack("B", self._data[1:2].tobytes())[0]

    @minor.setter
    def minor(self, value: int) -> None:
        self._data[1:2] = struct.pack("B", value)

    @property
    def build(self) -> int:
        return struct.unpack("<H", self._data[2:4].tobytes())[0]

    @build.setter
    def build(self, value: int) -> None:
        self._data[2:4] = struct.pack("<H", value)

    @property
    def reserved(self) -> bytes:
        return self._data[4:7].tobytes()

    @property
    def revision(self) -> int:
        return struct.unpack("B", self._data[7:8].tobytes())[0]

    @revision.setter
    def revision(self, value: int) -> None:
        self._data[7:8] = struct.pack("B", value)

    def __repr__(self) -> str:
        return "<{0}.{1} {2}.{3}.{4}.{5}>".format(
            type(self).__module__, type(self).__name__, self.major, self.minor, self.build, self.revision
        )

    def __str__(self) -> str:
        return "%s.%s.%s.%s" % (self.major, self.minor, self.build, self.revision)

    def pack(self) -> bytes:
        """Packs the structure to bytes."""
        return self._data.tobytes()

    @staticmethod
    def get_current() -> "Version":
        """Generates an NTLM Version structure based on the pyspnego package version."""
        versions = []
        for v in pyspnego_version.split(".", 3):
            if not v:
                continue

            match = re.match(r"^(\d+)", v)
            if match:
                versions.append(int(match.group(1)))

        versions += [0] * (3 - len(versions))

        return Version(major=int(versions[0]), minor=int(versions[1]), build=int(versions[2]))

    @staticmethod
    def unpack(b_data: bytes) -> "Version":
        """Creates a Version object from raw bytes."""
        return Version(_b_data=b_data)
