'''
DHCP protocol
=============

The DHCP implementation here is far from complete, but
already provides some basic functionality. Later it will
be extended with IPv6 support and more DHCP options
will be added.

Right now it can be interesting mostly to developers,
but not users and/or system administrators. So, the
development hints first.

The packet structure description is intentionally
implemented as for netlink packets. Later these two
parsers, netlink and generic, can be merged, so the
syntax is more or less compatible.

Packet fields
-------------

There are two big groups of items within any DHCP packet.
First, there are BOOTP/DHCP packet fields, they're defined
with the `fields` attribute::

    class dhcp4msg(msg):
        fields = ((name, format, policy),
                  (name, format, policy),
                  ...
                  (name, format, policy))

The `name` can be any literal. Format should be specified
as for the struct module, like `B` for `uint8`, or `i` for
`int32`, or `>Q` for big-endian uint64. There are also
aliases defined, so one can write `uint8` or `be16`, or
like that. Possible aliases can be seen in the
`pyroute2.protocols` module.

The `policy` is a bit complicated. It can be a number or
literal, and it will mean that it is a default value, that
should be encoded if no other value is given.

But when the `policy` is a dictionary, it can contain keys
as follows::

    'l2addr': {'format': '6B',
               'decode': ...,
               'encode': ...}

Keys `encode` and `decode` should contain filters to be used
in decoding and encoding procedures. The encoding filter
should accept the value from user's definition and should
return a value that can be packed using `format`. The decoding
filter should accept a value, decoded according to `format`,
and should return value that can be used by a user.

The `struct` module can not decode IP addresses etc, so they
should be decoded as `4s`, e.g. Further transformation from
4 bytes string to a string like '10.0.0.1' performs the filter.

DHCP options
------------

DHCP options are described in a similar way::

    options = ((code, name, format),
               (code, name, format),
               ...
               (code, name, format))

Code is a `uint8` value, name can be any string literal. Format
is a string, that must have a corresponding class, inherited from
`pyroute2.dhcp.option`. One can find these classes in
`pyroute2.dhcp` (more generic) or in `pyroute2.dhcp.dhcp4msg`
(IPv4-specific). The option class must reside within dhcp message
class.

Every option class can be decoded in two ways. If it has fixed
width fields, it can be decoded with ordinary `msg` routines, and
in this case it can look like that::

    class client_id(option):
        fields = (('type', 'uint8'),
                  ('key', 'l2addr'))

If it must be decoded by some custom rules, one can define the
policy just like for the fields above::

    class array8(option):
        policy = {'format': 'string',
                  'encode': lambda x: array('B', x).tobytes(),
                  'decode': lambda x: array('B', x).tolist()}

In the corresponding modules, like in `pyroute2.dhcp.dhcp4msg`,
one can define as many custom DHCP options, as one need. Just
be sure, that they are compatible with the DHCP server and all
fit into 1..254 (`uint8`) -- the 0 code is used for padding and
the code 255 is the end of options code.
'''

import logging
import struct
from array import array
from typing import ClassVar, NamedTuple, Optional, TypedDict, TypeVar, Union

from pyroute2.protocols import Policy, _decode_mac, _encode_mac, msg

from .enums.dhcp import MessageType, Option

LOG = logging.getLogger(__name__)


# hack because Self is not supported in py39
_dhcpmsgSelf = TypeVar('_dhcpmsgSelf', bound='dhcpmsg')
_optionSelf = TypeVar('_optionSelf', bound='option')


class ClientId(TypedDict):
    '''A dict with 'type' and 'key' keys.

    The types stores the client id type, and the key stores the value.
    See RFC for their meaning.
    '''

    type: int
    key: Union[bytes, str]


def _decode_client_id(value: bytes) -> ClientId:
    '''Decode a raw client id option into a dict with type and key.

    If the type is 1, the key is decoded as a mac address,
    otherwise it's just the raw bytes.
    '''
    type_ = value[0]
    key: Union[bytes, str] = value[1:]

    if type_ == 1:
        # ethernet
        assert isinstance(key, bytes)
        key = _decode_mac(value=key)
    return ClientId(type=type_, key=key)


def _encode_client_id(value: ClientId) -> bytes:
    '''Encode a client_id dict into bytes.'''
    type_ = value['type']
    key = value['key']
    if type_ == 1:
        assert isinstance(key, str), 'client_id must be a mac str when type=1'
        key = bytes(_encode_mac(key))
    assert isinstance(key, bytes), 'client_id must be bytes'
    return struct.pack('B', type_) + key


class option(msg):
    policy: ClassVar[Optional[Policy]] = None

    def __init__(self, content=None, buf=b'', offset=0, value=None, code=0):
        msg.__init__(
            self, content=content, buf=buf, offset=offset, value=value
        )
        self.code = code

    @property
    def length(self) -> Optional[int]:
        if self.data_length is None:
            return None
        if self.data_length == 0:
            return 1
        else:
            return self.data_length + 2

    def encode(self: _optionSelf) -> _optionSelf:
        # pack code
        self.buf += struct.pack('B', self.code)
        if self.code in (0, 255):
            return self
        # save buf
        save = self.buf
        self.buf = b''
        # pack data into the new buf
        if self.policy is not None:
            value = self.policy.encode(self.value)
            if self.policy.format == 'string':
                fmt = '%is' % len(value)
                if isinstance(value, list):
                    # Byte strings can be provided as a list of bytes
                    value = bytes(value)
            else:
                fmt = self.policy.format
            if isinstance(value, str):
                value = value.encode('utf-8')
            self.buf = struct.pack(fmt, value)
        else:
            msg.encode(self)
        # get the length
        data = self.buf
        self.buf = save
        self.buf += struct.pack('B', len(data))
        # attach the packed data
        self.buf += data
        return self

    def decode(self: _optionSelf) -> _optionSelf:
        try:
            self.data_length = struct.unpack(
                'B', self.buf[self.offset + 1 : self.offset + 2]
            )[0]
        except struct.error as err:
            raise ValueError(
                f'Cannot decode length for DHCP option {self.code}: {err}'
            )
        if self.policy is not None:
            if self.policy.format == 'string':
                fmt = '%is' % self.data_length
            else:
                fmt = self.policy.format
            try:
                value = struct.unpack(
                    fmt,
                    self.buf[
                        self.offset + 2 : self.offset + 2 + self.data_length
                    ],
                )
            except struct.error as err:
                raise ValueError(
                    f'Cannot decode option {self.code} '
                    f'as {self.policy.format}: {err}'
                )
            if len(value) == 1:
                value = value[0]
            value = self.policy.decode(value)
            if self.policy.format == 'string':
                if isinstance(value, bytes):
                    try:
                        # Try to decode as a string
                        value = value.decode()
                    except ValueError:
                        pass
                if isinstance(value, str):
                    # Strip trailing zeroes for strings
                    value = value.rstrip("\x00")
            if isinstance(value, bytes):
                # Turn bytes to lists of bytes so they're JSON-encodable
                value = list(value)
            self.value = value
        else:
            # remember current offset as msg.decode() will advance it
            offset = self.offset
            # move past the code and option length bytes so that msg.decode()
            # starts parsing at the right location
            self.offset += 2
            msg.decode(self)
            # restore offset so that dhcpmsg.decode() advances it correctly
            self.offset = offset

        return self


class CodeMapping(NamedTuple):
    name: str
    code: int
    format: str


class dhcpmsg(msg):
    options: ClassVar[tuple[tuple[Option, str], ...]] = ()

    def __init__(self, content=None, buf=b'', offset=0, value=None) -> None:
        super().__init__(content, buf, offset, value)
        self._encode_map: dict[str, CodeMapping] = {}
        self._decode_map: dict[int, CodeMapping] = {}
        self._register_options()

    def _register_options(self) -> None:
        for code, fmt in self.options:
            name = code.name.lower()
            self._decode_map[code] = self._encode_map[name] = CodeMapping(
                name=name, code=code, format=fmt
            )

    def decode(self: _dhcpmsgSelf) -> _dhcpmsgSelf:
        msg.decode(self)
        self['options'] = {}
        while self.offset < len(self.buf):
            code: int = struct.unpack(
                'B', self.buf[self.offset : self.offset + 1]
            )[0]
            if code == Option.PAD:
                self.offset += 1
                continue
            if code == Option.END:
                return self
            if code in self._decode_map:
                # use the known decoded & name
                option_class = getattr(self, self._decode_map[code].format)
                optname = self._decode_map[code].name
            else:
                # code is unknown
                # if we know this option number, get the value as a list
                # even if we don't know how to parse it
                option_class = self.array8
                try:
                    # if we know this option name, use it
                    code = Option(code)
                    optname = code.name.lower()
                except ValueError:
                    optname = f"option{code}"

            option = option_class(buf=self.buf, offset=self.offset, code=code)
            try:
                option.decode()
            except ValueError as err:
                # FIXME: maybe we would like the raw option data
                # if we can't decode it ? but that would complicate typing
                LOG.error("%s", err)
                break
            self.offset += option.length
            if option.value is not None:
                value = option.value
            else:
                value = option
            self['options'][optname] = value
        return self

    def encode(self: _dhcpmsgSelf) -> _dhcpmsgSelf:
        msg.encode(self)
        # put message type
        options = self.get('options') or {'message_type': MessageType.DISCOVER}

        self.buf += (
            self.uint8(code=Option.MESSAGE_TYPE, value=options['message_type'])
            .encode()
            .buf
        )
        for name, value in options.items():
            if name == 'message_type':
                continue
            if (code_mapping := self._encode_map.get(name)) is None:
                continue

            # name is known, ok
            option_class = getattr(self, code_mapping.format)
            if isinstance(value, dict):
                option = option_class(value, code=code_mapping.code)
            else:
                option = option_class(code=code_mapping.code, value=value)
            self.buf += option.encode().buf

        self.buf += self.none(code=Option.END).encode().buf
        return self

    class none(option):
        pass

    class be16(option):
        policy = Policy(format='>H')

    class be32(option):
        policy = Policy(format='>I')

    class sbe32(option):
        policy = Policy(format='>i')

    class uint8(option):
        policy = Policy(format='B')

    class string(option):
        policy = Policy(format='string')

    class array8(option):
        policy = Policy(
            format='string',
            encode=lambda x: array('B', x).tobytes(),
            decode=lambda x: array('B', x).tolist(),
        )

    class client_id(option):
        policy = Policy(
            format='string', encode=_encode_client_id, decode=_decode_client_id
        )

        def __init__(
            self, content=None, buf=b'', offset=0, value=None, code=0
        ):
            # FIXME: we have to override value w/ content here, because
            # when trying to encode an option, if it's a dict, its value is
            # set to None.
            super().__init__(content, buf, offset, value=content, code=code)

    class message_type(option):
        policy = Policy(format='B', decode=MessageType)

    class bool(option):
        policy = Policy(format='B', encode=int, decode=bool)
