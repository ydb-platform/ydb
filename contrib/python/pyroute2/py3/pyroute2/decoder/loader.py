import io
import json
import shlex
import struct
from collections import namedtuple
from importlib import import_module

from pyroute2.common import hexdump, load_dump

PcapMetaData = namedtuple(
    "pCAPMetaData",
    (
        "magic_number",
        "version_major",
        "version_minor",
        "thiszone",
        "sigfigs",
        "snaplen",
        "network",
    ),
)
PcapPacketHeader = namedtuple(
    "PcapPacketHeader",
    ("ts_sec", "ts_usec", "incl_len", "orig_len", "header_len"),
)
PcapLLHeader = namedtuple(
    "PcapLLHeader",
    ("pad0", "addr_type", "pad1", "pad2", "pad3", "family", "header_len"),
)


class Message:

    def __init__(self, packet_header, ll_header, met, key, data):
        self.packet_header = packet_header
        self.ll_header = ll_header
        self.cls = None
        self.met = met
        self.key = key
        self.kl = struct.calcsize(self.key)
        self.data = data
        self.exception = None
        self.msg = None

    def get_message_class(self):
        if hasattr(self.met, 'msg_map'):
            (msg_type,) = struct.unpack(self.key, self.data[4 : 4 + self.kl])
            return self.met.msg_map[msg_type]
        return self.met

    def decode(self):
        try:
            self.cls = self.get_message_class()
            self.msg = self.cls(self.data)
            self.msg.decode()
            self.msg = self.msg.dump()
        except Exception as e:
            self.exception = repr(e)
            self.msg = hexdump(self.data)

    def dump(self):
        return {
            "pcap header": repr(self.packet_header),
            "link layer header": repr(self.ll_header),
            "message class": repr(self.cls),
            "exception": self.exception,
            "data": self.msg,
        }

    def __repr__(self):
        return json.dumps(self.dump(), indent=4)


class MatchOps:
    '''
    Functions to match netlink messages.

    The matcher object maintains a stack, where every function
    leaves True or False. A message matches only when the stack
    contains True.

    Some functions take arguments from the command line, other
    like `AND` and `OR` work with the stack.
    '''

    @staticmethod
    def AND():
        '''
        Consumes values left on the stack by functions to the
        left and to the right in the expression, and leaves
        the result of AND operation::

            func_a{...} AND func_b{...}
        '''

        def f(packet_header, ll_header, raw, data_offset, stack):
            v1 = stack.pop()
            v2 = stack.pop()
            return v1 and v2

        return f

    @staticmethod
    def OR():
        '''
        Consumes values left on the stack by functions to the
        left and to the right in the expression, and leaves
        the result of OR operation::

            func_a{...} OR func_b{...}
        '''

        def f(packet_header, ll_header, raw, data_offset, stack):
            v1 = stack.pop()
            v2 = stack.pop()
            return v1 or v2

        return f

    @staticmethod
    def ll_header(family):
        '''
        Match link layer header fields. As for now only netlink
        family is supported, see `pyroute2.netlink` for netlink
        families (`NETLINK_.*`) constants::

            # match generic netlink messages
            ll_header{family=16}

            # match RTNL messages
            ll_header{family=0}
        '''
        if not isinstance(family, int) or family < 0 or family > 0xFFFF:
            raise TypeError('family must be unsigned short integer')

        def f(packet_header, ll_header, raw, data_offset, stack):
            if ll_header is None:
                return False
            return ll_header.family == family

        return f

    @staticmethod
    def data(fmt, offset, value):
        '''
        Match a voluntary data in the message. Use `struct` notation
        for the format, integers for offset and value::

            # match four bytes with offset 4 bytes and value 16,
            # or 10:00:00:00 in hex:

            data{fmt='I', offset=4, value=16}

            # match one byte with offset 16 and value 1, or 01 in hex

            data{fmt='B', offset=16, value=1}

        More examples::

            # match:
            #   * generic netlink protocol, 16
            #   * message type 37 -- IPVS protocol for this session
            #   * message command 1 -- IPVS_CMD_NEW_SERVICE
            ll_header{family=16}
                AND data{fmt='H', offset=4, value=37}
                AND data{fmt='B', offset=16, value=1}
        '''
        if not isinstance(fmt, str):
            raise TypeError('format must be string')
        if not isinstance(offset, int) or not isinstance(value, int):
            raise TypeError('offset and value must be integers')

        def f(packet_header, ll_header, raw, data_offset, stack):
            o = data_offset + offset
            s = struct.calcsize(fmt)
            return struct.unpack(fmt, raw[o : o + s])[0] == value

        return f


class Matcher:
    def __init__(self, script):
        self.parsed = []
        self.filters = []
        self.script = script
        self.shlex = shlex.shlex(instream=io.StringIO(script))
        self.shlex.wordchars += '-~'
        postpone = None
        while True:
            token = self.get_token(ignore=',')
            if token == '':
                break
            method = getattr(MatchOps, token)
            if token in ('AND', 'OR'):
                postpone = method
                continue
            kwarg = {}
            token = self.get_token(expect='{')
            while True:
                token = self.get_token(ignore=',')
                if token in ('}', ''):
                    break
                self.get_token(expect='=')
                value = self.get_token()
                if value[0] in ['"', "'"]:
                    # string
                    value = value[1:-1]
                else:
                    # int
                    value = int(value)
                kwarg[token] = value
            self.filters.append(method(**kwarg))
            if postpone is not None:
                self.filters.append(postpone())
                postpone = None

    def get_token(self, expect=None, ignore=None):
        token = self.shlex.get_token()
        self.parsed.append(token)
        if expect is not None and token != expect:
            raise SyntaxError(f"expected {expect}: {' '.join(self.parsed)} <-")
        if ignore is not None and token in ignore:
            token = self.shlex.get_token()
            self.parsed.append(token)
        return token

    def match(self, packet_header, ll_header, data, offset):
        stack = []
        for method in self.filters:
            stack.append(method(packet_header, ll_header, data, offset, stack))
        return all(stack)


class LoaderHex:

    def __init__(self, data, cls, key, data_offset, script):
        with open(data, 'r') as f:
            self.raw = load_dump(f)
        self.cls = cls
        self.key = key
        self.offset = 0
        self.matcher = Matcher(script)

    @property
    def data(self):
        while self.offset < len(self.raw):
            msg = Message(
                None, None, self.cls, self.key, self.raw[self.offset :]
            )
            msg.decode()
            if self.matcher.match(None, None, self.raw, self.offset):
                yield msg
            self.offset += msg.msg['header']['length']


class LoaderPcap:

    def __init__(self, data, cls, key, data_offset, script):
        with open(data, 'rb') as f:
            self.raw = f.read()
        self.metadata = PcapMetaData(*struct.unpack("IHHiIII", self.raw[:24]))
        self.offset = 24
        self.key = key
        self.data_offset = data_offset
        self.cls = cls
        self.matcher = Matcher(script)

    def decode_packet_header(self, data, offset):
        return PcapPacketHeader(
            *struct.unpack("IIII", data[offset : offset + 16]) + (16,)
        )

    def decode_ll_header(self, data, offset):
        return PcapLLHeader(
            *struct.unpack(">HHIIHH", data[offset : offset + 16]) + (16,)
        )

    @property
    def data(self):
        while self.offset < len(self.raw):
            packet_header = self.decode_packet_header(self.raw, self.offset)
            self.offset += packet_header.header_len
            ll_header = self.decode_ll_header(self.raw, self.offset)
            self.offset += ll_header.header_len
            length = packet_header.incl_len - ll_header.header_len
            off_length = length - self.data_offset
            if self.matcher.match(
                packet_header, ll_header, self.raw, self.offset
            ):
                offset = self.offset + self.data_offset
                msg = Message(
                    packet_header,
                    ll_header,
                    self.cls,
                    self.key,
                    self.raw[offset : offset + off_length],
                )
                msg.decode()
                yield msg
            self.offset += length


def get_loader(args):
    if args.cls:
        cls = args.cls.replace('/', '.').split('.')
        module_name = '.'.join(cls[:-1])
        cls_name = cls[-1]
        module = import_module(module_name)
        cls = getattr(module, cls_name)

    if args.format == 'pcap':
        return LoaderPcap(args.data, cls, args.key, args.offset, args.match)
    elif args.format == 'hex':
        return LoaderHex(args.data, cls, args.key, args.offset, args.match)
    else:
        raise ValueError('data format not supported')
