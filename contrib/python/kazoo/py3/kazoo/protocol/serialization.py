"""Zookeeper Serializers, Deserializers, and NamedTuple objects"""
from collections import namedtuple
import struct

from kazoo.exceptions import EXCEPTIONS
from kazoo.protocol.states import ZnodeStat
from kazoo.security import ACL
from kazoo.security import Id


# Struct objects with formats compiled
bool_struct = struct.Struct("B")
int_struct = struct.Struct("!i")
int_int_struct = struct.Struct("!ii")
int_int_long_struct = struct.Struct("!iiq")

int_long_int_long_struct = struct.Struct("!iqiq")
long_struct = struct.Struct("!q")
multiheader_struct = struct.Struct("!iBi")
reply_header_struct = struct.Struct("!iqi")
stat_struct = struct.Struct("!qqqqiiiqiiq")


def read_string(buffer, offset):
    """Reads an int specified buffer into a string and returns the
    string and the new offset in the buffer"""
    length = int_struct.unpack_from(buffer, offset)[0]
    offset += int_struct.size
    if length < 0:
        return None, offset
    else:
        index = offset
        offset += length
        return buffer[index : index + length].decode("utf-8"), offset


def read_acl(bytes, offset):
    perms = int_struct.unpack_from(bytes, offset)[0]
    offset += int_struct.size
    scheme, offset = read_string(bytes, offset)
    id, offset = read_string(bytes, offset)
    return ACL(perms, Id(scheme, id)), offset


def write_string(bytes):
    if not bytes:
        return int_struct.pack(-1)
    else:
        utf8_str = bytes.encode("utf-8")
        return int_struct.pack(len(utf8_str)) + utf8_str


def write_buffer(bytes):
    if bytes is None:
        return int_struct.pack(-1)
    else:
        return int_struct.pack(len(bytes)) + bytes


def read_buffer(bytes, offset):
    length = int_struct.unpack_from(bytes, offset)[0]
    offset += int_struct.size
    if length < 0:
        return None, offset
    else:
        index = offset
        offset += length
        return bytes[index : index + length], offset


class Close(namedtuple("Close", "")):
    type = -11

    @classmethod
    def serialize(cls):
        return b""


CloseInstance = Close()


class Ping(namedtuple("Ping", "")):
    type = 11

    @classmethod
    def serialize(cls):
        return b""


PingInstance = Ping()


class Connect(
    namedtuple(
        "Connect",
        "protocol_version last_zxid_seen"
        " time_out session_id passwd read_only",
    )
):
    type = None

    def serialize(self):
        b = bytearray()
        b.extend(
            int_long_int_long_struct.pack(
                self.protocol_version,
                self.last_zxid_seen,
                self.time_out,
                self.session_id,
            )
        )
        b.extend(write_buffer(self.passwd))
        b.extend([1 if self.read_only else 0])
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        proto_version, timeout, session_id = int_int_long_struct.unpack_from(
            bytes, offset
        )
        offset += int_int_long_struct.size
        password, offset = read_buffer(bytes, offset)

        try:
            read_only = bool_struct.unpack_from(bytes, offset)[0] == 1
            offset += bool_struct.size
        except struct.error:
            read_only = False
        return (
            cls(proto_version, 0, timeout, session_id, password, read_only),
            offset,
        )


class Create(namedtuple("Create", "path data acl flags")):
    type = 1

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(write_buffer(self.data))
        b.extend(int_struct.pack(len(self.acl)))
        for acl in self.acl:
            b.extend(
                int_struct.pack(acl.perms)
                + write_string(acl.id.scheme)
                + write_string(acl.id.id)
            )
        b.extend(int_struct.pack(self.flags))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        return read_string(bytes, offset)[0]


class Delete(namedtuple("Delete", "path version")):
    type = 2

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(int_struct.pack(self.version))
        return b

    @classmethod
    def deserialize(self, bytes, offset):
        return True


class Exists(namedtuple("Exists", "path watcher")):
    type = 3

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend([1 if self.watcher else 0])
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        stat = ZnodeStat._make(stat_struct.unpack_from(bytes, offset))
        return stat if stat.czxid != -1 else None


class GetData(namedtuple("GetData", "path watcher")):
    type = 4

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend([1 if self.watcher else 0])
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        data, offset = read_buffer(bytes, offset)
        stat = ZnodeStat._make(stat_struct.unpack_from(bytes, offset))
        return data, stat


class SetData(namedtuple("SetData", "path data version")):
    type = 5

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(write_buffer(self.data))
        b.extend(int_struct.pack(self.version))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        return ZnodeStat._make(stat_struct.unpack_from(bytes, offset))


class GetACL(namedtuple("GetACL", "path")):
    type = 6

    def serialize(self):
        return bytearray(write_string(self.path))

    @classmethod
    def deserialize(cls, bytes, offset):
        count = int_struct.unpack_from(bytes, offset)[0]
        offset += int_struct.size
        if count == -1:  # pragma: nocover
            return []

        acls = []
        for c in range(count):
            acl, offset = read_acl(bytes, offset)
            acls.append(acl)
        stat = ZnodeStat._make(stat_struct.unpack_from(bytes, offset))
        return acls, stat


class SetACL(namedtuple("SetACL", "path acls version")):
    type = 7

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(int_struct.pack(len(self.acls)))
        for acl in self.acls:
            b.extend(
                int_struct.pack(acl.perms)
                + write_string(acl.id.scheme)
                + write_string(acl.id.id)
            )
        b.extend(int_struct.pack(self.version))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        return ZnodeStat._make(stat_struct.unpack_from(bytes, offset))


class GetChildren(namedtuple("GetChildren", "path watcher")):
    type = 8

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend([1 if self.watcher else 0])
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        count = int_struct.unpack_from(bytes, offset)[0]
        offset += int_struct.size
        if count == -1:  # pragma: nocover
            return []

        children = []
        for c in range(count):
            child, offset = read_string(bytes, offset)
            children.append(child)
        return children


class Sync(namedtuple("Sync", "path")):
    type = 9

    def serialize(self):
        return write_string(self.path)

    @classmethod
    def deserialize(cls, buffer, offset):
        return read_string(buffer, offset)[0]


class GetChildren2(namedtuple("GetChildren2", "path watcher")):
    type = 12

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend([1 if self.watcher else 0])
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        count = int_struct.unpack_from(bytes, offset)[0]
        offset += int_struct.size
        if count == -1:  # pragma: nocover
            return []

        children = []
        for c in range(count):
            child, offset = read_string(bytes, offset)
            children.append(child)
        stat = ZnodeStat._make(stat_struct.unpack_from(bytes, offset))
        return children, stat


class CheckVersion(namedtuple("CheckVersion", "path version")):
    type = 13

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(int_struct.pack(self.version))
        return b


class Transaction(namedtuple("Transaction", "operations")):
    type = 14

    def serialize(self):
        b = bytearray()
        for op in self.operations:
            b.extend(
                MultiHeader(op.type, False, -1).serialize() + op.serialize()
            )
        return b + multiheader_struct.pack(-1, True, -1)

    @classmethod
    def deserialize(cls, bytes, offset):
        header = MultiHeader(None, False, None)
        results = []
        response = None
        while not header.done:
            if header.type == Create.type:
                response, offset = read_string(bytes, offset)
            elif header.type == Delete.type:
                response = True
            elif header.type == SetData.type:
                response = ZnodeStat._make(
                    stat_struct.unpack_from(bytes, offset)
                )
                offset += stat_struct.size
            elif header.type == CheckVersion.type:
                response = True
            elif header.type == -1:
                err = int_struct.unpack_from(bytes, offset)[0]
                offset += int_struct.size
                response = EXCEPTIONS[err]()
            if response:
                results.append(response)
            header, offset = MultiHeader.deserialize(bytes, offset)
        return results

    @staticmethod
    def unchroot(client, response):
        resp = []
        for result in response:
            if isinstance(result, str):
                resp.append(client.unchroot(result))
            else:
                resp.append(result)
        return resp


class Create2(namedtuple("Create2", "path data acl flags")):
    type = 15

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.path))
        b.extend(write_buffer(self.data))
        b.extend(int_struct.pack(len(self.acl)))
        for acl in self.acl:
            b.extend(
                int_struct.pack(acl.perms)
                + write_string(acl.id.scheme)
                + write_string(acl.id.id)
            )
        b.extend(int_struct.pack(self.flags))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        path, offset = read_string(bytes, offset)
        stat = ZnodeStat._make(stat_struct.unpack_from(bytes, offset))
        return path, stat


class Reconfig(
    namedtuple("Reconfig", "joining leaving new_members config_id")
):
    type = 16

    def serialize(self):
        b = bytearray()
        b.extend(write_string(self.joining))
        b.extend(write_string(self.leaving))
        b.extend(write_string(self.new_members))
        b.extend(long_struct.pack(self.config_id))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        data, offset = read_buffer(bytes, offset)
        stat = ZnodeStat._make(stat_struct.unpack_from(bytes, offset))
        return data, stat


class Auth(namedtuple("Auth", "auth_type scheme auth")):
    type = 100

    def serialize(self):
        return (
            int_struct.pack(self.auth_type)
            + write_string(self.scheme)
            + write_string(self.auth)
        )


class SASL(namedtuple("SASL", "challenge")):
    type = 102

    def serialize(self):
        b = bytearray()
        b.extend(write_buffer(self.challenge))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        challenge, offset = read_buffer(bytes, offset)
        return challenge, offset


class Watch(namedtuple("Watch", "type state path")):
    @classmethod
    def deserialize(cls, bytes, offset):
        """Given bytes and the current bytes offset, return the
        type, state, path, and new offset"""
        type, state = int_int_struct.unpack_from(bytes, offset)
        offset += int_int_struct.size
        path, offset = read_string(bytes, offset)
        return cls(type, state, path), offset


class ReplyHeader(namedtuple("ReplyHeader", "xid, zxid, err")):
    @classmethod
    def deserialize(cls, bytes, offset):
        """Given bytes and the current bytes offset, return a
        :class:`ReplyHeader` instance and the new offset"""
        new_offset = offset + reply_header_struct.size
        return (
            cls._make(reply_header_struct.unpack_from(bytes, offset)),
            new_offset,
        )


class MultiHeader(namedtuple("MultiHeader", "type done err")):
    def serialize(self):
        b = bytearray()
        b.extend(int_struct.pack(self.type))
        b.extend([1 if self.done else 0])
        b.extend(int_struct.pack(self.err))
        return b

    @classmethod
    def deserialize(cls, bytes, offset):
        t, done, err = multiheader_struct.unpack_from(bytes, offset)
        offset += multiheader_struct.size
        return cls(t, done == 1, err), offset
