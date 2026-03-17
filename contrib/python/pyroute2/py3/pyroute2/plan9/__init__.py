import builtins
import json
import struct

from pyroute2.netlink import nlmsg
from pyroute2.netlink.nlsocket import Marshal

Tversion = 100
Rversion = 101
Tauth = 102
Rauth = 103
Tattach = 104
Rattach = 105
Terror = 106  # illegal
Rerror = 107
Tflush = 108
Rflush = 109
Twalk = 110
Rwalk = 111
Topen = 112
Ropen = 113
Tcreate = 114
Rcreate = 115
Tread = 116
Rread = 117
Twrite = 118
Rwrite = 119
Tclunk = 120
Rclunk = 121
Tremove = 122
Rremove = 123
Tstat = 124
Rstat = 125
Twstat = 126
Rwstat = 127
Topenfd = 98
Ropenfd = 99

# 9P2000.pr2 extensions
Tcall = 80
Rcall = 81


class Plan9Exit(Exception):
    pass


def array(kind, header='H'):
    class CustomArray:

        @staticmethod
        def decode_count(data, offset):
            (count,) = struct.unpack_from(header, data, offset)
            return count, offset + struct.calcsize(header)

        @staticmethod
        def decode_from(data, offset):
            count, offset = CustomArray.decode_count(data, offset)
            ret = []
            for _ in range(count):
                value, offset = kind.decode_from(data, offset)
                ret.append(value)
            return ret, offset

        @staticmethod
        def encode_into(data, offset, value):
            if not isinstance(value, (tuple, list)):
                value = []
            data.extend([0] * struct.calcsize(header))
            struct.pack_into(header, data, offset, len(value))
            offset += struct.calcsize(header)
            for item in value:
                offset = kind.encode_into(data, offset, item)
            return offset

    return CustomArray


class Qid(dict):
    length = 13

    def __init__(self, qtype, vers, path):
        self['type'] = qtype
        self['vers'] = vers
        self['path'] = path

    @staticmethod
    def decode_from(data, offset):
        return (
            dict(
                zip(
                    ('type', 'vers', 'path'),
                    struct.unpack_from('=BIQ', data, offset),
                )
            ),
            offset + struct.calcsize('=BIQ'),
        )

    @staticmethod
    def encode_into(data, offset, value):
        data.extend([0] * Qid.length)
        struct.pack_into(
            '=BIQ', data, offset, value['type'], value['vers'], value['path']
        )
        return offset + Qid.length


class Stat(dict):
    header_fmt = 'H'

    def __init__(self):
        self['size'] = 58
        self['type'] = 0
        self['dev'] = 0
        self['qid.type'] = 0
        self['qid.vers'] = 0
        self['qid.path'] = 0
        self['mode'] = 0
        self['atime'] = 0
        self['mtime'] = 0
        self['length'] = 0
        self['name'] = ''
        self['uid'] = ''
        self['gid'] = ''
        self['muid'] = ''

    @staticmethod
    def decode_from(data, offset=0):
        ret = dict(
            zip(
                (
                    'size',
                    'type',
                    'dev',
                    'qid.type',
                    'qid.vers',
                    'qid.path',
                    'mode',
                    'atime',
                    'mtime',
                    'length',
                ),
                struct.unpack_from('=HHIBIQIIIQ', data, offset),
            )
        )
        offset += 41
        for key in ('name', 'uid', 'gid', 'muid'):
            ret[key], offset = String.decode_from(data, offset)
        return ret, offset

    @staticmethod
    def encode_into(data, offset, value):
        data.extend([0] * 41)
        # size of all the data except uint16 `size` header
        # 41 + 2 + name + 2 + uid + 2 + gid + 2 + muid
        value['size'] = (
            47
            + len(value['name'])
            + len(value['uid'])
            + len(value['gid'])
            + len(value['muid'])
        )
        struct.pack_into(
            '=HHIBIQIIIQ',
            data,
            offset,
            value['size'],
            value['type'],
            value['dev'],
            value['qid.type'],
            value['qid.vers'],
            value['qid.path'],
            value['mode'],
            value['atime'],
            value['mtime'],
            value['length'],
        )
        offset += 41
        for key in ('name', 'uid', 'gid', 'muid'):
            offset = String.encode_into(data, offset, value[key])
        return offset


class WStat(Stat):
    @staticmethod
    def decode_from(data, offset=0):
        # just ignore plength for now
        return Stat.decode_from(data, offset + 2)

    @staticmethod
    def encode_into(data, offset, value):
        data.extend([0] * 2)
        new_offset = Stat.encode_into(data, offset + 2, value)
        # size of all the data except uint16 `plength` header
        struct.pack_into('H', data, offset, new_offset - offset - 2)
        return new_offset


class CData:
    header_fmt = 'I'

    @staticmethod
    def decode_from(data, offset=0):
        (length,) = struct.unpack_from(CData.header_fmt, data, offset)
        offset += struct.calcsize(CData.header_fmt)
        return bytearray(data[offset : offset + length]), offset + length

    @staticmethod
    def encode_into(data, offset, value):
        length = len(value)
        if isinstance(value, str):
            value = value.encode('utf-8')
        data.extend([0] * (length + struct.calcsize(CData.header_fmt)))
        struct.pack_into(
            f'{CData.header_fmt}{length}s', data, offset, length, value
        )
        return offset + length + struct.calcsize(CData.header_fmt)


class String:
    header_fmt = 'H'

    @staticmethod
    def decode_from(data, offset=0):
        (length,) = struct.unpack_from(String.header_fmt, data, offset)
        offset += struct.calcsize(String.header_fmt)
        (value,) = struct.unpack_from(f'{length}s', data, offset)
        value = value.decode('utf-8')
        return value, offset + length

    @staticmethod
    def encode_into(data, offset, value):
        length = len(value)
        data.extend([0] * (length + struct.calcsize(String.header_fmt)))
        struct.pack_into(
            f'{String.header_fmt}{length}s',
            data,
            offset,
            length,
            value.encode('utf-8'),
        )
        return offset + length + struct.calcsize(String.header_fmt)


class msg_base(nlmsg):
    align = 0
    header = (('length', '=I'), ('type', 'B'), ('tag', 'H'))


class msg_terror(msg_base):
    defaults = {'header': {'type': Terror}}


class msg_rerror(msg_base):
    defaults = {'header': {'type': Rerror}}
    fields = (('ename', String),)


class msg_tversion(msg_base):
    defaults = {'header': {'type': Tversion}}
    fields = (('msize', 'I'), ('version', String))


class msg_rversion(msg_base):
    defaults = {'header': {'type': Rversion}}
    fields = (('msize', 'I'), ('version', String))


class msg_tauth(msg_base):
    defaults = {'header': {'type': Tauth}}
    fields = (('afid', 'I'), ('uname', String), ('aname', String))


class msg_rauth(msg_base):
    defaults = {'header': {'type': Rauth}}
    fields = (('aqid', '13B'),)


class msg_tattach(msg_base):
    defaults = {'header': {'type': Tattach}}
    fields = (
        ('fid', 'I'),
        ('afid', 'I'),
        ('uname', String),
        ('aname', String),
    )


class msg_rattach(msg_base):
    defaults = {'header': {'type': Rattach}}
    fields = (('qid', Qid),)


class msg_twalk(msg_base):
    defaults = {'header': {'type': Twalk}}
    fields = (('fid', 'I'), ('newfid', 'I'), ('wname', array(String)))


class msg_rwalk(msg_base):
    defaults = {'header': {'type': Rwalk}}
    fields = (('wqid', array(Qid)),)


class msg_tstat(msg_base):
    defaults = {'header': {'type': Tstat}}
    fields = (('fid', 'I'),)


class msg_rstat(msg_base):
    defaults = {'header': {'type': Rstat}}
    fields = (('stat', WStat),)


class msg_twstat(msg_base):
    defaults = {'header': {'type': Twstat}}
    fields = (('fid', 'I'), ('stat', WStat))


class msg_rwstat(msg_base):
    defaults = {'header': {'type': Rwstat}}


class msg_tclunk(msg_base):
    defaults = {'header': {'type': Tclunk}}
    fields = (('fid', 'I'),)


class msg_rclunk(msg_base):
    defaults = {'header': {'type': Rclunk}}
    pass


class msg_topen(msg_base):
    defaults = {'header': {'type': Topen}}
    fields = (('fid', 'I'), ('mode', 'B'))


class msg_ropen(msg_base):
    defaults = {'header': {'type': Ropen}}
    fields = (('qid', Qid), ('iounit', 'I'))


class msg_tread(msg_base):
    defaults = {'header': {'type': Tread}}
    fields = (('fid', 'I'), ('offset', 'Q'), ('count', 'I'))


class msg_rread(msg_base):
    defaults = {'header': {'type': Rread}, 'data': b''}
    fields = (('data', CData),)


class msg_twrite(msg_base):
    defaults = {'header': {'type': Twrite}}
    fields = (('fid', 'I'), ('offset', 'Q'), ('data', CData))


class msg_rwrite(msg_base):
    defaults = {'header': {'type': Rwrite}}
    fields = (('count', 'I'),)


class msg_tcall(msg_base):
    defaults = {'header': {'type': Tcall}}
    fields = (('fid', 'I'), ('text', String), ('data', CData))


class msg_rcall(msg_base):
    defaults = {'header': {'type': Rcall}}
    fields = (('err', 'H'), ('text', String), ('data', CData))


class Marshal9P(Marshal):
    default_message_class = msg_rerror

    msg_map = {
        Tversion: msg_tversion,
        Rversion: msg_rversion,
        Tauth: msg_tauth,
        Rauth: msg_rauth,
        Tattach: msg_tattach,
        Rattach: msg_rattach,
        Rerror: msg_rerror,
        Twalk: msg_twalk,
        Rwalk: msg_rwalk,
        Topen: msg_topen,
        Ropen: msg_ropen,
        Tread: msg_tread,
        Rread: msg_rread,
        Tclunk: msg_tclunk,
        Rclunk: msg_rclunk,
        Tstat: msg_tstat,
        Rstat: msg_rstat,
        Twstat: msg_twstat,
        Rwstat: msg_rwstat,
        Twrite: msg_twrite,
        Rwrite: msg_rwrite,
        Tcall: msg_tcall,
        Rcall: msg_rcall,
        Tcreate: msg_base,
        Rcreate: msg_base,
        Tremove: msg_base,
        Rremove: msg_base,
    }

    def parse(self, data, seq=None, callback=None, skip_alien_seq=False):
        offset = 0
        while offset <= len(data) - 5:
            (length, key, tag) = struct.unpack_from('=IBH', data, offset)
            if skip_alien_seq and tag != seq:
                continue
            if not 0 < length <= len(data):
                break
            parser = self.get_parser(key, 0, tag)
            msg = parser(data, offset, length)
            if key == Rerror:
                spec = json.loads(msg['ename'])
                if spec['class'] in dir(builtins):
                    cls = getattr(builtins, spec['class'])
                elif spec['class'] == 'Plan9Exit':
                    cls = Plan9Exit
                else:
                    cls = Exception
                if not spec.get('argv'):
                    spec['argv'] = [spec['str']]
                raise cls(*spec['argv'])
            offset += length
            if msg is None:
                continue
            yield msg
