import errno
import struct
import threading
from functools import partial
from typing import Callable, Generator, Optional

from pyroute2.netlink import (
    NLM_F_ACK_TLVS,
    NLMSG_DONE,
    NLMSG_ERROR,
    mtypes,
    nlmsg,
    nlmsgerr,
)
from pyroute2.netlink.exceptions import (
    NetlinkDecodeError,
    NetlinkError,
    NetlinkHeaderDecodeError,
)


class Marshal:
    '''
    Generic marshalling class
    '''

    msg_map = {}
    seq_map = None
    key_offset = None
    key_format = None
    key_mask = None
    debug = False
    default_message_class = nlmsg
    error_type = NLMSG_ERROR

    def __init__(self):
        self.lock = threading.Lock()
        self.msg_map = self.msg_map.copy()
        self.seq_map = {}
        self.defragmentation = {}

    def parse_one_message(
        self, key, flags, sequence_number, data, offset, length
    ):
        msg = None
        error = None
        msg_class = self.msg_map.get(key, self.default_message_class)
        # ignore length for a while
        # get the message
        if (key == self.error_type) or (
            key == NLMSG_DONE and flags & NLM_F_ACK_TLVS
        ):
            msg = nlmsgerr(data, offset=offset)
        else:
            msg = msg_class(data, offset=offset)

        try:
            msg.decode()
        except NetlinkHeaderDecodeError as e:
            msg = nlmsg()
            msg['header']['error'] = e
        except NetlinkDecodeError as e:
            msg['header']['error'] = e

        if isinstance(msg, nlmsgerr) and msg['error'] != 0:
            code = abs(msg['error'])
            if code == errno.ENOBUFS:
                error = OSError(code, msg.get_attr('NLMSGERR_ATTR_MSG'))
            else:
                error = NetlinkError(code, msg.get_attr('NLMSGERR_ATTR_MSG'))
                enc_type = struct.unpack_from('H', data, offset + 24)[0]
                enc_class = self.msg_map.get(enc_type, nlmsg)
                enc = enc_class(data, offset=offset + 20)
                enc.decode()
                msg['header']['errmsg'] = enc

        msg['header']['error'] = error
        return msg

    def get_parser(self, key, flags, sequence_number):
        return self.seq_map.get(
            sequence_number,
            partial(self.parse_one_message, key, flags, sequence_number),
        )

    def parse(
        self,
        data: bytes,
        seq: Optional[int] = None,
        callback: Optional[Callable] = None,
        skip_alien_seq: bool = False,
    ) -> Generator[nlmsg, None, None]:
        '''
        Parse string data.

        At this moment all transport, except of the native
        Netlink is deprecated in this library, so we should
        not support any defragmentation on that level
        '''
        offset = 0
        # there must be at least one header in the buffer,
        # 'IHHII' == 16 bytes
        while offset <= len(data) - 16:
            # pick type and length
            (length, key, flags, sequence_number) = struct.unpack_from(
                'IHHI', data, offset
            )
            if skip_alien_seq and sequence_number != seq:
                continue
            if not 0 < length <= len(data):
                break
            # support custom parser keys
            # see also: pyroute2.netlink.diag.MarshalDiag
            if self.key_format is not None:
                (key,) = struct.unpack_from(
                    self.key_format, data, offset + self.key_offset
                )
                if self.key_mask is not None:
                    key &= self.key_mask

            parser = self.get_parser(key, flags, sequence_number)
            msg = parser(data, offset, length)
            offset += length
            if msg is None:
                continue

            if callable(callback) and seq == sequence_number:
                try:
                    if callback(msg):
                        continue
                except Exception:
                    pass

            mtype = msg['header'].get('type', None)
            if mtype in (1, 2, 3, 4) and 'event' not in msg:
                msg['event'] = mtypes.get(mtype, 'none')
            self.fix_message(msg)
            yield msg

    def is_enough(self, msg):
        return msg['header']['type'] == NLMSG_DONE

    def fix_message(self, msg):
        pass
