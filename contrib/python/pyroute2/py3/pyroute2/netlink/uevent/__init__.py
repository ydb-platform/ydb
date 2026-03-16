import struct
from typing import Callable, Generator, Optional

from pyroute2.netlink import NETLINK_KOBJECT_UEVENT, nlmsg
from pyroute2.netlink.nlsocket import (
    AsyncNetlinkSocket,
    Marshal,
    NetlinkSocket,
)

MONITOR_GROUP_NONE = 0
MONITOR_GROUP_KERNEL = 1
MONITOR_GROUP_UDEV = 2


class ueventmsg(nlmsg):
    pass


class MarshalUevent(Marshal):
    def is_enough(self, _) -> bool:
        return False

    def parse(
        self,
        data: bytes,
        seq: Optional[int] = None,
        callback: Optional[Callable] = None,
        skip_alien_seq: bool = False,
    ) -> Generator[ueventmsg, None, None]:
        ret = ueventmsg()
        ret['header']['sequence_number'] = 0

        # handle udevd messages which have a binary header
        if data.startswith(b'libudev\x00'):
            # prefix, magic, header_size, properties_off, properties_len,
            #   filter_subsystem_hash, filter_tag_bloom_hi,
            #   filter_tag_bloom_low
            header_format = '8sIIIIIIII'
            header_length = struct.calcsize(header_format)

            if len(data) < header_length:
                return

            lines = data[header_length:].split(b'\x00')
            ret['header']['message'] = "libudev"
        else:
            lines = data.split(b'\x00')
            ret['header']['message'] = lines[0].decode('utf-8')

        wtf = []
        ret['header']['unparsed'] = b''
        for line in lines[1:]:
            if line.find(b'=') <= 0:
                wtf.append(line)
            else:
                if wtf:
                    ret['header']['unparsed'] = b'\x00'.join(wtf)
                    wtf = []

                try:
                    key, value = line.decode('utf-8').split('=', 1)
                except UnicodeDecodeError:
                    # should not happen but let's account for it for
                    # robustness' sake
                    wtf.append(line)
                    continue

                ret[key] = value

        del ret['value']
        yield ret


class AsyncUeventSocket(AsyncNetlinkSocket):
    def __init__(self):
        super().__init__(NETLINK_KOBJECT_UEVENT)
        self.set_marshal(MarshalUevent())

    async def bind(self, groups: int = -1) -> None:  # type: ignore[override]
        return await super().bind(groups=groups)


class UeventSocket(NetlinkSocket):
    def __init__(self):
        super().__init__(NETLINK_KOBJECT_UEVENT)
        self.set_marshal(MarshalUevent())

    def bind(self, groups: int = -1) -> None:
        return super().bind(groups=groups)
