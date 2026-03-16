import struct
from functools import partial

from pyroute2.netlink import NLMSG_DONE, nlmsg
from pyroute2.netlink.exceptions import NetlinkError
from pyroute2.netlink.rtnl import RTM_NEWROUTE
from pyroute2.netlink.rtnl.rtmsg import rtmsg


def get_header(data, offset):
    # get message header
    header = dict(
        zip(
            ('length', 'type', 'flags', 'sequence_number'),
            struct.unpack_from('IHHI', data, offset),
        )
    )
    header['error'] = None
    return header


def msg_done(header):
    msg = nlmsg()
    msg['header'] = header
    msg.length = msg['header']['length']
    return msg


def _export_routes(fd, data, offset, length):
    '''Export RTM_NEWROUTE messages binary data.

    Otherwise return NLMSG_DONE.
    '''
    header = get_header(data, offset)
    if header['type'] == NLMSG_DONE:
        return msg_done(header)
    elif header['type'] == RTM_NEWROUTE:
        fd.write(data[offset : offset + length])
        return
    raise NetlinkError()


def export_routes(fd):
    return partial(_export_routes, fd)


def default_routes(data, offset, length):
    '''
    Only for RTM_NEWROUTE.

    This parser returns:

    * rtmsg() -- only for default routes (no RTA_DST)
    * nlmsg() -- NLMSG_DONE
    * None for any other messages
    '''
    header = get_header(data, offset)
    if header['type'] == NLMSG_DONE:
        return msg_done(header)

    # skip to NLA: offset + nlmsg header + rtmsg data
    cursor = offset + 28

    # iterate NLA, if meet RTA_DST -- return None (not a default route)
    while cursor < offset + length:
        nla_length, nla_type = struct.unpack_from('HH', data, cursor)
        nla_length = (nla_length + 3) & ~3  # align, page size = 4
        cursor += nla_length
        if nla_type == 1:
            return

    # no RTA_DST, a default route -- spend time to decode using the
    # standard routine
    msg = rtmsg(data, offset=offset)
    msg.decode()
    msg['header']['error'] = None  # required
    return msg
