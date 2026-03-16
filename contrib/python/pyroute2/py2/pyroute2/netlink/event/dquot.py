'''
VFS_DQUOT module
================

Usage::

    from pyroute2 import DQuotSocket

    ds = DQuotSocket()
    msgs = ds.get()

Please notice, that `.get()` always returns a list, even if
only one message arrived. To get NLA values::

    msg = msgs[0]
    uid = msg.get_attr('QUOTA_NL_A_EXCESS_ID')
    major = msg.get_attr('QUOTA_NL_A_DEV_MAJOR')
    minor = msg.get_attr('QUOTA_NL_A_DEV_MINOR')
'''
from pyroute2.netlink import genlmsg
from pyroute2.netlink.nlsocket import Marshal
from pyroute2.netlink.event import EventSocket

QUOTA_NL_C_UNSPEC = 0
QUOTA_NL_C_WARNING = 1


class dquotmsg(genlmsg):
    prefix = 'QUOTA_NL_A_'
    nla_map = (('QUOTA_NL_A_UNSPEC', 'none'),
               ('QUOTA_NL_A_QTYPE', 'uint32'),
               ('QUOTA_NL_A_EXCESS_ID', 'uint64'),
               ('QUOTA_NL_A_WARNING', 'uint32'),
               ('QUOTA_NL_A_DEV_MAJOR', 'uint32'),
               ('QUOTA_NL_A_DEV_MINOR', 'uint32'),
               ('QUOTA_NL_A_CAUSED_ID', 'uint64'),
               ('QUOTA_NL_A_PAD', 'uint64'))


class MarshalDQuot(Marshal):
    msg_map = {QUOTA_NL_C_UNSPEC: dquotmsg,
               QUOTA_NL_C_WARNING: dquotmsg}


class DQuotSocket(EventSocket):
    marshal_class = MarshalDQuot
    genl_family = 'VFS_DQUOT'
