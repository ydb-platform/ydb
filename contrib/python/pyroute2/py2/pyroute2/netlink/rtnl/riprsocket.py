from pyroute2.netlink import rtnl
from pyroute2.netlink import NETLINK_ROUTE
from pyroute2.netlink.nlsocket import NetlinkSocket
from pyroute2.netlink.rtnl.marshal import MarshalRtnl


class RawIPRSocketMixin(object):

    def __init__(self, fileno=None):
        super(RawIPRSocketMixin, self).__init__(NETLINK_ROUTE, fileno=fileno)
        self.marshal = MarshalRtnl()

    def bind(self, groups=rtnl.RTMGRP_DEFAULTS, **kwarg):
        super(RawIPRSocketMixin, self).bind(groups, **kwarg)


class RawIPRSocket(RawIPRSocketMixin, NetlinkSocket):
    pass
