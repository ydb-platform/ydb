# -*- coding: utf-8 -*-
import sys

from pyroute2 import config
from pyroute2.iproute.linux import RTNL_API, IPBatch

# compatibility fix -- LNST:
from pyroute2.netlink.rtnl import (
    RTM_DELADDR,
    RTM_DELLINK,
    RTM_GETADDR,
    RTM_GETLINK,
    RTM_NEWADDR,
    RTM_NEWLINK,
)

AsyncIPRoute = None
if sys.platform.startswith('win'):
    from pyroute2.iproute.windows import (
        ChaoticIPRoute,
        IPRoute,
        NetNS,
        RawIPRoute,
    )
elif sys.platform.startswith('darwin'):
    from pyroute2.iproute.darwin import (
        ChaoticIPRoute,
        IPRoute,
        NetNS,
        RawIPRoute,
    )
elif config.uname[0][-3:] == 'BSD':
    from pyroute2.iproute.bsd import ChaoticIPRoute, IPRoute, NetNS, RawIPRoute
else:
    from pyroute2.iproute.linux import (
        AsyncIPRoute,
        ChaoticIPRoute,
        IPRoute,
        NetNS,
        RawIPRoute,
    )

classes = [
    AsyncIPRoute,
    RTNL_API,
    IPBatch,
    IPRoute,
    RawIPRoute,
    ChaoticIPRoute,
    NetNS,
]

constants = [
    RTM_GETLINK,
    RTM_NEWLINK,
    RTM_DELLINK,
    RTM_GETADDR,
    RTM_NEWADDR,
    RTM_DELADDR,
]
