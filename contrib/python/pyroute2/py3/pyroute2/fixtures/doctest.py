'''
This module only prepares the environment for doctests
in `pyroute2.fixtures`.
'''

from unittest.mock import MagicMock

from pyroute2 import config
from pyroute2.iproute.linux import AsyncIPRoute, IPRoute
from pyroute2.netlink.rtnl.ifinfmsg import ifinfmsg

# config setup
config.mock_netlink = True
config.mock_netns = True

# mock modules
subprocess = MagicMock()
pytest = MagicMock()

# mock fixtures
nsname = MagicMock()
test_link_ifinfmsg = ifinfmsg()
async_ipr = AsyncIPRoute()
sync_ipr = IPRoute()
