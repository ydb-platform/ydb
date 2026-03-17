##
#
# This module contains all the public symbols from the library.
#

##
#
# Version
#
try:
    from pyroute2.config.version import __version__
except ImportError:
    __version__ = 'unknown'
import sys

##
#
# Logging setup
#
# See the history:
#  * https://github.com/svinota/pyroute2/issues/246
#  * https://github.com/svinota/pyroute2/issues/255
#  * https://github.com/svinota/pyroute2/issues/270
#  * https://github.com/svinota/pyroute2/issues/573
#  * https://github.com/svinota/pyroute2/issues/601
#
from pyroute2.config import log
from pyroute2.conntrack import AsyncConntrack, Conntrack, ConntrackEntry
from pyroute2.devlink import DL, AsyncDL
from pyroute2.ethtool.ethtool import Ethtool
from pyroute2.ipdb import IPDB, CommitException, CreateException
from pyroute2.iproute import (
    AsyncIPRoute,
    ChaoticIPRoute,
    IPBatch,
    IPRoute,
    NetNS,
    RawIPRoute,
)
from pyroute2.ipset import AsyncIPSet, IPSet
from pyroute2.ipvs import IPVS, AsyncIPVS, IPVSDest, IPVSService
from pyroute2.iwutil import IW, AsyncIW
from pyroute2.ndb.main import NDB
from pyroute2.netlink.connector.cn_proc import ProcEventSocket
from pyroute2.netlink.devlink import AsyncDevlinkSocket, DevlinkSocket
from pyroute2.netlink.diag import DiagSocket, ss2
from pyroute2.netlink.event import AsyncEventSocket, EventSocket
from pyroute2.netlink.event.acpi_event import (
    AcpiEventSocket,
    AsyncAcpiEventSocket,
)
from pyroute2.netlink.event.dquot import AsyncDQuotSocket, DQuotSocket
from pyroute2.netlink.event.thermal import (
    AsyncThermalEventSocket,
    ThermalEventSocket,
)
from pyroute2.netlink.exceptions import (
    ChaoticException,
    NetlinkDecodeError,
    NetlinkDumpInterrupted,
    NetlinkError,
)
from pyroute2.netlink.generic import (
    AsyncGenericNetlinkSocket,
    GenericNetlinkSocket,
)
from pyroute2.netlink.generic.ethtool import AsyncNlEthtool, NlEthtool
from pyroute2.netlink.generic.ipvs import AsyncIPVSSocket, IPVSSocket
from pyroute2.netlink.generic.l2tp import AsyncL2tp, L2tp
from pyroute2.netlink.generic.mptcp import MPTCP, AsyncMPTCP
from pyroute2.netlink.generic.wireguard import AsyncWireGuard, WireGuard
from pyroute2.netlink.ipq import IPQSocket
from pyroute2.netlink.nfnetlink.nfctsocket import AsyncNFCTSocket, NFCTSocket
from pyroute2.netlink.nfnetlink.nftsocket import AsyncNFTSocket, NFTSocket
from pyroute2.netlink.nl80211 import NL80211, AsyncNL80211
from pyroute2.netlink.rtnl.iprsocket import AsyncIPRSocket, IPRSocket
from pyroute2.netlink.taskstats import AsyncTaskStats, TaskStats
from pyroute2.netlink.uevent import UeventSocket
from pyroute2.nslink.nspopen import NSPopen
from pyroute2.plan9.client import Plan9ClientSocket
from pyroute2.plan9.server import Plan9ServerSocket
from pyroute2.wiset import WiSet

##
#
# Windows platform specific: socket module monkey patching
#
# To use the library on Windows, run::
#   pip install win-inet-pton
#
if sys.platform.startswith('win'):  # noqa: E402
    import win_inet_pton  # noqa: F401


modules = [
    AcpiEventSocket,
    AsyncAcpiEventSocket,
    AsyncConntrack,
    AsyncDL,
    AsyncDQuotSocket,
    AsyncDevlinkSocket,
    AsyncEventSocket,
    AsyncGenericNetlinkSocket,
    AsyncIPRSocket,
    AsyncIPRoute,
    AsyncIPSet,
    AsyncIPVS,
    AsyncIPVSSocket,
    AsyncIW,
    AsyncL2tp,
    AsyncMPTCP,
    AsyncNFCTSocket,
    AsyncNFTSocket,
    AsyncNL80211,
    AsyncNlEthtool,
    AsyncTaskStats,
    AsyncThermalEventSocket,
    AsyncWireGuard,
    ChaoticException,
    ChaoticIPRoute,
    CommitException,
    Conntrack,
    ConntrackEntry,
    CreateException,
    DL,
    DQuotSocket,
    DevlinkSocket,
    DiagSocket,
    Ethtool,
    EventSocket,
    GenericNetlinkSocket,
    IPBatch,
    IPDB,
    IPQSocket,
    IPRSocket,
    IPRoute,
    IPSet,
    IPVS,
    IPVSDest,
    IPVSService,
    IPVSSocket,
    IW,
    L2tp,
    MPTCP,
    NDB,
    NFCTSocket,
    NFTSocket,
    NL80211,
    NSPopen,
    NetNS,
    NetlinkDecodeError,
    NetlinkDumpInterrupted,
    NetlinkError,
    NlEthtool,
    Plan9ClientSocket,
    Plan9ServerSocket,
    ProcEventSocket,
    RawIPRoute,
    TaskStats,
    ThermalEventSocket,
    UeventSocket,
    WiSet,
    WireGuard,
    log,
    ss2,
]

__all__ = []
__all__.extend(modules)
