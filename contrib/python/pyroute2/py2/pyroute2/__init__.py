##
#
# This module contains all the public symbols from the library.
#
import sys
import struct

##
#
# Version
#
try:
    from pyroute2.config.version import __version__
except ImportError:
    __version__ = 'unknown'

##
#
# Windows platform specific: socket module monkey patching
#
# To use the library on Windows, run::
#   pip install win-inet-pton
#
if sys.platform.startswith('win'):  # noqa: E402
    import win_inet_pton            # noqa: F401

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

##
#
# Platform independent modules
#
from pyroute2.ipdb.exceptions import (DeprecationException,
                                      CommitException,
                                      CreateException,
                                      PartialCommitException)
from pyroute2.netlink.exceptions import (NetlinkError,
                                         NetlinkDecodeError)
from pyroute2.netlink.rtnl.req import (IPRouteRequest,
                                       IPLinkRequest)
from pyroute2.iproute import (IPRoute,
                              IPBatch,
                              RawIPRoute,
                              RemoteIPRoute)
from pyroute2.netlink.rtnl.iprsocket import IPRSocket
from pyroute2.ipdb.main import IPDB

##
#
# Linux specific code
#
if sys.platform.startswith('linux'):
    from pyroute2.ipset import IPSet
    from pyroute2.iwutil import IW
    from pyroute2.devlink import DL
    from pyroute2.ethtool import Ethtool
    from pyroute2.conntrack import Conntrack
    from pyroute2.nftables.main import NFTables
    from pyroute2.netns.nslink import NetNS
    from pyroute2.netns.process.proxy import NSPopen
    from pyroute2.inotify.inotify_fd import Inotify
    from pyroute2.netlink.taskstats import TaskStats
    from pyroute2.netlink.nl80211 import NL80211
    from pyroute2.netlink.devlink import DevlinkSocket
    from pyroute2.netlink.event.acpi_event import AcpiEventSocket
    from pyroute2.netlink.event.dquot import DQuotSocket
    from pyroute2.netlink.ipq import IPQSocket
    from pyroute2.netlink.diag import DiagSocket
    from pyroute2.netlink.uevent import UeventSocket
    from pyroute2.netlink.generic import GenericNetlinkSocket
    from pyroute2.netlink.generic.l2tp import L2tp
    from pyroute2.netlink.generic.mptcp import MPTCP
    from pyroute2.netlink.generic.wireguard import WireGuard
    from pyroute2.netlink.nfnetlink.nftsocket import NFTSocket
    from pyroute2.netlink.nfnetlink.nfctsocket import NFCTSocket
    from pyroute2.netns.manager import NetNSManager
#
# The NDB module has extra requirements that may not be present.
# It is not the core functionality, so simply skip the import if
# requirements are not met.
#
try:
    from pyroute2.ndb.main import NDB
    from pyroute2.ipdb.noipdb import NoIPDB
    HAS_NDB = True
except ImportError:
    HAS_NDB = False
#
# The Console class is a bit special, it tries to engage
# modules from stdlib, that are sometimes stripped. Some
# of them are optional, but some aren't. So catch possible
# errors here.
try:
    from pyroute2.cli.console import Console
    from pyroute2.cli.server import Server
    HAS_CONSOLE = True
except ImportError:
    HAS_CONSOLE = False

try:
    # probe, if the bytearray can be used in struct.unpack_from()
    struct.unpack_from('I', bytearray((1, 0, 0, 0)), 0)
except:
    if sys.version_info[0] < 3:
        # monkeypatch for old Python versions
        log.warning('patching struct.unpack_from()')

        def wrapped(fmt, buf, offset=0):
            return struct._u_f_orig(fmt, str(buf), offset)
        struct._u_f_orig = struct.unpack_from
        struct.unpack_from = wrapped
    else:
        raise

# re-export exceptions
exceptions = [NetlinkError,
              NetlinkDecodeError,
              DeprecationException,
              CommitException,
              CreateException,
              PartialCommitException]

# re-export classes
classes = [IPRouteRequest,
           IPLinkRequest,
           IPRoute,
           IPBatch,
           RawIPRoute,
           RemoteIPRoute,
           IPRSocket,
           IPDB]

if sys.platform.startswith('linux'):
    classes.extend([IPSet,
                    IW,
                    DL,
                    Ethtool,
                    Conntrack,
                    NFTables,
                    NetNS,
                    NSPopen,
                    TaskStats,
                    NL80211,
                    DevlinkSocket,
                    AcpiEventSocket,
                    DQuotSocket,
                    IPQSocket,
                    DiagSocket,
                    UeventSocket,
                    GenericNetlinkSocket,
                    L2tp,
                    MPTCP,
                    WireGuard,
                    NFTSocket,
                    NFCTSocket,
                    Inotify,
                    NetNSManager])

if HAS_CONSOLE:
    classes.append(Console)
    classes.append(Server)
else:
    log.warning("Couldn't import the Console class")

if HAS_NDB:
    classes.append(NDB)
    classes.append(NoIPDB)
else:
    log.warning("Couldn't import NDB")

log.warning("Beware of the coming changes in the project packaging")
log.warning("https://github.com/svinota/pyroute2/discussions/786")
__all__ = []
__all__.extend([x.__name__ for x in exceptions])
__all__.extend([x.__name__ for x in classes])
