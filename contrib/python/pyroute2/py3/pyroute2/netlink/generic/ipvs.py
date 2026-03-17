from pyroute2.netlink import genlmsg, nla
from pyroute2.netlink.generic import (
    AsyncGenericNetlinkSocket,
    GenericNetlinkSocket,
)

GENL_NAME = "IPVS"
GENL_VERSION = 0x1

IPVS_CMD_UNSPEC = 0

IPVS_CMD_NEW_SERVICE = 1
IPVS_CMD_SET_SERVICE = 2
IPVS_CMD_DEL_SERVICE = 3
IPVS_CMD_GET_SERVICE = 4

IPVS_CMD_NEW_DEST = 5
IPVS_CMD_SET_DEST = 6
IPVS_CMD_DEL_DEST = 7
IPVS_CMD_GET_DEST = 8

IPVS_CMD_NEW_DAEMON = 9
IPVS_CMD_DEL_DAEMON = 10
IPVS_CMD_GET_DAEMON = 11

IPVS_CMD_SET_CONFIG = 12
IPVS_CMD_GET_CONFIG = 13

IPVS_CMD_SET_INFO = 14
IPVS_CMD_GET_INFO = 15

IPVS_CMD_ZERO = 16
IPVS_CMD_FLUSH = 17


class ipvsstats:
    class stats(nla):
        nla_map = (
            ("IPVS_STATS_ATTR_UNSPEC", "none"),
            ("IPVS_STATS_ATTR_CONNS", "uint32"),
            ("IPVS_STATS_ATTR_INPKTS", "uint32"),
            ("IPVS_STATS_ATTR_OUTPKTS", "uint32"),
            ("IPVS_STATS_ATTR_INBYTES", "uint64"),
            ("IPVS_STATS_ATTR_OUTBYTES", "uint64"),
            ("IPVS_STATS_ATTR_CPS", "uint32"),
            ("IPVS_STATS_ATTR_INPPS", "uint32"),
            ("IPVS_STATS_ATTR_OUTPPS", "uint32"),
            ("IPVS_STATS_ATTR_INBPS", "uint32"),
            ("IPVS_STATS_ATTR_OUTBPS", "uint32"),
        )

    class stats64(nla):
        nla_map = (
            ("IPVS_STATS_ATTR_UNSPEC", "none"),
            ("IPVS_STATS_ATTR_CONNS", "uint64"),
            ("IPVS_STATS_ATTR_INPKTS", "uint64"),
            ("IPVS_STATS_ATTR_OUTPKTS", "uint64"),
            ("IPVS_STATS_ATTR_INBYTES", "uint64"),
            ("IPVS_STATS_ATTR_OUTBYTES", "uint64"),
            ("IPVS_STATS_ATTR_CPS", "uint64"),
            ("IPVS_STATS_ATTR_INPPS", "uint64"),
            ("IPVS_STATS_ATTR_OUTPPS", "uint64"),
            ("IPVS_STATS_ATTR_INBPS", "uint64"),
            ("IPVS_STATS_ATTR_OUTBPS", "uint64"),
        )


class ipvsmsg(genlmsg):
    prefix = "IPVS_CMD_ATTR_"
    nla_map = (
        ("IPVS_CMD_ATTR_UNSPEC", "none"),
        ("IPVS_CMD_ATTR_SERVICE", "service"),
        ("IPVS_CMD_ATTR_DEST", "dest"),
        ("IPVS_CMD_ATTR_DAEMON", "hex"),
        ("IPVS_CMD_ATTR_TIMEOUT_TCP", "hex"),
        ("IPVS_CMD_ATTR_TIMEOUT_TCP_FIN", "hex"),
        ("IPVS_CMD_ATTR_TIMEOUT_UDP", "hex"),
    )

    class service(nla, ipvsstats):
        prefix = "IPVS_SVC_ATTR_"
        nla_map = (
            ("IPVS_SVC_ATTR_UNSPEC", "none"),
            ("IPVS_SVC_ATTR_AF", "uint16"),
            ("IPVS_SVC_ATTR_PROTOCOL", "uint16"),
            ("IPVS_SVC_ATTR_ADDR", "target(nla,IPVS_SVC_ATTR_AF)"),
            ("IPVS_SVC_ATTR_PORT", "be16"),
            ("IPVS_SVC_ATTR_FWMARK", "uint32"),
            ("IPVS_SVC_ATTR_SCHED_NAME", "asciiz"),
            ("IPVS_SVC_ATTR_FLAGS", "flags"),
            ("IPVS_SVC_ATTR_TIMEOUT", "uint32"),
            ("IPVS_SVC_ATTR_NETMASK", "ip4addr"),
            ("IPVS_SVC_ATTR_STATS", "stats"),
            ("IPVS_SVC_ATTR_PE_NAME", "asciiz"),
            ("IPVS_SVC_ATTR_STATS64", "stats64"),
        )

        class flags(nla):
            fields = (("flags", "I"), ("mask", "I"))

    class dest(nla, ipvsstats):
        prefix = "IPVS_DEST_ATTR_"
        nla_map = (
            ("IPVS_DEST_ATTR_UNSPEC", "none"),
            ("IPVS_DEST_ATTR_ADDR", "target(nla,IPVS_DEST_ATTR_ADDR_FAMILY)"),
            ("IPVS_DEST_ATTR_PORT", "be16"),
            ("IPVS_DEST_ATTR_FWD_METHOD", "uint32"),
            ("IPVS_DEST_ATTR_WEIGHT", "uint32"),
            ("IPVS_DEST_ATTR_U_THRESH", "uint32"),
            ("IPVS_DEST_ATTR_L_THRESH", "uint32"),
            ("IPVS_DEST_ATTR_ACTIVE_CONNS", "uint32"),
            ("IPVS_DEST_ATTR_INACT_CONNS", "uint32"),
            ("IPVS_DEST_ATTR_PERSIST_CONNS", "uint32"),
            ("IPVS_DEST_ATTR_STATS", "stats"),
            ("IPVS_DEST_ATTR_ADDR_FAMILY", "uint16"),
            ("IPVS_DEST_ATTR_STATS64", "stats64"),
            ("IPVS_DEST_ATTR_TUN_TYPE", "uint8"),
            ("IPVS_DEST_ATTR_TUN_PORT", "uint16"),
            ("IPVS_DEST_ATTR_TUN_FLAGS", "uint16"),
        )


class AsyncIPVSSocket(AsyncGenericNetlinkSocket):

    async def setup_endpoint(self):
        if getattr(self.local, 'transport', None) is not None:
            return
        await super().setup_endpoint()
        await self.bind(GENL_NAME, ipvsmsg)


class IPVSSocket(GenericNetlinkSocket):
    async_class = AsyncIPVSSocket
