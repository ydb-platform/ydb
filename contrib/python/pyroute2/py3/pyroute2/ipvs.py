'''
IPVS -- IP Virtual Server
-------------------------

IPVS configuration is done via generic netlink protocol.
At the low level one can use it with a GenericNetlinkSocket,
binding it to "IPVS" generic netlink family.

But for the convenience the library provides utility classes:

    * IPVS -- a socket class to access the API
    * IPVSService -- a class to define IPVS service records
    * IPVSDest -- a class to define real server records

Dump all the records::

    from pyroute2 import IPVS, IPVSDest, IPVSService

    # run the socket
    ipvs = IPVS()

    # iterate all the IPVS services
    for s in ipvs.service("dump"):

        # create a utility object from a netlink message
        service = IPVSService.from_message(s)
        print("Service: ", service)

        # iterate all the real servers for this service
        for d in ipvs.dest("dump", service=service):

            # create and print a utility object
            dest = IPVSDest.from_message(d)
            print("  Real server: ", dest)

Create a service and a real server record::

    from socket import IPPROTO_TCP
    from pyroute2 import IPVS, IPVSDest, IPVSService

    ipvs = IPVS()

    service = IPVSService(addr="192.168.122.1", port=80, protocol=IPPROTO_TCP)
    real_server = IPVSDest(addr="10.0.2.20", port=80)

    ipvs.service("add", service=service)
    ipvs.dest("add", service=service, dest=real_server)

Delete a service::

    from pyroute2 import IPVS, IPVSService

    ipvs = IPVS()
    ipvs.service("del",
        service=IPVSService(
            addr="192.168.122.1",
            port=80,
            protocol=IPPROTO_TCP
        )
    )

'''

from socket import AF_INET

from pyroute2.common import get_address_family
from pyroute2.netlink.generic import ipvs
from pyroute2.netlink.nlsocket import NetlinkRequest
from pyroute2.requests.common import NLAKeyTransform
from pyroute2.requests.main import RequestProcessor


class ServiceFieldFilter(NLAKeyTransform):
    _nla_prefix = 'IPVS_SVC_ATTR_'

    def set_addr(self, context, value):
        ret = {"addr": value}
        if "af" in context.keys():
            family = context["af"]
        else:
            family = ret["af"] = get_address_family(value)
        if family == AF_INET and "netmask" not in context.keys():
            ret["netmask"] = "255.255.255.255"
        return ret


class DestFieldFilter(NLAKeyTransform):
    _nla_prefix = 'IPVS_DEST_ATTR_'

    def set_addr(self, context, value):
        ret = {"addr": value}
        if "addr_family" not in context.keys():
            ret["addr_family"] = get_address_family(value)
        return ret


class NLAFilter(RequestProcessor):
    msg = None
    keys = tuple()
    field_filters = None
    nla = None
    default_values = {}

    def __init__(self, **kwarg):
        dict.update(self, self.default_values)
        # save field filters
        flt = self.field_filters
        # init resets the filters, not fixed yet
        super().__init__(prime=kwarg)
        # restore filters
        for f in flt:
            self.add_filter(f)

    @classmethod
    def from_message(cls, msg):
        obj = cls()
        for key, value in msg.get(cls.nla)["attrs"]:
            obj[key] = value
        obj.pop("stats", None)
        obj.pop("stats64", None)

        # Determine the prefix based on the _nla_prefix attribute of the class
        prefix = (
            getattr(cls.field_filters[0], "_nla_prefix", "")
            if cls.field_filters
            else ""
        )

        # af / addr_family
        if "af" not in obj and f"{prefix}AF" in obj:
            obj["af"] = obj[f"{prefix}AF"]
        if "addr_family" not in obj and f"{prefix}ADDR_FAMILY" in obj:
            obj["addr_family"] = obj[f"{prefix}ADDR_FAMILY"]

        # protocol
        if "protocol" not in obj and f"{prefix}PROTOCOL" in obj:
            obj["protocol"] = obj[f"{prefix}PROTOCOL"]
        # port
        if "port" not in obj and f"{prefix}PORT" in obj:
            obj["port"] = obj[f"{prefix}PORT"]
        # addr
        if "addr" not in obj and f"{prefix}ADDR" in obj:
            obj["addr"] = obj[f"{prefix}ADDR"]
        # sched_name
        if "sched_name" in obj:
            obj["sched_name"] = obj[f"{prefix}SCHED_NAME"]

        return obj

    def dump_nla(self, items=None):
        if items is None:
            items = self.items()
        self.update(self)
        self.finalize()
        return {
            "attrs": list(
                map(lambda x: (self.msg.name2nla(x[0]), x[1]), items)
            )
        }

    def dump_key(self):
        return self.dump_nla(
            items=filter(lambda x: x[0] in self.key_fields, self.items())
        )


class IPVSService(NLAFilter):
    field_filters = [ServiceFieldFilter()]
    msg = ipvs.ipvsmsg.service
    key_fields = ("af", "protocol", "addr", "port")
    nla = "IPVS_CMD_ATTR_SERVICE"
    default_values = {
        "timeout": 0,
        "sched_name": "wlc",
        "flags": {"flags": 0, "mask": 0xFFFF},
    }


class IPVSDest(NLAFilter):
    field_filters = [DestFieldFilter()]
    msg = ipvs.ipvsmsg.dest
    nla = "IPVS_CMD_ATTR_DEST"
    default_values = {
        "fwd_method": 3,
        "weight": 1,
        "tun_type": 0,
        "tun_port": 0,
        "tun_flags": 0,
        "u_thresh": 0,
        "l_thresh": 0,
    }


class AsyncIPVS(ipvs.AsyncIPVSSocket):

    async def service(self, command, service=None):
        command_map = {
            "add": (ipvs.IPVS_CMD_NEW_SERVICE, "create"),
            "set": (ipvs.IPVS_CMD_SET_SERVICE, "change"),
            "update": (ipvs.IPVS_CMD_DEL_SERVICE, "change"),
            "del": (ipvs.IPVS_CMD_DEL_SERVICE, "req"),
            "get": (ipvs.IPVS_CMD_GET_SERVICE, "get"),
            "dump": (ipvs.IPVS_CMD_GET_SERVICE, "dump"),
        }
        cmd, flags = NetlinkRequest.calculate_request_type(
            command, command_map
        )
        msg = ipvs.ipvsmsg()
        msg["cmd"] = cmd
        msg["version"] = ipvs.GENL_VERSION
        if service is not None:
            msg["attrs"] = [("IPVS_CMD_ATTR_SERVICE", service.dump_nla())]
        return await self._do_request(msg, msg_flags=flags)

    async def dest(self, command, service, dest=None):
        command_map = {
            "add": (ipvs.IPVS_CMD_NEW_DEST, "create"),
            "set": (ipvs.IPVS_CMD_SET_DEST, "change"),
            "update": (ipvs.IPVS_CMD_DEL_DEST, "change"),
            "del": (ipvs.IPVS_CMD_DEL_DEST, "req"),
            "get": (ipvs.IPVS_CMD_GET_DEST, "get"),
            "dump": (ipvs.IPVS_CMD_GET_DEST, "dump"),
        }
        cmd, flags = NetlinkRequest.calculate_request_type(
            command, command_map
        )
        msg = ipvs.ipvsmsg()
        msg["cmd"] = cmd
        msg["version"] = 0x1
        msg["attrs"] = [("IPVS_CMD_ATTR_SERVICE", service.dump_key())]
        if dest is not None:
            msg["attrs"].append(("IPVS_CMD_ATTR_DEST", dest.dump_nla()))
        return await self._do_request(msg, msg_flags=flags)


class IPVS(ipvs.IPVSSocket):
    async_class = AsyncIPVS

    def service(self, command, service=None):
        return self._run_with_cleanup(self.asyncore.service, command, service)

    def dest(self, command, service, dest=None):
        return self._run_with_cleanup(
            self.asyncore.dest, command, service, dest
        )
