# -*- coding: utf-8 -*-
import asyncio
import errno
import io
import logging
import os
import struct
import time
import warnings
from functools import partial
from socket import AF_INET, AF_INET6, AF_UNSPEC

from pyroute2 import config, netns
from pyroute2.common import AF_MPLS, basestring, get_time
from pyroute2.netlink import NLM_F_ACK, NLM_F_DUMP, NLM_F_REQUEST, NLMSG_ERROR
from pyroute2.netlink.exceptions import (
    NetlinkDumpInterrupted,
    NetlinkError,
    SkipInode,
)
from pyroute2.netlink.nlsocket import NetlinkRequest, NetlinkSocket
from pyroute2.netlink.rtnl import (
    RTM_DELADDR,
    RTM_DELLINK,
    RTM_DELLINKPROP,
    RTM_DELNEIGH,
    RTM_DELQDISC,
    RTM_DELROUTE,
    RTM_DELRULE,
    RTM_DELTCLASS,
    RTM_DELTFILTER,
    RTM_GETADDR,
    RTM_GETLINK,
    RTM_GETNEIGH,
    RTM_GETNEIGHTBL,
    RTM_GETNSID,
    RTM_GETQDISC,
    RTM_GETROUTE,
    RTM_GETRULE,
    RTM_GETSTATS,
    RTM_GETTCLASS,
    RTM_GETTFILTER,
    RTM_NEWADDR,
    RTM_NEWLINK,
    RTM_NEWLINKPROP,
    RTM_NEWNEIGH,
    RTM_NEWNETNS,
    RTM_NEWNSID,
    RTM_NEWPROBE,
    RTM_NEWQDISC,
    RTM_NEWROUTE,
    RTM_NEWRULE,
    RTM_NEWTCLASS,
    RTM_NEWTFILTER,
    RTM_SETLINK,
    RTMGRP_DEFAULTS,
    RTMGRP_IPV4_IFADDR,
    RTMGRP_IPV4_ROUTE,
    RTMGRP_IPV4_RULE,
    RTMGRP_IPV6_IFADDR,
    RTMGRP_IPV6_ROUTE,
    RTMGRP_IPV6_RULE,
    RTMGRP_LINK,
    RTMGRP_MPLS_ROUTE,
    RTMGRP_NEIGH,
    ndmsg,
)
from pyroute2.netlink.rtnl.fibmsg import fibmsg
from pyroute2.netlink.rtnl.ifaddrmsg import ifaddrmsg
from pyroute2.netlink.rtnl.ifinfmsg import ifinfmsg
from pyroute2.netlink.rtnl.ifstatsmsg import ifstatsmsg
from pyroute2.netlink.rtnl.iprsocket import AsyncIPRSocket, ChaoticIPRSocket
from pyroute2.netlink.rtnl.ndtmsg import ndtmsg
from pyroute2.netlink.rtnl.nsidmsg import nsidmsg
from pyroute2.netlink.rtnl.nsinfmsg import nsinfmsg
from pyroute2.netlink.rtnl.probe_msg import probe_msg
from pyroute2.netlink.rtnl.rtmsg import rtmsg
from pyroute2.netlink.rtnl.tcmsg import plugins as tc_plugins
from pyroute2.netlink.rtnl.tcmsg import tcmsg
from pyroute2.requests.address import AddressFieldFilter, AddressIPRouteFilter
from pyroute2.requests.bridge import (
    BridgeFieldFilter,
    BridgeIPRouteFilter,
    BridgePortFieldFilter,
)
from pyroute2.requests.link import LinkFieldFilter, LinkIPRouteFilter
from pyroute2.requests.main import RequestProcessor
from pyroute2.requests.neighbour import (
    NeighbourFieldFilter,
    NeighbourIPRouteFilter,
)
from pyroute2.requests.probe import ProbeFieldFilter
from pyroute2.requests.route import RouteFieldFilter, RouteIPRouteFilter
from pyroute2.requests.rule import RuleFieldFilter, RuleIPRouteFilter
from pyroute2.requests.tc import TcIPRouteFilter, TcRequestFilter

from .parsers import default_routes, export_routes

DEFAULT_TABLE = 254
IPROUTE2_DUMP_MAGIC = 0x45311224
log = logging.getLogger(__name__)


def get_default_request_filters(mode, command):
    filters = {
        'link': [LinkFieldFilter(), LinkIPRouteFilter(command)],
        'addr': [AddressFieldFilter(), AddressIPRouteFilter(command)],
        'neigh': [NeighbourFieldFilter(), NeighbourIPRouteFilter(command)],
        'route': [
            RouteFieldFilter(add_defaults=(command not in ('dump', 'show'))),
            RouteIPRouteFilter(command),
        ],
        'rule': [RuleFieldFilter(), RuleIPRouteFilter(command)],
        'tc': [TcRequestFilter(), TcIPRouteFilter(command)],
        'brport': [BridgePortFieldFilter(command)],
        'vlan_filter': [BridgeFieldFilter(), BridgeIPRouteFilter(command)],
        'probe': [ProbeFieldFilter()],
        'stats': [],
    }
    return filters[mode]


def get_dump_filter(mode, command, query, parameters=None):
    if 'dump_filter' in query:
        return query.pop('dump_filter'), query
    if command not in ('dump', 'show'):
        return RequestProcessor(parameters=parameters), query
    new_query = {}
    if 'family' in query:
        new_query['family'] = query.pop('family')
    if 'ext_mask' in query:
        new_query['ext_mask'] = query.pop('ext_mask')
    if 'match' in query:
        query = query['match']
    if callable(query):
        return query, {}
    dump_filter = RequestProcessor(
        context=query, prime=query, parameters=parameters
    )
    for rf in query.pop(
        'dump_filter', get_default_request_filters(mode, command)
    ):
        dump_filter.add_filter(rf)
    dump_filter.finalize()
    return dump_filter, new_query


def get_arguments_processor(mode, command, query, parameters=None):
    if 'request_filter' in query:
        return query['request_filter']
    parameters = parameters or {}
    # if not query:
    #    return RequestProcessor()
    processor = RequestProcessor(context=query, prime=query)
    for pname, pvalue in parameters.items():
        processor.set_parameter(pname, pvalue)
    for rf in get_default_request_filters(mode, command):
        processor.add_filter(rf)
    processor.finalize()
    return processor


def transform_handle(handle):
    if isinstance(handle, basestring):
        (major, minor) = [int(x if x else '0', 16) for x in handle.split(':')]
        handle = (major << 8 * 2) | minor
    return handle


class RTNL_API:
    '''A mixin RTNL API class.

    `RTNL_API` should not be instantiated by itself, it is intended
    to be used as a mixin class. Following classes use `RTNL_API`:

    * `AsyncIPRoute` -- Asynchronous RTNL API
    * `IPRoute` -- Synchronous RTNL API
    * `NetNS` -- Legace netns-enabled RTNL API

    This class was started as iproute2 ip/tc equivalent, but as a
    Python API. It does not provide any complicated logic, but instead
    runs simple RTNL queries:
    It is an old-school API, that provides access to rtnetlink as is.
    It helps you to retrieve and change almost all the data, available
    through rtnetlink:

    .. testcode:: cls01

        from pyroute2 import IPRoute

        ipr = IPRoute()

        # create an interface
        ipr.link("add", ifname="brx", kind="bridge")

        # lookup the index
        dev = ipr.link_lookup(ifname="brx")[0]

        # bring it up
        ipr.link("set", index=dev, state="up")

        # change the interface MAC address and rename it just for fun
        ipr.link(
            "set",
            index=dev,
            address="00:11:22:33:44:55",
            ifname="br-ctrl",
        )

        # add primary IP address
        ipr.addr(
            "add",
            index=dev,
            address="10.0.0.1",
            prefixlen=24,
            broadcast="10.0.0.255",
        )
        # add secondary IP address
        ipr.addr(
            "add",
            index=dev,
            address="10.0.0.2",
            prefixlen=24,
            broadcast="10.0.0.255",
        )

    .. testcode:: cls01
        :hide:

        br = ipr.link("get", index=dev)[0]
        assert br.get("flags") & 1
        try:
            assert br.get("flags") & 2
        except AssertionError:
            pass
        assert br.get("ifname") == "br-ctrl"
        assert br.get("address") == "00:11:22:33:44:55"
        addr1, addr2 = ipr.addr("dump", index=dev)
        assert addr1.get("address") == "10.0.0.1"
        assert addr2.get("address") == "10.0.0.2"
        assert addr1.get("broadcast") == "10.0.0.255"
        assert addr2.get("broadcast") == "10.0.0.255"
        assert addr1.get("prefixlen") == 24
        assert addr2.get("prefixlen") == 24
    '''

    def filter_messages(self, dump_filter, msgs):
        '''
        Filter messages using `dump_filter`. The filter might be a
        callable, then it will be called for every message in the list.
        Or it might be a dict, where keys are used to get values
        from messages, and dict values are used to match the message.

        The method might be called directly. It is also used by calls
        like `ipr.link('dump', ....)`, where keyword arguments work as
        `dump_filter` for `ipr.filter_messages()`.

        A callable `dump_filter` must return True or False:


        .. testcode:: fm01

            # get all links with names starting with eth:
            #
            for link in ipr.filter_messages(
                lambda x: x.get("ifname").startswith("eth"),
                ipr.link("dump"),
            ):
                print(link.get("ifname"))

        .. testoutput:: fm01

            eth0

        A dict `dump_filter` can have callables as values:

        .. testcode:: fm02

            # get all links with names starting with eth, and
            # MAC address in a database:
            #
            database = [
                "52:54:00:72:58:b2",
            ]

            for link in ipr.filter_messages(
                {
                    "ifname": lambda x: x.startswith("eth"),
                    "address": lambda x: x in database,
                },
                ipr.link("dump"),
            ):
                print(link.get("ifname"))

        .. testoutput:: fm02

            eth0

        ... or constants to compare with:

        .. testcode:: fm03

            # get all links in state up:
            #
            for link in ipr.filter_messages(
                {"state": "up"}, ipr.link("dump"),
            ):
                print(link.get("ifname"))

        .. testoutput:: fm03

            lo
            eth0
        '''
        # filtered results, the generator version
        for msg in msgs:
            if NetlinkRequest.match_one_message(dump_filter, msg):
                yield msg

    # 8<---------------------------------------------------------------
    #
    async def dump(self, groups=None):
        '''
        Dump network objects.

        * get_links()
        * get_addr()
        * get_neighbours()
        * get_vlans()
        * dump FDB
        * IPv4 and IPv6 rules
        '''
        groups_map = {
            RTMGRP_LINK: [
                self.get_links,
                self.get_vlans,
                partial(self.fdb, 'dump'),
            ],
            RTMGRP_IPV4_IFADDR: [partial(self.get_addr, family=AF_INET)],
            RTMGRP_IPV6_IFADDR: [partial(self.get_addr, family=AF_INET6)],
            RTMGRP_NEIGH: [self.get_neighbours],
            RTMGRP_IPV4_ROUTE: [partial(self.get_routes, family=AF_INET)],
            RTMGRP_IPV6_ROUTE: [partial(self.get_routes, family=AF_INET6)],
            RTMGRP_MPLS_ROUTE: [partial(self.get_routes, family=AF_MPLS)],
            RTMGRP_IPV4_RULE: [partial(self.get_rules, family=AF_INET)],
            RTMGRP_IPV6_RULE: [partial(self.get_rules, family=AF_INET6)],
        }

        async def ret():
            for group, methods in groups_map.items():
                if group & (groups if groups is not None else self.groups):
                    for method in methods:
                        async for msg in await method():
                            yield msg

        return ret()

    async def ensure(
        self, method, present=True, timeout=10, interval=0.2, **spec
    ):
        '''Ensure object's state.

        The issue with RTNL calls is that they are not synchronous:
        even if the kernel returns success for a call, the changes
        may become visible after some short time. Because of that
        adding dependent object immediately one after another may fail.
        Say, adding a route directly after the required interface
        address.

        Since pyroute2 RTNL API is more or less one to one mapping
        of the kernel RTNL, it has the same problem.

        This method aims to mitigate this issue.

        * if `present == True`, try to add/set an object by the spec.
        * if `present == False`, try to remove
        * and finally wait up to `timeout` for the changes to be applied.

        Example:

        .. testcode::

            interface = ipr.ensure(ipr.link,
                present=True,
                ifname='test0',
                kind='dummy',
                state='up',
            )
            ipr.ensure(ipr.addr,
                present=True,
                index=interface,
                address='192.168.0.2/24',
            )
        '''
        state = [x async for x in await method('dump', **spec)]
        if present:
            if state:
                return state
            try:
                await method('add', **spec)
            except NetlinkError as e:
                if e.code != errno.EEXIST:
                    raise
                await method('set', **spec)
        else:
            if not state:
                return state
            try:
                await method('del', **spec)
            except NetlinkError as e:
                if e.code not in (
                    errno.ENODEV,
                    errno.ENOENT,
                    errno.ENONET,
                    errno.EADDRNOTAVAIL,
                ):
                    raise
        return await self.poll(
            method, 'dump', present, timeout, interval, **spec
        )

    async def poll(
        self, method, command, present=True, timeout=10, interval=0.2, **spec
    ):
        '''Wait for a method to succeed.

        Run `method` with a positional argument `command` and keyword
        arguments `**spec` every `interval` seconds, but not more than
        `timeout`, until it returns a result which doesn't evaluate to
        `False`.

        Example:

        .. testcode:: p0

            # create a bridge interface and wait for it:
            #
            spec = {
                'ifname': 'br0',
                'kind': 'bridge',
                'state': 'up',
            }
            ipr.link('add', **spec)
            ret = ipr.poll(ipr.link, 'dump', **spec)

            assert ret[0].get('ifname') == 'br0'
            assert ret[0].get('flags') & 1
            assert ret[0].get('state') == 'up'
            assert ret[0].get(('linkinfo', 'kind')) == 'bridge'

        .. testcode:: p1
            :hide:

            try:
                ipr.poll(ipr.link, 'dump', ifname='br1')
            except TimeoutError:
                pass
        '''
        ctime = get_time()
        ret = tuple()
        while ctime + timeout > get_time():
            try:
                ret = await method(command, **spec)
                if not isinstance(ret, list):
                    ret = [x async for x in ret]
                if (ret and present) or (not ret and not present):
                    return ret
                await asyncio.sleep(interval)
            except NetlinkDumpInterrupted:
                pass
        raise asyncio.TimeoutError()

    # 8<---------------------------------------------------------------
    #
    # Diagnostics
    #
    async def probe(self, command, **kwarg):
        '''Run a network probe.

        The API will trigger a network probe from the environment it
        works in. For NetNS it will be the network namespace, for
        remote IPRoute instances it will be the host it runs on.

        Running probes via API allows to test network connectivity
        between the environments in a simple uniform way.

        Supported arguments:

        * kind -- probe type, for now only ping is supported
        * dst -- target to run the probe against
        * num -- number of probes to run
        * timeout -- timeout for the whole request

        Examples::

            ipr.probe("add", kind="ping", dst="10.0.0.1")

        By default ping probe will send one ICMP request towards
        the target. To change this, use num argument::

            ipr.probe(
                "add",
                kind="ping",
                dst="10.0.0.1",
                num=4,
                timeout=10
            )

        Timeout for the ping probe by default is 1 second, which
        may not be enough to run multiple requests.

        In the next release more probe types are planned, like TCP
        port probe.
        '''
        msg = probe_msg()
        arguments = get_arguments_processor('probe', command, kwarg)
        request = NetlinkRequest(
            self,
            msg,
            msg_type=RTM_NEWPROBE,
            msg_flags=1,
            request_filter=arguments,
        )
        await request.send()
        return [x async for x in request.response()]

    # 8<---------------------------------------------------------------
    #
    # Binary streams methods
    #
    async def route_dump(self, fd, family=AF_UNSPEC, fmt='iproute2'):
        '''Save routes as a binary dump into a file object.

        fd -- an open file object, must support `write()`
        family -- AF_UNSPEC, AF_INET, etc. -- filter routes by family
        fmt -- dump format, "iproute2" (default) or "raw"

        The binary dump is just a set of unparsed netlink messages.
        The `iproute2` prepends the dump with a magic uint32, so
        `IPRoute` does the same for compatibility. If you want a raw
        dump without any additional magic data, use `fmt="raw"`.

        This routine neither close the file object, nor uses `seek()`
        to rewind, it's up to the user.
        '''

        if fmt == 'iproute2':
            fd.write(struct.pack('I', IPROUTE2_DUMP_MAGIC))
        elif fmt != 'raw':
            raise TypeError('dump format not supported')
        msg = rtmsg()
        msg['family'] = family
        request = NetlinkRequest(
            self,
            msg,
            msg_type=RTM_GETROUTE,
            msg_flags=NLM_F_DUMP | NLM_F_REQUEST,
            parser=export_routes(fd),
        )
        await request.send()
        return [x async for x in request.response()]

    async def route_dumps(self, family=AF_UNSPEC, fmt='iproute2'):
        '''Save routes and returns as a `bytes` object.

        The same as `.route_dump()`, but returns `bytes`.
        '''
        fd = io.BytesIO()
        await self.route_dump(fd, family, fmt)
        return fd.getvalue()

    async def route_load(self, fd, fmt='iproute2'):
        '''Load routes from a binary dump.

        fd -- an open file object, must support `read()`
        fmt -- dump format, "iproute2" (default) or "raw"

        The current version parses the dump and loads routes one
        by one. This behavior will be changed in the future to
        optimize the performance, but the result will be the same.

        If `fmt == "iproute2"`, then the loader checks the magic iproute2
        prefix in the dump. Otherwise it parses the data from byte 0.
        '''
        if fmt == 'iproute2':
            if (
                not struct.unpack('I', fd.read(struct.calcsize('I')))[0]
                == IPROUTE2_DUMP_MAGIC
            ):
                raise TypeError('wrong dump magic')
        elif fmt != 'raw':
            raise TypeError('dump format not supported')
        ret = []
        for msg in self.marshal.parse(fd.read()):
            request = NetlinkRequest(
                self,
                msg,
                command='replace',
                command_map={'replace': (RTM_NEWROUTE, 'replace')},
            )
            await request.send()
            ret.extend(
                [
                    x['header']['error'] is None
                    async for x in request.response()
                ]
            )
        if not all(ret):
            raise NetlinkError('error loading route dump')
        return []

    async def route_loads(self, data, fmt='iproute2'):
        '''Load routes from a `bytes` object.

        Like `.route_load()`, but accepts `bytes` instead of an file file.
        '''
        fd = io.BytesIO()
        fd.write(data)
        fd.seek(0)
        return await self.route_load(fd, fmt)

    # 8<---------------------------------------------------------------
    #
    # Listing methods
    #
    async def get_qdiscs(self, index=None):
        '''
        Get all queue disciplines for all interfaces or for
        the selected one.

        A compatibility method, == .tc("dump")
        '''
        return await self.tc('dump', index=index)

    async def get_filters(self, index=0, handle=0, parent=0):
        '''
        Get filters for specified interface, handle and parent.
        '''
        msg = tcmsg()
        msg['family'] = AF_UNSPEC
        msg['index'] = index
        msg['handle'] = transform_handle(handle)
        msg['parent'] = transform_handle(parent)
        request = NetlinkRequest(self, msg, msg_type=RTM_GETTFILTER)
        await request.send()
        return request.response()

    async def get_classes(self, index=0):
        '''
        Get classes for specified interface.
        '''
        msg = tcmsg()
        msg['family'] = AF_UNSPEC
        msg['index'] = index
        request = NetlinkRequest(self, msg, msg_type=RTM_GETTCLASS)
        await request.send()
        return request.response()

    async def get_vlans(self, **kwarg):
        '''
        Dump available vlan info on bridge ports
        '''
        # IFLA_EXT_MASK, extended info mask
        #
        # include/uapi/linux/rtnetlink.h
        # 1 << 0 => RTEXT_FILTER_VF
        # 1 << 1 => RTEXT_FILTER_BRVLAN
        # 1 << 2 => RTEXT_FILTER_BRVLAN_COMPRESSED
        # 1 << 3 => RTEXT_FILTER_SKIP_STATS
        #
        # maybe place it as mapping into ifinfomsg.py?
        #
        return await self.link(
            'dump', family=config.AF_BRIDGE, ext_mask=2, match=kwarg
        )

    async def get_links(self, *argv, **kwarg):
        '''
        Get network interfaces.

        By default returns all interfaces. Arguments vector
        can contain interface indices or a special keyword
        'all'::

            ip.get_links()
            ip.get_links('all')
            ip.get_links(1, 2, 3)

            interfaces = [1, 2, 3]
            ip.get_links(*interfaces)
        '''
        links = argv or [0]
        if links[0] == 'all':  # compat syntax
            links = [0]

        if links[0] == 0:
            return await self.link('dump', **kwarg)

        async def dump():
            for index in links:
                for link in await self.link('get', index=index, **kwarg):
                    yield link

        return dump()

    async def get_neighbours(self, family=AF_UNSPEC, match=None, **kwarg):
        '''
        Dump ARP cache records.

        The `family` keyword sets the family for the request:
        e.g. `AF_INET` or `AF_INET6` for arp cache, `AF_BRIDGE`
        for fdb.

        If other keyword arguments not empty, they are used as
        filter. Also, one can explicitly set filter as a function
        with the `match` parameter.

        Examples::

            # get neighbours on the 3rd link:
            ip.get_neighbours(ifindex=3)

            # get a particular record by dst:
            ip.get_neighbours(dst='172.16.0.1')

            # get fdb records:
            ip.get_neighbours(AF_BRIDGE)

            # and filter them by a function:
            ip.get_neighbours(AF_BRIDGE, match=lambda x: x['state'] == 2)
        '''
        return await self.neigh('dump', family=family, match=match or kwarg)

    async def get_ntables(self, family=AF_UNSPEC):
        '''
        Get neighbour tables
        '''
        msg = ndtmsg()
        msg['family'] = family
        request = NetlinkRequest(self, msg, msg_type=RTM_GETNEIGHTBL)
        await request.send()
        return request.response()

    async def get_addr(self, family=AF_UNSPEC, match=None, **kwarg):
        '''
        Dump addresses.

        If family is not specified, both AF_INET and AF_INET6 addresses
        will be dumped::

            # get all addresses
            ip.get_addr()

        It is possible to apply filters on the results::

            # get addresses for the 2nd interface
            ip.get_addr(index=2)

            # get addresses with IFA_LABEL == 'eth0'
            ip.get_addr(label='eth0')

            # get all the subnet addresses on the interface, identified
            # by broadcast address (should be explicitly specified upon
            # creation)
            ip.get_addr(index=2, broadcast='192.168.1.255')

        A custom predicate can be used as a filter::

            ip.get_addr(match=lambda x: x['index'] == 1)
        '''
        return await self.addr('dump', family=family, match=match or kwarg)

    async def get_rules(self, family=AF_UNSPEC, match=None, **kwarg):
        '''
        Get all rules. By default return all rules. To explicitly
        request the IPv4 rules use `family=AF_INET`.

        Example::
            ip.get_rules() # get all the rules for all families
            ip.get_rules(family=AF_INET6)  # get only IPv6 rules
        '''
        return await self.rule('dump', family=family, match=match or kwarg)

    async def get_routes(self, family=255, match=None, **kwarg):
        '''
        Get all routes. You can specify the table. There
        are up to 4294967295 routing classes (tables), and the kernel
        returns all the routes on each request. So the
        routine filters routes from full output. Note the number of
        tables is increased from 255 in Linux 2.6+.

        Example::

            ip.get_routes()  # get all the routes for all families
            ip.get_routes(family=AF_INET6)  # get only IPv6 routes
            ip.get_routes(table=254)  # get routes from 254 table

        The default family=255 is a hack. Despite the specs,
        the kernel returns only IPv4 routes for AF_UNSPEC family.
        But it returns all the routes for all the families if one
        uses an invalid value here. Hack but true. And let's hope
        the kernel team will not fix this bug.
        '''

        # get a particular route?
        async def dump(dst):
            for route in await self.route('get', dst=dst):
                yield route

        if isinstance(kwarg.get('dst'), str):
            return dump(kwarg['dst'])
        else:
            return await self.route(
                'dump', family=family, match=match or kwarg
            )

    # 8<---------------------------------------------------------------

    # 8<---------------------------------------------------------------
    @staticmethod
    def open_file(path):
        '''Open a file (read only) and return its (fd, inode).'''
        fd = os.open(path, os.O_RDONLY)
        inode = os.fstat(fd).st_ino
        return (fd, inode)

    @staticmethod
    def close_file(fd):
        '''Close a file that was previously opened with open_file().'''
        os.close(fd)

    @staticmethod
    def get_pid():
        '''Return the PID of the current process.'''
        return os.getpid()

    def register_link_kind(self, path=None, pkg=None, module=None):
        return ifinfmsg.ifinfo.register_link_kind(path, pkg, module)

    def unregister_link_kind(self, kind):
        return ifinfmsg.ifinfo.unregister_link_kind(kind)

    def list_link_kind(self):
        return ifinfmsg.ifinfo.list_link_kind()

    #
    # List NetNS info
    #
    async def _dump_one_ns(self, path, registry):
        item = nsinfmsg()
        item['netnsid'] = 0xFFFFFFFF  # default netnsid "unknown"
        nsfd = 0
        info = nsidmsg()
        msg = nsidmsg()
        try:
            (nsfd, inode) = self.open_file(path)
            item['inode'] = inode
            #
            # if the inode is registered, skip it
            #
            if item['inode'] in registry:
                raise SkipInode()
            registry.add(item['inode'])
            #
            # request NETNSA_NSID
            #
            # may not work on older kernels ( <4.20 ?)
            #
            msg['attrs'] = [('NETNSA_FD', nsfd)]
            request = NetlinkRequest(
                self, msg, msg_type=RTM_GETNSID, msg_flags=NLM_F_REQUEST
            )
            await request.send()
            try:
                async for info in request.response():
                    # response to nlm_request() is a list or a generator,
                    # that's why loop
                    item['netnsid'] = info.get_attr('NETNSA_NSID')
                    break
            except Exception:
                pass
            item['attrs'] = [('NSINFO_PATH', path)]
        except OSError as e:
            raise SkipInode(e.errno)
        finally:
            if nsfd > 0:
                self.close_file(nsfd)
        item['header']['type'] = RTM_NEWNETNS
        item['header']['target'] = self.target
        item['event'] = 'RTM_NEWNETNS'
        return item

    async def _dump_dir(self, path, registry):
        for name in os.listdir(path):
            # strictly speaking, there is no need to use os.sep,
            # since the code is not portable outside of Linux
            nspath = '%s%s%s' % (path, os.sep, name)
            try:
                yield await self._dump_one_ns(nspath, registry)
            except SkipInode:
                pass

    async def _dump_proc(self, registry):
        for name in os.listdir('/proc'):
            try:
                int(name)
            except ValueError:
                continue

            try:
                yield await self._dump_one_ns(
                    '/proc/%s/ns/net' % name, registry
                )
            except SkipInode:
                pass

    async def get_netnsid(
        self, nsid=None, pid=None, fd=None, target_nsid=None
    ):
        '''Return a dict containing the result of a RTM_GETNSID query.
        This loosely corresponds to the "ip netns list-id" command.
        '''
        msg = nsidmsg()

        if nsid is not None:
            msg['attrs'].append(('NETNSA_NSID', nsid))

        if pid is not None:
            msg['attrs'].append(('NETNSA_PID', pid))

        if fd is not None:
            msg['attrs'].append(('NETNSA_FD', fd))

        if target_nsid is not None:
            msg['attrs'].append(('NETNSA_TARGET_NSID', target_nsid))

        request = NetlinkRequest(
            self, msg, msg_type=RTM_GETNSID, msg_flags=NLM_F_REQUEST
        )
        await request.send()
        async for r in request.response():
            return {
                'nsid': r.get_attr('NETNSA_NSID'),
                'current_nsid': r.get_attr('NETNSA_CURRENT_NSID'),
            }

        return None

    async def get_netns_info(self, list_proc=False):
        '''
        A prototype method to list available netns and associated
        interfaces. A bit weird to have it here and not under
        `pyroute2.netns`, but it uses RTNL to get all the info.
        '''
        #
        # register all the ns inodes, not to repeat items in the output
        #
        registry = set()
        #
        # fetch veth peers
        #
        peers = {}
        async for peer in await self.link('dump'):
            netnsid = peer.get_attr('IFLA_LINK_NETNSID')
            if netnsid is not None:
                if netnsid not in peers:
                    peers[netnsid] = []
                peers[netnsid].append(peer.get_attr('IFLA_IFNAME'))
        #
        # chain iterators:
        #
        # * one iterator for every item in self.path
        # * one iterator for /proc/<pid>/ns/net
        #
        views = []
        for path in self.status['netns_path']:
            views.append(self._dump_dir(path, registry))
        if list_proc:
            views.append(self._dump_proc(registry))

        #
        # iterate all the items
        #
        async def ret():
            for view in views:
                try:
                    async for item in view:
                        #
                        # remove uninitialized 'value' field
                        #
                        del item['value']
                        #
                        # fetch peers for that ns
                        #
                        for peer in peers.get(item['netnsid'], []):
                            item['attrs'].append(('NSINFO_PEER', peer))
                        yield item
                except OSError:
                    pass

        return ret()

    def set_netnsid(self, nsid=None, pid=None, fd=None):
        '''Assigns an id to a peer netns using RTM_NEWNSID query.
        The kernel chooses an unique id if nsid is omitted.
        This corresponds to the "ip netns set" command.
        '''
        msg = nsidmsg()

        if nsid is None or nsid < 0:
            # kernel auto select
            msg['attrs'].append(('NETNSA_NSID', 4294967295))
        else:
            msg['attrs'].append(('NETNSA_NSID', nsid))

        if pid is not None:
            msg['attrs'].append(('NETNSA_PID', pid))

        if fd is not None:
            msg['attrs'].append(('NETNSA_FD', fd))

        return self.nlm_request(msg, RTM_NEWNSID, NLM_F_REQUEST | NLM_F_ACK)

    # 8<---------------------------------------------------------------

    # 8<---------------------------------------------------------------
    #
    # Shortcuts
    #
    async def get_default_routes(self, family=AF_UNSPEC, table=DEFAULT_TABLE):
        '''
        Get default routes
        '''
        msg = rtmsg()
        msg['family'] = family
        dump_filter, _ = get_dump_filter(
            'route', 'dump', {'table': table} if table is not None else {}
        )
        request = NetlinkRequest(
            self,
            msg,
            msg_type=RTM_GETROUTE,
            msg_flags=NLM_F_DUMP | NLM_F_REQUEST,
            parser=default_routes,
        )
        await request.send()
        return request.response()

    async def link_lookup(self, match=None, **kwarg):
        '''
        Lookup interface index (indices) by first level NLA
        value.

        Example::

            ip.link_lookup(address="52:54:00:9d:4e:3d")
            ip.link_lookup(ifname="lo")
            ip.link_lookup(operstate="UP")

        Please note, that link_lookup() returns list, not one
        value.
        '''
        if kwarg and set(kwarg) < {'index', 'ifname', 'altname'}:
            # shortcut for index and ifname
            try:
                for link in await self.link('get', **kwarg):
                    return [link['index']]
            except (NetlinkError, KeyError):
                return []
        else:
            # otherwise fallback to the userspace filter
            return [
                link['index']
                async for link in await self.get_links(match=match or kwarg)
            ]

    # 8<---------------------------------------------------------------

    # 8<---------------------------------------------------------------
    #
    # Shortcuts to flush RTNL objects
    #
    async def flush_routes(self, *argv, **kwarg):
        '''
        Flush routes -- purge route records from a table.
        Arguments are the same as for `get_routes()`
        routine. Actually, this routine implements a pipe from
        `get_routes()` to `nlm_request()`.
        '''
        ret = []
        async for route in await self.get_routes(*argv, **kwarg):
            request = NetlinkRequest(
                self,
                route,
                msg_type=RTM_DELROUTE,
                msg_flags=NLM_F_REQUEST | NLM_F_ACK,
            )
            await request.send()
            ret.extend([y async for y in request.response()])
        return ret

    async def flush_addr(self, *argv, **kwarg):
        '''
        Flush IP addresses.

        Examples::

            # flush all addresses on the interface with index 2:
            ipr.flush_addr(index=2)

            # flush all addresses with IFA_LABEL='eth0':
            ipr.flush_addr(label='eth0')
        '''
        ret = []
        work = []
        async for addr in await self.get_addr(*argv, **kwarg):
            work.append(
                {
                    'index': addr.get('index'),
                    'address': addr.get('address'),
                    'prefixlen': addr.get('prefixlen'),
                }
            )
        for addr in work:
            try:
                ret.extend(await self.addr('del', **addr))
            except NetlinkError:
                if not ret:
                    raise
        return ret

    async def flush_rules(self, *argv, **kwarg):
        '''
        Flush rules. Please keep in mind, that by default the function
        operates on **all** rules of **all** families. To work only on
        IPv4 rules, one should explicitly specify `family=AF_INET`.

        Examples::

            # flush all IPv4 rule with priorities above 5 and below 32000
            ipr.flush_rules(family=AF_INET, priority=lambda x: 5 < x < 32000)

            # flush all IPv6 rules that point to table 250:
            ipr.flush_rules(family=socket.AF_INET6, table=250)
        '''
        ret = []
        for rule in tuple([x async for x in await self.rule('dump', **kwarg)]):
            request = NetlinkRequest(
                self,
                rule,
                msg_type=RTM_DELRULE,
                msg_flags=NLM_F_REQUEST | NLM_F_ACK,
            )
            await request.send()
            ret.extend([y async for y in request.response()])
        return ret

    # 8<---------------------------------------------------------------

    # 8<---------------------------------------------------------------
    #
    # Extensions to low-level functions
    #
    async def brport(self, command, **kwarg):
        '''
        Set bridge port parameters. Example::

            idx = ip.link_lookup(ifname='eth0')
            ip.brport("set", index=idx, unicast_flood=0, cost=200)
            ip.brport("show", index=idx)

        Possible keywords are NLA names for the `protinfo_bridge` class,
        without the prefix and in lower letters.
        '''

        # forward the set command to link()
        if command == 'set':
            linkkwarg = dict()
            linkkwarg['index'] = kwarg.pop('index', 0)
            linkkwarg['kind'] = 'bridge_slave'
            for key in kwarg:
                linkkwarg[key] = kwarg[key]
            return await self.link(command, **linkkwarg)

        command_map = {
            'dump': (RTM_GETLINK, 'dump'),
            'show': (RTM_GETLINK, 'dump'),
        }
        if command not in command_map:
            raise TypeError('command not supported')
        dump_filter, kwarg = get_dump_filter('brport', command, kwarg)
        arguments = get_arguments_processor('brport', command, kwarg)
        request = NetlinkRequest(
            self, ifinfmsg(), command, command_map, dump_filter, arguments
        )
        await request.send()
        return request.response()

    async def vlan_filter(self, command, **kwarg):
        '''
        Vlan filters is another approach to support vlans in Linux.
        Before vlan filters were introduced, there was only one way
        to bridge vlans: one had to create vlan interfaces and
        then add them as ports:

        .. aafig::

                    +------+      +----------+
            net --> | eth0 | <--> | eth0.500 | <---+
                    +------+      +----------+     |
                                                   v
                    +------+                    +-----+
            net --> | eth1 |                    | br0 |
                    +------+                    +-----+
                                                   ^
                    +------+      +----------+     |
            net --> | eth2 | <--> | eth2.500 | <---+
                    +------+      +----------+

        It means that one has to create as many bridges, as there were
        vlans. Vlan filters allow to bridge together underlying interfaces
        and create vlans already on the bridge:

        .. aafig::

            # v500 label shows which interfaces have vlan filter

                    +------+ v500
            net --> | eth0 | <-------+
                    +------+         |
                                     v
                    +------+      +-----+    +---------+
            net --> | eth1 | <--> | br0 |<-->| br0v500 |
                    +------+      +-----+    +---------+
                                     ^
                    +------+ v500    |
            net --> | eth2 | <-------+
                    +------+

        In this example vlan 500 will be allowed only on ports `eth0` and
        `eth2`, though all three eth nics are bridged.

        Some example code::

            # create bridge
            ip.link("add",
                    ifname="br0",
                    kind="bridge")

            # attach a port
            ip.link("set",
                    index=ip.link_lookup(ifname="eth0")[0],
                    master=ip.link_lookup(ifname="br0")[0])

            # set vlan filter
            ip.vlan_filter("add",
                           index=ip.link_lookup(ifname="eth0")[0],
                           vlan_info={"vid": 500})

            # create vlan interface on the bridge
            ip.link("add",
                    ifname="br0v500",
                    kind="vlan",
                    link=ip.link_lookup(ifname="br0")[0],
                    vlan_id=500)

            # set all UP
            ip.link("set",
                    index=ip.link_lookup(ifname="br0")[0],
                    state="up")
            ip.link("set",
                    index=ip.link_lookup(ifname="br0v500")[0],
                    state="up")
            ip.link("set",
                    index=ip.link_lookup(ifname="eth0")[0],
                    state="up")

            # set IP address
            ip.addr("add",
                    index=ip.link_lookup(ifname="br0v500")[0],
                    address="172.16.5.2",
                    mask=24)

            Now all the traffic to the network 172.16.5.2/24 will go
            to vlan 500 only via ports that have such vlan filter.

        Required arguments for `vlan_filter()`: `index` and `vlan_info`.

        Vlan info dict::

            ip.vlan_filter('add',
                            index=<ifindex>,
                            vlan_info =
                            {'vid': <single or range>,
                            'pvid': <bool>,
                            'flags': int or list}

        More details:
            * kernel:Documentation/networking/switchdev.txt
            * pyroute2.netlink.rtnl.ifinfmsg:... vlan_info

        Setting PVID or specifying a range will specify the appropriate flags.

        One can specify `flags` as int or as a list of flag names:
            * `master` == 0x1
            * `pvid` == 0x2
            * `untagged` == 0x4
            * `range_begin` == 0x8
            * `range_end` == 0x10
            * `brentry` == 0x20

        E.g.::

            {'vid': 20, 'pvid': true }

            # is equal to
            {'vid': 20, 'flags': ['pvid', 'untagged']}

            # is equal to
            {'vid': 20, 'flags': 6}

            # range
            {'vid': '100-199'}

        Required arguments for `vlan_filter()`: `index` and `vlan_tunnel_info`.

        Vlan tunnel info dict::

            ip.vlan_filter('add',
                          index=<ifindex>,
                          vlan_tunnel_info =
                          {'vid': <single or range>,
                          'id': <single or range>}

        vlan_tunnel_info appears to only use the 'range_begin' and 'range_end'
        flags from vlan_info. Specifying a range will automatically send the
        needed flags.

        Example::

            {'vid': 20, 'id: 20}
            {'vid': '200-299', 'id': '200-299'}

        The above directives can be combined as in the example::

          ip.vlan_filter('add',
                        index=7,
                        vlan_info={'vid': 600},
                        vlan_tunnel_info={'vid': 600, 'id': 600})

        Commands:

        **add**

        Add vlan filter to a bridge port. Example::

          ip.vlan_filter("add", index=2, vlan_info={"vid": 200})

        **del**

        Remove vlan filter from a bridge port. Example::

          ip.vlan_filter("del", index=2, vlan_info={"vid": 200})

        '''
        command_map = {
            'add': (RTM_SETLINK, 'req'),
            'del': (RTM_DELLINK, 'req'),
        }
        kwarg['family'] = config.AF_BRIDGE
        kwarg['command_map'] = command_map
        kwarg['dump_filter'] = None
        kwarg['request_filter'] = get_arguments_processor(
            'vlan_filter', command, kwarg
        )
        return await self.link(command, **kwarg)

    async def fdb(self, command, **kwarg):
        '''
        Bridge forwarding database management.

        More details:
            * kernel:Documentation/networking/switchdev.txt
            * pyroute2.netlink.rtnl.ndmsg

        **add**

        Add a new FDB record. Works in the same way as ARP cache
        management, but some additional NLAs can be used::

            # simple FDB record
            #
            ip.fdb('add',
                   ifindex=ip.link_lookup(ifname='br0')[0],
                   lladdr='00:11:22:33:44:55',
                   dst='10.0.0.1')

            # specify vlan
            # NB: vlan should exist on the device, use
            # `vlan_filter()`
            #
            ip.fdb('add',
                   ifindex=ip.link_lookup(ifname='br0')[0],
                   lladdr='00:11:22:33:44:55',
                   dst='10.0.0.1',
                   vlan=200)

            # specify vxlan id and port
            # NB: works only for vxlan devices, use
            # `link("add", kind="vxlan", ...)`
            #
            # if port is not specified, the default one is used
            # by the kernel.
            #
            # if vni (vxlan id) is equal to the device vni,
            # the kernel doesn't report it back
            #
            ip.fdb('add',
                   ifindex=ip.link_lookup(ifname='vx500')[0]
                   lladdr='00:11:22:33:44:55',
                   dst='10.0.0.1',
                   port=5678,
                   vni=600)

            # or specify src_vni for a vlan-aware vxlan device
            ip.fdb('add',
                   ifindex=ip.link_lookup(ifname='vx500')[0]
                   lladdr='00:11:22:33:44:55',
                   dst='10.0.0.1',
                   port=5678,
                   src_vni=600)

        **append**

        Append a new FDB record. The same syntax as for **add**.

        **del**

        Remove an existing FDB record. The same syntax as for **add**.

        **dump**

        Dump all the FDB records. If any `**kwarg` is provided,
        results will be filtered::

            # dump all the records
            ip.fdb('dump')

            # show only specific lladdr, dst, vlan etc.
            ip.fdb('dump', lladdr='00:11:22:33:44:55')
            ip.fdb('dump', dst='10.0.0.1')
            ip.fdb('dump', vlan=200)

        '''
        kwarg['family'] = config.AF_BRIDGE
        # nud -> state
        if 'nud' in kwarg:
            kwarg['state'] = kwarg.pop('nud')
        if (command in ('add', 'del', 'append')) and not (
            kwarg.get('state', 0) & ndmsg.states['noarp']
        ):
            # state must contain noarp in add / del / append
            kwarg['state'] = kwarg.pop('state', 0) | ndmsg.states['noarp']
            # other assumptions
            if not kwarg.get('state', 0) & (
                ndmsg.states['permanent'] | ndmsg.states['reachable']
            ):
                # permanent (default) or reachable
                kwarg['state'] |= ndmsg.states['permanent']
            if not kwarg.get('flags', 0) & (
                ndmsg.flags['self'] | ndmsg.flags['master']
            ):
                # self (default) or master
                kwarg['flags'] = kwarg.get('flags', 0) | ndmsg.flags['self']
        #
        return await self.neigh(command, **kwarg)

    # 8<---------------------------------------------------------------
    #
    # General low-level configuration methods
    #
    async def neigh(self, command, **kwarg):
        '''
        Neighbours operations, same as `ip neigh` or `bridge fdb`

        **add**

        Add a neighbour record, e.g.::

            from pyroute2 import IPRoute
            from pyroute2.netlink.rtnl import ndmsg

            # add a permanent record on veth0
            idx = ip.link_lookup(ifname='veth0')[0]
            ip.neigh('add',
                     dst='172.16.45.1',
                     lladdr='00:11:22:33:44:55',
                     ifindex=idx,
                     state=ndmsg.states['permanent'])

        **set**

        Set an existing record or create a new one, if it doesn't exist.
        The same as above, but the command is "set"::

            ip.neigh('set',
                     dst='172.16.45.1',
                     lladdr='00:11:22:33:44:55',
                     ifindex=idx,
                     state=ndmsg.states['permanent'])


        **change**

        Change an existing record. If the record doesn't exist, fail.

        **del**

        Delete an existing record.

        **dump**

        Dump all the records in the NDB::

            ip.neigh('dump')

        **get**

        Get specific record (dst and ifindex are mandatory). Available
        only on recent kernel::

            ip.neigh('get',
                     dst='172.16.45.1',
                     ifindex=idx)
        '''
        command_map = {
            'add': (RTM_NEWNEIGH, 'create'),
            'set': (RTM_NEWNEIGH, 'replace'),
            'replace': (RTM_NEWNEIGH, 'replace'),
            'change': (RTM_NEWNEIGH, 'change'),
            'del': (RTM_DELNEIGH, 'req'),
            'remove': (RTM_DELNEIGH, 'req'),
            'delete': (RTM_DELNEIGH, 'req'),
            'dump': (RTM_GETNEIGH, 'dump'),
            'get': (RTM_GETNEIGH, 'get'),
            'append': (RTM_NEWNEIGH, 'append'),
        }
        if isinstance(kwarg.get('match'), str):
            kwarg['match'] = {'ifname': kwarg['match']}
        dump_filter, kwarg = get_dump_filter('neigh', command, kwarg)
        arguments = get_arguments_processor('neigh', command, kwarg)
        request = NetlinkRequest(
            self, ndmsg.ndmsg(), command, command_map, dump_filter, arguments
        )
        await request.send()
        if command == 'dump':
            return request.response()
        return [x async for x in request.response()]

    async def link(self, command, **kwarg):
        '''
        Link operations.

        Keywords to set up ifinfmsg fields:
            * index -- interface index
            * family -- AF_BRIDGE for bridge operations, otherwise 0
            * flags -- device flags
            * change -- change mask

        All other keywords will be translated to NLA names, e.g.
        `mtu -> IFLA_MTU`, `af_spec -> IFLA_AF_SPEC` etc. You can
        provide a complete NLA structure or let filters do it for
        you. E.g., these pairs show equal statements::

            # set device MTU
            ip.link("set", index=x, mtu=1000)
            ip.link("set", index=x, IFLA_MTU=1000)

            # add vlan device
            ip.link("add", ifname="test", kind="dummy")
            ip.link("add", ifname="test",
                    IFLA_LINKINFO={'attrs': [['IFLA_INFO_KIND', 'dummy']]})

        Filters are implemented in the `pyroute2.iproute.req` module.
        You can contribute your own if you miss shortcuts.

        Commands:

        **add**

        To create an interface, one should specify the interface kind::

            ip.link("add",
                    ifname="test",
                    kind="dummy")

        The kind can be any of those supported by kernel. It can be
        `dummy`, `bridge`, `bond` etc. On modern kernels one can specify
        even interface index::

            ip.link("add",
                    ifname="br-test",
                    kind="bridge",
                    index=2345)

        Specific type notes:

        ► geneve

        Create GENEVE tunnel::

            ip.link("add",
                    ifname="genx",
                    kind="geneve",
                    geneve_id=42,
                    geneve_remote="172.16.0.101")

        Support for GENEVE over IPv6 is also included; use `geneve_remote6`
        to configure a remote IPv6 address.

        ► gre

        Create GRE tunnel::

            ip.link("add",
                    ifname="grex",
                    kind="gre",
                    gre_local="172.16.0.1",
                    gre_remote="172.16.0.101",
                    gre_ttl=16)

        The keyed GRE requires explicit iflags/oflags specification::

            ip.link("add",
                    ifname="grex",
                    kind="gre",
                    gre_local="172.16.0.1",
                    gre_remote="172.16.0.101",
                    gre_ttl=16,
                    gre_ikey=10,
                    gre_okey=10,
                    gre_iflags=32,
                    gre_oflags=32)

        Support for GRE over IPv6 is also included; use `kind=ip6gre` and
        `ip6gre_` as the prefix for its values.

        ► ipip

        Create ipip tunnel::

            ip.link("add",
                    ifname="tun1",
                    kind="ipip",
                    ipip_local="172.16.0.1",
                    ipip_remote="172.16.0.101",
                    ipip_ttl=16)

        Support for sit and ip6tnl is also included; use `kind=sit` and `sit_`
        as prefix for sit tunnels, and `kind=ip6tnl` and `ip6tnl_` prefix for
        ip6tnl tunnels.

        ► macvlan

        Macvlan interfaces act like VLANs within OS. The macvlan driver
        provides an ability to add several MAC addresses on one interface,
        where every MAC address is reflected with a virtual interface in
        the system.

        In some setups macvlan interfaces can replace bridge interfaces,
        providing more simple and at the same time high-performance
        solution::

            ip.link("add",
                    ifname="mvlan0",
                    kind="macvlan",
                    link=ip.link_lookup(ifname="em1")[0],
                    macvlan_mode="private").commit()

        Several macvlan modes are available: "private", "vepa", "bridge",
        "passthru". Usually the default is "vepa".

        ► macvtap

        Almost the same as macvlan, but creates also a character tap device::

            ip.link("add",
                    ifname="mvtap0",
                    kind="macvtap",
                    link=ip.link_lookup(ifname="em1")[0],
                    macvtap_mode="vepa").commit()

        Will create a device file `"/dev/tap%s" % index`

        ► tuntap

        Possible `tuntap` keywords:

        * `mode` — "tun" or "tap"
        * `uid` — integer
        * `gid` — integer
        * `ifr` — dict of tuntap flags (see ifinfmsg:... tuntap_data)

        Create a tap interface::

            ip.link("add",
                    ifname="tap0",
                    kind="tuntap",
                    mode="tap")

        Tun/tap interfaces are created using `ioctl()`, but the library
        provides a transparent way to manage them using netlink API.

        ► veth

        To properly create `veth` interface, one should specify
        `peer` also, since `veth` interfaces are created in pairs::

            # simple call
            ip.link("add", ifname="v1p0", kind="veth", peer="v1p1")

            # set up specific veth peer attributes
            ip.link("add",
                    ifname="v1p0",
                    kind="veth",
                    peer={"ifname": "v1p1",
                          "net_ns_fd": "test_netns"})

        ► vlan

        VLAN interfaces require additional parameters, `vlan_id` and
        `link`, where `link` is a master interface to create VLAN on::

            ip.link("add",
                    ifname="v100",
                    kind="vlan",
                    link=ip.link_lookup(ifname="eth0")[0],
                    vlan_id=100)

        There is a possibility to create also 802.1ad interfaces::

            # create external vlan 802.1ad, s-tag
            ip.link("add",
                    ifname="v100s",
                    kind="vlan",
                    link=ip.link_lookup(ifname="eth0")[0],
                    vlan_id=100,
                    vlan_protocol=0x88a8)

            # create internal vlan 802.1q, c-tag
            ip.link("add",
                    ifname="v200c",
                    kind="vlan",
                    link=ip.link_lookup(ifname="v100s")[0],
                    vlan_id=200,
                    vlan_protocol=0x8100)


        ► vrf

        VRF interfaces (see linux/Documentation/networking/vrf.txt)::

            ip.link("add",
                    ifname="vrf-foo",
                    kind="vrf",
                    vrf_table=42)

        ► vxlan

        VXLAN interfaces are like VLAN ones, but require a bit more
        parameters::

            ip.link("add",
                    ifname="vx101",
                    kind="vxlan",
                    vxlan_link=ip.link_lookup(ifname="eth0")[0],
                    vxlan_id=101,
                    vxlan_group='239.1.1.1',
                    vxlan_ttl=16)

        All possible vxlan parameters are listed in the module
        `pyroute2.netlink.rtnl.ifinfmsg:... vxlan_data`.

        ► ipoib

        IPoIB driver provides an ability to create several ip interfaces
        on one interface.
        IPoIB interfaces requires the following parameter:

        `link` : The master interface to create IPoIB on.

        The following parameters can also be provided:

        * `pkey`- Inifiniband partition key the ip interface is associated with
        * `mode`- Underlying infiniband transport mode. One
          of:  ['datagram' ,'connected']
        * `umcast`- If set(1), multicast group membership for this interface is
          handled by user space.

        Example::

            ip.link("add",
                    ifname="ipoib1",
                    kind="ipoib",
                    link=ip.link_lookup(ifname="ib0")[0],
                    pkey=10)

        **set**

        Set interface attributes::

            # get interface index
            x = ip.link_lookup(ifname="eth0")[0]
            # put link down
            ip.link("set", index=x, state="down")
            # rename and set MAC addr
            ip.link("set", index=x, address="00:11:22:33:44:55", name="bala")
            # set MTU and TX queue length
            ip.link("set", index=x, mtu=1000, txqlen=2000)
            # bring link up
            ip.link("set", index=x, state="up")

        Setting bridge or tunnel attributes require `kind` to be
        specified in order to properly encode `IFLA_LINKINFO`::

            ip.link("set",
                    index=x,
                    kind="bridge",
                    br_forward_delay=2000)

            ip.link("set",
                    index=x,
                    kind="gre",
                    gre_local="10.0.0.1",
                    gre_remote="10.1.0.103")

        Keyword "state" is reserved. State can be "up" or "down",
        it is a shortcut::

            state="up":   flags=1, change=1
            state="down": flags=0, change=1

        SR-IOV virtual function setup::

            # get PF index
            x = ip.link_lookup(ifname="eth0")[0]
            # setup macaddr
            ip.link("set",
                    index=x,                          # PF index
                    vf={"vf": 0,                      # VF index
                        "mac": "00:11:22:33:44:55"})  # address
            # setup vlan
            ip.link("set",
                    index=x,           # PF index
                    vf={"vf": 0,       # VF index
                        "vlan": 100})  # the simplest case
            # setup QinQ
            ip.link("set",
                    index=x,                           # PF index
                    vf={"vf": 0,                       # VF index
                        "vlan": [{"vlan": 100,         # vlan id
                                  "proto": 0x88a8},    # 802.1ad
                                 {"vlan": 200,         # vlan id
                                  "proto": 0x8100}]})  # 802.1q

        **update**

        Almost the same as `set`, except it uses different flags
        and message type. Mostly does the same, but in some cases
        differs. If you're not sure what to use, use `set`.

        **del**

        Destroy the interface::

            ip.link("del", index=ip.link_lookup(ifname="dummy0")[0])

        **dump**

        Dump info for all interfaces

        **get**

        Get specific interface info::

            ip.link("get", index=ip.link_lookup(ifname="br0")[0])

        Get extended attributes like SR-IOV setup::

            ip.link("get", index=3, ext_mask=1)
        '''
        command_map = {
            'set': (RTM_NEWLINK, 'req'),
            'update': (RTM_SETLINK, 'create'),
            'add': (RTM_NEWLINK, 'create'),
            'del': (RTM_DELLINK, 'req'),
            'property_add': (RTM_NEWLINKPROP, 'append'),
            'property_del': (RTM_DELLINKPROP, 'req'),
            'remove': (RTM_DELLINK, 'req'),
            'delete': (RTM_DELLINK, 'req'),
            'dump': (RTM_GETLINK, 'dump'),
            'get': (RTM_GETLINK, 'get'),
        }
        if isinstance(kwarg.get('match'), str):
            kwarg['match'] = {'ifname': kwarg['match']}
        if 'command_map' in kwarg:
            command_map = kwarg.pop('command_map')
        dump_filter, kwarg = get_dump_filter('link', command, kwarg)
        arguments = get_arguments_processor('link', command, kwarg)
        request = NetlinkRequest(
            self, ifinfmsg(), command, command_map, dump_filter, arguments
        )
        await request.send()
        if command == 'dump':
            return request.response()
        return [x async for x in request.response()]

    async def addr(self, command, **kwarg):
        '''
        Address operations

        * command -- add, delete, replace, dump
        * index -- device index
        * address -- IPv4 or IPv6 address
        * mask -- address mask
        * family -- socket.AF_INET for IPv4 or socket.AF_INET6 for IPv6
        * scope -- the address scope, see /etc/iproute2/rt_scopes
        * kwarg -- dictionary, any ifaddrmsg field or NLA

        Later the method signature will be changed to::

            def addr(self, command, match=None, **kwarg):
                # the method body

        So only keyword arguments (except of the command) will be accepted.
        The reason for this change is an unification of API.

        Example::

            idx = 62
            ip.addr('add', index=idx, address='10.0.0.1', mask=24)
            ip.addr('add', index=idx, address='10.0.0.2', mask=24)

        With more NLAs::

            # explicitly set broadcast address
            ip.addr('add', index=idx,
                    address='10.0.0.3',
                    broadcast='10.0.0.255',
                    prefixlen=24)

            # make the secondary address visible to ifconfig: add label
            ip.addr('add', index=idx,
                    address='10.0.0.4',
                    broadcast='10.0.0.255',
                    prefixlen=24,
                    label='eth0:1')

        Configure p2p address on an interface::

            ip.addr('add', index=idx,
                    address='10.1.1.2',
                    mask=24,
                    local='10.1.1.1')
        '''
        if command in ('get', 'set'):
            return
        if 'mask' in kwarg:
            warnings.warn(
                'usage of mask is deprecated, use prefixlen instead',
                DeprecationWarning,
            )
        command_map = {
            'add': (RTM_NEWADDR, 'create'),
            'del': (RTM_DELADDR, 'req'),
            'remove': (RTM_DELADDR, 'req'),
            'delete': (RTM_DELADDR, 'req'),
            'replace': (RTM_NEWADDR, 'replace'),
            'dump': (RTM_GETADDR, 'dump'),
        }
        # quirks in the filter: flags are supplied as NLA, not as a field
        dump_filter, kwarg = get_dump_filter('addr', command, kwarg)
        arguments = get_arguments_processor('addr', command, kwarg)
        request = NetlinkRequest(
            self,
            ifaddrmsg(),
            command,
            command_map,
            dump_filter,
            arguments,
            terminate=lambda x: x['header']['type'] == NLMSG_ERROR,
        )
        await request.send()
        if command == 'dump':
            return request.response()
        return [x async for x in request.response()]

    async def tc(self, command, kind=None, index=None, handle=None, **kwarg):
        '''
        "Swiss knife" for traffic control. With the method you can
        dump, add, delete or modify qdiscs, classes and filters.

        * command -- add or delete qdisc, class, filter.
        * kind -- a string identifier -- "sfq", "htb", "u32" and so on.
        * handle -- integer or string

        Command can be one of ("add", "del", "add-class", "del-class",
        "add-filter", "del-filter") (see `commands` dict in the code).

        Handle notice: traditional iproute2 notation, like "1:0", actually
        represents two parts in one four-bytes integer::

            1:0    ->    0x10000
            1:1    ->    0x10001
            ff:0   ->   0xff0000
            ffff:1 -> 0xffff0001

        Target notice: if your target is a class/qdisc that applies an
        algorithm that can only apply to upstream traffic profile, but your
        keys variable explicitly references a match that is only relevant for
        upstream traffic, the kernel will reject the filter.  Unless you're
        dealing with devices like IMQs

        For pyroute2 tc() you can use both forms: integer like 0xffff0000
        or string like 'ffff:0000'. By default, handle is 0, so you can add
        simple classless queues w/o need to specify handle. Ingress queue
        causes handle to be 0xffff0000.

        So, to set up sfq queue on interface 1, the function call
        will be like that::

            ip = IPRoute()
            ip.tc("add", "sfq", 1)

        Instead of string commands ("add", "del"...), you can use also
        module constants, `RTM_NEWQDISC`, `RTM_DELQDISC` and so on::

            ip = IPRoute()
            flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL
            ip.tc((RTM_NEWQDISC, flags), "sfq", 1)

        It should be noted that "change", "change-class" and
        "change-filter" work like "replace", "replace-class" and
        "replace-filter", except they will fail if the node doesn't
        exist (while it would have been created by "replace"). This is
        not the same behaviour as with "tc" where "change" can be used
        to modify the value of some options while leaving the others
        unchanged. However, as not all entities support this
        operation, we believe the "change" commands as implemented
        here are more useful.


        Also available "modules" (returns tc plugins dict) and "help"
        commands::

            help(ip.tc("modules")["htb"])
            print(ip.tc("help", "htb"))
        '''
        if command == 'set':
            return

        if command == 'modules':
            return tc_plugins

        if command == 'help':
            p = tc_plugins.get(kind)
            if p is not None and hasattr(p, '__doc__'):
                return p.__doc__
            else:
                return 'No help available'

        command_map = {
            'dump': (RTM_GETQDISC, 'dump'),
            'get': (RTM_GETQDISC, 'req'),
            'add': (RTM_NEWQDISC, 'create'),
            'del': (RTM_DELQDISC, 'req'),
            'remove': (RTM_DELQDISC, 'req'),
            'delete': (RTM_DELQDISC, 'req'),
            'change': (RTM_NEWQDISC, 'change'),
            'replace': (RTM_NEWQDISC, 'replace'),
            'dump-class': (RTM_GETTCLASS, 'dump'),
            'get-class': (RTM_GETTCLASS, 'dump'),
            'add-class': (RTM_NEWTCLASS, 'create'),
            'del-class': (RTM_DELTCLASS, 'req'),
            'change-class': (RTM_NEWTCLASS, 'change'),
            'replace-class': (RTM_NEWTCLASS, 'replace'),
            'dump-filter': (RTM_GETTFILTER, 'dump'),
            'get-filter': (RTM_GETTFILTER, 'dump'),
            'add-filter': (RTM_NEWTFILTER, 'create'),
            'del-filter': (RTM_DELTFILTER, 'req'),
            'change-filter': (RTM_NEWTFILTER, 'change'),
            'replace-filter': (RTM_NEWTFILTER, 'replace'),
        }

        if command[:3] in ('add', 'cha'):
            if kind is None:
                raise ValueError('must specify kind for add/change commands')
        if kind is not None:
            kwarg['kind'] = kind
        # 8<-----------------------------------------------
        # compatibility section, to be cleaned up?
        if index is not None:
            kwarg['index'] = index
        if handle is not None:
            kwarg['handle'] = handle
        # 8<-----------------------------------------------
        dump_filter, kwarg = get_dump_filter('tc', command, kwarg)
        arguments = get_arguments_processor('tc', command, kwarg)

        request = NetlinkRequest(
            self, tcmsg(), command, command_map, dump_filter, arguments
        )
        await request.send()
        if command.startswith('dump'):
            return request.response()
        return [x async for x in request.response()]

    async def route(self, command, **kwarg):
        '''
        Route operations.

        Keywords to set up rtmsg fields:

        * dst_len, src_len -- destination and source mask(see `dst` below)
        * tos -- type of service
        * table -- routing table
        * proto -- `redirect`, `boot`, `static` (see `rt_proto`)
        * scope -- routing realm
        * type -- `unicast`, `local`, etc. (see `rt_type`)

        `pyroute2/netlink/rtnl/rtmsg.py` rtmsg.nla_map:

        * table -- routing table to use (default: 254)
        * gateway -- via address
        * prefsrc -- preferred source IP address
        * dst -- the same as `prefix`
        * iif -- incoming traffic interface
        * oif -- outgoing traffic interface

        etc.

        One can specify mask not as `dst_len`, but as a part of `dst`,
        e.g.: `dst="10.0.0.0/24"`.

        Commands:

        **add**

        Example::

            ipr.route("add", dst="10.0.0.0/24", gateway="192.168.0.1")

        ...

        More `route()` examples. Blackhole route::

            ipr.route(
                "add",
                dst="10.0.0.0/24",
                type="blackhole",
            )

        Create a route with metrics::

            ipr.route(
                "add",
                dst="172.16.0.0/24",
                gateway="10.0.0.10",
                metrics={
                    "mtu": 1400,
                    "hoplimit": 16,
                },
            )

        Multipath route::

            ipr.route(
                "add",
                dst="10.0.0.0/24",
                multipath=[
                    {"gateway": "192.168.0.1", "hops": 2},
                    {"gateway": "192.168.0.2", "hops": 1},
                    {"gateway": "192.168.0.3"},
                ],
            )

        MPLS lwtunnel on eth0::

            ipr.route(
                "add",
                dst="10.0.0.0/24",
                oif=ipr.link_lookup(ifname="eth0"),
                encap={
                    "type": "mpls",
                    "labels": "200/300",
                },
            )

        IPv6 next hop for IPv4 dst::

            ipr.route(
                "add",
                prefsrc="10.127.30.4",
                dst="172.16.0.0/24",
                via={"family": AF_INET6, "addr": "fe80::1337"},
                oif=ipr.link_lookup(ifname="eth0"),
                table=100,
            )

        Create MPLS route: push label::

            # $ sudo modprobe mpls_router
            # $ sudo sysctl net.mpls.platform_labels=1024
            ipr.route(
                "add",
                family=AF_MPLS,
                oif=ipr.link_lookup(ifname="eth0"),
                dst=0x200,
                newdst=[0x200, 0x300],
            )

        MPLS multipath::

            ipr.route(
                "add",
                dst="10.0.0.0/24",
                table=20,
                multipath=[
                    {
                        "gateway": "192.168.0.1",
                        "encap": {"type": "mpls", "labels": 200},
                    },
                    {
                        "ifindex": ipr.link_lookup(ifname="eth0"),
                        "encap": {"type": "mpls", "labels": 300},
                    },
                ],
            )

        MPLS target can be int, string, dict or list::

            "labels": 300    # simple label
            "labels": "300"  # the same
            "labels": (200, 300)  # stacked
            "labels": "200/300"   # the same

            # explicit label definition
            "labels": {
                "bos": 1,
                "label": 300,
                "tc": 0,
                "ttl": 16,
            }

        Create SEG6 tunnel encap mode (kernel >= 4.10)::

            ipr.route(
                "add",
                dst="2001:0:0:10::2/128",
                oif=idx,
                encap={
                    "type": "seg6",
                    "mode": "encap",
                    "segs": "2000::5,2000::6",
                },
            )

        Create SEG6 tunnel inline mode (kernel >= 4.10)::

            ipr.route(
                "add",
                dst="2001:0:0:10::2/128",
                oif=idx,
                encap={
                    "type": "seg6",
                    "mode": "inline",
                    "segs": ["2000::5", "2000::6"],
                },
            )

        Create SEG6 tunnel inline mode with hmac (kernel >= 4.10)::

            ipr.route(
                "add",
                dst="2001:0:0:22::2/128",
                oif=idx,
                encap={
                    "type": "seg6",
                    "mode": "inline",
                    "segs": "2000::5,2000::6,2000::7,2000::8",
                    "hmac": 0xf,
                },
            )

        Create SEG6 tunnel with ip4ip6 encapsulation (kernel >= 4.14)::

            ipr.route(
                "add",
                dst="172.16.0.0/24",
                oif=idx,
                encap={
                    "type": "seg6",
                    "mode": "encap",
                    "segs": "2000::5,2000::6",
                },
            )

        Create SEG6LOCAL tunnel End.DX4 action (kernel >= 4.14)::

            ipr.route(
                "add",
                dst="2001:0:0:10::2/128",
                oif=idx,
                encap={
                    "type": "seg6local",
                    "action": "End.DX4",
                    "nh4": "172.16.0.10",
                },
            )

        Create SEG6LOCAL tunnel End.DT6 action (kernel >= 4.14)::

            ipr.route(
                "add",
                dst="2001:0:0:10::2/128",
                oif=idx,
                encap={
                    "type": "seg6local",
                    "action": "End.DT6",
                    "table": "10",
                },
            )

        Create SEG6LOCAL tunnel End.DT4 action (kernel >= 5.11)::

            # $ sudo modprobe vrf
            # $ sudo sysctl -w net.vrf.strict_mode=1
            ipr.link(
                "add",
                ifname="vrf-foo",
                kind="vrf",
                vrf_table=10,
            )
            ipr.route(
                "add",
                dst="2001:0:0:10::2/128",
                oif=idx,
                encap={
                    "type": "seg6local",
                    "action": "End.DT4",
                    "vrf_table": 10,
                },
            )

        Create SEG6LOCAL tunnel End.DT46 action (kernel >= 5.14)::

            # $ sudo modprobe vrf
            # $ sudo sysctl -w net.vrf.strict_mode=1

            ip.link('add',
                    ifname='vrf-foo',
                    kind='vrf',
                    vrf_table=10)

            ip.route('add',
                     dst='2001:0:0:10::2/128',
                     oif=idx,
                     encap={'type': 'seg6local',
                            'action': 'End.DT46',
                            'vrf_table': 10})

        Create SEG6LOCAL tunnel End.B6 action (kernel >= 4.14)::

            ipr.route(
                "add",
                dst="2001:0:0:10::2/128",
                oif=idx,
                encap={
                    "type": "seg6local",
                    "action": "End.B6",
                    "srh": {"segs": "2000::5,2000::6"},
                },
            )

        Create SEG6LOCAL tunnel End.B6 action with hmac (kernel >= 4.14)::

            ipr.route(
                "add",
                dst="2001:0:0:10::2/128",
                oif=idx,
                encap={
                    "type": "seg6local",
                    "action": "End.B6",
                    "srh": {
                        "segs": "2000::5,2000::6",
                        "hmac": 0xf,
                    },
                },
            )

        **change**, **replace**, **append**

        Commands `change`, `replace` and `append` have the same meanings
        as in ip-route(8): `change` modifies only existing route, while
        `replace` creates a new one, if there is no such route yet.
        `append` allows to create an IPv6 multipath route.

        **del**

        Remove the route. The same syntax as for **add**.

        **get**

        Get route by spec.

        **dump**

        Dump all routes.
        '''
        # transform kwarg
        if command in ('add', 'set', 'replace', 'change', 'append'):
            kwarg['proto'] = kwarg.get('proto', 'static') or 'static'
            kwarg['type'] = kwarg.get('type', 'unicast') or 'unicast'

        command_map = {
            'add': (RTM_NEWROUTE, 'create'),
            'set': (RTM_NEWROUTE, 'replace'),
            'replace': (RTM_NEWROUTE, 'replace'),
            'change': (RTM_NEWROUTE, 'change'),
            'append': (RTM_NEWROUTE, 'append'),
            'del': (RTM_DELROUTE, 'req'),
            'remove': (RTM_DELROUTE, 'req'),
            'delete': (RTM_DELROUTE, 'req'),
            'get': (RTM_GETROUTE, 'get'),
            'show': (RTM_GETROUTE, 'dump'),
            'dump': (RTM_GETROUTE, 'dump'),
        }
        msg = rtmsg()
        parameters = {'strict_check': self.status['strict_check']}
        dump_filter, kwarg = get_dump_filter(
            'route', command, kwarg, parameters
        )

        arguments = get_arguments_processor(
            'route', command, kwarg, parameters
        )

        request = NetlinkRequest(
            self, msg, command, command_map, dump_filter, arguments
        )
        await request.send()
        if command in ('dump', 'show'):
            return request.response()
        return [x async for x in request.response()]

    async def rule(self, command, **kwarg):
        '''
        Rule operations

            - command — add, delete
            - table — 0 < table id < 253
            - priority — 0 < rule's priority < 32766
            - action — type of rule, default 'FR_ACT_NOP' (see fibmsg.py)
            - rtscope — routing scope, default RT_SCOPE_UNIVERSE
                `(RT_SCOPE_UNIVERSE|RT_SCOPE_SITE|\
                RT_SCOPE_LINK|RT_SCOPE_HOST|RT_SCOPE_NOWHERE)`
            - family — rule's family (socket.AF_INET (default) or
                socket.AF_INET6)
            - src — IP source for Source Based (Policy Based) routing's rule
            - dst — IP for Destination Based (Policy Based) routing's rule
            - src_len — Mask for Source Based (Policy Based) routing's rule
            - dst_len — Mask for Destination Based (Policy Based) routing's
                rule
            - iifname — Input interface for Interface Based (Policy Based)
                routing's rule
            - oifname — Output interface for Interface Based (Policy Based)
                routing's rule
            - uid_range — Range of user identifiers, a string like "1000:1234"
            - dport_range — Range of destination ports, a string like "80-120"
            - sport_range — Range of source ports, as a string like "80-120"

        All packets route via table 10::

            # 32000: from all lookup 10
            # ...
            ip.rule('add', table=10, priority=32000)

        Default action::

            # 32001: from all lookup 11 unreachable
            # ...
            iproute.rule('add',
                         table=11,
                         priority=32001,
                         action='FR_ACT_UNREACHABLE')

        Use source address to choose a routing table::

            # 32004: from 10.64.75.141 lookup 14
            # ...
            iproute.rule('add',
                         table=14,
                         priority=32004,
                         src='10.64.75.141')

        Use dst address to choose a routing table::

            # 32005: from 10.64.75.141/24 lookup 15
            # ...
            iproute.rule('add',
                         table=15,
                         priority=32005,
                         dst='10.64.75.141',
                         dst_len=24)

        Match fwmark::

            # 32006: from 10.64.75.141 fwmark 0xa lookup 15
            # ...
            iproute.rule('add',
                         table=15,
                         priority=32006,
                         dst='10.64.75.141',
                         fwmark=10)
        '''
        if command == 'set':
            return []

        command_map = {
            'add': (RTM_NEWRULE, 'create'),
            'del': (RTM_DELRULE, 'req'),
            'remove': (RTM_DELRULE, 'req'),
            'delete': (RTM_DELRULE, 'req'),
            'dump': (RTM_GETRULE, 'root'),
        }
        if isinstance(kwarg.get('match'), str):
            kwarg['match'] = {'ifname': kwarg['match']}

        msg = fibmsg()
        dump_filter, kwarg = get_dump_filter('rule', command, kwarg)
        arguments = get_arguments_processor('rule', command, kwarg)
        request = NetlinkRequest(
            self, msg, command, command_map, dump_filter, arguments
        )
        await request.send()
        if command == 'dump':
            return request.response()
        return [x async for x in request.response()]

    async def stats(self, command, **kwarg):
        '''
        Stats prototype.
        '''
        command_map = {
            'dump': (RTM_GETSTATS, 'dump'),
            'get': (RTM_GETSTATS, 'get'),
        }

        msg = ifstatsmsg()
        msg['filter_mask'] = kwarg.get('filter_mask', 31)
        msg['ifindex'] = kwarg.get('ifindex', 0)
        dump_filter, kwarg = get_dump_filter('stats', command, kwarg)
        request = NetlinkRequest(self, msg, command, command_map, dump_filter)
        await request.send()
        if command == 'dump':
            return request.response()
        return [x async for x in request.response()]

    # 8<---------------------------------------------------------------


class AsyncIPRoute(AsyncIPRSocket, RTNL_API):
    '''
    Regular ordinary async utility class, provides RTNL API using
    AsyncIPRSocket as the transport level.

    .. warning::
        The project core is currently undergoing refactoring, so
        some methods may still use the old synchronous API. This
        will be addressed in future updates.

    The main RTNL API class is built on an asyncio core. All methods
    that send netlink requests are asynchronous and return awaitables.
    Dump requests return asynchronous generators, while other requests
    return iterables, such as tuples or lists.

    This design choice addresses the fact that RTNL dumps, such as
    routes or neighbors, can return an extremely large number of objects.
    Buffering the entire response in memory could lead to performance
    issues.

    .. testcode::

        import asyncio

        from pyroute2 import AsyncIPRoute


        async def main():
            async with AsyncIPRoute() as ipr:
                # create a link: immediate evaluation
                await ipr.link("add", ifname="test0", kind="dummy")

                # dump links: lazy evaluation
                async for link in await ipr.link("dump"):
                    print(link.get("ifname"))

        asyncio.run(main())

    .. testoutput::

        lo
        eth0
        test0
    '''

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.close()


class IPRoute(NetlinkSocket):
    '''
    A synchronous version of AsyncIPRoute. All the same API, but
    sync. Provides a legacy API for the old code that is not using
    asyncio.

    This API is designed to be compatible with the old synchronous `IPRoute`
    from version 0.8.x and earlier:

    .. testcode::

        from pyroute2 import IPRoute

        with IPRoute() as ipr:
            for msg in ipr.addr("dump"):
                addr = msg.get("address")
                mask = msg.get("prefixlen")
                print(f"{addr}/{mask}")

    .. testoutput::

        127.0.0.1/8
        192.168.122.28/24

    .. testcode::

        from pyroute2 import IPRoute

        with IPRoute() as ipr:

            # this request returns one match, one interface index
            eth0 = ipr.link_lookup(ifname="eth0")
            assert len(eth0) == 1  # 1 if exists else 0

            # this requests uses a lambda to filter interfaces
            # and returns all interfaces that are up
            nics_up = set(ipr.link_lookup(lambda x: x.get("flags") & 1))
            assert len(nics_up) == 2
            assert nics_up == {1, 2}
    '''

    def __init__(
        self,
        port=None,
        pid=None,
        fileno=None,
        sndbuf=1048576,
        rcvbuf=1048576,
        rcvsize=16384,
        all_ns=False,
        async_qsize=None,
        nlm_generator=None,
        target='localhost',
        ext_ack=False,
        strict_check=False,
        groups=RTMGRP_DEFAULTS,
        nlm_echo=False,
        netns=None,
        flags=os.O_CREAT,
        libc=None,
        use_socket=None,
        use_event_loop=None,
        telemetry=None,
    ):
        self.asyncore = AsyncIPRoute(
            port=port,
            pid=pid,
            fileno=fileno,
            sndbuf=sndbuf,
            rcvbuf=rcvbuf,
            rcvsize=rcvsize,
            all_ns=all_ns,
            async_qsize=async_qsize,
            nlm_generator=nlm_generator,
            target=target,
            ext_ack=ext_ack,
            strict_check=strict_check,
            groups=groups,
            nlm_echo=nlm_echo,
            use_socket=use_socket,
            netns=netns,
            flags=flags,
            libc=libc,
            use_event_loop=use_event_loop,
            telemetry=telemetry,
        )
        self.asyncore.status['event_loop'] = (
            'new' if use_event_loop is None else 'auto'
        )
        self.asyncore.local.keep_event_loop = True
        self.asyncore.event_loop.run_until_complete(
            self.asyncore.setup_endpoint()
        )
        if self.asyncore.socket.fileno() == -1:
            raise OSError(9, 'Bad file descriptor')

    @classmethod
    def from_asyncore(cls, iproute):
        ret = cls()
        ret.asyncore = iproute
        return ret

    def ensure(self, method, present=True, timeout=10, interval=0.2, **spec):
        # method points to the sync API, and is a partial() wrapper
        # extract async method from the wrapper's arguments
        method = method.args[0]
        return self._run_with_cleanup(
            self.asyncore.ensure, method, present, timeout, interval, **spec
        )

    def poll(self, method, command, timeout=10, interval=0.2, **spec):
        ctime = get_time()
        ret = tuple()
        while ctime + timeout > get_time():
            try:
                ret = [x for x in method(command, **spec)]
                if ret:
                    return ret
                time.sleep(interval)
            except NetlinkDumpInterrupted:
                pass
        raise TimeoutError()

    def _run_force_sync(self, func, *argv, **kwarg):
        return tuple(self._generate_with_cleanup(func, *argv, **kwarg))

    def _run_generic_rtnl(self, func, *argv, **kwarg):
        if len(argv) and argv[0] in ('dump', 'show'):
            if not config.nlm_generator:
                return tuple(self._generate_with_cleanup(func, *argv, **kwarg))
            return self._generate_with_cleanup(func, *argv, **kwarg)
        return self._run_with_cleanup(func, *argv, **kwarg)

    def __getattr__(self, name):
        generic_methods = set(
            (
                'addr',
                'link',
                'neigh',
                'route',
                'rule',
                'tc',
                'fdb',
                'brport',
                'probe',
                'stats',
                'link_lookup',
                'vlan_filter',
                'flush_addr',
                'flush_rules',
                'flush_routes',
                'get_netnsid',
                'route_dump',
                'route_dumps',
                'route_load',
                'route_loads',
            )
        )
        sync_methods = set(
            (
                'list_link_kind',
                'unregister_link_kind',
                'register_link_kind',
                'get_pid',
                'close_file',
                'open_file',
                'filter_messages',
                'set_netnsid',
            )
        )

        symbol = getattr(self.asyncore, name)
        if name in set(RTNL_API.__dict__.keys()) - sync_methods:
            if name in generic_methods:
                return partial(self._run_generic_rtnl, symbol)
            if not config.nlm_generator:
                return partial(self._run_force_sync, symbol)
            return partial(self._generate_with_cleanup, symbol)
        return symbol


class IPBatch(IPRoute):
    '''
    Netlink requests compiler. Does not send any requests, but
    instead stores them in the internal binary buffer. The
    contents of the buffer can be used to send batch requests,
    to test custom netlink parsers and so on.

    Uses `RTNL_API` and provides all the same API as normal
    `IPRoute` objects::

        # create the batch compiler
        ipb = IPBatch()
        # compile requests into the internal buffer
        ipb.link("add", index=550, ifname="test", kind="dummy")
        ipb.link("set", index=550, state="up")
        ipb.addr("add", index=550, address="10.0.0.2", mask=24)
        # save the buffer
        data = ipb.batch
        # reset the buffer
        ipb.reset()
        ...
        # send the buffer
        IPRoute().sendto(data, (0, 0))

    '''

    def __init__(self):
        super().__init__()
        self.reset()

    def reset(self):
        self.asyncore.batch = bytearray()


class RawIPRoute(IPRoute):
    def __init__(self):
        super().__init__()
        self.asyncore.request_proxy = None


class NetNS(IPRoute):
    '''
    The `NetNS` class, prior to version 0.9.1, was used to run the RTNL API
    in a network namespace. Starting with pyroute2 version 0.9.1, the network
    namespace functionality has been integrated into the library core. To run
    an `IPRoute` or `AsyncIPRoute` instance in a network namespace, simply use
    the `netns` argument:

    .. testcode::

        from pyroute2 import IPRoute

        with IPRoute(netns="test") as ipr:
            assert ipr.status["netns"] == "test"

    After initialization, the netns name is available as `.status["netns"]`.

    The old synchronous `NetNS` class is still available for compatibility
    but now serves as a wrapper around `IPRoute`.

    .. testcode::

        from pyroute2 import NetNS

        with NetNS("test") as ns:
            assert ns.status["netns"] == "test"
    '''

    def __init__(
        self,
        netns=None,
        flags=os.O_CREAT,
        target='localhost',
        libc=None,
        groups=RTMGRP_DEFAULTS,
    ):
        super().__init__(
            target=target, netns=netns, flags=flags, libc=libc, groups=groups
        )

    def remove(self):
        self.close()
        netns.remove(self.status['netns'])


class ChaoticIPRoute(RTNL_API, ChaoticIPRSocket):
    '''
    IPRoute interface for chaotic tests - raising exceptions randomly.
    '''

    pass
