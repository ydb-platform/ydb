# -*- coding: utf-8 -*-
import os
import types
import logging
from socket import AF_INET
from socket import AF_INET6
from socket import AF_UNSPEC
from functools import partial
from pyroute2 import config
from pyroute2.config import AF_BRIDGE
from pyroute2.netlink import NLMSG_ERROR
from pyroute2.netlink import NLM_F_ATOMIC
from pyroute2.netlink import NLM_F_ROOT
from pyroute2.netlink import NLM_F_REPLACE
from pyroute2.netlink import NLM_F_REQUEST
from pyroute2.netlink import NLM_F_ACK
from pyroute2.netlink import NLM_F_DUMP
from pyroute2.netlink import NLM_F_CREATE
from pyroute2.netlink import NLM_F_EXCL
from pyroute2.netlink import NLM_F_APPEND
from pyroute2.netlink.rtnl import RTM_NEWADDR
from pyroute2.netlink.rtnl import RTM_GETADDR
from pyroute2.netlink.rtnl import RTM_DELADDR
from pyroute2.netlink.rtnl import RTM_NEWLINK
from pyroute2.netlink.rtnl import RTM_NEWLINKPROP
from pyroute2.netlink.rtnl import RTM_DELLINKPROP
from pyroute2.netlink.rtnl import RTM_GETLINK
from pyroute2.netlink.rtnl import RTM_DELLINK
from pyroute2.netlink.rtnl import RTM_NEWQDISC
from pyroute2.netlink.rtnl import RTM_GETQDISC
from pyroute2.netlink.rtnl import RTM_DELQDISC
from pyroute2.netlink.rtnl import RTM_NEWTFILTER
from pyroute2.netlink.rtnl import RTM_GETTFILTER
from pyroute2.netlink.rtnl import RTM_DELTFILTER
from pyroute2.netlink.rtnl import RTM_NEWTCLASS
from pyroute2.netlink.rtnl import RTM_GETTCLASS
from pyroute2.netlink.rtnl import RTM_DELTCLASS
from pyroute2.netlink.rtnl import RTM_NEWRULE
from pyroute2.netlink.rtnl import RTM_GETRULE
from pyroute2.netlink.rtnl import RTM_DELRULE
from pyroute2.netlink.rtnl import RTM_NEWROUTE
from pyroute2.netlink.rtnl import RTM_GETROUTE
from pyroute2.netlink.rtnl import RTM_DELROUTE
from pyroute2.netlink.rtnl import RTM_NEWNEIGH
from pyroute2.netlink.rtnl import RTM_GETNEIGH
from pyroute2.netlink.rtnl import RTM_DELNEIGH
from pyroute2.netlink.rtnl import RTM_SETLINK
from pyroute2.netlink.rtnl import RTM_GETNEIGHTBL
from pyroute2.netlink.rtnl import RTM_GETNSID
from pyroute2.netlink.rtnl import RTM_NEWNETNS
from pyroute2.netlink.rtnl import RTM_GETSTATS
from pyroute2.netlink.rtnl import TC_H_ROOT
from pyroute2.netlink.rtnl import rt_type
from pyroute2.netlink.rtnl import rt_scope
from pyroute2.netlink.rtnl import rt_proto
from pyroute2.netlink.rtnl.req import IPLinkRequest
from pyroute2.netlink.rtnl.req import IPBridgeRequest
from pyroute2.netlink.rtnl.req import IPBrPortRequest
from pyroute2.netlink.rtnl.req import IPRouteRequest
from pyroute2.netlink.rtnl.req import IPRuleRequest
from pyroute2.netlink.rtnl.req import IPAddrRequest
from pyroute2.netlink.rtnl.tcmsg import plugins as tc_plugins
from pyroute2.netlink.rtnl.tcmsg import tcmsg
from pyroute2.netlink.rtnl.rtmsg import rtmsg
from pyroute2.netlink.rtnl import ndmsg
from pyroute2.netlink.rtnl.ndtmsg import ndtmsg
from pyroute2.netlink.rtnl.fibmsg import fibmsg
from pyroute2.netlink.rtnl.ifinfmsg import ifinfmsg
from pyroute2.netlink.rtnl.ifinfmsg import IFF_NOARP
from pyroute2.netlink.rtnl.ifaddrmsg import ifaddrmsg
from pyroute2.netlink.rtnl.ifstatsmsg import ifstatsmsg
from pyroute2.netlink.rtnl.iprsocket import IPRSocket
from pyroute2.netlink.rtnl.iprsocket import IPBatchSocket
from pyroute2.netlink.rtnl.riprsocket import RawIPRSocket
from pyroute2.netlink.rtnl.nsidmsg import nsidmsg
from pyroute2.netlink.rtnl.nsinfmsg import nsinfmsg
from pyroute2.netlink.exceptions import SkipInode
from pyroute2.netlink.exceptions import NetlinkError

from pyroute2.common import AF_MPLS
from pyroute2.common import basestring
from pyroute2.common import getbroadcast

DEFAULT_TABLE = 254
log = logging.getLogger(__name__)


def transform_handle(handle):
    if isinstance(handle, basestring):
        (major, minor) = [int(x if x else '0', 16) for x in handle.split(':')]
        handle = (major << 8 * 2) | minor
    return handle


class RTNL_API(object):
    '''
    `RTNL_API` should not be instantiated by itself. It is intended
    to be used as a mixin class. Following classes use `RTNL_API`:

    * `IPRoute` -- RTNL API to the current network namespace
    * `NetNS` -- RTNL API to another network namespace
    * `IPBatch` -- RTNL compiler
    * `ShellIPR` -- RTNL via standard I/O, runs IPRoute in a shell

    It is an old-school API, that provides access to rtnetlink as is.
    It helps you to retrieve and change almost all the data, available
    through rtnetlink::

        from pyroute2 import IPRoute
        ipr = IPRoute()
        # create an interface
        ipr.link('add', ifname='brx', kind='bridge')
        # lookup the index
        dev = ipr.link_lookup(ifname='brx')[0]
        # bring it down
        ipr.link('set', index=dev, state='down')
        # change the interface MAC address and rename it just for fun
        ipr.link('set', index=dev,
                 address='00:11:22:33:44:55',
                 ifname='br-ctrl')
        # add primary IP address
        ipr.addr('add', index=dev,
                 address='10.0.0.1', mask=24,
                 broadcast='10.0.0.255')
        # add secondary IP address
        ipr.addr('add', index=dev,
                 address='10.0.0.2', mask=24,
                 broadcast='10.0.0.255')
        # bring it up
        ipr.link('set', index=dev, state='up')
    '''
    def __init__(self, *argv, **kwarg):
        if 'netns_path' in kwarg:
            self.netns_path = kwarg['netns_path']
        else:
            self.netns_path = config.netns_path
        super(RTNL_API, self).__init__(*argv, **kwarg)
        if not self.nlm_generator:

            def _match(*argv, **kwarg):
                return tuple(self._genmatch(*argv, **kwarg))

            self._genmatch = self._match
            self._match = _match

    def _match(self, match, msgs):
        # filtered results, the generator version
        for msg in msgs:
            if hasattr(match, '__call__'):
                if match(msg):
                    yield msg
            elif isinstance(match, dict):
                matches = []
                for key in match:
                    KEY = msg.name2nla(key)
                    if isinstance(match[key], types.FunctionType):
                        if msg.get(key) is not None:
                            matches.append(match[key](msg.get(key)))
                        elif msg.get_attr(KEY) is not None:
                            matches.append(match[key](msg.get_attr(KEY)))
                        else:
                            matches.append(False)
                    else:
                        matches.append(msg.get(key) == match[key] or
                                       msg.get_attr(KEY) ==
                                       match[key])
                if all(matches):
                    yield msg

    # 8<---------------------------------------------------------------
    #
    def dump(self):
        '''
        Iterate all the objects -- links, routes, addresses etc.
        '''
        ##
        # Well, it's the Linux API, why OpenBSD / FreeBSD here?
        #
        # 'Cause when you run RemoteIPRoute, it uses this class,
        # and the code may be run on BSD systems as well, though
        # BSD systems have only subset of the API
        #
        if self.uname[0] == 'OpenBSD':
            methods = (self.get_links,
                       self.get_addr,
                       self.get_neighbours,
                       self.get_routes)
        else:
            methods = (self.get_links,
                       self.get_addr,
                       self.get_neighbours,
                       self.get_routes,
                       self.get_vlans,
                       partial(self.fdb, 'dump'),
                       partial(self.get_rules, family=AF_INET),
                       partial(self.get_rules, family=AF_INET6))
        for method in methods:
            for msg in method():
                yield msg

    # 8<---------------------------------------------------------------
    #
    # Listing methods
    #
    def get_qdiscs(self, index=None):
        '''
        Get all queue disciplines for all interfaces or for specified
        one.
        '''
        msg = tcmsg()
        msg['family'] = AF_UNSPEC
        ret = self.nlm_request(msg, RTM_GETQDISC)
        if index is None:
            return ret
        else:
            return [x for x in ret if x['index'] == index]

    def get_filters(self, index=0, handle=0, parent=0):
        '''
        Get filters for specified interface, handle and parent.
        '''
        msg = tcmsg()
        msg['family'] = AF_UNSPEC
        msg['index'] = index
        msg['handle'] = handle
        msg['parent'] = parent
        return self.nlm_request(msg, RTM_GETTFILTER)

    def get_classes(self, index=0):
        '''
        Get classes for specified interface.
        '''
        msg = tcmsg()
        msg['family'] = AF_UNSPEC
        msg['index'] = index
        return self.nlm_request(msg, RTM_GETTCLASS)

    def get_vlans(self, **kwarg):
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
        match = kwarg.get('match', None) or kwarg or None
        return self.link('dump',
                         family=AF_BRIDGE,
                         ext_mask=2,
                         match=match)

    def get_links(self, *argv, **kwarg):
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
        result = []
        links = argv or [0]
        if links[0] == 'all':  # compat syntax
            links = [0]

        if links[0] == 0:
            cmd = 'dump'
        else:
            cmd = 'get'

        for index in links:
            kwarg['index'] = index
            result.extend(self.link(cmd, **kwarg))
        return result

    def get_neighbours(self, family=AF_UNSPEC, match=None, **kwarg):
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
        return self.neigh('dump', family=family, match=match or kwarg)

    def get_ntables(self, family=AF_UNSPEC):
        '''
        Get neighbour tables
        '''
        msg = ndtmsg()
        msg['family'] = family
        return self.nlm_request(msg, RTM_GETNEIGHTBL)

    def get_addr(self, family=AF_UNSPEC, match=None, **kwarg):
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
        return self.addr('dump', family=family, match=match or kwarg)

    def get_rules(self, family=AF_UNSPEC, match=None, **kwarg):
        '''
        Get all rules. By default return all rules. To explicitly
        request the IPv4 rules use `family=AF_INET`.

        Example::
            ip.get_rules() # get all the rules for all families
            ip.get_rules(family=AF_INET6)  # get only IPv6 rules
        '''
        return self.rule((RTM_GETRULE,
                          NLM_F_REQUEST | NLM_F_ROOT | NLM_F_ATOMIC),
                         family=family,
                         match=match or kwarg)

    def get_routes(self, family=255, match=None, **kwarg):
        '''
        Get all routes. You can specify the table. There
        are 255 routing classes (tables), and the kernel
        returns all the routes on each request. So the
        routine filters routes from full output.

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
        if isinstance(kwarg.get('dst'), basestring):
            return self.route('get', dst=kwarg['dst'])
        else:
            return self.route('dump',
                              family=family,
                              match=match or kwarg)
    # 8<---------------------------------------------------------------

    # 8<---------------------------------------------------------------
    #
    # List NetNS info
    #
    def _dump_one_ns(self, path, registry):
        item = nsinfmsg()
        item['netnsid'] = 0xffffffff  # default netnsid "unknown"
        nsfd = 0
        info = nsidmsg()
        msg = nsidmsg()
        try:
            nsfd = os.open(path, os.O_RDONLY)
            item['inode'] = os.fstat(nsfd).st_ino
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
            try:
                for info in self.nlm_request(msg,
                                             RTM_GETNSID,
                                             NLM_F_REQUEST):
                    # response to nlm_request() is a list or a generator,
                    # that's why loop
                    item['netnsid'] = info.get_attr('NETNSA_NSID')
                    break
            except Exception:
                pass
            item['attrs'] = [('NSINFO_PATH', path)]
        except OSError:
            raise SkipInode()
        finally:
            if nsfd > 0:
                os.close(nsfd)
        item['header']['type'] = RTM_NEWNETNS
        item['header']['target'] = self.target
        item['event'] = 'RTM_NEWNETNS'
        return item

    def _dump_dir(self, path, registry):
        for name in os.listdir(path):
            # strictly speaking, there is no need to use os.sep,
            # since the code is not portable outside of Linux
            nspath = '%s%s%s' % (path, os.sep, name)
            try:
                yield self._dump_one_ns(nspath, registry)
            except SkipInode:
                pass

    def _dump_proc(self, registry):
        for name in os.listdir('/proc'):
            try:
                int(name)
            except ValueError:
                continue

            try:
                yield self._dump_one_ns('/proc/%s/ns/net' % name, registry)
            except SkipInode:
                pass

    def get_netns_info(self, list_proc=False):
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
        for peer in self.get_links():
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
        for path in self.netns_path:
            views.append(self._dump_dir(path, registry))
        if list_proc:
            views.append(self._dump_proc(registry))
        #
        # iterate all the items
        #
        for view in views:
            try:
                for item in view:
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
    # 8<---------------------------------------------------------------

    # 8<---------------------------------------------------------------
    #
    # Shortcuts
    #
    def get_default_routes(self, family=AF_UNSPEC, table=DEFAULT_TABLE):
        '''
        Get default routes
        '''
        # according to iproute2/ip/iproute.c:print_route()
        return [x for x in self.get_routes(family, table=table)
                if (x.get_attr('RTA_DST', None) is None and
                    x['dst_len'] == 0)]

    def link_lookup(self, **kwarg):
        '''
        Lookup interface index (indeces) by first level NLA
        value.

        Example::

            ip.link_lookup(address="52:54:00:9d:4e:3d")
            ip.link_lookup(ifname="lo")
            ip.link_lookup(operstate="UP")

        Please note, that link_lookup() returns list, not one
        value.
        '''
        if set(kwarg) in ({'index', }, {'ifname', }, {'index', 'ifname'}):
            # shortcut for index and ifname
            try:
                for link in self.link('get', **kwarg):
                    return [link['index']]
            except NetlinkError:
                return []
        else:
            # otherwise fallback to the userspace filter
            return [link['index'] for link in self.get_links(match=kwarg)]
    # 8<---------------------------------------------------------------

    # 8<---------------------------------------------------------------
    #
    # Shortcuts to flush RTNL objects
    #
    def flush_routes(self, *argv, **kwarg):
        '''
        Flush routes -- purge route records from a table.
        Arguments are the same as for `get_routes()`
        routine. Actually, this routine implements a pipe from
        `get_routes()` to `nlm_request()`.
        '''
        ret = []
        for route in self.get_routes(*argv, **kwarg):
            self.put(route, msg_type=RTM_DELROUTE, msg_flags=NLM_F_REQUEST)
            ret.append(route)
        return ret

    def flush_addr(self, *argv, **kwarg):
        '''
        Flush IP addresses.

        Examples::

            # flush all addresses on the interface with index 2:
            ipr.flush_addr(index=2)

            # flush all addresses with IFA_LABEL='eth0':
            ipr.flush_addr(label='eth0')
        '''
        flags = NLM_F_CREATE | NLM_F_EXCL | NLM_F_REQUEST
        ret = []
        for addr in self.get_addr(*argv, **kwarg):
            self.put(addr, msg_type=RTM_DELADDR, msg_flags=flags)
            ret.append(addr)
        return ret

    def flush_rules(self, *argv, **kwarg):
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
        flags = NLM_F_CREATE | NLM_F_EXCL | NLM_F_REQUEST
        ret = []
        for rule in self.get_rules(*argv, **kwarg):
            self.put(rule, msg_type=RTM_DELRULE, msg_flags=flags)
            ret.append(rule)
        return ret
    # 8<---------------------------------------------------------------

    # 8<---------------------------------------------------------------
    #
    # Extensions to low-level functions
    #
    def brport(self, command, **kwarg):
        '''
        Set bridge port parameters. Example::

            idx = ip.link_lookup(ifname='eth0')
            ip.brport("set", index=idx, unicast_flood=0, cost=200)
            ip.brport("show", index=idx)

        Possible keywords are NLA names for the `protinfo_bridge` class,
        without the prefix and in lower letters.
        '''
        if (command in ('dump', 'show')) and ('match' not in kwarg):
            match = kwarg
        else:
            match = kwarg.pop('match', None)

        flags_dump = NLM_F_REQUEST | NLM_F_DUMP
        flags_req = NLM_F_REQUEST | NLM_F_ACK
        commands = {'set': (RTM_SETLINK, flags_req),
                    'dump': (RTM_GETLINK, flags_dump),
                    'show': (RTM_GETLINK, flags_dump)}
        (command, msg_flags) = commands.get(command, command)

        msg = ifinfmsg()
        if command == RTM_GETLINK:
            msg['index'] = kwarg.get('index', 0)
        else:
            msg['index'] = kwarg.pop('index', 0)
        msg['family'] = AF_BRIDGE
        protinfo = IPBrPortRequest(kwarg)
        msg['attrs'].append(('IFLA_PROTINFO', protinfo, 0x8000))
        ret = self.nlm_request(msg,
                               msg_type=command,
                               msg_flags=msg_flags)
        if match is not None:
            ret = self._match(match, ret)

        if not (command == RTM_GETLINK and self.nlm_generator):
            ret = tuple(ret)

        return ret

    def vlan_filter(self, command, **kwarg):
        '''
        Vlan filters is another approach to support vlans in Linux.
        Before vlan filters were introduced, there was only one way
        to bridge vlans: one had to create vlan interfaces and
        then add them as ports::

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
        and create vlans already on the bridge::

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


        Required arguments for `vlan_filter()` -- `index` and `vlan_info`.
        Vlan info struct::

            {"vid": uint16,
             "flags": uint16}

        More details:
            * kernel:Documentation/networking/switchdev.txt
            * pyroute2.netlink.rtnl.ifinfmsg:... vlan_info

        One can specify `flags` as int or as a list of flag names:
            * `master` == 0x1
            * `pvid` == 0x2
            * `untagged` == 0x4
            * `range_begin` == 0x8
            * `range_end` == 0x10
            * `brentry` == 0x20

        E.g.::

            {"vid": 20,
             "flags": ["pvid", "untagged"]}

            # is equal to
            {"vid": 20,
             "flags": 6}

        Commands:

        **add**

        Add vlan filter to a bridge port. Example::

            ip.vlan_filter("add", index=2, vlan_info={"vid": 200})

        **del**

        Remove vlan filter from a bridge port. Example::

            ip.vlan_filter("del", index=2, vlan_info={"vid": 200})

        '''
        flags_req = NLM_F_REQUEST | NLM_F_ACK
        commands = {'add': (RTM_SETLINK, flags_req),
                    'del': (RTM_DELLINK, flags_req)}

        kwarg['family'] = AF_BRIDGE
        kwarg['kwarg_filter'] = IPBridgeRequest

        (command, flags) = commands.get(command, command)
        return tuple(self.link((command, flags), **kwarg))

    def fdb(self, command, **kwarg):
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
        kwarg['family'] = AF_BRIDGE
        # nud -> state
        if 'nud' in kwarg:
            kwarg['state'] = kwarg.pop('nud')
        if (command in ('add', 'del', 'append')) and \
                not (kwarg.get('state', 0) & ndmsg.states['noarp']):
            # state must contain noarp in add / del / append
            kwarg['state'] = kwarg.pop('state', 0) | ndmsg.states['noarp']
            # other assumptions
            if not kwarg.get('state', 0) & (ndmsg.states['permanent'] |
                                            ndmsg.states['reachable']):
                # permanent (default) or reachable
                kwarg['state'] |= ndmsg.states['permanent']
            if not kwarg.get('flags', 0) & (ndmsg.flags['self'] |
                                            ndmsg.flags['master']):
                # self (default) or master
                kwarg['flags'] = kwarg.get('flags', 0) | ndmsg.flags['self']
        #
        return self.neigh(command, **kwarg)

    # 8<---------------------------------------------------------------
    #
    # General low-level configuration methods
    #
    def neigh(self, command, **kwarg):
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
        if (command == 'dump') and ('match' not in kwarg):
            match = kwarg
        else:
            match = kwarg.pop('match', None)

        flags_dump = NLM_F_REQUEST | NLM_F_DUMP
        flags_base = NLM_F_REQUEST | NLM_F_ACK
        flags_make = flags_base | NLM_F_CREATE | NLM_F_EXCL
        flags_append = flags_base | NLM_F_CREATE | NLM_F_APPEND
        flags_change = flags_base | NLM_F_REPLACE
        flags_replace = flags_change | NLM_F_CREATE

        commands = {'add': (RTM_NEWNEIGH, flags_make),
                    'set': (RTM_NEWNEIGH, flags_replace),
                    'replace': (RTM_NEWNEIGH, flags_replace),
                    'change': (RTM_NEWNEIGH, flags_change),
                    'del': (RTM_DELNEIGH, flags_make),
                    'remove': (RTM_DELNEIGH, flags_make),
                    'delete': (RTM_DELNEIGH, flags_make),
                    'dump': (RTM_GETNEIGH, flags_dump),
                    'get': (RTM_GETNEIGH, flags_base),
                    'append': (RTM_NEWNEIGH, flags_append)}

        (command, flags) = commands.get(command, command)
        if 'nud' in kwarg:
            kwarg['state'] = kwarg.pop('nud')
        msg = ndmsg.ndmsg()
        for field in msg.fields:
            msg[field[0]] = kwarg.pop(field[0], 0)
        msg['family'] = msg['family'] or AF_INET
        msg['attrs'] = []
        # fix nud kwarg
        if isinstance(msg['state'], basestring):
            msg['state'] = ndmsg.states_a2n(msg['state'])

        for key in kwarg:
            nla = ndmsg.ndmsg.name2nla(key)
            if kwarg[key] is not None:
                msg['attrs'].append([nla, kwarg[key]])

        ret = self.nlm_request(msg,
                               msg_type=command,
                               msg_flags=flags)
        if match:
            ret = self._match(match, ret)

        if not (command == RTM_GETNEIGH and self.nlm_generator):
            ret = tuple(ret)

        return ret

    def link(self, command, **kwarg):
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

        Filters are implemented in the `pyroute2.netlink.rtnl.req` module.
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
        "passthru". Ususally the default is "vepa".

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

            - `mode` — "tun" or "tap"
            - `uid` — integer
            - `gid` — integer
            - `ifr` — dict of tuntap flags (see ifinfmsg:... tuntap_data)

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

        `pkey` : Inifiniband partition key the ip interface is associated with
        `mode` : Underlying infiniband transport mode.
                 One of:  ['datagram' ,'connected']
        `umcast` : If set(1), multicast group membership for this interface is
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

        Keyword "state" is reserved. State can be "up" or "down",
        it is a shortcut::

            state="up":   flags=1, mask=1
            state="down": flags=0, mask=0

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
        if (command == 'dump') and ('match' not in kwarg):
            match = kwarg
        else:
            match = kwarg.pop('match', None)

        if command[:4] == 'vlan':
            log.warning('vlan filters are managed via `vlan_filter()`')
            log.warning('this compatibility hack will be removed soon')
            return self.vlan_filter(command[5:], **kwarg)

        flags_dump = NLM_F_REQUEST | NLM_F_DUMP
        flags_req = NLM_F_REQUEST | NLM_F_ACK
        flags_create = flags_req | NLM_F_CREATE | NLM_F_EXCL
        flag_append = flags_create | NLM_F_APPEND
        commands = {'set': (RTM_NEWLINK, flags_req),
                    'update': (RTM_SETLINK, flags_create),
                    'add': (RTM_NEWLINK, flags_create),
                    'del': (RTM_DELLINK, flags_create),
                    'property_add': (RTM_NEWLINKPROP, flag_append),
                    'property_del': (RTM_DELLINKPROP, flags_req),
                    'remove': (RTM_DELLINK, flags_create),
                    'delete': (RTM_DELLINK, flags_create),
                    'dump': (RTM_GETLINK, flags_dump),
                    'get': (RTM_GETLINK, NLM_F_REQUEST)}

        msg = ifinfmsg()
        # ifinfmsg fields
        #
        # ifi_family
        # ifi_type
        # ifi_index
        # ifi_flags
        # ifi_change
        #
        msg['family'] = kwarg.pop('family', 0)
        lrq = kwarg.pop('kwarg_filter', IPLinkRequest)
        (command, msg_flags) = commands.get(command, command)
        # index
        msg['index'] = kwarg.pop('index', 0)
        # flags
        flags = kwarg.pop('flags', 0) or 0
        # change
        mask = kwarg.pop('mask', 0) or kwarg.pop('change', 0) or 0

        # UP/DOWN shortcut
        if 'state' in kwarg:
            mask = 1                  # IFF_UP mask
            if kwarg['state'].lower() == 'up':
                flags = 1             # 0 (down) or 1 (up)
            del kwarg['state']

        # arp on/off shortcut
        if 'arp' in kwarg:
            mask |= IFF_NOARP
            if not kwarg.pop('arp'):
                flags |= IFF_NOARP

        msg['flags'] = flags
        msg['change'] = mask

        if 'altname' in kwarg:
            altname = kwarg.pop("altname")
            if command in (RTM_NEWLINKPROP, RTM_DELLINKPROP):
                if not isinstance(altname, (list, tuple, set)):
                    altname = [altname]

                kwarg["IFLA_PROP_LIST"] = {"attrs": [
                    ("IFLA_ALT_IFNAME", alt_ifname)
                    for alt_ifname in altname
                ]}
            else:
                kwarg["IFLA_ALT_IFNAME"] = altname

        # apply filter
        kwarg = lrq(kwarg)

        # attach NLA
        for key in kwarg:
            nla = type(msg).name2nla(key)
            if kwarg[key] is not None:
                msg['attrs'].append([nla, kwarg[key]])

        ret = self.nlm_request(msg,
                               msg_type=command,
                               msg_flags=msg_flags)
        if match is not None:
            ret = self._match(match, ret)

        if not (command == RTM_GETLINK and self.nlm_generator):
            ret = tuple(ret)

        return ret

    def addr(self, command, index=None, address=None, mask=None,
             family=None, scope=None, match=None, **kwarg):
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
        lrq = kwarg.pop('kwarg_filter', IPAddrRequest)

        flags_dump = NLM_F_REQUEST | NLM_F_DUMP
        flags_base = NLM_F_REQUEST | NLM_F_ACK
        flags_create = flags_base | NLM_F_CREATE | NLM_F_EXCL
        flags_replace = flags_base | NLM_F_REPLACE | NLM_F_CREATE
        commands = {'add': (RTM_NEWADDR, flags_create),
                    'del': (RTM_DELADDR, flags_create),
                    'remove': (RTM_DELADDR, flags_create),
                    'delete': (RTM_DELADDR, flags_create),
                    'replace': (RTM_NEWADDR, flags_replace),
                    'dump': (RTM_GETADDR, flags_dump)}
        (command, flags) = commands.get(command, command)

        # fetch args
        index = index or kwarg.pop('index', 0)
        family = family or kwarg.pop('family', None)
        prefixlen = mask or kwarg.pop('mask', 0) or kwarg.pop('prefixlen', 0)
        scope = scope or kwarg.pop('scope', 0)

        # move address to kwarg
        # FIXME: add deprecation notice
        if address:
            kwarg['address'] = address

        # try to guess family, if it is not forced
        if kwarg.get('address') and family is None:
            if address.find(":") > -1:
                family = AF_INET6
                mask = mask or 128
            else:
                family = AF_INET
                mask = mask or 32

        # setup the message
        msg = ifaddrmsg()
        msg['index'] = index
        msg['family'] = family or 0
        msg['prefixlen'] = prefixlen
        msg['scope'] = scope

        kwarg = lrq(kwarg)
        try:
            kwarg.sync_cacheinfo()
        except AttributeError:
            pass

        # inject IFA_LOCAL, if family is AF_INET and IFA_LOCAL is not set
        if family == AF_INET and \
                kwarg.get('address') and \
                kwarg.get('local') is None:
            kwarg['local'] = kwarg['address']

        # patch broadcast, if needed
        if kwarg.get('broadcast') is True:
            kwarg['broadcast'] = getbroadcast(address, mask, family)

        # work on NLA
        for key in kwarg:
            nla = ifaddrmsg.name2nla(key)
            if kwarg[key] not in (None, ''):
                msg['attrs'].append([nla, kwarg[key]])

        ret = self.nlm_request(msg,
                               msg_type=command,
                               msg_flags=flags,
                               terminate=lambda x: x['header']['type'] ==
                               NLMSG_ERROR)
        if match:
            ret = self._match(match, ret)

        if not (command == RTM_GETADDR and self.nlm_generator):
            ret = tuple(ret)

        return ret

    def tc(self, command, kind=None, index=0, handle=0, **kwarg):
        '''
        "Swiss knife" for traffic control. With the method you can
        add, delete or modify qdiscs, classes and filters.

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

        flags_base = NLM_F_REQUEST | NLM_F_ACK
        flags_make = flags_base | NLM_F_CREATE | NLM_F_EXCL
        flags_change = flags_base | NLM_F_REPLACE
        flags_replace = flags_change | NLM_F_CREATE

        commands = {'add': (RTM_NEWQDISC, flags_make),
                    'del': (RTM_DELQDISC, flags_make),
                    'remove': (RTM_DELQDISC, flags_make),
                    'delete': (RTM_DELQDISC, flags_make),
                    'change': (RTM_NEWQDISC, flags_change),
                    'replace': (RTM_NEWQDISC, flags_replace),
                    'add-class': (RTM_NEWTCLASS, flags_make),
                    'del-class': (RTM_DELTCLASS, flags_make),
                    'change-class': (RTM_NEWTCLASS, flags_change),
                    'replace-class': (RTM_NEWTCLASS, flags_replace),
                    'add-filter': (RTM_NEWTFILTER, flags_make),
                    'del-filter': (RTM_DELTFILTER, flags_make),
                    'change-filter': (RTM_NEWTFILTER, flags_change),
                    'replace-filter': (RTM_NEWTFILTER, flags_replace)}
        if isinstance(command, int):
            command = (command, flags_make)
        command, flags = commands.get(command, command)
        msg = tcmsg()
        # transform handle, parent and target, if needed:
        handle = transform_handle(handle)
        for item in ('parent', 'target', 'default'):
            if item in kwarg and kwarg[item] is not None:
                kwarg[item] = transform_handle(kwarg[item])
        msg['index'] = index
        msg['handle'] = handle
        opts = kwarg.get('opts', None)
        ##
        #
        #
        if kind in tc_plugins:
            p = tc_plugins[kind]
            msg['parent'] = kwarg.pop('parent', getattr(p, 'parent', 0))
            if hasattr(p, 'fix_msg'):
                p.fix_msg(msg, kwarg)
            if kwarg:
                if command in (RTM_NEWTCLASS, RTM_DELTCLASS):
                    opts = p.get_class_parameters(kwarg)
                else:
                    opts = p.get_parameters(kwarg)
        else:
            msg['parent'] = kwarg.get('parent', TC_H_ROOT)

        if kind is not None:
            msg['attrs'].append(['TCA_KIND', kind])
        if opts is not None:
            msg['attrs'].append(['TCA_OPTIONS', opts])
        return tuple(self.nlm_request(msg, msg_type=command, msg_flags=flags))

    def route(self, command, **kwarg):
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

            ip.route("add", dst="10.0.0.0/24", gateway="192.168.0.1")

        It is possible to set also route metrics. There are two ways
        to do so. The first is to use 'raw' NLA notation::

            ip.route("add",
                     dst="10.0.0.0",
                     mask=24,
                     gateway="192.168.0.1",
                     metrics={"attrs": [["RTAX_MTU", 1400],
                                        ["RTAX_HOPLIMIT", 16]]})

        The second way is to use shortcuts, provided by `IPRouteRequest`
        class, which is applied to `**kwarg` automatically::

            ip.route("add",
                     dst="10.0.0.0/24",
                     gateway="192.168.0.1",
                     metrics={"mtu": 1400,
                              "hoplimit": 16})

        ...

        More `route()` examples. Blackhole route::

            ip.route("add",
                     dst="10.0.0.0/24",
                     type="blackhole")

        Create a route with metrics::

            ip.route('add',
                     dst='172.16.0.0/24',
                     gateway='10.0.0.10',
                     metrics={'mtu': 1400,
                              'hoplimit': 16})

        Multipath route::

            ip.route("add",
                     dst="10.0.0.0/24",
                     multipath=[{"gateway": "192.168.0.1", "hops": 2},
                                {"gateway": "192.168.0.2", "hops": 1},
                                {"gateway": "192.168.0.3"}])

        MPLS lwtunnel on eth0::

            idx = ip.link_lookup(ifname='eth0')[0]
            ip.route("add",
                     dst="10.0.0.0/24",
                     oif=idx,
                     encap={"type": "mpls",
                            "labels": "200/300"})

        Create MPLS route: push label::

            # $ sudo modprobe mpls_router
            # $ sudo sysctl net.mpls.platform_labels=1024
            ip.route('add',
                     family=AF_MPLS,
                     oif=idx,
                     dst=0x200,
                     newdst=[0x200, 0x300])

        MPLS multipath::

            idx = ip.link_lookup(ifname='eth0')[0]
            ip.route("add",
                     dst="10.0.0.0/24",
                     table=20,
                     multipath=[{"gateway": "192.168.0.1",
                                 "encap": {"type": "mpls",
                                           "labels": 200}},
                                {"ifindex": idx,
                                 "encap": {"type": "mpls",
                                           "labels": 300}}])

        MPLS target can be int, string, dict or list::

            "labels": 300    # simple label
            "labels": "300"  # the same
            "labels": (200, 300)  # stacked
            "labels": "200/300"   # the same

            # explicit label definition
            "labels": {"bos": 1,
                       "label": 300,
                       "tc": 0,
                       "ttl": 16}

        Create SEG6 tunnel encap mode (kernel >= 4.10)::

            ip.route('add',
                     dst='2001:0:0:10::2/128',
                     oif=idx,
                     encap={'type': 'seg6',
                            'mode': 'encap',
                            'segs': '2000::5,2000::6'})

        Create SEG6 tunnel inline mode (kernel >= 4.10)::

            ip.route('add',
                     dst='2001:0:0:10::2/128',
                     oif=idx,
                     encap={'type': 'seg6',
                            'mode': 'inline',
                            'segs': ['2000::5', '2000::6']})

        Create SEG6 tunnel inline mode with hmac (kernel >= 4.10)::

            ip.route('add',
                     dst='2001:0:0:22::2/128',
                     oif=idx,
                     encap={'type': 'seg6',
                            'mode': 'inline',
                            'segs':'2000::5,2000::6,2000::7,2000::8',
                            'hmac':0xf})

        Create SEG6 tunnel with ip4ip6 encapsulation (kernel >= 4.14)::

            ip.route('add',
                     dst='172.16.0.0/24',
                     oif=idx,
                     encap={'type': 'seg6',
                            'mode': 'encap',
                            'segs': '2000::5,2000::6'})

        Create SEG6LOCAL tunnel End.DX4 action (kernel >= 4.14)::

            ip.route('add',
                     dst='2001:0:0:10::2/128',
                     oif=idx,
                     encap={'type': 'seg6local',
                            'action': 'End.DX4',
                            'nh4': '172.16.0.10'})

        Create SEG6LOCAL tunnel End.DT6 action (kernel >= 4.14)::

            ip.route('add',
                     dst='2001:0:0:10::2/128',
                     oif=idx,
                     encap={'type': 'seg6local',
                            'action': 'End.DT6',
                            'table':'10'})

        Create SEG6LOCAL tunnel End.B6 action (kernel >= 4.14)::

            ip.route('add',
                     dst='2001:0:0:10::2/128',
                     oif=idx,
                     encap={'type': 'seg6local',
                            'action': 'End.B6',
                            'srh':{'segs': '2000::5,2000::6'}})

        Create SEG6LOCAL tunnel End.B6 action with hmac (kernel >= 4.14)::

            ip.route('add',
                     dst='2001:0:0:10::2/128',
                     oif=idx,
                     encap={'type': 'seg6local',
                            'action': 'End.B6',
                            'srh': {'segs': '2000::5,2000::6',
                                    'hmac':0xf}})

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
        # 8<----------------------------------------------------
        # FIXME
        # flags should be moved to some more general place
        flags_dump = NLM_F_DUMP | NLM_F_REQUEST
        flags_base = NLM_F_REQUEST | NLM_F_ACK
        flags_make = flags_base | NLM_F_CREATE | NLM_F_EXCL
        flags_change = flags_base | NLM_F_REPLACE
        flags_replace = flags_change | NLM_F_CREATE
        flags_append = flags_base | NLM_F_CREATE | NLM_F_APPEND
        # 8<----------------------------------------------------
        # transform kwarg

        if command in ('add', 'set', 'replace', 'change', 'append'):
            kwarg['proto'] = kwarg.get('proto', 'static') or 'static'
            kwarg['type'] = kwarg.get('type', 'unicast') or 'unicast'
        kwarg = IPRouteRequest(kwarg)
        if 'match' not in kwarg and command in ('dump', 'show'):
            match = kwarg
        else:
            match = kwarg.pop('match', None)
        callback = kwarg.pop('callback', None)

        commands = {'add': (RTM_NEWROUTE, flags_make),
                    'set': (RTM_NEWROUTE, flags_replace),
                    'replace': (RTM_NEWROUTE, flags_replace),
                    'change': (RTM_NEWROUTE, flags_change),
                    'append': (RTM_NEWROUTE, flags_append),
                    'del': (RTM_DELROUTE, flags_make),
                    'remove': (RTM_DELROUTE, flags_make),
                    'delete': (RTM_DELROUTE, flags_make),
                    'get': (RTM_GETROUTE, NLM_F_REQUEST),
                    'show': (RTM_GETROUTE, flags_dump),
                    'dump': (RTM_GETROUTE, flags_dump)}
        (command, flags) = commands.get(command, command)
        msg = rtmsg()

        # table is mandatory; by default == 254
        # if table is not defined in kwarg, save it there
        # also for nla_attr:
        table = kwarg.get('table', 254)
        msg['table'] = table if table <= 255 else 252
        msg['family'] = kwarg.pop('family', AF_INET)
        msg['scope'] = kwarg.pop('scope', rt_scope['universe'])
        msg['dst_len'] = kwarg.pop('dst_len', None) or kwarg.pop('mask', 0)
        msg['src_len'] = kwarg.pop('src_len', 0)
        msg['tos'] = kwarg.pop('tos', 0)
        msg['flags'] = kwarg.pop('flags', 0)
        msg['type'] = kwarg.pop('type', rt_type['unspec'])
        msg['proto'] = kwarg.pop('proto', rt_proto['unspec'])
        msg['attrs'] = []

        if msg['family'] == AF_MPLS:
            for key in tuple(kwarg):
                if key not in ('dst', 'newdst', 'via', 'multipath', 'oif'):
                    kwarg.pop(key)

        for key in kwarg:
            nla = rtmsg.name2nla(key)
            if nla == 'RTA_DST' and not kwarg[key]:
                continue
            if kwarg[key] is not None:
                msg['attrs'].append([nla, kwarg[key]])
                # fix IP family, if needed
                if msg['family'] in (AF_UNSPEC, 255):
                    if key in ('dst', 'src', 'gateway', 'prefsrc', 'newdst') \
                            and isinstance(kwarg[key], basestring):
                        msg['family'] = AF_INET6 if kwarg[key].find(':') >= 0 \
                            else AF_INET
                    elif key == 'multipath' and len(kwarg[key]) > 0:
                        hop = kwarg[key][0]
                        attrs = hop.get('attrs', [])
                        for attr in attrs:
                            if attr[0] == 'RTA_GATEWAY':
                                msg['family'] = AF_INET6 if \
                                    attr[1].find(':') >= 0 else AF_INET
                                break

        ret = self.nlm_request(msg,
                               msg_type=command,
                               msg_flags=flags,
                               callback=callback)
        if match:
            ret = self._match(match, ret)

        if not (command == RTM_GETROUTE and self.nlm_generator):
            ret = tuple(ret)

        return ret

    def rule(self, command, *argv, **kwarg):
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
            return

        flags_base = NLM_F_REQUEST | NLM_F_ACK
        flags_make = flags_base | NLM_F_CREATE | NLM_F_EXCL
        flags_dump = NLM_F_REQUEST | NLM_F_ROOT | NLM_F_ATOMIC

        commands = {'add': (RTM_NEWRULE, flags_make),
                    'del': (RTM_DELRULE, flags_make),
                    'remove': (RTM_DELRULE, flags_make),
                    'delete': (RTM_DELRULE, flags_make),
                    'dump': (RTM_GETRULE, flags_dump)}
        if isinstance(command, int):
            command = (command, flags_make)
        command, flags = commands.get(command, command)

        if argv:
            # this code block will be removed in some release
            log.error('rule(): positional parameters are deprecated')
            names = ['table', 'priority', 'action', 'family',
                     'src', 'src_len', 'dst', 'dst_len', 'fwmark',
                     'iifname', 'oifname']
            kwarg.update(dict(zip(names, argv)))

        kwarg = IPRuleRequest(kwarg)
        msg = fibmsg()
        table = kwarg.get('table', 0)
        msg['table'] = table if table <= 255 else 252
        for key in ('family',
                    'src_len',
                    'dst_len',
                    'action',
                    'tos',
                    'flags'):
            msg[key] = kwarg.pop(key, 0)
        msg['attrs'] = []

        for key in kwarg:
            nla = fibmsg.name2nla(key)
            if kwarg[key] is not None:
                msg['attrs'].append([nla, kwarg[key]])

        ret = self.nlm_request(msg,
                               msg_type=command,
                               msg_flags=flags)

        if 'match' in kwarg:
            ret = self._match(kwarg['match'], ret)

        if not (command == RTM_GETRULE and self.nlm_generator):
            ret = tuple(ret)

        return ret

    def stats(self, command, **kwarg):
        '''
        Stats prototype.
        '''
        if (command == 'dump') and ('match' not in kwarg):
            match = kwarg
        else:
            match = kwarg.pop('match', None)

        commands = {'dump': (RTM_GETSTATS, NLM_F_REQUEST | NLM_F_DUMP),
                    'get': (RTM_GETSTATS, NLM_F_REQUEST | NLM_F_ACK)}
        command, flags = commands.get(command, command)

        msg = ifstatsmsg()
        msg['filter_mask'] = kwarg.get('filter_mask', 31)
        msg['ifindex'] = kwarg.get('ifindex', 0)

        ret = self.nlm_request(msg,
                               msg_type=command,
                               msg_flags=flags)
        if match is not None:
            ret = self._match(match, ret)

        if not (command == RTM_GETSTATS and self.nlm_generator):
            ret = tuple(ret)

        return ret
    # 8<---------------------------------------------------------------


class IPBatch(RTNL_API, IPBatchSocket):
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
    pass


class IPRoute(RTNL_API, IPRSocket):
    '''
    Regular ordinary utility class, see RTNL API for the list of methods.
    '''
    pass


class RawIPRoute(RTNL_API, RawIPRSocket):
    '''
    The same as `IPRoute`, but does not use the netlink proxy.
    Thus it can not manage e.g. tun/tap interfaces.
    '''
    pass
