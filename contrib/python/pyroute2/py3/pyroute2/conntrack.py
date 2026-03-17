import socket

from pyroute2.netlink.nfnetlink.nfctsocket import (
    IP_CT_TCP_FLAG_TO_NAME,
    IPSBIT_TO_NAME,
    TCP_CONNTRACK_TO_NAME,
    AsyncNFCTSocket,
    NFCTAttrTuple,
    NFCTSocket,
)


class NFCTATcpProtoInfo(object):
    __slots__ = (
        'state',
        'wscale_orig',
        'wscale_reply',
        'flags_orig',
        'flags_reply',
    )

    def __init__(
        self,
        state,
        wscale_orig=None,
        wscale_reply=None,
        flags_orig=None,
        flags_reply=None,
    ):
        self.state = state
        self.wscale_orig = wscale_orig
        self.wscale_reply = wscale_reply
        self.flags_orig = flags_orig
        self.flags_reply = flags_reply

    def state_name(self):
        return TCP_CONNTRACK_TO_NAME.get(self.state, "UNKNOWN")

    def flags_name(self, flags):
        if flags is None:
            return ''
        s = ''
        for bit, name in IP_CT_TCP_FLAG_TO_NAME.items():
            if flags & bit:
                s += '{},'.format(name)
        return s[:-1]

    @classmethod
    def from_netlink(cls, ndmsg):
        cta_tcp = ndmsg.get_attr('CTA_PROTOINFO_TCP')
        state = cta_tcp.get_attr('CTA_PROTOINFO_TCP_STATE')

        # second argument is the mask returned by kernel but useless for us
        flags_orig, _ = cta_tcp.get_attr('CTA_PROTOINFO_TCP_FLAGS_ORIGINAL')
        flags_reply, _ = cta_tcp.get_attr('CTA_PROTOINFO_TCP_FLAGS_REPLY')
        return cls(state=state, flags_orig=flags_orig, flags_reply=flags_reply)

    def __repr__(self):
        return 'TcpInfo(state={}, orig_flags={}, reply_flags={})'.format(
            self.state_name(),
            self.flags_name(self.flags_orig),
            self.flags_name(self.flags_reply),
        )


class ConntrackEntry(object):
    __slots__ = (
        'tuple_orig',
        'tuple_reply',
        'status',
        'timeout',
        'protoinfo',
        'mark',
        'id',
        'use',
        'zone',
    )

    def __init__(
        self,
        family,
        tuple_orig,
        tuple_reply,
        cta_status,
        cta_timeout,
        cta_protoinfo,
        cta_mark,
        cta_id,
        cta_use,
        cta_zone,
    ):
        self.tuple_orig = NFCTAttrTuple.from_netlink(family, tuple_orig)
        self.tuple_reply = NFCTAttrTuple.from_netlink(family, tuple_reply)

        self.status = cta_status
        self.timeout = cta_timeout

        if self.tuple_orig.proto == socket.IPPROTO_TCP:
            self.protoinfo = NFCTATcpProtoInfo.from_netlink(cta_protoinfo)
        else:
            self.protoinfo = None

        self.mark = cta_mark
        self.id = cta_id
        self.use = cta_use
        self.zone = cta_zone

    def status_name(self):
        s = ''
        for bit, name in IPSBIT_TO_NAME.items():
            if self.status & bit:
                s += '{},'.format(name)
        return s[:-1]

    def __repr__(self):
        s = 'Entry(orig={}, reply={}, status={}'.format(
            self.tuple_orig, self.tuple_reply, self.status_name()
        )
        if self.protoinfo is not None:
            s += ', protoinfo={}'.format(self.protoinfo)
        if self.zone is not None:
            s += f', zone={self.zone}'
        s += ')'
        return s


class AsyncConntrack(AsyncNFCTSocket):
    """
    High level conntrack functions
    """

    async def stat(self):
        """Return current statistics per CPU

        Same result than conntrack -S command but a list of dictionaries
        """
        stats = []

        for msg in await super().stat():
            stats.append({'cpu': msg['res_id']})
            stats[-1].update(
                (k[10:].lower(), v)
                for k, v in msg['attrs']
                if k.startswith('CTA_STATS_')
            )

        return stats

    async def count(self):
        """Return current number of conntrack entries

        Same result than /proc/sys/net/netfilter/nf_conntrack_count file
        or conntrack -C command
        """
        for ndmsg in await super().count():
            return ndmsg.get_attr('CTA_STATS_GLOBAL_ENTRIES')

    async def conntrack_max_size(self):
        """
        Return the max size of connection tracking table
        /proc/sys/net/netfilter/nf_conntrack_max
        """
        for ndmsg in await super().conntrack_max_size():
            return ndmsg.get_attr('CTA_STATS_GLOBAL_MAX_ENTRIES')

    async def delete(self, entry):
        if isinstance(entry, ConntrackEntry):
            tuple_orig = entry.tuple_orig
        elif isinstance(entry, NFCTAttrTuple):
            tuple_orig = entry
        else:
            raise NotImplementedError()
        for ndmsg in await self.entry('del', tuple_orig=tuple_orig):
            return ndmsg

    async def entry(self, cmd, **kwarg):
        for res in await super().entry(cmd, **kwarg):
            return res

    async def _dump_entries_task(
        self,
        mark=None,
        mark_mask=0xFFFFFFFF,
        tuple_orig=None,
        tuple_reply=None,
        zone=None,
    ):
        async for ndmsg in await self.dump(
            mark=mark,
            mark_mask=mark_mask,
            tuple_orig=tuple_orig,
            tuple_reply=tuple_reply,
            zone=zone,
        ):
            if tuple_orig is not None and not tuple_orig.nla_eq(
                ndmsg['nfgen_family'], ndmsg.get_attr('CTA_TUPLE_ORIG')
            ):
                continue

            if tuple_reply is not None and not tuple_reply.nla_eq(
                ndmsg['nfgen_family'], ndmsg.get_attr('CTA_TUPLE_REPLY')
            ):
                continue

            yield ConntrackEntry(
                ndmsg['nfgen_family'],
                ndmsg.get_attr('CTA_TUPLE_ORIG'),
                ndmsg.get_attr('CTA_TUPLE_REPLY'),
                ndmsg.get_attr('CTA_STATUS'),
                ndmsg.get_attr('CTA_TIMEOUT'),
                ndmsg.get_attr('CTA_PROTOINFO'),
                ndmsg.get_attr('CTA_MARK'),
                ndmsg.get_attr('CTA_ID'),
                ndmsg.get_attr('CTA_USE'),
                ndmsg.get_attr('CTA_ZONE'),
            )

    async def dump_entries(
        self,
        mark=None,
        mark_mask=0xFFFFFFFF,
        tuple_orig=None,
        tuple_reply=None,
        zone=None,
    ):
        """
        Dump all entries from conntrack table with filters

        Filters can be only part of a conntrack tuple

        :param NFCTAttrTuple tuple_orig: filter on original tuple
        :param NFCTAttrTuple tuple_reply: filter on reply tuple
        :param int zone: CTA_ZONE

        Examples::
            # Filter only on tcp connections
            for entry in ct.dump_entries(tuple_orig=NFCTAttrTuple(
                                             proto=socket.IPPROTO_TCP)):
                print("This entry is tcp: {}".format(entry))

            # Filter only on icmp message to 8.8.8.8
            for entry in ct.dump_entries(tuple_orig=NFCTAttrTuple(
                                             proto=socket.IPPROTO_ICMP,
                                             daddr='8.8.8.8')):
                print("This entry is icmp to 8.8.8.8: {}".format(entry))
        """
        return self._dump_entries_task(
            mark, mark_mask, tuple_orig, tuple_reply, zone
        )


class Conntrack(NFCTSocket):

    def __init__(self, nlm_generator=True, **kwarg):
        self.asyncore = AsyncConntrack(**kwarg)

    def stat(self):
        return self._run_with_cleanup(self.asyncore.stat)

    def count(self):
        return self._run_with_cleanup(self.asyncore.count)

    def conntrack_max_size(self):
        return self._run_with_cleanup(self.asyncore.conntrack_max_size)

    def delete(self, entry):
        return self._run_with_cleanup(self.asyncore.delete, entry)

    def entry(self, cmd, **kwarg):
        return self._run_with_cleanup(self.asyncore.entry, cmd, **kwarg)

    def dump_entries(
        self,
        mark=None,
        mark_mask=0xFFFFFFFF,
        tuple_orig=None,
        tuple_reply=None,
        zone=None,
    ):
        return self._generate_with_cleanup(
            self.asyncore.dump_entries,
            mark,
            mark_mask,
            tuple_orig,
            tuple_reply,
            zone,
        )
