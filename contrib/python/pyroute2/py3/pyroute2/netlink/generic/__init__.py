# -*- coding: utf-8 -*-
'''
Generic netlink
===============

Describe
'''
import errno
import logging

from pyroute2.netlink import (
    CTRL_CMD_GETFAMILY,
    CTRL_CMD_GETPOLICY,
    GENL_ID_CTRL,
    NETLINK_ADD_MEMBERSHIP,
    NETLINK_DROP_MEMBERSHIP,
    NLM_F_ACK,
    NLM_F_DUMP,
    NLM_F_REQUEST,
    SOL_NETLINK,
    ctrlmsg,
)
from pyroute2.netlink.nlsocket import AsyncNetlinkSocket, NetlinkSocket


class AsyncGenericNetlinkSocket(AsyncNetlinkSocket):
    '''
    Low-level socket interface. Provides all the
    usual socket does, can be used in poll/select,
    doesn't create any implicit threads.
    '''

    mcast_groups = {}
    module_err_message = None
    module_err_level = 'error'
    _prid = None

    @property
    def prid(self):
        if self._prid is None:
            raise RuntimeError(
                'generic netlink protocol id is not obtained'
                ' yet, run bind() before placing any requests'
            )
        else:
            return self._prid

    async def _do_dump(self, msg, msg_flags=NLM_F_REQUEST | NLM_F_DUMP):
        return await self.nlm_request(
            msg, msg_type=self.prid, msg_flags=msg_flags
        )

    async def _do_request(self, msg, msg_flags=NLM_F_REQUEST | NLM_F_ACK):
        return [x async for x in await self._do_dump(msg, msg_flags)]

    async def bind(self, proto, msg_class, groups=0, pid=None, **kwarg):
        '''
        Bind the socket and performs generic netlink
        proto lookup. The `proto` parameter is a string,
        like "TASKSTATS", `msg_class` is a class to
        parse messages with.
        '''
        await super().bind(groups, pid, **kwarg)
        self.marshal.msg_map[GENL_ID_CTRL] = ctrlmsg
        msg = await self.discovery(proto)
        self._prid = msg.get_attr('CTRL_ATTR_FAMILY_ID')
        self.mcast_groups = dict(
            [
                (
                    x.get_attr('CTRL_ATTR_MCAST_GRP_NAME'),
                    x.get_attr('CTRL_ATTR_MCAST_GRP_ID'),
                )
                for x in msg.get_attr('CTRL_ATTR_MCAST_GROUPS', [])
            ]
        )
        self.marshal.msg_map[self.prid] = msg_class

    def add_membership(self, group):
        self.setsockopt(
            SOL_NETLINK, NETLINK_ADD_MEMBERSHIP, self.mcast_groups[group]
        )

    def drop_membership(self, group):
        self.setsockopt(
            SOL_NETLINK, NETLINK_DROP_MEMBERSHIP, self.mcast_groups[group]
        )

    async def discovery(self, proto):
        '''
        Resolve generic netlink protocol -- takes a string
        as the only parameter, return protocol description
        '''
        msg = ctrlmsg()
        msg['cmd'] = CTRL_CMD_GETFAMILY
        msg['version'] = 1
        msg['attrs'].append(['CTRL_ATTR_FAMILY_NAME', proto])
        msg['header']['type'] = GENL_ID_CTRL
        msg['header']['flags'] = NLM_F_REQUEST
        msg['header']['pid'] = self.pid
        msg.encode()
        self.sendto(msg.data, (0, 0))
        (msg,) = [x async for x in self.get()]
        err = msg['header'].get('error', None)
        if err is not None:
            if hasattr(err, 'code') and err.code == errno.ENOENT:
                err.extra_code = errno.ENOTSUP
                logger = getattr(logging, self.module_err_level)
                logger('Generic netlink protocol %s not found' % proto)
                logger('Please check if the protocol module is loaded')
                if self.module_err_message is not None:
                    logger(self.module_err_message)
            raise err
        return msg

    async def policy(self, proto):
        '''
        Extract policy information for a generic netlink protocol -- takes
        a string as the only parameter, return protocol policy
        '''
        self.marshal.msg_map[GENL_ID_CTRL] = ctrlmsg
        msg = ctrlmsg()
        msg['cmd'] = CTRL_CMD_GETPOLICY
        msg['attrs'].append(['CTRL_ATTR_FAMILY_NAME', proto])
        return tuple(
            [
                x
                async for x in await self.nlm_request(
                    msg,
                    msg_type=GENL_ID_CTRL,
                    msg_flags=NLM_F_REQUEST | NLM_F_DUMP | NLM_F_ACK,
                )
            ]
        )


class GenericNetlinkSocket(NetlinkSocket):

    async_class = AsyncGenericNetlinkSocket
    class_gen_sync = False

    @property
    def prid(self):
        return self.asyncore.prid

    @property
    def mcast_groups(self):
        return self.asyncore.mcast_groups

    def bind(self, proto, msg_class, groups=0, pid=None, **kwarg):
        return self._run_with_cleanup(
            self.asyncore.bind, proto, msg_class, groups, pid, **kwarg
        )

    def add_membership(self, group):
        return self.asyncore.add_membership(group)

    def drop_membership(self, group):
        return self.asyncore.drop_membership(group)

    def discovery(self, proto):
        return self._run_with_cleanup(self.asyncore.discovery, proto)

    def policy(self, proto):
        return self._run_with_cleanup(self.asyncore.policy, proto)
