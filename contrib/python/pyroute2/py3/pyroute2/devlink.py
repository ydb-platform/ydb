import logging

from pyroute2.netlink import NLM_F_DUMP, NLM_F_REQUEST
from pyroute2.netlink.devlink import (
    DEVLINK_NAMES,
    AsyncDevlinkSocket,
    DevlinkSocket,
    devlinkcmd,
)

log = logging.getLogger(__name__)


class AsyncDL(AsyncDevlinkSocket):

    async def setup_endpoint(self):
        if getattr(self.local, 'transport', None) is not None:
            return
        await super().setup_endpoint()
        await self.bind()

    async def list(self):
        return await self.get_dump()

    async def get_dump(self):
        msg = devlinkcmd()
        msg['cmd'] = DEVLINK_NAMES['DEVLINK_CMD_GET']
        return await self.nlm_request(
            msg, msg_type=self.prid, msg_flags=NLM_F_REQUEST | NLM_F_DUMP
        )

    async def port_list(self):
        return await self.get_port_dump()

    async def get_port_dump(self):
        msg = devlinkcmd()
        msg['cmd'] = DEVLINK_NAMES['DEVLINK_CMD_PORT_GET']
        return await self.nlm_request(
            msg, msg_type=self.prid, msg_flags=NLM_F_REQUEST | NLM_F_DUMP
        )


class DL(DevlinkSocket):
    async_class = AsyncDL

    def list(self):
        return self._run_sync_cleanup(self.asyncore.list)

    def get_dump(self):
        return self._run_sync_cleanup(self.asyncore.get_dump)

    def port_list(self):
        return self._run_sync_cleanup(self.asyncore.port_list)

    def get_port_dump(self):
        return self._run_sync_cleanup(self.asyncore.get_port_dump)
