from pyroute2.netlink.generic import (
    AsyncGenericNetlinkSocket,
    GenericNetlinkSocket,
)


class AsyncEventSocket(AsyncGenericNetlinkSocket):
    genl_family = None

    async def setup_endpoint(self):
        if getattr(self.local, 'transport', None) is not None:
            return
        await super().setup_endpoint()
        await self.bind()
        for group in self.mcast_groups:
            self.add_membership(group)

    async def bind(self, groups=0, **kwarg):
        await super().bind(
            self.genl_family,
            self.marshal_class.msg_map[0],
            groups,
            None,
            **kwarg,
        )


class EventSocket(GenericNetlinkSocket):
    async_class = AsyncEventSocket
    marshal_class = None
    genl_family = None

    def __init__(self, *args, **kwarg):
        if self.marshal_class is not None:
            self.async_class.marshal_class = self.marshal_class
        if self.genl_family is not None:
            self.async_class.genl_family = self.genl_family
        super().__init__(*args, **kwarg)

    def bind(self, groups=0, **kwarg):
        return self._run_with_cleanup(
            self.asyncore.bind, groups=groups, **kwarg
        )
