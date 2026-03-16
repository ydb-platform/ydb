import asyncio
import sys
from typing import Literal, Optional

from pyroute2.iproute.linux import AsyncIPRoute
from pyroute2.netlink.exceptions import NetlinkError
from pyroute2.netlink.rtnl import RTMGRP_LINK

IfaceState = Literal['up', 'down']


class InterfaceNotFound(LookupError):
    '''Raised when an interface is not found.'''


class InterfaceStateWatcher:
    '''Async context manager, fires events when an interface changes state.

    Used by the dhcp client to restart itself in such cases.
    '''

    def __init__(self, interface: str) -> None:
        self.interface = interface
        self.up = asyncio.Event()
        self.down = asyncio.Event()
        self._state: Optional[IfaceState] = None
        self._watcher: Optional[asyncio.Task] = None
        self._ipr: Optional[AsyncIPRoute] = None

    @property
    def ipr(self) -> AsyncIPRoute:
        '''The async iproute context. Only usable in the context manager.'''
        assert self._ipr, 'need to use as a context manager'
        return self._ipr

    @property
    def state(self) -> IfaceState:
        '''The current state of the watched interface.'''
        assert self._state, "not yet started"
        return self._state

    @state.setter
    def state(self, value: IfaceState) -> None:
        '''Set the state & trigger the relevant event.'''
        self._state = value
        if value == 'up':
            self.up.set()
            self.down.clear()
        elif value == 'down':
            self.down.set()
            self.up.clear()

    async def _fetch_current_state(self) -> IfaceState:
        '''Get the initial state before we're notified of changes.'''
        lookup_results = await self.ipr.link_lookup(ifname=self.interface)
        if not lookup_results:
            raise InterfaceNotFound(self.interface)
        get_results = await self.ipr.link("get", index=lookup_results[0])
        return get_results[0].get('state')

    async def _watch_changes(self) -> None:
        '''Updates `state` in real time forever.'''
        while True:
            # TODO: svinota says we should call .link('dump') here
            # to empty the buffer before starting
            async for msg in self.ipr.get():
                if msg.get('IFLA_IFNAME') == self.interface:
                    self.state = msg.get('state')

    async def __aenter__(self) -> 'InterfaceStateWatcher':
        self._ipr = await AsyncIPRoute().__aenter__()
        await self.ipr.bind(RTMGRP_LINK)
        self.state = await self._fetch_current_state()
        self._watcher = asyncio.create_task(self._watch_changes())
        return self

    async def __aexit__(self, *_) -> None:
        self.ipr.close()
        if self._watcher:
            try:
                await self._watcher
            except NetlinkError as exc:
                if exc.code == 104:
                    # we called close() so that is expected
                    pass
                else:
                    raise
            self._watcher = None
        self._ipr = None


async def main(iface: str, state: str) -> None:
    '''Very basic entrypoint for commandline testing.'''
    assert state in ('up', 'down')
    async with InterfaceStateWatcher(iface) as watcher:
        print('Will exit when', iface, 'is', state)
        await getattr(watcher, state).wait()


if __name__ == '__main__':
    try:
        asyncio.run(main(sys.argv[1], sys.argv[2]))
    except IndexError:
        print('Usage:', sys.argv[0], '<interface> <status>')
