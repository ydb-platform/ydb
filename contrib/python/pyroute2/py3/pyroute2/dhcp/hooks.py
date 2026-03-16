'''Hooks called by the DHCP client when bound, a leases expires, etc.'''

import asyncio
import errno
from enum import auto
from logging import getLogger
from typing import Callable, Iterable, NamedTuple, Optional, Protocol

from pyroute2.compat import StrEnum
from pyroute2.dhcp.leases import Lease, MissingOptionError
from pyroute2.iproute.linux import AsyncIPRoute
from pyroute2.netlink.exceptions import NetlinkError

LOG = getLogger(__name__)


class Trigger(StrEnum):
    '''Events that can trigger hooks in the client.'''

    # The client has obtained a new lease
    BOUND = auto()
    # The client has voluntarily relinquished its lease
    UNBOUND = auto()
    # The client has renewed its lease after the renewal timer expired
    RENEWED = auto()
    # The client has rebound its lease after the rebinding timer expired
    REBOUND = auto()
    # The lease has expired (the client will restart the lease process)
    EXPIRED = auto()


class HookFunc(Protocol):
    '''Signature for functions that can be passed to the hook decorator.'''

    __name__: str

    async def __call__(self, lease: Lease) -> None:
        pass  # pragma: no cover


class Hook(NamedTuple):
    '''Stores a hook function and its triggers.

    Returned by the `hook()` decorator; no need to subclass or instantiate.
    '''

    func: HookFunc
    triggers: set[Trigger]

    async def __call__(self, lease: Lease) -> None:
        '''Call the hook function.'''
        await self.func(lease=lease)

    @property
    def name(self) -> str:
        '''Shortcut for the function name.'''
        return self.func.__name__


async def run_hooks(
    hooks: Iterable[Hook],
    lease: Lease,
    trigger: Trigger,
    timeout: Optional[float] = None,
):
    '''Called by the client to run the hooks registered for the given trigger.

    The optional `timeout` arguments causes individual hooks to timeout
    if they exceed it.

    Exceptions are handled and printed, but don't prevent the other hooks from
    running.

    .. warning::
        The timeout is async, which means that hooks that block on non-async
        code can ignore it and freeze the whole DHCP client !

    '''
    if hooks := list(hooks):
        LOG.debug('Running %s hooks', trigger)
        for i in filter(lambda y: trigger in y.triggers, hooks):
            try:
                await asyncio.wait_for(i(lease), timeout=timeout)
            except asyncio.exceptions.TimeoutError:
                LOG.error('Hook %r timed out', i.name)
            except Exception as exc:
                LOG.error('Hook %s failed: %r', i.name, exc)


def hook(*triggers: Trigger) -> Callable[[HookFunc], Hook]:
    '''Decorator for dhcp client hooks.

    A hook is an async function that takes a lease.
    Hooks set in `ClientConfig.hooks` will be called in order by the client
    when one of the triggers passed to the decorator happens.

    For example::

        @hook(Trigger.RENEWED)
        async def lease_was_renewed(lease: Lease):
            print(lease.server_mac, 'renewed our lease !')

    .. warning::
        - blocking non-async code in hooks might freeze the client
        - long-running async hooks might be canceled after a timeout

    The decorator returns a `Hook` instance, a utility class storing the hook
    function and its triggers.

    .. warning::
        The hooks API might still change.

    '''

    def decorator(hook_func: HookFunc) -> Hook:
        return Hook(func=hook_func, triggers=set(triggers))

    return decorator


@hook(Trigger.BOUND)
async def configure_ip(lease: Lease):
    '''Add the IP allocated in the lease to its interface.

    Use the `remove_ip` hook in addition to this one for cleanup.
    The DHCP server must have set the subnet mask and broadcast address.
    '''
    LOG.info(
        'Adding %s/%s to %s', lease.ip, lease.subnet_mask, lease.interface
    )
    try:
        bcast: Optional[str] = lease.broadcast_address
    except MissingOptionError as err:
        LOG.debug("%s", err)
        bcast = None
    async with AsyncIPRoute(ext_ack=True, strict_check=True) as ipr:
        await ipr.addr(
            'replace',
            index=await ipr.link_lookup(ifname=lease.interface),
            address=lease.ip,
            prefixlen=lease.subnet_mask,
            broadcast=bcast,
        )


@hook(Trigger.UNBOUND, Trigger.EXPIRED)
async def remove_ip(lease: Lease):
    '''Remove the IP in the lease from its interface.'''
    LOG.info(
        'Removing %s/%s from %s', lease.ip, lease.subnet_mask, lease.interface
    )
    async with AsyncIPRoute(ext_ack=True, strict_check=True) as ipr:
        await ipr.addr(
            'del',
            index=await ipr.link_lookup(ifname=lease.interface),
            address=lease.ip,
            prefixlen=lease.subnet_mask,
        )


@hook(Trigger.BOUND)
async def add_default_gw(lease: Lease):
    '''Configures the default gateway set in the lease.

    Use in addition to `remove_default_gw` for cleanup.
    '''
    LOG.info(
        'Adding %s as default route through %s',
        lease.default_gateway,
        lease.interface,
    )
    async with AsyncIPRoute(ext_ack=True, strict_check=True) as ipr:
        ifindex = (await ipr.link_lookup(ifname=lease.interface),)
        await ipr.route(
            'replace',
            dst='0.0.0.0/0',
            gateway=lease.default_gateway,
            oif=ifindex,
        )


@hook(Trigger.UNBOUND, Trigger.EXPIRED)
async def remove_default_gw(lease: Lease):
    '''Removes the default gateway set in the lease.'''
    LOG.info('Removing %s as default route', lease.default_gateway)
    async with AsyncIPRoute(ext_ack=True, strict_check=True) as ipr:
        ifindex = await ipr.link_lookup(ifname=lease.interface)
        try:
            await ipr.route(
                'del',
                dst='0.0.0.0/0',
                gateway=lease.default_gateway,
                oif=ifindex,
            )
        except NetlinkError as err:
            if err.code == errno.ESRCH:
                LOG.info(
                    'Default route was already removed by another process'
                )
            else:
                LOG.error('Got a netlink error: %s', err)
