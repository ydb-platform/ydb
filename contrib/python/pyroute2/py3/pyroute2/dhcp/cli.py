import asyncio
import logging
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from importlib import import_module
from typing import Any, Optional

from pyroute2.dhcp.client import AsyncDHCPClient, ClientConfig
from pyroute2.dhcp.fsm import State
from pyroute2.dhcp.hooks import Hook
from pyroute2.dhcp.iface_status import InterfaceNotFound, InterfaceStateWatcher
from pyroute2.dhcp.leases import Lease

LOG = logging.getLogger(__name__)


def import_dotted_name(name: str) -> Any:
    '''Import anything by name. Return None if the import wasn't successful.'''
    try:
        module_name, obj_name = name.rsplit('.', 1)
        module = import_module(module_name)
        return getattr(module, obj_name)
    except (ValueError, ImportError, AttributeError):
        return None


def get_psr() -> ArgumentParser:
    psr = ArgumentParser(
        description='A DHCP client based on pyroute2. '
        'Tries to obtain & keep a lease on an interface, running '
        'configurable hooks to assign the obtained IP address and gw to it.',
        epilog='Send a SIGUSR1 to renew the current lease, SIGUSR2 to rebind '
        'it and SIGHUP to reset & get a new lease.',
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    psr.add_argument(
        'interface', help='The interface to request an address for.'
    )
    psr.add_argument(
        '--lease-type',
        help='Class to use for leases. '
        'Must be a subclass of `pyroute2.dhcp.leases.Lease`.',
        type=str,
        default='pyroute2.dhcp.leases.JSONFileLease',
        metavar='dotted.name',
    )
    psr.add_argument(
        '--hook',
        help='Hooks to load. '
        'These are used to run async python code when, '
        'for example, renewing or expiring a lease. '
        'Defaults to adding & removing ip & gateway.',
        action='append',
        type=str,
        metavar='dotted.name',
    )
    psr.add_argument(
        '--disable-hooks',
        help='Disable all hooks.',
        default=False,
        action='store_true',
    )
    psr.add_argument(
        '-x',
        '--exit-on-timeout',
        metavar='N',
        help='Wait for max N seconds for a lease, '
        'exit if none could be obtained.',
        type=float,
    )
    psr.add_argument(
        '--log-level',
        help='Logging level to use.',
        choices=('DEBUG', 'INFO', 'WARNING', 'ERROR'),
        default='INFO',
    )
    psr.add_argument(
        '-p',
        '--write-pidfile',
        default=False,
        action='store_true',
        help='Write a pid file in the working directory. ',
    )
    psr.add_argument(
        '-R',
        '--no-release',
        default=False,
        action='store_true',
        help='Do not send a DHCPRELEASE on exit.',
    )
    # TODO: add options for parameters, retransmission, timeouts...
    return psr


async def run_client(
    cfg: ClientConfig, exit_timeout: Optional[float] = None
) -> None:
    '''Run the client until interrupted, or a timeout occurs.

    The optional `exit_timeout` controls 2 things when provided:
    - How long to wait for the interface to be up
    - How long to wait for the client to be bound when starting up
    '''

    acli = AsyncDHCPClient(cfg)

    async with InterfaceStateWatcher(cfg.interface) as iface_watcher:
        while True:
            # Open the socket, read existing lease, etc
            if iface_watcher.state != 'up':
                LOG.info('Waiting for %s to go up...', cfg.interface)
            await asyncio.wait_for(
                iface_watcher.up.wait(), timeout=exit_timeout
            )
            async with acli:
                # Bootstrap the client by sending a DISCOVER or a REQUEST
                await acli.bootstrap()
                if exit_timeout:
                    # Wait a bit for a lease, and raise if we have none
                    await acli.wait_for_state(
                        State.BOUND, timeout=exit_timeout
                    )
                    break
                await iface_watcher.down.wait()
                LOG.warning('%s went down', cfg.interface)


async def main() -> None:
    psr = get_psr()
    args = psr.parse_args()
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(name)s:%(funcName)s] %(message)s'
    )
    logging.getLogger('pyroute2.dhcp').setLevel(args.log_level)

    LOG.setLevel(args.log_level)

    # parse lease type
    lease_type = import_dotted_name(args.lease_type)
    if not (isinstance(lease_type, type) and issubclass(lease_type, Lease)):
        psr.error(f'{args.lease_type!r} must point to a Lease subclass.')

    # parse hooks
    hooks: list[Hook] = []
    if not args.disable_hooks:
        if not args.hook:
            args.hook = [
                'pyroute2.dhcp.hooks.configure_ip',
                'pyroute2.dhcp.hooks.add_default_gw',
                'pyroute2.dhcp.hooks.remove_default_gw',
                'pyroute2.dhcp.hooks.remove_ip',
            ]
        LOG.debug('Configured hooks:')
        for dotted_hook_name in args.hook:
            hook = import_dotted_name(dotted_hook_name)
            if not isinstance(hook, Hook):
                psr.error(f'{dotted_hook_name!r} must point to a valid hook.')
            hooks.append(hook)
            LOG.debug("- %s", hook.name)

    # Create configuration
    cfg = ClientConfig(
        interface=args.interface,
        lease_type=lease_type,
        hooks=hooks,
        write_pidfile=args.write_pidfile,
        release=not args.no_release,
        handle_signals=True,
    )

    try:
        await run_client(cfg, exit_timeout=args.exit_on_timeout)
    except InterfaceNotFound as err:
        psr.error(f"Interface not found: {err}")


def run():
    # for the setup.cfg entrypoint
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':  # pragma: no cover
    run()
