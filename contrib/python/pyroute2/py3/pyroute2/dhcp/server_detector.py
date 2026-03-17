import argparse
import asyncio
import dataclasses
import json
import logging
import time
from asyncio.exceptions import CancelledError, TimeoutError
from secrets import SystemRandom
from typing import AsyncGenerator, Iterable

from pyroute2.dhcp import messages
from pyroute2.dhcp.dhcp4socket import AsyncDHCP4Socket
from pyroute2.dhcp.enums.dhcp import Option

DHCPResponse = tuple[str, messages.ReceivedDHCPMessage]

LOG = logging.getLogger('dhcp-server-detector')

DEFAULT_PARAMETERS = (
    Option.SUBNET_MASK,
    Option.ROUTER,
    Option.BROADCAST_ADDRESS,
    Option.NAME_SERVER,
)


class DHCPServerDetector:
    '''Sends `DISCOVER`s on interfaces and listens for responses.'''

    def __init__(
        self,
        *interfaces: str,
        duration: float = 25.0,
        interval: float = 4.0,
        requested_parameters: messages.Parameters = DEFAULT_PARAMETERS,
        sport: int = 68,
    ):
        self.interfaces = interfaces
        self.interval = interval
        self.duration = duration
        self.sport = sport
        # The DISCOVERs that will be sent repeatedly per interface
        self.discover_messages = self._make_discover_msgs(
            interfaces=self.interfaces, params=requested_parameters
        )
        # All received responses are put here by the dedicated tasks,
        # along with the name of the interface they were received on.
        self._responses_queue: asyncio.Queue[DHCPResponse] = asyncio.Queue()

    @classmethod
    def _make_discover_msgs(
        cls, interfaces: Iterable[str], params: messages.Parameters
    ) -> dict[str, messages.SentDHCPMessage]:
        '''Generate DISCOVERs with a different xid for each interface .'''
        msgs: dict[str, messages.SentDHCPMessage] = {}
        rand = SystemRandom().randint
        for i in interfaces:
            msgs[i] = messages.discover(parameter_list=params)
            # use a different xid per interface
            msgs[i].dhcp['xid'] = rand(0xFF, 0xFFFFFFFF)
        return msgs

    async def _send_forever(self, sock: AsyncDHCP4Socket):
        '''Send the `DISCOVER` message at `interval` until cancelled.'''
        msg = self.discover_messages[sock.ifname]
        while True:
            LOG.info('[%s] -> %s', sock.ifname, msg)
            try:
                await sock.put(msg)
                await asyncio.sleep(self.interval)
            except CancelledError:
                break

    async def _get_offers(self, interface: str):
        '''Send DISCOVERs and wait for responses on an interface.'''
        expected_xids = {
            ifname: msg.dhcp['xid']
            for ifname, msg in self.discover_messages.items()
        }
        async with AsyncDHCP4Socket(ifname=interface, port=self.sport) as sock:
            send_task = asyncio.create_task(
                self._send_forever(sock), name=f'Send DISCOVERS on {interface}'
            )
            # if send_task returns, that means _send_forever crashed
            while not send_task.done():
                try:
                    get_next_msg = asyncio.Task(
                        sock.get(), name=f'Get message from {interface}'
                    )
                    # wait for a message and put it in the queue
                    _, pending = await asyncio.wait(
                        [send_task, get_next_msg],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    # if get is still pending, that means send_task is over
                    if get_next_msg in pending:
                        get_next_msg.cancel()
                        try:
                            await get_next_msg
                        except CancelledError:
                            pass
                        continue
                    # we have a new message
                    next_msg = await get_next_msg
                    if next_msg.dhcp['xid'] != expected_xids[sock.ifname]:
                        LOG.debug(
                            '[%s] Got %s with xid mismatch, ignoring',
                            interface,
                            next_msg.message_type.name,
                        )
                        continue
                    LOG.info('[%s] <- %s', interface, next_msg)
                    await self._responses_queue.put((interface, next_msg))
                except CancelledError:
                    LOG.debug('[%s] stop discovery', interface)
                    send_task.cancel()
                    await send_task
                    get_next_msg.cancel()
                    try:
                        await get_next_msg
                    except CancelledError:
                        pass
                    break
            else:
                await send_task

    async def detect_servers(self) -> AsyncGenerator[DHCPResponse, None]:
        '''Detect DHCP servers on `interfaces` for `duration`.

        Yields tuples of (interface name, response).
        '''
        discover_tasks = [
            asyncio.create_task(self._get_offers(i), name=i)
            for i in self.interfaces
        ]
        started = time.time()
        remaining = self.duration
        try:
            while remaining > 0 and discover_tasks:
                get_response = asyncio.create_task(self._responses_queue.get())
                try:
                    done, _ = await asyncio.wait(
                        [get_response, *discover_tasks],
                        timeout=remaining,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if get_response in done:
                        yield await get_response
                        done.remove(get_response)
                    for i in done:
                        if task_exc := i.exception():
                            LOG.error("%r: %s", i.get_name(), task_exc)
                        discover_tasks.remove(i)
                    remaining -= time.time() - started
                except TimeoutError:
                    break
                finally:
                    if not get_response.done():
                        get_response.cancel()
                        try:
                            await get_response
                        except CancelledError:
                            pass
        finally:
            for i in discover_tasks:
                i.cancel()
                await i


def get_argparser() -> argparse.ArgumentParser:
    psr = argparse.ArgumentParser(
        description='Send DHCP DISCOVER messages on the given interface(s) '
        'and collect all responses. '
        'Responses and their metadata are printed as JSON to stdout.',
        epilog='Exits successfully if at least one response was received.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    psr.add_argument(
        'interface', nargs='+', help='Interface(s) to DISCOVER on.'
    )
    psr.add_argument(
        '-d',
        '--duration',
        type=float,
        default=30.0,
        help='Number of seconds spent collecting responses.',
        metavar='SEC',
    )
    psr.add_argument(
        '-i',
        '--interval',
        type=float,
        default=4.0,
        help='Interval in seconds between DISCOVERs.',
        metavar='SEC',
    )
    psr.add_argument(
        '-s',
        '--source-port',
        type=int,
        default=68,
        help='Source port to bind to.',
        metavar='PORT',
    )
    psr.add_argument(
        '-1',
        '--exit-on-first-offer',
        action='store_true',
        default=False,
        help="Exit as soon as a response is received.",
    )
    psr.add_argument(
        '-l',
        '--log-level',
        default='WARNING',
        help="Log level. Set to INFO to log sent & received messages.",
        metavar='LEVEL',
    )
    return psr


async def main() -> int:
    '''Commandline entrypoint. Returns the number of received responses.'''
    args = get_argparser().parse_args()
    LOG.setLevel(args.log_level)
    detector = DHCPServerDetector(
        *args.interface,
        interval=args.interval,
        duration=args.duration,
        sport=args.source_port,
    )
    response_count: int = 0
    async for interface, msg in detector.detect_servers():
        response_count += 1
        print(
            json.dumps(
                {'interface': interface, 'message': dataclasses.asdict(msg)},
                indent=2,
            )
        )
        if args.exit_on_first_offer:
            break
    return response_count


def run():
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(name)s %(message)s'
    )
    # Exit on failure if there were no received responses
    exit(asyncio.run(main()) == 0)


if __name__ == '__main__':
    run()
