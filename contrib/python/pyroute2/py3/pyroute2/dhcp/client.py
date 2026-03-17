import asyncio
import errno
import os
import random
from dataclasses import dataclass, field
from logging import getLogger
from math import floor
from pathlib import Path
from signal import Signals
from socket import gethostname
from time import time
from typing import Callable, DefaultDict, Iterable, Iterator, Optional, Union

from pyroute2.dhcp import fsm, messages
from pyroute2.dhcp.dhcp4socket import AsyncDHCP4Socket
from pyroute2.dhcp.enums import dhcp
from pyroute2.dhcp.hooks import Hook, Trigger, run_hooks
from pyroute2.dhcp.leases import JSONFileLease, Lease
from pyroute2.dhcp.timers import LeaseTimers
from pyroute2.dhcp.xids import Xid

LOG = getLogger(__name__)


# TODO: maybe move retransmission stuff to its own file
Retransmission = Callable[[], Union[Iterator[int], Iterator[float]]]


def randomized_increasing_backoff(
    wait_first: float = 4.0, wait_max: float = 32.0, factor: float = 2.0
) -> Iterator[float]:
    '''Yields seconds to wait until the next retry, forever.'''
    delay = wait_first
    while True:
        yield delay
        if delay <= wait_max:
            delay = min(random.uniform(delay, delay * factor), wait_max)


@dataclass
class ClientConfig:
    '''Stores configuration option for the DHCP client.'''

    # FIXME: this is probably not enough if we want the client to work with
    # WLAN interfaces, as we would have to handle the SSID somehow ?
    # The interface to bind to and obtain a lease for.
    interface: str
    # The lease type to use, allows flexibility on where and how to store it.
    lease_type: type[Lease] = JSONFileLease
    # A list of hooks that will be called when the required triggers are met.
    hooks: Iterable[Hook] = ()
    # The DHCP parameters requested by the client.
    requested_parameters: Iterable[dhcp.Option] = (
        dhcp.Option.SUBNET_MASK,
        dhcp.Option.ROUTER,
        dhcp.Option.NAME_SERVER,
        dhcp.Option.DOMAIN_NAME,
        dhcp.Option.LEASE_TIME,
        dhcp.Option.RENEWAL_TIME,
        dhcp.Option.REBINDING_TIME,
    )
    # Timeouts for various client states.
    # If the client reaches the specified amount of seconds in one of the
    # configured states, it will reset the lease process and start anew.
    timeouts: dict[fsm.State, int] = field(
        default_factory=lambda: {
            # No use staying too long in REBOOTING state if nobody is answering
            fsm.State.REBOOTING: 10,
            # When we get an OFFER, how long should we wait for an ACK ?
            fsm.State.REQUESTING: 30,
        }
    )
    # FIXME: we send too many retries according to the RFC
    # In both RENEWING and REBINDING states, if the client receives no
    # response to its DHCPREQUEST message, the client SHOULD wait one-half
    # of the remaining time until T2 (in RENEWING state) and one-half of
    # the remaining lease time (in REBINDING state), down to a minimum of
    # 60 seconds, before retransmitting the DHCPREQUEST message.
    retransmission: Retransmission = randomized_increasing_backoff
    # Whether to write a pidfile in the working directory
    write_pidfile: bool = False
    # Send a DHCPRELEASE on client exit
    release: bool = True
    # Maximum execution duration for a single hook
    hook_timeout: Optional[float] = 2.0
    # Custom client id. The mac is used as the client id if not provided.
    client_id: Optional[bytes] = None
    # Whether to handle USR1 (renew), USR2 (rebind) and HUP (reset)
    handle_signals: bool = True
    # optional vendor_id & hostname options included in requests
    vendor_id: Optional[str] = 'pyroute2'
    host_name: Optional[str] = field(default_factory=gethostname)

    @property
    def pidfile_path(self) -> Path:
        '''Where to write the pid file. It's named after the interface.'''
        return (
            Path.cwd()
            .joinpath(self.interface)
            .with_name(f"{self.interface}.pid")
        )


class AsyncDHCPClient:
    '''A DHCP client based on pyroute2.

    Implemented as an async context manager running a finite state machine.

    The client will try to acquire and keep a lease as long as it's running.

    It can run hooks specified in its configuration to perform various
    actions such as adding an IP address to an interface, configuring routing,
    when the specified events are triggered (see the `hook()` decorator.)

    Exiting the context manager causes the lease to be released and relevant
    hooks to be run.

    Example usage::

        from pyroute2.dhcp.client import AsyncDHCPClient, ClientConfig
        from pyroute2.dhcp.fsm import State

        cfg = ClientConfig(interface='eth3')
        async with AsyncDHCPClient(cfg) as client:
            # Bootstrap the client by sending a DISCOVER or a REQUEST
            await client.bootstrap()
            # Wait 20s until the client gets a lease
            await client.wait_for_state(State.BOUND, timeout=20.0)

    '''

    def __init__(self, config: ClientConfig) -> None:
        self.config = config
        # The raw socket used to send and receive packets
        self._sock: AsyncDHCP4Socket = AsyncDHCP4Socket(self.config.interface)
        # Current client state
        self._state: fsm.State = fsm.State.OFF
        # Current lease, read from persistent storage or sent by a server
        self._lease: Optional[Lease] = None
        # dhcp messages put in this queue are sent by _send_forever
        self._sendq: asyncio.Queue[Optional[messages.SentDHCPMessage]] = (
            asyncio.Queue()
        )
        # Handle to run _send_forever for the context manager's lifetime
        self._sender_task: Optional[asyncio.Task] = None
        # Handle to run _recv_forever for the context manager's lifetime
        self._receiver_task: Optional[asyncio.Task] = None
        # Timers to run callbacks on lease timeouts expiration
        self.lease_timers = LeaseTimers()
        # Calls reset() after a timeout in some states to avoid getting stuck
        self._state_watchdog: asyncio.Task | None = None
        # Allows to easily track the state when running the client from python
        self._states: DefaultDict[fsm.State, asyncio.Event] = DefaultDict(
            asyncio.Event
        )
        # Used to compute lease times, taking into account the request time
        self.last_state_change: float = time()
        self._xid: Optional[Xid] = None

    # 'public api'

    async def wait_for_state(
        self, state: fsm.State, timeout: Optional[float] = None
    ) -> None:
        '''Waits until the client is in the target state.'''
        try:
            await asyncio.wait_for(self._states[state].wait(), timeout=timeout)
        except asyncio.exceptions.TimeoutError as err:
            raise asyncio.exceptions.TimeoutError(
                f'Timed out waiting for the {state.name} state. '
                f'Current state: {self.state.name}'
            ) from err

    @fsm.state_guard(fsm.State.INIT, fsm.State.INIT_REBOOT)
    async def bootstrap(self) -> None:
        '''Send a `DISCOVER` or a `REQUEST`,

        depending on whether we're initializing or rebooting.

        Use this to get a lease when running the client from Python code.
        '''
        if self.state is fsm.State.INIT:
            # send discover
            await self.transition(
                to=fsm.State.SELECTING,
                send=messages.discover(
                    parameter_list=self.config.requested_parameters
                ),
            )
        elif self.state is fsm.State.INIT_REBOOT:
            assert self.lease, 'cannot init_reboot without a lease'
            # send request for lease
            await self.transition(
                to=fsm.State.REBOOTING,
                send=messages.request_for_lease(
                    parameter_list=self.config.requested_parameters,
                    lease=self.lease,
                    state=fsm.State.REBOOTING,
                ),
            )
        # the decorator prevents the needs for an else

    # properties

    @property
    def lease(self) -> Optional[Lease]:
        '''The current lease, if we have one.'''
        return self._lease

    @lease.setter
    def lease(self, value: Lease) -> None:
        '''Set a fresh lease; only call this when a server grants one.'''

        self._lease = value
        self.lease_timers.arm(
            lease=self._lease,
            renewal=self._renew,
            rebinding=self._rebind,
            expiration=self._expire_lease,
        )
        # whatever error might happen when writing a lease,
        # it should never make the client crash
        try:
            self._lease.dump()
        except Exception as exc:
            LOG.error('Could not dump lease: %s', exc)

    @property
    def state(self) -> fsm.State:
        '''The current client state.'''
        return self._state

    @state.setter
    def state(self, value: fsm.State) -> None:
        '''Check the client can transition to the state, and set it.

        Only supposed to be called by the client internally.
        '''
        old_state = self.state
        if value and value not in fsm.TRANSITIONS[old_state]:
            raise ValueError(
                f'Cannot transition from {self._state.name} to {value.name}'
            )
        LOG.info('%s -> %s', old_state.name, value.name)
        if self._state_watchdog:
            self._state_watchdog.cancel()
            self._state_watchdog = None
        if old_state in self._states:
            self._states[old_state].clear()
        self._state = value
        self.last_state_change = time()
        self._states[value].set()
        if state_timeout := self.config.timeouts.get(value):
            self._state_watchdog = asyncio.Task(
                self.reset(delay=state_timeout)
            )

    @property
    def xid(self) -> Xid:
        '''The xid currently in use by the client.

        Only available when the context manager is running.
        '''
        if self._xid is None:
            raise AttributeError('xid is not defined')
        return self._xid

    # Timer callbacks

    @fsm.state_guard(fsm.State.BOUND)
    async def _renew(self, signal: Optional[Signals] = None) -> None:
        '''Called when the renewal time defined in the lease expires.'''
        assert self.lease, 'cannot renew without an existing lease'
        LOG.info(
            '%s, renewing lease',
            f'{signal.name} received' if signal else 'T1 expired',
        )
        self.lease_timers._reset_timer('renewal')  # FIXME should be automatic
        await self.transition(
            to=fsm.State.RENEWING,
            send=messages.request_for_lease(
                parameter_list=self.config.requested_parameters,
                lease=self.lease,
                state=fsm.State.RENEWING,
            ),
        )

    @fsm.state_guard(fsm.State.RENEWING, fsm.State.BOUND)
    async def _rebind(self, signal: Optional[Signals] = None) -> None:
        '''Called when the rebinding time defined in the lease expires.'''
        assert self.lease, 'cannot rebind without an existing lease'
        LOG.info(
            '%s, rebinding lease',
            f'{signal.name} received' if signal else 'T2 expired',
        )
        # if the user asks for a rebind before the renewal timer expires,
        # better cancel the renewal timer too
        self.lease_timers._reset_timer('renewal')
        self.lease_timers._reset_timer('rebinding')  # FIXME
        await self.transition(
            to=fsm.State.REBINDING,
            send=messages.request_for_lease(
                parameter_list=self.config.requested_parameters,
                lease=self.lease,
                state=fsm.State.REBINDING,
            ),
        )

    async def _expire_lease(self) -> None:
        '''Called when the expiration time defined in the lease expires.'''
        LOG.info('Lease expired')
        self.lease_timers._reset_timer('expiration')
        await self._run_hooks(Trigger.EXPIRED)
        await self.reset()

    # DHCP packet sending & receiving coroutines

    async def _send_message(self, msg: messages.SentDHCPMessage) -> None:
        '''Set secs, xid & client id on the message, and send it.'''

        # Set secs to the time elapsed since the last state change
        # (max 16 bits)
        msg.dhcp['secs'] = min(floor(time() - self.last_state_change), 0xFFFF)
        msg.dhcp['xid'] = self.xid.for_state(self.state)

        # set the client id, rfc says:
        # A hardware type of 0 (zero) should be used when the value field
        # contains an identifier other than a hardware address (e.g. a fully
        # qualified domain name).
        if self.config.client_id:
            client_id = {'type': 0, 'key': self.config.client_id}
        else:
            # default behavior, use hw type & hw addr as client id
            client_id = {'type': 1, 'key': self._sock.l2addr}
        msg.dhcp['options']['client_id'] = client_id
        # set hostname & vendor id if configured to do so
        if self.config.host_name:
            msg.dhcp['options']['host_name'] = self.config.host_name
        if self.config.vendor_id:
            msg.dhcp['options']['vendor_id'] = self.config.vendor_id
        LOG.info('Sending %s', msg)
        await self._sock.put(msg)

    async def _send_forever(self) -> None:
        '''Send packets from `_sendq` until the client is in `State.OFF`.'''
        msg_to_send: Optional[messages.SentDHCPMessage] = None
        # Called to get the interval value below
        interval_factory: Optional[Union[Iterator[int], Iterator[float]]] = (
            None
        )
        # How long to sleep before retrying
        interval: Union[int, float] = 1

        # this is triggered by __aexit__
        wait_til_off = asyncio.Task(self.wait_for_state(fsm.State.OFF))
        while not (wait_til_off.done() and self._sendq.empty()):

            # FIXME: interval handling is pretty awkward and convoluted
            if interval_factory:
                interval = next(interval_factory)
            else:
                interval = 9999999

            wait_for_msg_to_send = asyncio.Task(
                self._sendq.get(), name='wait for packet to send'
            )
            if msg_to_send:
                LOG.debug('%.1f seconds until retransmission', interval)
            done, pending = await asyncio.wait(
                (wait_til_off, wait_for_msg_to_send),
                return_when=asyncio.FIRST_COMPLETED,
                timeout=interval,
            )
            if wait_for_msg_to_send in done:
                if msg_to_send := wait_for_msg_to_send.result():
                    # There is a new message to send, reset the interval
                    interval_factory = self.config.retransmission()
                else:
                    # No need to retry anything
                    interval_factory = None
            elif wait_for_msg_to_send in pending:
                wait_for_msg_to_send.cancel()
            if msg_to_send:
                if (
                    msg_to_send.message_type != dhcp.MessageType.RELEASE
                    and wait_til_off.done()
                ):
                    LOG.debug(
                        'Not sending %s, client is shutting down',
                        msg_to_send.message_type.name,
                    )
                    continue
                try:
                    await self._send_message(msg_to_send)
                except OSError as err:
                    # That happens when the interface goes down.
                    # In these cases, the client is supposed to be restarted
                    if err.errno == errno.ENETDOWN:
                        LOG.error('Could not send, network is down')
                        return
                    raise

    async def _recv_forever(self) -> None:
        '''Receive & process DHCP messages until the client stops.'''

        # TODO: is there a better way to wait for the client to stop ?
        wait_til_off = asyncio.Task(self.wait_for_state(fsm.State.OFF))

        while not wait_til_off.done():
            wait_for_received_msg = asyncio.Task(
                coro=self._sock.get(),
                name=f'wait for DHCP messages on {self.config.interface}',
            )
            # sleep until a new message is received, or the client is OFF
            done, pending = await asyncio.wait(
                (wait_til_off, wait_for_received_msg),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if wait_for_received_msg in done:
                try:
                    received_msg = wait_for_received_msg.result()
                except OSError as err:
                    # That happens when the interface goes down.
                    # In these cases, the client is supposed to be restarted
                    if err.errno == errno.ENETDOWN:
                        LOG.error('Could not recv, network is down')
                        return
                    raise
                await self._process_msg(received_msg)

            elif wait_for_received_msg in pending:
                wait_for_received_msg.cancel()

    # Callbacks for received DHCP messages

    @fsm.state_guard(
        fsm.State.REQUESTING,
        fsm.State.REBOOTING,
        fsm.State.REBINDING,
        fsm.State.RENEWING,
    )
    async def ack_received(self, msg: messages.ReceivedDHCPMessage) -> None:
        '''Called when an ACK is received.

        Stores the lease and puts the client in the BOUND state.
        '''
        # FIXME: according to the RFC:
        # When the client receives a DHCPACK from the server, the client
        # computes the lease expiration time as the sum of the time at which
        # the client sent the DHCPREQUEST message and the duration of the lease
        # in the DHCPACK message.

        # The state the client was in when sending the message
        request_state = msg.xid.request_state

        if request_state in (fsm.State.REQUESTING, fsm.State.REBOOTING):
            trigger = Trigger.BOUND
        elif request_state == fsm.State.RENEWING:
            trigger = Trigger.RENEWED
        elif request_state == fsm.State.REBINDING:
            trigger = Trigger.REBOUND
        else:
            LOG.warning('Invalid request state for xid %s', msg.xid)
            return

        if 'lease_time' not in msg.dhcp['options']:
            # that would not make sense at all, but still...
            LOG.warning('Server did not define a lease time, ignoring ACK.')
            return

        self.lease = self.config.lease_type(
            ack=msg.dhcp,
            interface=self.config.interface,
            server_mac=msg.eth_src,
        )
        LOG.info(
            'Got lease for %s from %s (%s)',
            self.lease.ip,
            self.lease.server_id,
            msg.eth_src,
        )
        await self.transition(to=fsm.State.BOUND)
        await self._run_hooks(trigger)

    @fsm.state_guard(
        fsm.State.REQUESTING,
        fsm.State.REBOOTING,
        fsm.State.RENEWING,
        fsm.State.REBINDING,
    )
    async def nak_received(self, msg: messages.ReceivedDHCPMessage) -> None:
        '''Called when a NAK is received.

        Resets the client and starts looking for a new IP.
        '''
        await self.reset()

    @fsm.state_guard(fsm.State.SELECTING)
    async def offer_received(self, msg: messages.ReceivedDHCPMessage) -> None:
        '''Called when an OFFER is received.

        Sends a REQUEST for the offered IP address.
        '''
        # FIXME: we should probably validate the offer some more,
        # like checking if the offered options are satisfying
        await self.transition(
            to=fsm.State.REQUESTING,
            send=messages.request_for_offer(
                parameter_list=self.config.requested_parameters, offer=msg
            ),
        )

    # Async context manager methods

    async def __aenter__(self) -> 'AsyncDHCPClient':
        '''Set up the client so it's ready to obtain an IP.

        Tries to load a lease for the client's interface,
        opens the socket, starts the sender & receiver tasks
        and allocates a request ID.
        '''
        if self.config.handle_signals:
            self._register_signal_handlers()
        self._xid = Xid()
        if self.config.write_pidfile:
            self.config.pidfile_path.write_text(str(os.getpid()))
            LOG.debug('Wrote pidfile to %s', self.config.pidfile_path)
        if loaded_lease := self.config.lease_type.load(self.config.interface):
            self._lease = loaded_lease
            self.state = fsm.State.INIT_REBOOT
        else:
            LOG.debug('No current lease')
            self.state = fsm.State.INIT
        await self._sock.__aenter__()

        self._receiver_task = asyncio.Task(
            self._recv_forever(),
            name=f'Listen for DHCP packets on {self.config.interface}',
        )
        self._sender_task = asyncio.Task(
            self._send_forever(),
            name=f'Send outgoing DHCP packets on {self.config.interface}',
        )
        return self

    async def __aexit__(self, *_) -> None:
        '''Shut down the client.

        If there's an active lease, send a RELEASE for it first.
        '''
        if self.config.handle_signals:
            self._remove_signal_handlers()
        self.lease_timers.cancel()
        if self.lease and self.config.release:
            await self._run_hooks(Trigger.UNBOUND)
            if not self.lease.expired:
                await self._sendq.put(messages.release(lease=self.lease))
        self.state = fsm.State.OFF
        if self._sender_task:
            await self._sender_task
        if self._receiver_task:
            await self._receiver_task
        await self._sock.__aexit__()
        self._xid = None
        if self.config.write_pidfile:
            self.config.pidfile_path.unlink(missing_ok=True)
            LOG.debug('Removed pidfile at %s', self.config.pidfile_path)

    # internal methods

    async def transition(
        self, to: fsm.State, send: Optional[messages.SentDHCPMessage] = None
    ) -> None:
        '''Change the client's state, and start sending a message repeatedly.

        If the message is None, any current message will stop being sent.
        '''
        self.state = to
        await self._sendq.put(send)

    async def reset(
        self, delay: float = 0.0, signal: Optional[Signals] = None
    ) -> None:
        '''Called internally to restart the lease acquisition process.

        - When the client receives a NAK;
        - When it spends too much time in a configured state
          (see `ClientConfig.timeouts`)
        - When the current lease expires.
        - Upon receiving a SIGHUP

        Erases the lease, cancel timers, resets the xid, and sends
        DISCOVERs to get a new lease.
        '''
        if delay:
            await asyncio.sleep(delay=delay)
            LOG.warning('Resetting after %.1f seconds', delay)
        if signal:
            LOG.info('%s received, resetting', signal.name)
        await self.transition(to=fsm.State.INIT)
        # Reset lease & timers and start again
        self._lease = None
        self.lease_timers.cancel()
        self._xid = Xid()
        await self.bootstrap()

    async def _run_hooks(self, trigger: Trigger) -> None:
        '''Run hooks for the given trigger.

        Hooks are awaited and not run in the background; that means they could
        cause issues if they run for too long. Hence, each hook is limited to
        the `hook_timeout` config setting.
        '''
        assert self.lease, 'tried to run hooks without a lease'
        await run_hooks(
            hooks=self.config.hooks,
            lease=self.lease,
            trigger=trigger,
            timeout=self.config.hook_timeout,
        )

    async def _process_msg(self, msg: messages.ReceivedDHCPMessage) -> None:
        '''Processes a received DHCP message.

        The messages's xid is checked against the client's xid, and the
        appropriate async handler (`self.<msg type>_received`)
        is then called if it exists.
        '''
        msg_type = dhcp.MessageType(msg.dhcp['options']['message_type'])
        LOG.info('Received %s', msg)
        if not self.xid.matches(msg.xid):
            LOG.error(
                'Incorrect xid %s (expected %sX), discarding',
                msg.xid,
                hex(self.xid.random_part)[:-1],
            )
            return
        handler_name = f'{msg_type.name.lower()}_received'
        handler = getattr(self, handler_name, None)
        if not handler:
            LOG.debug('DHCP %s messages are not handled', msg_type.name)
        else:
            await handler(msg)

    def _add_signal_handler(
        self,
        loop: asyncio.AbstractEventLoop,
        signal: Signals,
        callback: fsm.AsyncCallback,
    ):
        '''Arranges for `callback` to be called when receiving `signal`.

        `callback` must take a `signal` keyword argument.
        '''
        loop.add_signal_handler(
            sig=signal,
            callback=lambda: asyncio.create_task(callback(signal=signal)),
        )

    def _register_signal_handlers(self):
        '''Add signal handlers to catch USR1, USR2 and HUP.

        Called by the context manager when `handle_signals` is True.

        - SIGUSR1 causes a renew,
        - SIGUSR2 causes a rebind,
        - SIGHUP causes a reset.
        '''
        LOG.info('Registering signal handlers for USR1, USR2 and HUP')
        loop = asyncio.get_running_loop()
        self._add_signal_handler(loop, Signals.SIGUSR1, self._renew)
        self._add_signal_handler(loop, Signals.SIGUSR2, self._rebind)
        self._add_signal_handler(loop, Signals.SIGHUP, self.reset)

    def _remove_signal_handlers(self):
        remove_handler = asyncio.get_running_loop().remove_signal_handler
        LOG.info('Removing signal handlers for USR1, USR2 and HUP')
        remove_handler(Signals.SIGUSR1)
        remove_handler(Signals.SIGUSR2)
        remove_handler(Signals.SIGHUP)
