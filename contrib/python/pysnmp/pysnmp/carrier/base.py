#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# Copyright (C) 2024, LeXtudio Inc. <support@lextudio.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from typing import Any, Callable

from pysnmp.carrier import error


class TimerCallable:
    """Timer callable object."""

    __callback: Callable
    __next_call: float
    __call_interval: float

    def __init__(self, cbFun: Callable, callInterval: float):
        """Create a timer callable object."""
        self.__callback = cbFun
        self.__next_call = 0

        self.__call_interval = callInterval

    def __call__(self, timeNow: float):
        """Call the callback function if the time is right."""
        if self.__next_call <= timeNow:
            self.__callback(timeNow)
            self.__next_call = timeNow + self.interval

    def __eq__(self, cbFun: Callable):
        """Return True if the callback function is the same."""
        return self.__callback == cbFun

    def __ne__(self, cbFun: Callable):
        """Return True if the callback function is not the same."""
        return self.__callback != cbFun

    @property
    def interval(self):
        """Return the call interval."""
        return self.__call_interval

    @interval.setter
    def interval(self, callInterval: float):
        self.__call_interval = callInterval


class AbstractTransportAddress:
    """Abstract transport address interface."""

    _local_address = None

    def set_local_address(self, s):
        """Set the local address."""
        self._local_address = s
        return self

    def get_local_address(self):
        """Return the local address."""
        return self._local_address

    def clone(self, localAddress=None):
        """Clone the address."""
        return self.__class__(self).set_local_address(
            localAddress is None and self.get_local_address() or localAddress
        )


class AbstractTransport:
    """Abstract transport interface."""

    PROTO_TRANSPORT_DISPATCHER = None
    ADDRESS_TYPE = AbstractTransportAddress
    _callback_function = None

    @classmethod
    def is_compatible_with_dispatcher(
        cls, transportDispatcher: "AbstractTransportDispatcher"
    ):
        """Return True if the transport dispatcher is compatible."""
        if cls.PROTO_TRANSPORT_DISPATCHER is None:
            raise error.CarrierError(
                f"Protocol transport dispatcher not specified for {cls}"
            )
        return isinstance(transportDispatcher, cls.PROTO_TRANSPORT_DISPATCHER)

    def register_callback(self, cbFun):
        """Register the callback function."""
        if self._callback_function:
            raise error.CarrierError(
                f"Callback function {self._callback_function} already registered at {self}"
            )
        self._callback_function = cbFun

    def unregister_callback(self):
        """Unregister the callback function."""
        self._callback_function = None

    def close_transport(self):
        """Close the transport."""
        self.unregister_callback()

    # Public API

    def open_client_mode(self, iface: "tuple[str, int] | None" = None):
        """Open client mode."""
        raise error.CarrierError("Method not implemented")

    def open_server_mode(self, iface: "tuple[str, int]"):
        """Open server mode."""
        raise error.CarrierError("Method not implemented")

    def send_message(self, outgoingMessage, transportAddress: AbstractTransportAddress):
        """Send a message to the transport."""
        raise error.CarrierError("Method not implemented")


class AbstractTransportDispatcher:
    """Abstract transport dispatcher interface."""

    __transports: "dict[tuple[int, ...], AbstractTransport]"
    __transport_domain_map: "dict[AbstractTransport, tuple[int, ...]]"
    __recv_callables: "dict['tuple[int, ...] | str | None', Callable]"
    __timer_callables: "list[TimerCallable]"
    __ticks: int
    __timer_resolution: float
    __timer_delta: float
    __next_time: float
    __routing_callback: "Callable[[tuple[int, ...], AbstractTransportAddress, Any], 'tuple[int, ...]'] | None"  # fix message type

    def __init__(self):
        """Create a transport dispatcher object."""
        self.__transports = {}
        self.__transport_domain_map = {}
        self.__jobs = {}
        self.__recv_callables = {}
        self.__timer_callables = []
        self.__ticks = 0
        self.__timer_resolution = 0.1
        self.__timer_delta = self.__timer_resolution * 0.05
        self.__next_time = 0
        self.__routing_callback = None

    def _callback_function(
        self,
        incomingTransport: AbstractTransport,
        transportAddress: AbstractTransportAddress,
        incomingMessage,
    ):
        if incomingTransport in self.__transport_domain_map:
            transportDomain = self.__transport_domain_map[incomingTransport]
        else:
            raise error.CarrierError(f"Unregistered transport {incomingTransport}")

        if self.__routing_callback:
            recvId = self.__routing_callback(
                transportDomain, transportAddress, incomingMessage
            )
        else:
            recvId = None

        if recvId in self.__recv_callables:
            self.__recv_callables[recvId](
                self, transportDomain, transportAddress, incomingMessage
            )
        else:
            raise error.CarrierError(
                f'No callback for "{recvId!r}" found - losing incoming event'
            )

    # Dispatcher API

    def register_routing_callback(
        self,
        routingCbFun: "Callable[[tuple[int, ...], AbstractTransportAddress, Any], 'tuple[int, ...]'] | None",
    ):
        """Register a routing callback."""
        if self.__routing_callback:
            raise error.CarrierError("Data routing callback already registered")
        self.__routing_callback = routingCbFun

    def unregister_routing_callback(self):
        """Unregister a routing callback."""
        if self.__routing_callback:
            self.__routing_callback = None

    def register_recv_callback(
        self, recvCb, recvId: "tuple[int, ...] | str | None" = None
    ):
        """Register a receive callback."""
        if recvId in self.__recv_callables:
            raise error.CarrierError(
                "Receive callback {!r} already registered".format(
                    recvId is None and "<default>" or recvId
                )
            )
        self.__recv_callables[recvId] = recvCb

    def unregister_recv_callback(self, recvId: "tuple[int, ...] | None" = None):
        """Unregister a receive callback."""
        if recvId in self.__recv_callables:
            del self.__recv_callables[recvId]

    def register_timer_callback(
        self, timerCbFun: Callable, tickInterval: "float | None" = None
    ):
        """Register a timer callback."""
        if not tickInterval:
            tickInterval = self.__timer_resolution
        self.__timer_callables.append(TimerCallable(timerCbFun, tickInterval))

    def unregister_timer_callback(self, timerCbFun: "TimerCallable | None" = None):
        """Unregister a timer callback."""
        if timerCbFun:
            self.__timer_callables.remove(timerCbFun)
        else:
            self.__timer_callables = []

    def register_transport(
        self, tDomain: "tuple[int, ...]", transport: AbstractTransport
    ):
        """Register a transport."""
        if tDomain in self.__transports:
            raise error.CarrierError(f"Transport {tDomain} already registered")
        transport.register_callback(self._callback_function)
        self.__transports[tDomain] = transport
        self.__transport_domain_map[transport] = tDomain

    def unregister_transport(self, tDomain: "tuple[int, ...]"):
        """Unregister a transport."""
        if tDomain not in self.__transports:
            raise error.CarrierError(f"Transport {tDomain} not registered")
        self.__transports[tDomain].unregister_callback()
        del self.__transport_domain_map[self.__transports[tDomain]]
        del self.__transports[tDomain]

    def get_transport(self, transportDomain: "tuple[int, ...]"):
        """Return the transport object."""
        if transportDomain in self.__transports:
            return self.__transports[transportDomain]
        raise error.CarrierError(f"Transport {transportDomain} not registered")

    def send_message(
        self,
        outgoingMessage,
        transportDomain: "tuple[int, ...]",
        transportAddress: AbstractTransportAddress,
    ):
        """Send a message to the transport."""
        if transportDomain in self.__transports:
            self.__transports[transportDomain].send_message(
                outgoingMessage, transportAddress
            )
        else:
            raise error.CarrierError(
                f"No suitable transport domain for {transportDomain}"
            )

    def get_timer_resolution(self):
        """Return the timer resolution."""
        return self.__timer_resolution

    def set_timer_resolution(self, timerResolution: float):
        """Set the timer resolution."""
        if timerResolution < 0.01 or timerResolution > 10:
            raise error.CarrierError("Impossible timer resolution")

        for timerCallable in self.__timer_callables:
            if timerCallable.interval == self.__timer_resolution:
                # Update periodics for default resolutions
                timerCallable.interval = timerResolution

        self.__timer_resolution = timerResolution
        self.__timer_delta = timerResolution * 0.05

    def get_timer_ticks(self):
        """Return the number of timer ticks."""
        return self.__ticks

    def handle_timer_tick(self, timeNow: float):
        """Handle timer tick."""
        if self.__next_time == 0:  # initial initialization
            self.__next_time = timeNow + self.__timer_resolution - self.__timer_delta

        if self.__next_time >= timeNow:
            return

        self.__ticks += 1
        self.__next_time = timeNow + self.__timer_resolution - self.__timer_delta

        for timerCallable in self.__timer_callables:
            timerCallable(timeNow)

    def job_started(self, jobId, count: int = 1):
        """Mark a job as started."""
        if jobId in self.__jobs:
            self.__jobs[jobId] += count
        else:
            self.__jobs[jobId] = count

    def job_finished(self, jobId, count: int = 1):
        """Mark a job as finished."""
        self.__jobs[jobId] -= count
        if self.__jobs[jobId] == 0:
            del self.__jobs[jobId]

    def jobs_are_pending(self):
        """Return True if there are pending jobs."""
        return bool(self.__jobs)

    def run_dispatcher(self, timeout: float = 0.0):
        """Run the dispatcher."""
        raise error.CarrierError("Method not implemented")

    def close_dispatcher(self):
        """Close the dispatcher."""
        for tDomain in list(self.__transports):
            self.__transports[tDomain].close_transport()
            self.unregister_transport(tDomain)
        self.__transports.clear()
        self.unregister_recv_callback()
        self.unregister_timer_callback()
