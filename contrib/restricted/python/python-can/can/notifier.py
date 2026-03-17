"""
This module contains the implementation of :class:`~can.Notifier`.
"""

import asyncio
import functools
import logging
import threading
import time
from collections.abc import Awaitable, Iterable
from contextlib import AbstractContextManager
from types import TracebackType
from typing import (
    Any,
    Callable,
    Final,
    NamedTuple,
    Optional,
    Union,
)

from can.bus import BusABC
from can.listener import Listener
from can.message import Message

logger = logging.getLogger("can.Notifier")

MessageRecipient = Union[Listener, Callable[[Message], Union[Awaitable[None], None]]]


class _BusNotifierPair(NamedTuple):
    bus: "BusABC"
    notifier: "Notifier"


class _NotifierRegistry:
    """A registry to manage the association between CAN buses and Notifiers.

    This class ensures that a bus is not added to multiple active Notifiers.
    """

    def __init__(self) -> None:
        """Initialize the registry with an empty list of bus-notifier pairs and a threading lock."""
        self.pairs: list[_BusNotifierPair] = []
        self.lock = threading.Lock()

    def register(self, bus: BusABC, notifier: "Notifier") -> None:
        """Register a bus and its associated notifier.

        Ensures that a bus is not added to multiple active :class:`~can.Notifier` instances.

        :param bus:
            The CAN bus to register.
        :param notifier:
            The :class:`~can.Notifier` instance associated with the bus.
        :raises ValueError:
            If the bus is already assigned to an active Notifier.
        """
        with self.lock:
            for pair in self.pairs:
                if bus is pair.bus and not pair.notifier.stopped:
                    raise ValueError(
                        "A bus can not be added to multiple active Notifier instances."
                    )
            self.pairs.append(_BusNotifierPair(bus, notifier))

    def unregister(self, bus: BusABC, notifier: "Notifier") -> None:
        """Unregister a bus and its associated notifier.

        Removes the bus-notifier pair from the registry.

        :param bus:
            The CAN bus to unregister.
        :param notifier:
            The :class:`~can.Notifier` instance associated with the bus.
        """
        with self.lock:
            registered_pairs_to_remove: list[_BusNotifierPair] = []
            for pair in self.pairs:
                if pair.bus is bus and pair.notifier is notifier:
                    registered_pairs_to_remove.append(pair)
            for pair in registered_pairs_to_remove:
                self.pairs.remove(pair)

    def find_instances(self, bus: BusABC) -> tuple["Notifier", ...]:
        """Find the :class:`~can.Notifier` instances associated with a given CAN bus.

        This method searches the registry for the :class:`~can.Notifier`
        that is linked to the specified bus. If the bus is found, the
        corresponding :class:`~can.Notifier` instances are returned. If the bus is not
        found in the registry, an empty tuple is returned.

        :param bus:
            The CAN bus for which to find the associated :class:`~can.Notifier` .
        :return:
            A tuple of :class:`~can.Notifier` instances associated with the given bus.
        """
        instance_list = []
        with self.lock:
            for pair in self.pairs:
                if bus is pair.bus:
                    instance_list.append(pair.notifier)
        return tuple(instance_list)


class Notifier(AbstractContextManager["Notifier"]):

    _registry: Final = _NotifierRegistry()

    def __init__(
        self,
        bus: Union[BusABC, list[BusABC]],
        listeners: Iterable[MessageRecipient],
        timeout: float = 1.0,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        """Manages the distribution of :class:`~can.Message` instances to listeners.

        Supports multiple buses and listeners.

        .. Note::

            Remember to call :meth:`~can.Notifier.stop` after all messages are received as
            many listeners carry out flush operations to persist data.


        :param bus:
            A :ref:`bus` or a list of buses to consume messages from.
        :param listeners:
            An iterable of :class:`~can.Listener` or callables that receive a :class:`~can.Message`
            and return nothing.
        :param timeout:
            An optional maximum number of seconds to wait for any :class:`~can.Message`.
        :param loop:
            An :mod:`asyncio` event loop to schedule the ``listeners`` in.
        :raises ValueError:
            If a passed in *bus* is already assigned to an active :class:`~can.Notifier`.
        """
        self.listeners: list[MessageRecipient] = list(listeners)
        self._bus_list: list[BusABC] = []
        self.timeout = timeout
        self._loop = loop

        #: Exception raised in thread
        self.exception: Optional[Exception] = None

        self._stopped = False
        self._lock = threading.Lock()

        self._readers: list[Union[int, threading.Thread]] = []
        _bus_list: list[BusABC] = bus if isinstance(bus, list) else [bus]
        for each_bus in _bus_list:
            self.add_bus(each_bus)

    @property
    def bus(self) -> Union[BusABC, tuple["BusABC", ...]]:
        """Return the associated bus or a tuple of buses."""
        if len(self._bus_list) == 1:
            return self._bus_list[0]
        return tuple(self._bus_list)

    def add_bus(self, bus: BusABC) -> None:
        """Add a bus for notification.

        :param bus:
            CAN bus instance.
        :raises ValueError:
            If the *bus* is already assigned to an active :class:`~can.Notifier`.
        """
        # add bus to notifier registry
        Notifier._registry.register(bus, self)

        # add bus to internal bus list
        self._bus_list.append(bus)

        file_descriptor: int = -1
        try:
            file_descriptor = bus.fileno()
        except NotImplementedError:
            # Bus doesn't support fileno, we fall back to thread based reader
            pass

        if self._loop is not None and file_descriptor >= 0:
            # Use bus file descriptor to watch for messages
            self._loop.add_reader(file_descriptor, self._on_message_available, bus)
            self._readers.append(file_descriptor)
        else:
            reader_thread = threading.Thread(
                target=self._rx_thread,
                args=(bus,),
                name=f'{self.__class__.__qualname__} for bus "{bus.channel_info}"',
            )
            reader_thread.daemon = True
            reader_thread.start()
            self._readers.append(reader_thread)

    def stop(self, timeout: float = 5.0) -> None:
        """Stop notifying Listeners when new :class:`~can.Message` objects arrive
        and call :meth:`~can.Listener.stop` on each Listener.

        :param timeout:
            Max time in seconds to wait for receive threads to finish.
            Should be longer than timeout given at instantiation.
        """
        self._stopped = True
        end_time = time.time() + timeout
        for reader in self._readers:
            if isinstance(reader, threading.Thread):
                now = time.time()
                if now < end_time:
                    reader.join(end_time - now)
            elif self._loop:
                # reader is a file descriptor
                self._loop.remove_reader(reader)
        for listener in self.listeners:
            if hasattr(listener, "stop"):
                listener.stop()

        # remove bus from registry
        for bus in self._bus_list:
            Notifier._registry.unregister(bus, self)

    def _rx_thread(self, bus: BusABC) -> None:
        # determine message handling callable early, not inside while loop
        if self._loop:
            handle_message: Callable[[Message], Any] = functools.partial(
                self._loop.call_soon_threadsafe,
                self._on_message_received,  # type: ignore[arg-type]
            )
        else:
            handle_message = self._on_message_received

        while not self._stopped:
            try:
                if msg := bus.recv(self.timeout):
                    with self._lock:
                        handle_message(msg)
            except Exception as exc:  # pylint: disable=broad-except
                self.exception = exc
                if self._loop is not None:
                    self._loop.call_soon_threadsafe(self._on_error, exc)
                    # Raise anyway
                    raise
                elif not self._on_error(exc):
                    # If it was not handled, raise the exception here
                    raise
                else:
                    # It was handled, so only log it
                    logger.debug("suppressed exception: %s", exc)

    def _on_message_available(self, bus: BusABC) -> None:
        if msg := bus.recv(0):
            self._on_message_received(msg)

    def _on_message_received(self, msg: Message) -> None:
        for callback in self.listeners:
            res = callback(msg)
            if res and self._loop and asyncio.iscoroutine(res):
                # Schedule coroutine
                self._loop.create_task(res)

    def _on_error(self, exc: Exception) -> bool:
        """Calls ``on_error()`` for all listeners if they implement it.

        :returns: ``True`` if at least one error handler was called.
        """
        was_handled = False

        for listener in self.listeners:
            if hasattr(listener, "on_error"):
                try:
                    listener.on_error(exc)
                except NotImplementedError:
                    pass
                else:
                    was_handled = True

        return was_handled

    def add_listener(self, listener: MessageRecipient) -> None:
        """Add new Listener to the notification list.
        If it is already present, it will be called two times
        each time a message arrives.

        :param listener: Listener to be added to the list to be notified
        """
        self.listeners.append(listener)

    def remove_listener(self, listener: MessageRecipient) -> None:
        """Remove a listener from the notification list. This method
        throws an exception if the given listener is not part of the
        stored listeners.

        :param listener: Listener to be removed from the list to be notified
        :raises ValueError: if `listener` was never added to this notifier
        """
        self.listeners.remove(listener)

    @property
    def stopped(self) -> bool:
        """Return ``True``, if Notifier was properly shut down with :meth:`~can.Notifier.stop`."""
        return self._stopped

    @staticmethod
    def find_instances(bus: BusABC) -> tuple["Notifier", ...]:
        """Find :class:`~can.Notifier` instances associated with a given CAN bus.

        This method searches the registry for the :class:`~can.Notifier`
        that is linked to the specified bus. If the bus is found, the
        corresponding :class:`~can.Notifier` instances are returned. If the bus is not
        found in the registry, an empty tuple is returned.

        :param bus:
            The CAN bus for which to find the associated :class:`~can.Notifier` .
        :return:
            A tuple of :class:`~can.Notifier` instances associated with the given bus.
        """
        return Notifier._registry.find_instances(bus)

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if not self._stopped:
            self.stop()
