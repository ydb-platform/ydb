"""
Exposes several methods for transmitting cyclic messages.

The main entry point to these classes should be through
:meth:`can.BusABC.send_periodic`.
"""

import abc
import logging
import platform
import sys
import threading
import time
import warnings
from collections.abc import Sequence
from typing import (
    TYPE_CHECKING,
    Callable,
    Final,
    Optional,
    Union,
    cast,
)

from can import typechecking
from can.message import Message

if TYPE_CHECKING:
    from can.bus import BusABC


log = logging.getLogger("can.bcm")
NANOSECONDS_IN_SECOND: Final[int] = 1_000_000_000


class _Pywin32Event:
    handle: int


class _Pywin32:
    def __init__(self) -> None:
        import pywintypes  # noqa: PLC0415 # pylint: disable=import-outside-toplevel,import-error
        import win32event  # noqa: PLC0415 # pylint: disable=import-outside-toplevel,import-error

        self.pywintypes = pywintypes
        self.win32event = win32event

    def create_timer(self) -> _Pywin32Event:
        try:
            event = self.win32event.CreateWaitableTimerEx(
                None,
                None,
                self.win32event.CREATE_WAITABLE_TIMER_HIGH_RESOLUTION,
                self.win32event.TIMER_ALL_ACCESS,
            )
        except (
            AttributeError,
            OSError,
            self.pywintypes.error,  # pylint: disable=no-member
        ):
            event = self.win32event.CreateWaitableTimer(None, False, None)

        return cast("_Pywin32Event", event)

    def set_timer(self, event: _Pywin32Event, period_ms: int) -> None:
        self.win32event.SetWaitableTimer(event.handle, 0, period_ms, None, None, False)

    def stop_timer(self, event: _Pywin32Event) -> None:
        self.win32event.SetWaitableTimer(event.handle, 0, 0, None, None, False)

    def wait_0(self, event: _Pywin32Event) -> None:
        self.win32event.WaitForSingleObject(event.handle, 0)

    def wait_inf(self, event: _Pywin32Event) -> None:
        self.win32event.WaitForSingleObject(
            event.handle,
            self.win32event.INFINITE,
        )


PYWIN32: Optional[_Pywin32] = None
if sys.platform == "win32" and sys.version_info < (3, 11):
    try:
        PYWIN32 = _Pywin32()
    except ImportError:
        pass


class CyclicTask(abc.ABC):
    """
    Abstract Base for all cyclic tasks.
    """

    @abc.abstractmethod
    def stop(self) -> None:
        """Cancel this periodic task.

        :raises ~can.exceptions.CanError:
            If stop is called on an already stopped task.
        """


class CyclicSendTaskABC(CyclicTask, abc.ABC):
    """
    Message send task with defined period
    """

    def __init__(
        self, messages: Union[Sequence[Message], Message], period: float
    ) -> None:
        """
        :param messages:
            The messages to be sent periodically.
        :param period: The rate in seconds at which to send the messages.

        :raises ValueError: If the given messages are invalid
        """
        messages = self._check_and_convert_messages(messages)

        # Take the Arbitration ID of the first element
        self.arbitration_id = messages[0].arbitration_id
        self.period = period
        self.period_ns = round(period * 1e9)
        self.messages = messages

    @staticmethod
    def _check_and_convert_messages(
        messages: Union[Sequence[Message], Message],
    ) -> tuple[Message, ...]:
        """Helper function to convert a Message or Sequence of messages into a
        tuple, and raises an error when the given value is invalid.

        Performs error checking to ensure that all Messages have the same
        arbitration ID and channel.

        Should be called when the cyclic task is initialized.

        :raises ValueError: If the given messages are invalid
        """
        if not isinstance(messages, (list, tuple)):
            if isinstance(messages, Message):
                messages = [messages]
            else:
                raise ValueError("Must be either a list, tuple, or a Message")
        if not messages:
            raise ValueError("Must be at least a list or tuple of length 1")
        messages = tuple(messages)

        all_same_id = all(
            message.arbitration_id == messages[0].arbitration_id for message in messages
        )
        if not all_same_id:
            raise ValueError("All Arbitration IDs should be the same")

        all_same_channel = all(
            message.channel == messages[0].channel for message in messages
        )
        if not all_same_channel:
            raise ValueError("All Channel IDs should be the same")

        return messages


class LimitedDurationCyclicSendTaskABC(CyclicSendTaskABC, abc.ABC):
    def __init__(
        self,
        messages: Union[Sequence[Message], Message],
        period: float,
        duration: Optional[float],
    ) -> None:
        """Message send task with a defined duration and period.

        :param messages:
            The messages to be sent periodically.
        :param period: The rate in seconds at which to send the messages.
        :param duration:
            Approximate duration in seconds to continue sending messages. If
            no duration is provided, the task will continue indefinitely.

        :raises ValueError: If the given messages are invalid
        """
        super().__init__(messages, period)
        self.duration = duration
        self.end_time: Optional[float] = None


class RestartableCyclicTaskABC(CyclicSendTaskABC, abc.ABC):
    """Adds support for restarting a stopped cyclic task"""

    @abc.abstractmethod
    def start(self) -> None:
        """Restart a stopped periodic task."""


class ModifiableCyclicTaskABC(CyclicSendTaskABC, abc.ABC):
    def _check_modified_messages(self, messages: tuple[Message, ...]) -> None:
        """Helper function to perform error checking when modifying the data in
        the cyclic task.

        Performs error checking to ensure the arbitration ID and the number of
        cyclic messages hasn't changed.

        Should be called when modify_data is called in the cyclic task.

        :raises ValueError: If the given messages are invalid
        """
        if len(self.messages) != len(messages):
            raise ValueError(
                "The number of new cyclic messages to be sent must be equal to "
                "the number of messages originally specified for this task"
            )
        if self.arbitration_id != messages[0].arbitration_id:
            raise ValueError(
                "The arbitration ID of new cyclic messages cannot be changed "
                "from when the task was created"
            )

    def modify_data(self, messages: Union[Sequence[Message], Message]) -> None:
        """Update the contents of the periodically sent messages, without
        altering the timing.

        :param messages:
            The messages with the new :attr:`Message.data`.

            Note: The arbitration ID cannot be changed.

            Note: The number of new cyclic messages to be sent must be equal
            to the original number of messages originally specified for this
            task.

        :raises ValueError: If the given messages are invalid
        """
        messages = self._check_and_convert_messages(messages)
        self._check_modified_messages(messages)

        self.messages = messages


class MultiRateCyclicSendTaskABC(CyclicSendTaskABC, abc.ABC):
    """A Cyclic send task that supports switches send frequency after a set time."""

    def __init__(
        self,
        channel: typechecking.Channel,
        messages: Union[Sequence[Message], Message],
        count: int,  # pylint: disable=unused-argument
        initial_period: float,  # pylint: disable=unused-argument
        subsequent_period: float,
    ) -> None:
        """
        Transmits a message `count` times at `initial_period` then continues to
        transmit messages at `subsequent_period`.

        :param channel: See interface specific documentation.
        :param messages:
        :param count:
        :param initial_period:
        :param subsequent_period:

        :raises ValueError: If the given messages are invalid
        """
        super().__init__(messages, subsequent_period)
        self._channel = channel


class ThreadBasedCyclicSendTask(
    LimitedDurationCyclicSendTaskABC, ModifiableCyclicTaskABC, RestartableCyclicTaskABC
):
    """Fallback cyclic send task using daemon thread."""

    def __init__(
        self,
        bus: "BusABC",
        lock: threading.Lock,
        messages: Union[Sequence[Message], Message],
        period: float,
        duration: Optional[float] = None,
        on_error: Optional[Callable[[Exception], bool]] = None,
        autostart: bool = True,
        modifier_callback: Optional[Callable[[Message], None]] = None,
    ) -> None:
        """Transmits `messages` with a `period` seconds for `duration` seconds on a `bus`.

        The `on_error` is called if any error happens on `bus` while sending `messages`.
        If `on_error` present, and returns ``False`` when invoked, thread is
        stopped immediately, otherwise, thread continuously tries to send `messages`
        ignoring errors on a `bus`. Absence of `on_error` means that thread exits immediately
        on error.

        :param on_error: The callable that accepts an exception if any
                         error happened on a `bus` while sending `messages`,
                         it shall return either ``True`` or ``False`` depending
                         on desired behaviour of `ThreadBasedCyclicSendTask`.

        :raises ValueError: If the given messages are invalid
        """
        super().__init__(messages, period, duration)
        self.bus = bus
        self.send_lock = lock
        self.stopped = True
        self.thread: Optional[threading.Thread] = None
        self.on_error = on_error
        self.modifier_callback = modifier_callback

        self.period_ms = int(round(period * 1000, 0))

        self.event: Optional[_Pywin32Event] = None
        if PYWIN32:
            if self.period_ms == 0:
                # A period of 0 would mean that the timer is signaled only once
                raise ValueError("The period cannot be smaller than 0.001 (1 ms)")
            self.event = PYWIN32.create_timer()
        elif (
            sys.platform == "win32"
            and sys.version_info < (3, 11)
            and platform.python_implementation() == "CPython"
        ):
            warnings.warn(
                f"{self.__class__.__name__} may achieve better timing accuracy "
                f"if the 'pywin32' package is installed.",
                RuntimeWarning,
                stacklevel=1,
            )

        if autostart:
            self.start()

    def stop(self) -> None:
        self.stopped = True
        if self.event and PYWIN32:
            # Reset and signal any pending wait by setting the timer to 0
            PYWIN32.stop_timer(self.event)

    def start(self) -> None:
        self.stopped = False
        if self.thread is None or not self.thread.is_alive():
            name = f"Cyclic send task for 0x{self.messages[0].arbitration_id:X}"
            self.thread = threading.Thread(target=self._run, name=name)
            self.thread.daemon = True

            self.end_time: Optional[float] = (
                time.perf_counter() + self.duration if self.duration else None
            )

            if self.event and PYWIN32:
                PYWIN32.set_timer(self.event, self.period_ms)

            self.thread.start()

    def _run(self) -> None:
        msg_index = 0
        msg_due_time_ns = time.perf_counter_ns()

        if self.event and PYWIN32:
            # Make sure the timer is non-signaled before entering the loop
            PYWIN32.wait_0(self.event)

        while not self.stopped:
            if self.end_time is not None and time.perf_counter() >= self.end_time:
                self.stop()
                break

            try:
                if self.modifier_callback is not None:
                    self.modifier_callback(self.messages[msg_index])
                with self.send_lock:
                    # Prevent calling bus.send from multiple threads
                    self.bus.send(self.messages[msg_index])
            except Exception as exc:  # pylint: disable=broad-except
                log.exception(exc)

                # stop if `on_error` callback was not given
                if self.on_error is None:
                    self.stop()
                    raise exc

                # stop if `on_error` returns False
                if not self.on_error(exc):
                    self.stop()
                    break

            if not self.event:
                msg_due_time_ns += self.period_ns

            msg_index = (msg_index + 1) % len(self.messages)

            if self.event and PYWIN32:
                PYWIN32.wait_inf(self.event)
            else:
                # Compensate for the time it takes to send the message
                delay_ns = msg_due_time_ns - time.perf_counter_ns()
                if delay_ns > 0:
                    time.sleep(delay_ns / NANOSECONDS_IN_SECOND)
