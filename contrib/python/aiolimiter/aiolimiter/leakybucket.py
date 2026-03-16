# SPDX-License-Identifier: MIT
# Copyright (c) 2019 Martijn Pieters
# Licensed under the MIT license as detailed in LICENSE.txt

import asyncio
import os
import sys
import warnings
from contextlib import AbstractAsyncContextManager
from functools import partial
from heapq import heappop, heappush
from itertools import count
from types import TracebackType
from typing import List, Optional, Tuple, Type

LIMITER_REUSED_ACROSS_LOOPS_WARNING = (
    "This AsyncLimiter instance is being re-used across loops. Please create "
    "a new limiter per event loop as re-use can lead to undefined behaviour."
)

if sys.version_info >= (3, 12):  # pragma: no cover
    _warn_reuse = partial(
        warnings.warn,
        message=LIMITER_REUSED_ACROSS_LOOPS_WARNING,
        category=RuntimeWarning,
        skip_file_prefixes=(os.path.dirname(__file__),),
    )
else:
    # no support for dynamic stack levels, disable stack location
    _warn_reuse = partial(
        warnings.warn,
        message=LIMITER_REUSED_ACROSS_LOOPS_WARNING,
        category=RuntimeWarning,
        stacklevel=0,
    )


class AsyncLimiter(AbstractAsyncContextManager):
    """A leaky bucket rate limiter.

    This is an :ref:`asynchronous context manager <async-context-managers>`;
    when used with :keyword:`async with`, entering the context acquires
    capacity::

        limiter = AsyncLimiter(10)
        for foo in bar:
            async with limiter:
                # process foo elements at 10 items per minute

    :param max_rate: Allow up to `max_rate` / `time_period` acquisitions before
       blocking.
    :param time_period: duration, in seconds, of the time period in which to
       limit the rate. Note that up to `max_rate` acquisitions are allowed
       within this time period in a burst.

    """

    __slots__ = (
        "max_rate",
        "time_period",
        "_rate_per_sec",
        "_level",
        "_last_check",
        "_event_loop",
        "_waiters",
        "_next_count",
        "_waker_handle",
    )

    max_rate: float  #: The configured `max_rate` value for this limiter.
    time_period: float  #: The configured `time_period` value for this limiter.

    def __init__(self, max_rate: float, time_period: float = 60) -> None:
        self.max_rate = max_rate
        self.time_period = time_period
        self._rate_per_sec = max_rate / time_period
        self._level = 0.0
        self._last_check = 0.0

        # timer until next waiter can resume
        self._waker_handle: asyncio.TimerHandle | None = None
        # min-heap with (amount requested, order, future) for waiting tasks
        self._waiters: List[Tuple[float, int, "asyncio.Future[None]"]] = []
        # counter used to order waiting tasks
        self._next_count = partial(next, count())

    @property
    def _loop(self) -> asyncio.AbstractEventLoop:
        self._event_loop: asyncio.AbstractEventLoop
        try:
            loop = self._event_loop
            if loop.is_closed():
                # limiter is being reused across loops; make a best-effort
                # attempt at recovery. Existing waiters are ditched, with
                # the assumption that they are no longer viable.
                loop = self._event_loop = asyncio.get_running_loop()
                self._waiters = [
                    (amt, cnt, fut)
                    for amt, cnt, fut in self._waiters
                    if fut.get_loop() == loop
                ]
                _warn_reuse()

        except AttributeError:
            loop = self._event_loop = asyncio.get_running_loop()
        return loop

    def _leak(self) -> None:
        """Drip out capacity from the bucket."""
        now = self._loop.time()
        if self._level:
            # drip out enough level for the elapsed time since
            # we last checked
            elapsed = now - self._last_check
            decrement = elapsed * self._rate_per_sec
            self._level = max(self._level - decrement, 0)
        self._last_check = now

    def has_capacity(self, amount: float = 1) -> bool:
        """Check if there is enough capacity remaining in the limiter

        :param amount: How much capacity you need to be available.

        """
        self._leak()
        return self._level + amount <= self.max_rate

    async def acquire(self, amount: float = 1) -> None:
        """Acquire capacity in the limiter.

        If the limit has been reached, blocks until enough capacity has been
        freed before returning.

        :param amount: How much capacity you need to be available.
        :exception: Raises :exc:`ValueError` if `amount` is greater than
           :attr:`max_rate`.

        """
        if amount > self.max_rate:
            raise ValueError("Can't acquire more than the maximum capacity")

        loop = self._loop
        while not self.has_capacity(amount):
            # Add a future to the _waiters heapq to be notified when capacity
            # has come up. The future callback uses call_soon so other tasks
            # are checked *after* completing capacity acquisition in this task.
            fut = loop.create_future()
            fut.add_done_callback(partial(loop.call_soon, self._wake_next))
            heappush(self._waiters, (amount, self._next_count(), fut))
            self._wake_next()
            await fut

        self._level += amount
        # reset the waker to account for the new, lower level.
        self._wake_next()

        return None

    def _wake_next(self, *_args: object) -> None:
        """Wake the next waiting future or set a timer"""
        # clear timer and any cancelled futures at the top of the heap
        heap, handle, self._waker_handle = self._waiters, self._waker_handle, None
        if handle is not None:
            handle.cancel()
        while heap and heap[0][-1].done():
            heappop(heap)

        if not heap:
            # nothing left waiting
            return

        amount, _, fut = heap[0]
        self._leak()
        needed = amount - self.max_rate + self._level
        if needed <= 0:
            heappop(heap)
            fut.set_result(None)
            # fut.set_result triggers another _wake_next call
            return

        wake_next_at = self._last_check + (1 / self._rate_per_sec * needed)
        self._waker_handle = self._loop.call_at(wake_next_at, self._wake_next)

    def __repr__(self) -> str:  # pragma: no cover
        args = f"max_rate={self.max_rate!r}, time_period={self.time_period!r}"
        state = f"level: {self._level:f}, waiters: {len(self._waiters)}"
        if (handle := self._waker_handle) and not handle.cancelled():
            microseconds = int((handle.when() - self._loop.time()) * 10**6)
            if microseconds > 0:
                state += f", waking in {microseconds} \N{MICRO SIGN}s"
        return f"<AsyncLimiter({args}) at {id(self):#x} [{state}]>"

    async def __aenter__(self) -> None:
        await self.acquire()
        return None

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        return None
