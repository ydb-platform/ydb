"""
A minimal scheduler to schedule tasks to run in the future (async version).

Inspired to the standard library `sched.scheduler`, but designed for
multi-thread usage from the ground up, not as an afterthought. Tasks can be
scheduled in front of the one currently running and `Scheduler.run()` can be
left running without any tasks scheduled.

Tasks are called "Task", not "Event", here, because we actually make use of
`[threading/asyncio].Event` and the two would be confusing.
"""

# Copyright (C) 2021 The Psycopg Team

from __future__ import annotations

import logging
from time import monotonic
from heapq import heappop, heappush
from typing import Any
from collections.abc import Callable

from ._task import Task
from ._acompat import AEvent, ALock

logger = logging.getLogger(__name__)

if True:  # ASYNC
    from asyncio import CancelledError

    # The exceptions that we need to capture in order to keep the pool
    # consistent and avoid losing connections on errors in callers code.
    CLIENT_EXCEPTIONS = (Exception, CancelledError)
else:
    CLIENT_EXCEPTIONS = Exception


class AsyncScheduler:
    def __init__(self) -> None:
        self._queue: list[Task] = []
        self._lock = ALock()
        self._event = AEvent()

    EMPTY_QUEUE_TIMEOUT = 600.0

    async def enter(self, delay: float, action: Callable[[], Any] | None) -> Task:
        """Enter a new task in the queue delayed in the future.

        Schedule a `!None` to stop the execution.
        """
        time = monotonic() + delay
        return await self.enterabs(time, action)

    async def enterabs(self, time: float, action: Callable[[], Any] | None) -> Task:
        """Enter a new task in the queue at an absolute time.

        Schedule a `!None` to stop the execution.
        """
        task = Task(time, action)
        async with self._lock:
            heappush(self._queue, task)
            first = self._queue[0] is task

        if first:
            self._event.set()

        return task

    async def run(self) -> None:
        """Execute the events scheduled."""
        q = self._queue
        while True:
            async with self._lock:
                now = monotonic()
                if task := (q[0] if q else None):
                    if task.time <= now:
                        heappop(q)
                    else:
                        delay = task.time - now
                        task = None
                else:
                    delay = self.EMPTY_QUEUE_TIMEOUT
                self._event.clear()

            if task:
                if not task.action:
                    break
                try:
                    await task.action()
                except CLIENT_EXCEPTIONS as e:
                    logger.warning(
                        "scheduled task run %s failed: %s: %s",
                        task.action,
                        e.__class__.__name__,
                        e,
                    )
            else:
                # Block for the expected timeout or until a new task scheduled
                await self._event.wait_timeout(delay)
