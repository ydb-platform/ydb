#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import random
import time
from logging import getLogger
from typing import Any, Callable

logger = getLogger(__name__)

try:
    from threading import _Timer as Timer
except ImportError:
    from threading import Timer

DEFAULT_MASTER_VALIDITY_IN_SECONDS = 4 * 60 * 60  # seconds


class HeartBeatTimer(Timer):
    """A thread which executes a function every client_session_keep_alive_heartbeat_frequency seconds."""

    def __init__(
        self, client_session_keep_alive_heartbeat_frequency: int, f: Callable
    ) -> None:
        interval = client_session_keep_alive_heartbeat_frequency
        super().__init__(interval, f)
        # Mark this as a daemon thread, so that it won't prevent Python from exiting.
        self.daemon = True

    def run(self) -> None:
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                try:
                    self.function()
                except Exception as e:
                    logger.debug("failed to heartbeat: %s", e)


def get_time_millis() -> int:
    """Returns the current time in milliseconds."""
    return int(time.time() * 1000)


class DecorrelateJitterBackoff:
    # Decorrelate Jitter backoff
    # https://www.awsarchitectureblog.com/2015/03/backoff.html
    def __init__(self, base: int, cap: int) -> None:
        self._base = base
        self._cap = cap

    def next_sleep(self, _: Any, sleep: int) -> int:
        return min(self._cap, random.randint(self._base, sleep * 3))


class TimerContextManager:
    """Context manager class to easily measure execution of a code block.

    Once the context manager finishes, the class should be cast into an int to retrieve
    result.

    Example:

        with TimerContextManager() as measured_time:
            pass
        download_metric = measured_time.get_timing_millis()
    """

    def __init__(self) -> None:
        self._start: int | None = None
        self._end: int | None = None

    def __enter__(self) -> TimerContextManager:
        self._start = get_time_millis()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._end = get_time_millis()

    def get_timing_millis(self) -> int:
        """Get measured timing in milliseconds."""
        if self._start is None or self._end is None:
            raise Exception(
                "Trying to get timing before TimerContextManager has finished"
            )
        return self._end - self._start
