"""
This module contains generic utility functions which don't have dependencies on any
other modules.
"""
from __future__ import annotations
import logging
import time
import typing
from collections.abc import Callable

logger = logging.getLogger("pytds")
T = typing.TypeVar("T")


def exponential_backoff(
    work: Callable[[float], T],
    ex_handler: Callable[[Exception], None],
    max_time_sec: float,
    first_attempt_time_sec: float,
    backoff_factor: float = 2,
) -> T:
    """
    Perform work with exponential backoff if work fails.
    Will raise TimeoutError if `max_time_sec` is exceeded.
    Work handler receives time limit in seconds for the attempt,
    it should use it to setup timeout for it's operations.
    The `ex_handler` is called every time work raises exception,
    if `ex_handler` can raise exception itself to stop
    """
    try_time = first_attempt_time_sec
    end_time = time.time() + max_time_sec
    while True:
        try_start_time = time.time()
        try:
            return work(try_time)
        except Exception as ex:
            logger.info("Work attempt failed", exc_info=ex)
            work_actual_time = time.time() - try_start_time
            if work_actual_time > try_time:
                logger.warning(
                    "Work attempt exceeded it's allocated time %f, actual time was %f.",
                    try_time,
                    work_actual_time,
                )
            ex_handler(ex)
            cur_time = time.time()
            remaining_attempt_time = try_time - (cur_time - try_start_time)
            logger.info("Will retry after %f seconds", remaining_attempt_time)
            if remaining_attempt_time > 0:
                time.sleep(remaining_attempt_time)
            cur_time += remaining_attempt_time
            if cur_time >= end_time:
                raise TimeoutError() from ex
            try_time *= backoff_factor
            try_time = min(try_time, end_time - cur_time)


def parse_server(server: str) -> tuple[str, str]:
    """
    Split server name in MSSQL format (host\\instance) into server host and instance
    """
    instance = ""
    if "\\" in server:
        server, instance = server.split("\\")

    # support MS methods of connecting locally
    if server in (".", "(local)"):
        server = "localhost"

    return server, instance.upper()


def ver_to_int(ver: str) -> int:
    """
    Convert version string into 32-bit integer format
    """
    res = ver.split(".")
    if len(res) < 2:
        logger.warning(
            'Invalid version %s, it should have 2 parts at least separated by "."', ver
        )
        return 0
    maj, minor, _ = ver.split(".")
    return (int(maj) << 24) + (int(minor) << 16)
