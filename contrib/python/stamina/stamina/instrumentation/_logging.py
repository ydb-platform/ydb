# SPDX-FileCopyrightText: 2022 Hynek Schlawack <hs@ox.cx>
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

from ._data import RetryDetails, RetryHook, RetryHookFactory


def init_logging(log_level: int = 30) -> RetryHook:
    """
    Initialize logging using the standard library.

    Returned hook logs scheduled retries at *log_level*.

    .. versionadded:: 23.2.0
    """
    import logging

    logger = logging.getLogger("stamina")

    def log_retries(details: RetryDetails) -> None:
        logger.log(
            log_level,
            "stamina.retry_scheduled",
            extra={
                "stamina.callable": details.name,
                "stamina.args": tuple(repr(a) for a in details.args),
                "stamina.kwargs": dict(details.kwargs.items()),
                "stamina.retry_num": details.retry_num,
                "stamina.caused_by": repr(details.caused_by),
                "stamina.wait_for": round(details.wait_for, 2),
                "stamina.waited_so_far": round(details.waited_so_far, 2),
            },
        )

    return log_retries


LoggingOnRetryHook = RetryHookFactory(init_logging)
