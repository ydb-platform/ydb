from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import threading
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

# A few things about signals and asyncio:
# - https://docs.python.org/3/library/asyncio-eventloop.html#unix-signals
# - If you don't use loop.add_signal_handler, your handler has no effect on the queue
# - asyncio signal primitives don't give you a way to get the current handler for a
#   signal
# - If a signal has an asyncio handler, its "standard handler (installed through
#   signal.signal) is ignored (you can't get it, you can't set one, it doesn't get
#   called on the signal).
# - asyncio handlers receive no parameter
# This all mean we have to save the previous signals before installing ours, and for
# uninstalling, we need to remove the async handler and only then re-add the normal
# one. And hope that there was no previously set async handler.


@contextlib.contextmanager
def on_stop(callback: Callable[[], None]):
    if os.name == "nt":
        logger.warning(
            "Skipping signal handling, does not work on Windows",
            extra={"action": "skip_signal_handlers"},
        )
        yield
        return
    if threading.current_thread() is not threading.main_thread():
        logger.warning(
            "Skipping signal handling, because this is not the main thread",
            extra={"action": "skip_signal_handlers"},
        )
        yield
        return

    sigint_handler = signal.getsignal(signal.SIGINT)
    sigterm_handler = signal.getsignal(signal.SIGTERM)

    uninstalled = False
    loop: asyncio.AbstractEventLoop | None
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    def uninstall_and_callback(*args: Any) -> None:
        nonlocal uninstalled
        uninstalled = True
        uninstall(
            loop=loop, sigint_handler=sigint_handler, sigterm_handler=sigterm_handler
        )
        callback()

    try:
        install(loop=loop, handler=uninstall_and_callback)
        yield
    finally:
        if not uninstalled:
            uninstall(
                loop=loop,
                sigint_handler=sigint_handler,
                sigterm_handler=sigterm_handler,
            )


def install(loop: asyncio.AbstractEventLoop | None, handler: Callable):
    logger.debug(
        "Installing signal handler", extra={"action": "install_signal_handlers"}
    )
    if loop:
        loop.add_signal_handler(signal.SIGINT, handler)
        loop.add_signal_handler(signal.SIGTERM, handler)
    else:
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)


def uninstall(
    loop: asyncio.AbstractEventLoop | None,
    sigint_handler: Any,
    sigterm_handler: Any,
):
    logger.debug(
        "Resetting previous signal handler",
        extra={"action": "uninstall_signal_handlers"},
    )
    if loop:
        loop.remove_signal_handler(signal.SIGINT)
        loop.remove_signal_handler(signal.SIGTERM)
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigterm_handler)
