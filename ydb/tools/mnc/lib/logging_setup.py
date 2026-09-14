"""
Logging setup module for MNC CLI.

Provides centralized logging configuration with RichHandler integration.
When a MyProgress instance is active, routes log output through its console
so that log lines don't break the live progress display.
"""

import logging
from rich.logging import RichHandler

from ydb.tools.mnc.lib import output


_rich_handler: RichHandler = None


def setup_loggers() -> None:
    """
    Configure logging with RichHandler based on current verbosity mode.

    Log levels by verbosity mode:
    - QUIET: WARNING
    - NORMAL: WARNING
    - VERBOSE: DEBUG

    Also suppresses noisy loggers at NORMAL level.
    """
    global _rich_handler

    mode = output.get_mode()

    if mode == output.VerbosityMode.VERBOSE:
        log_level = logging.DEBUG
    elif mode == output.VerbosityMode.QUIET:
        log_level = logging.WARNING
    else:  # NORMAL
        log_level = logging.WARNING

    console = output.get_stderr_console()

    _rich_handler = RichHandler(console=console, show_time=False, show_path=False)

    logging.basicConfig(
        level=log_level,
        format='%(message)s',
        handlers=[_rich_handler]
    )

    if mode == output.VerbosityMode.NORMAL:
        logging.getLogger('aiohttp.access').setLevel(logging.WARNING)
        logging.getLogger('asyncio').setLevel(logging.WARNING)


def update_handler_console() -> None:
    """Switch the RichHandler console to the active progress console (or back to stderr).

    Called by output._set_active_progress() whenever a MyProgress context is
    entered or exited. Rich's Progress.console already handles interleaving
    correctly, so routing logs through it prevents display corruption.
    """
    if _rich_handler is None:
        return

    active = output.get_active_progress()
    if active is not None:
        _rich_handler.console = active.console()
    else:
        try:
            _rich_handler.console = output.get_stderr_console()
        except RuntimeError:
            pass
