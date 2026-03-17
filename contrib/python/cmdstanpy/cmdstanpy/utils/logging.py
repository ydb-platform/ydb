"""
CmdStanPy logging
"""

import functools
import logging
import types
from contextlib import AbstractContextManager
from typing import Optional, Type


@functools.lru_cache(maxsize=None)
def get_logger() -> logging.Logger:
    """cmdstanpy logger"""
    logger = logging.getLogger("cmdstanpy")
    if not logger.hasHandlers():
        # send all messages to handlers
        logger.setLevel(logging.DEBUG)
        # add a default handler to the logger to INFO and higher
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "%H:%M:%S",
            )
        )
        logger.addHandler(handler)
    return logger


class ToggleLogging(AbstractContextManager):
    def __init__(self, disable: bool) -> None:
        self.disable = disable
        self.logger = get_logger()
        self.prev_state = self.logger.disabled
        self.logger.disabled = self.disable

    def __repr__(self) -> str:
        return ""

    def __enter__(self) -> "ToggleLogging":
        self.logger.disabled = self.disable
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[types.TracebackType],
    ) -> None:
        self.logger.disabled = self.prev_state


def enable_logging() -> ToggleLogging:
    """Enable cmdstanpy logging. Can be used as a context manager"""
    return ToggleLogging(disable=False)


def disable_logging() -> ToggleLogging:
    """Disable cmdstanpy logging. Can be used as a context manager"""
    return ToggleLogging(disable=True)
