"""Pymodbus: Modbus Protocol Implementation.

Released under the BSD license
"""
from __future__ import annotations

import logging
from binascii import b2a_hex
from logging import NullHandler as __null

from pymodbus.utilities import hexlify_packets


# ---------------------------------------------------------------------------#
#  Block unhandled logging
# ---------------------------------------------------------------------------#
logging.getLogger("pymodbus_internal").addHandler(__null())


def pymodbus_apply_logging_config(
    level: str | int = logging.DEBUG, log_file_name: str | None = None
):
    """Apply basic logging configuration used by default by Pymodbus maintainers.

    :param level: (optional) set log level, if not set it is inherited.
    :param log_file_name: (optional) log additional to file

    Please call this function to format logging appropriately when opening issues.
    """
    if isinstance(level, str):
        level = level.upper()
    Log.apply_logging_config(level, log_file_name)


class Log:
    """Class to hide logging complexity.

    :meta private:
    """

    _logger = logging.getLogger(__name__)

    @classmethod
    def apply_logging_config(cls, level, log_file_name):
        """Apply basic logging configuration."""
        if level == logging.NOTSET:
            level = cls._logger.getEffectiveLevel()
        if isinstance(level, str):
            level = level.upper()
        log_stream_handler = logging.StreamHandler()
        log_formatter = logging.Formatter(
            "%(asctime)s %(levelname)-5s %(module)s:%(lineno)s %(message)s"
        )
        log_stream_handler.setFormatter(log_formatter)
        cls._logger.addHandler(log_stream_handler)
        if log_file_name:
            log_file_handler = logging.FileHandler(log_file_name)
            log_file_handler.setFormatter(log_formatter)
            cls._logger.addHandler(log_file_handler)
        cls.setLevel(level)

    @classmethod
    def setLevel(cls, level):
        """Apply basic logging level."""
        cls._logger.setLevel(level)

    @classmethod
    def build_msg(cls, txt, *args):
        """Build message."""
        string_args = []
        count_args = len(args) - 1
        skip = False
        for i in range(count_args + 1):
            if skip:
                skip = False
                continue
            if (
                i < count_args
                and isinstance(args[i + 1], str)
                and args[i + 1][0] == ":"
            ):
                if args[i + 1] == ":hex":
                    string_args.append(hexlify_packets(args[i]))
                elif args[i + 1] == ":str":
                    string_args.append(str(args[i]))
                elif args[i + 1] == ":b2a":
                    string_args.append(b2a_hex(args[i]))
                skip = True
            else:
                string_args.append(args[i])
        return txt.format(*string_args)

    @classmethod
    def info(cls, txt, *args):
        """Log info messages."""
        if cls._logger.isEnabledFor(logging.INFO):
            cls._logger.info(cls.build_msg(txt, *args), stacklevel=2)

    @classmethod
    def debug(cls, txt, *args):
        """Log debug messages."""
        if cls._logger.isEnabledFor(logging.DEBUG):
            cls._logger.debug(cls.build_msg(txt, *args), stacklevel=2)

    @classmethod
    def warning(cls, txt, *args):
        """Log warning messages."""
        if cls._logger.isEnabledFor(logging.WARNING):
            cls._logger.warning(cls.build_msg(txt, *args), stacklevel=2)

    @classmethod
    def error(cls, txt, *args):
        """Log error messages."""
        if cls._logger.isEnabledFor(logging.ERROR):
            cls._logger.error(cls.build_msg(txt, *args), stacklevel=2)

    @classmethod
    def critical(cls, txt, *args):
        """Log critical messages."""
        if cls._logger.isEnabledFor(logging.CRITICAL):
            cls._logger.critical(cls.build_msg(txt, *args), stacklevel=2)
