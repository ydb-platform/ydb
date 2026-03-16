#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import logging

from pysmi import __version__
from pysmi import error

FLAG_NONE = 0x0000
FLAG_SEARCHER = 0x0001
FLAG_READER = 0x0002
FLAG_LEXER = 0x0004
FLAG_PARSER = 0x0008
FLAG_GRAMMAR = 0x0010
FLAG_CODEGEN = 0x0020
FLAG_WRITER = 0x0040
FLAG_COMPILER = 0x0080
FLAG_BORROWER = 0x0100
FLAG_ALL = 0xFFFF

FLAG_MAP = {
    "searcher": FLAG_SEARCHER,
    "reader": FLAG_READER,
    "lexer": FLAG_LEXER,
    "parser": FLAG_PARSER,
    "grammar": FLAG_GRAMMAR,
    "codegen": FLAG_CODEGEN,
    "writer": FLAG_WRITER,
    "compiler": FLAG_COMPILER,
    "borrower": FLAG_BORROWER,
    "all": FLAG_ALL,
}


class Printer:
    def __init__(self, logger=None, handler=None, formatter=None):
        if logger is None:
            logger = logging.getLogger("pysmi")

        logger.setLevel(logging.DEBUG)

        if handler is None:
            handler = logging.StreamHandler()

        if formatter is None:
            formatter = logging.Formatter("%(asctime)s %(name)s: %(message)s")

        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)

        logger.addHandler(handler)

        self.__logger = logger

    def __call__(self, msg):
        self.__logger.debug(msg)

    def __str__(self):
        """Return a string representation of the instance."""
        return "<python built-in logging>"

    def get_current_logger(self):
        return self.__logger


NullHandler = logging.NullHandler


class Debug:
    defaultPrinter = None
    _printer: Printer

    def __init__(self, *flags, **options):
        self._flags = FLAG_NONE
        if options.get("printer") is not None:
            self._printer = options.get("printer")  # type: ignore

        elif self.defaultPrinter is not None:
            self._printer = self.defaultPrinter

        else:
            if "loggerName" in options:
                # route our logs to parent logger
                self._printer = Printer(
                    logger=logging.getLogger(options["loggerName"]),
                    handler=NullHandler(),
                )
            else:
                self._printer = Printer()

        self(f"running pysmi version {__version__}")

        for flag in flags:
            inverse = flag and flag[0] in ("!", "~")

            if inverse:
                flag = flag[1:]

            try:
                if inverse:
                    self._flags &= ~FLAG_MAP[flag]
                else:
                    self._flags |= FLAG_MAP[flag]

            except KeyError:
                raise error.PySmiError(f"bad debug flag {flag}")

            self(f"debug category '{flag}' {'disabled' if inverse else 'enabled'}")

    def __str__(self):
        """Return a string representation of the instance."""
        return f"logger {self._printer}, flags {self._flags:x}"

    def __call__(self, msg):
        """Log a message."""
        self._printer(msg)

    def __and__(self, flag):
        """Return a bitwise and of the instance and a flag."""
        return self._flags & flag

    def __rand__(self, flag):
        """Return a bitwise and of a flag and the instance."""
        return flag & self._flags

    def get_current_printer(self):
        return self._printer

    def get_current_logger(self):
        return self._printer and self._printer.get_current_logger() or None


# This will yield false from bitwise and with a flag, and save
# on unnecessary calls
logger = 0


def set_logger(value):
    global logger
    logger = value
