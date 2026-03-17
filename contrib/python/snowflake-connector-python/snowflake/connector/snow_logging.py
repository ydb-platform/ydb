#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import logging
import warnings


def getSnowLogger(name=None, extra=None):
    if name:
        logger = logging.getLogger(name)
        return SnowLogger(logger, extra)


class SnowLogger(logging.LoggerAdapter):
    """Snowflake Python logger wrapper of the built-in Python logger.

    This logger wrapper supports user-provided logging info about
    file name, function name and line number. This wrapper can be
    used in Cython code (.pyx).
    """

    def debug(self, msg, path_name=None, func_name=None, *args, **kwargs):
        self.log(logging.DEBUG, msg, path_name, func_name, *args, **kwargs)

    def info(self, msg, path_name=None, func_name=None, *args, **kwargs):
        self.log(logging.INFO, msg, path_name, func_name, *args, **kwargs)

    def warning(self, msg, path_name=None, func_name=None, *args, **kwargs):
        self.log(logging.WARNING, msg, path_name, func_name, *args, **kwargs)

    def warn(self, msg, path_name=None, func_name=None, *args, **kwargs):
        warnings.warn(
            "The 'warn' method is deprecated, " "use 'warning' instead",
            DeprecationWarning,
            2,
        )
        self.warning(msg, path_name, func_name, *args, **kwargs)

    def error(self, msg, path_name=None, func_name=None, *args, **kwargs):
        self.log(logging.ERROR, msg, path_name, func_name, *args, **kwargs)

    def exception(
        self, msg, path_name=None, func_name=None, *args, exc_info=True, **kwargs
    ):
        """Convenience method for logging an ERROR with exception information."""
        self.error(msg, path_name, func_name, *args, exc_info=exc_info, **kwargs)

    def critical(self, msg, path_name=None, func_name=None, *args, **kwargs):
        self.log(logging.CRITICAL, msg, path_name, func_name, *args, **kwargs)

    fatal = critical

    def log(
        self,
        level: int,
        msg: str,
        path_name: str | None = None,
        func_name: str | None = None,
        line_num: int = 0,
        *args,
        **kwargs,
    ):
        """Generalized log method of SnowLogger wrapper.

        Args:
            level: Logging level.
            msg: Logging message.
            path_name: Absolute or relative path of the file where the logger gets called.
            func_name: Function inside which the logger gets called.
            line_num: Line number at which the logger gets called.
        """
        if not path_name:
            path_name = "path_name not provided"
        if not func_name:
            func_name = "func_name not provided"
        if not isinstance(level, int):
            if logging.raiseExceptions:
                raise TypeError("level must be an integer")
            else:
                return
        if self.logger.isEnabledFor(level):
            record = self.logger.makeRecord(
                self.logger.name,
                level,
                path_name,
                line_num,
                msg,
                args,
                None,
                func_name,
                **kwargs,
            )
            self.logger.handle(record)
