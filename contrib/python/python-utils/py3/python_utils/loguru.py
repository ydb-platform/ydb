"""
This module provides a `Logurud` class that integrates the `loguru` logger
with the base logging functionality defined in `logger_module.LoggerBase`.

Classes:
    Logurud: A class that extends `LoggerBase` and uses `loguru` for logging.

Usage example:
    >>> from python_utils.loguru import Logurud
    >>> class MyClass(Logurud):
    ...     def __init__(self):
    ...         Logurud.__init__(self)
    >>> my_class = MyClass()
    >>> my_class.logger.info('This is an info message')
"""

from __future__ import annotations

import typing

import loguru

from . import logger as logger_module

__all__ = ['Logurud']


class Logurud(logger_module.LoggerBase):
    """
    A class that extends `LoggerBase` and uses `loguru` for logging.

    Attributes:
        logger (loguru.Logger): The `loguru` logger instance.
    """

    logger: loguru.Logger

    def __new__(cls, *args: typing.Any, **kwargs: typing.Any) -> Logurud:
        """
        Creates a new instance of `Logurud` and initializes the `loguru`
        logger.

        Args:
            *args (typing.Any): Variable length argument list.
            **kwargs (typing.Any): Arbitrary keyword arguments.

        Returns:
            Logurud: A new instance of `Logurud`.
        """
        cls.logger: loguru.Logger = loguru.logger.opt(depth=1)
        return super().__new__(cls)
