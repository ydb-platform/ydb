# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations as _

import asyncio as _asyncio
from contextlib import suppress as _suppress
from logging import (
    CRITICAL as _CRITICAL,
    DEBUG as _DEBUG,
    ERROR as _ERROR,
    Filter as _Filter,
    Formatter as _Formatter,
    getLogger as _getLogger,
    INFO as _INFO,
    StreamHandler as _StreamHandler,
    WARNING as _WARNING,
)
from sys import stderr as _stderr

# ignore TC001 to make sphinx not completely drop the ball
from . import _typing as _t  # noqa: TC001


__all__ = [
    "Watcher",
    "watch",
]


class _ColourFormatter(_Formatter):
    """Colour formatter for pretty log output."""

    def format(self, record):
        s = super().format(record)
        if record.levelno == _CRITICAL:
            return f"\x1b[31;1m{s}\x1b[0m"  # bright red
        elif record.levelno == _ERROR:
            return f"\x1b[33;1m{s}\x1b[0m"  # bright yellow
        elif record.levelno == _WARNING:
            return f"\x1b[33m{s}\x1b[0m"  # yellow
        elif record.levelno == _INFO:
            return f"\x1b[37m{s}\x1b[0m"  # white
        elif record.levelno == _DEBUG:
            return f"\x1b[36m{s}\x1b[0m"  # cyan
        else:
            return s


class _TaskIdFilter(_Filter):
    """Injecting async task id into log records."""

    def filter(self, record):
        try:
            record.task = id(_asyncio.current_task())
        except RuntimeError:
            record.task = None
        return True


class Watcher:
    """
    Log watcher for easier logging setup.

    Example::

        from neo4j.debug import Watcher

        with Watcher("neo4j"):
            # DEBUG logging to stderr enabled within this context
            ...  # do something

    .. note:: The Watcher class is not thread-safe. Having Watchers in multiple
        threads can lead to duplicate log messages as the context manager will
        enable logging for all threads.

    .. note::
        The exact logging format and messages are not part of the API contract
        and might change at any time without notice. They are meant for
        debugging purposes and human consumption only.

    :param logger_names: Names of loggers to watch.
    :param default_level: Default minimum log level to show.
        The level can be overridden by setting ``level`` when calling
        :meth:`.watch`.
    :param default_out: Default output stream for all loggers.
        The level can be overridden by setting ``out`` when calling
        :meth:`.watch`.
    :type default_out: stream or file-like object
    :param colour: Whether the log levels should be indicated with ANSI colour
        codes.
    :param thread_info: whether to include information about the current
        thread in the log message. Defaults to :data:`True`.
    :param task_info: whether to include information about the current
        async task in the log message. Defaults to :data:`True`.

    .. versionchanged:: 5.3

        * Added ``thread_info`` and ``task_info`` parameters.
        * Logging format around thread and task information changed.
    """

    def __init__(
        self,
        *logger_names: str | None,
        default_level: int = _DEBUG,
        default_out: _t.TextIO = _stderr,
        colour: bool = False,
        thread_info: bool = True,
        task_info: bool = True,
    ) -> None:
        super().__init__()
        self._logger_names = logger_names
        self._loggers = [_getLogger(name) for name in self._logger_names]
        self._default_level = default_level
        self._default_out = default_out
        self._handlers: dict[str, _StreamHandler] = {}
        self._task_info = task_info

        format_ = "%(asctime)s  %(message)s"
        if task_info:
            format_ = "[Task %(task)-15s] " + format_
        if thread_info:
            format_ = "[Thread %(thread)d] " + format_
        if not colour:
            format_ = "[%(levelname)-8s] " + format_
        formatter_cls = _ColourFormatter if colour else _Formatter
        self.formatter = formatter_cls(format_)

    def __enter__(self) -> Watcher:
        """Enable logging for all loggers."""
        self.watch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Disable logging for all loggers."""
        self.stop()

    def watch(
        self, level: int | None = None, out: _t.TextIO | None = None
    ) -> None:
        """
        Enable logging for all loggers.

        :param level: Minimum log level to show.
            If :data:`None`, the ``default_level`` is used.
        :param out: Output stream for all loggers.
            If :data:`None`, the ``default_out`` is used.
        :type out: stream or file-like object
        """
        if level is None:
            level = self._default_level
        if out is None:
            out = self._default_out
        self.stop()
        handler = _StreamHandler(out)
        handler.setFormatter(self.formatter)
        handler.setLevel(level)
        if self._task_info:
            handler.addFilter(_TaskIdFilter())
        for logger in self._loggers:
            self._handlers[logger.name] = handler
            logger.addHandler(handler)
            if logger.getEffectiveLevel() > level:
                logger.setLevel(level)

    def stop(self) -> None:
        """Disable logging for all loggers."""
        for logger in self._loggers:
            with _suppress(KeyError):
                logger.removeHandler(self._handlers.pop(logger.name))


def watch(
    *logger_names: str | None,
    level: int = _DEBUG,
    out: _t.TextIO = _stderr,
    colour: bool = False,
    thread_info: bool = True,
    task_info: bool = True,
) -> Watcher:
    """
    Quick wrapper for using  :class:`.Watcher`.

    Create a Watcher with the given configuration, enable watching and return
    it.

    Example::

        from neo4j.debug import watch

        watch("neo4j")
        # from now on, DEBUG logging to stderr is enabled in the driver

    .. note::
        The exact logging format and messages are not part of the API contract
        and might change at any time without notice. They are meant for
        debugging purposes and human consumption only.

    :param logger_names: Names of loggers to watch.
    :param level: see ``default_level`` of :class:`.Watcher`.
    :param out: see ``default_out`` of :class:`.Watcher`.
    :param colour: see ``colour`` of :class:`.Watcher`.
    :param thread_info: see ``thread_info`` of :class:`.Watcher`.
    :param task_info: see ``task_info`` of :class:`.Watcher`.

    :returns: Watcher instance
    :rtype: :class:`.Watcher`

    .. versionchanged:: 5.3

        * Added ``thread_info`` and ``task_info`` parameters.
        * Logging format around thread and task information changed.
    """
    watcher = Watcher(
        *logger_names,
        default_level=level,
        default_out=out,
        colour=colour,
        thread_info=thread_info,
        task_info=task_info,
    )
    watcher.watch()
    return watcher
