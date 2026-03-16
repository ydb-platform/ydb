from __future__ import annotations

import atexit
import contextlib
import datetime
import io
import logging
import os
import re
import sys
from types import TracebackType
from typing import Iterable, Iterator

from python_utils import types
from python_utils.converters import scale_1024
from python_utils.terminal import get_terminal_size
from python_utils.time import epoch, format_time, timedelta_to_seconds

from progressbar import base, env, terminal

if types.TYPE_CHECKING:
    from .bar import ProgressBar, ProgressBarMixinBase

# Make sure these are available for import
assert timedelta_to_seconds is not None
assert get_terminal_size is not None
assert format_time is not None
assert scale_1024 is not None
assert epoch is not None

StringT = types.TypeVar('StringT', bound=types.StringTypes)


def deltas_to_seconds(
    *deltas: None | datetime.timedelta | float,
    default: types.Optional[types.Type[ValueError]] = ValueError,
) -> int | float | None:
    """
    Convert timedeltas and seconds as int to seconds as float while coalescing.

    >>> deltas_to_seconds(datetime.timedelta(seconds=1, milliseconds=234))
    1.234
    >>> deltas_to_seconds(123)
    123.0
    >>> deltas_to_seconds(1.234)
    1.234
    >>> deltas_to_seconds(None, 1.234)
    1.234
    >>> deltas_to_seconds(0, 1.234)
    0.0
    >>> deltas_to_seconds()
    Traceback (most recent call last):
    ...
    ValueError: No valid deltas passed to `deltas_to_seconds`
    >>> deltas_to_seconds(None)
    Traceback (most recent call last):
    ...
    ValueError: No valid deltas passed to `deltas_to_seconds`
    >>> deltas_to_seconds(default=0.0)
    0.0
    """
    for delta in deltas:
        if delta is None:
            continue
        if isinstance(delta, datetime.timedelta):
            return timedelta_to_seconds(delta)
        elif not isinstance(delta, float):
            return float(delta)
        else:
            return delta

    if default is ValueError:
        raise ValueError('No valid deltas passed to `deltas_to_seconds`')
    else:
        # mypy doesn't understand the `default is ValueError` check
        return default  # type: ignore


def no_color(value: StringT) -> StringT:
    """
    Return the `value` without ANSI escape codes.

    >>> no_color(b'\u001b[1234]abc')
    b'abc'
    >>> str(no_color('\u001b[1234]abc'))
    'abc'
    >>> str(no_color('\u001b[1234]abc'))
    'abc'
    >>> no_color(123)
    Traceback (most recent call last):
    ...
    TypeError: `value` must be a string or bytes, got 123
    """
    if isinstance(value, bytes):
        pattern: bytes = bytes(terminal.ESC, 'ascii') + b'\\[.*?[@-~]'
        return re.sub(pattern, b'', value)  # type: ignore
    elif isinstance(value, str):
        return re.sub('\x1b\\[.*?[@-~]', '', value)  # type: ignore
    else:
        raise TypeError(f'`value` must be a string or bytes, got {value!r}')


def len_color(value: types.StringTypes) -> int:
    """
    Return the length of `value` without ANSI escape codes.

    >>> len_color(b'\u001b[1234]abc')
    3
    >>> len_color('\u001b[1234]abc')
    3
    >>> len_color('\u001b[1234]abc')
    3
    """
    return len(no_color(value))


class WrappingIO:
    buffer: io.StringIO
    target: base.IO
    capturing: bool
    listeners: set
    needs_clear: bool = False

    def __init__(
        self,
        target: base.IO,
        capturing: bool = False,
        listeners: types.Optional[types.Set[ProgressBar]] = None,
    ) -> None:
        self.buffer = io.StringIO()
        self.target = target
        self.capturing = capturing
        self.listeners = listeners or set()
        self.needs_clear = False

    def write(self, value: str) -> int:
        ret = 0
        if self.capturing:
            ret += self.buffer.write(value)
            if '\n' in value:  # pragma: no branch
                self.needs_clear = True
                for listener in self.listeners:  # pragma: no branch
                    listener.update()
        else:
            ret += self.target.write(value)
            if '\n' in value:  # pragma: no branch
                self.flush_target()

        return ret

    def flush(self) -> None:
        self.buffer.flush()

    def _flush(self) -> None:
        if value := self.buffer.getvalue():
            self.flush()
            self.target.write(value)
            self.buffer.seek(0)
            self.buffer.truncate(0)
            self.needs_clear = False

        # when explicitly flushing, always flush the target as well
        self.flush_target()

    def flush_target(self) -> None:  # pragma: no cover
        if not self.target.closed and getattr(self.target, 'flush', None):
            self.target.flush()

    def __enter__(self) -> WrappingIO:
        return self

    def fileno(self) -> int:
        return self.target.fileno()

    def isatty(self) -> bool:
        return self.target.isatty()

    def read(self, n: int = -1) -> str:
        return self.target.read(n)

    def readable(self) -> bool:
        return self.target.readable()

    def readline(self, limit: int = -1) -> str:
        return self.target.readline(limit)

    def readlines(self, hint: int = -1) -> list[str]:
        return self.target.readlines(hint)

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        return self.target.seek(offset, whence)

    def seekable(self) -> bool:
        return self.target.seekable()

    def tell(self) -> int:
        return self.target.tell()

    def truncate(self, size: types.Optional[int] = None) -> int:
        return self.target.truncate(size)

    def writable(self) -> bool:
        return self.target.writable()

    def writelines(self, lines: Iterable[str]) -> None:
        return self.target.writelines(lines)

    def close(self) -> None:
        self.flush()
        self.target.close()

    def __next__(self) -> str:
        return self.target.__next__()

    def __iter__(self) -> Iterator[str]:
        return self.target.__iter__()

    def __exit__(
        self,
        __t: type[BaseException] | None,
        __value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> None:
        self.close()


class StreamWrapper:
    """Wrap stdout and stderr globally."""

    stdout: base.TextIO | WrappingIO
    stderr: base.TextIO | WrappingIO
    original_excepthook: types.Callable[
        [
            types.Type[BaseException],
            BaseException,
            TracebackType | None,
        ],
        None,
    ]
    wrapped_stdout: int = 0
    wrapped_stderr: int = 0
    wrapped_excepthook: int = 0
    capturing: int = 0
    listeners: set

    def __init__(self) -> None:
        self.stdout = self.original_stdout = sys.stdout
        self.stderr = self.original_stderr = sys.stderr
        self.original_excepthook = sys.excepthook
        self.wrapped_stdout = 0
        self.wrapped_stderr = 0
        self.wrapped_excepthook = 0
        self.capturing = 0
        self.listeners = set()

        if env.env_flag('WRAP_STDOUT', default=False):  # pragma: no cover
            self.wrap_stdout()

        if env.env_flag('WRAP_STDERR', default=False):  # pragma: no cover
            self.wrap_stderr()

    def start_capturing(self, bar: ProgressBarMixinBase | None = None) -> None:
        if bar:  # pragma: no branch
            self.listeners.add(bar)

        self.capturing += 1
        self.update_capturing()

    def stop_capturing(self, bar: ProgressBarMixinBase | None = None) -> None:
        if bar:  # pragma: no branch
            with contextlib.suppress(KeyError):
                self.listeners.remove(bar)

        self.capturing -= 1
        self.update_capturing()

    def update_capturing(self) -> None:  # pragma: no cover
        if isinstance(self.stdout, WrappingIO):
            self.stdout.capturing = self.capturing > 0

        if isinstance(self.stderr, WrappingIO):
            self.stderr.capturing = self.capturing > 0

        if self.capturing <= 0:
            self.flush()

    def wrap(self, stdout: bool = False, stderr: bool = False) -> None:
        if stdout:
            self.wrap_stdout()

        if stderr:
            self.wrap_stderr()

    def wrap_stdout(self) -> WrappingIO:
        self.wrap_excepthook()

        if not self.wrapped_stdout:
            self.stdout = sys.stdout = WrappingIO(  # type: ignore
                self.original_stdout,
                listeners=self.listeners,
            )
        self.wrapped_stdout += 1

        return sys.stdout  # type: ignore

    def wrap_stderr(self) -> WrappingIO:
        self.wrap_excepthook()

        if not self.wrapped_stderr:
            self.stderr = sys.stderr = WrappingIO(  # type: ignore
                self.original_stderr,
                listeners=self.listeners,
            )
        self.wrapped_stderr += 1

        return sys.stderr  # type: ignore

    def unwrap_excepthook(self) -> None:
        if self.wrapped_excepthook:
            self.wrapped_excepthook -= 1
            sys.excepthook = self.original_excepthook

    def wrap_excepthook(self) -> None:
        if not self.wrapped_excepthook:
            logger.debug('wrapping excepthook')
            self.wrapped_excepthook += 1
            sys.excepthook = self.excepthook

    def unwrap(self, stdout: bool = False, stderr: bool = False) -> None:
        if stdout:
            self.unwrap_stdout()

        if stderr:
            self.unwrap_stderr()

    def unwrap_stdout(self) -> None:
        if self.wrapped_stdout > 1:
            self.wrapped_stdout -= 1
        else:
            sys.stdout = self.original_stdout
            self.wrapped_stdout = 0

    def unwrap_stderr(self) -> None:
        if self.wrapped_stderr > 1:
            self.wrapped_stderr -= 1
        else:
            sys.stderr = self.original_stderr
            self.wrapped_stderr = 0

    def needs_clear(self) -> bool:  # pragma: no cover
        stdout_needs_clear = getattr(self.stdout, 'needs_clear', False)
        stderr_needs_clear = getattr(self.stderr, 'needs_clear', False)
        return stderr_needs_clear or stdout_needs_clear

    def flush(self) -> None:
        if self.wrapped_stdout and isinstance(self.stdout, WrappingIO):
            try:
                self.stdout._flush()
            except io.UnsupportedOperation:  # pragma: no cover
                self.wrapped_stdout = False
                logger.warning(
                    'Disabling stdout redirection, %r is not seekable',
                    sys.stdout,
                )

        if self.wrapped_stderr and isinstance(self.stderr, WrappingIO):
            try:
                self.stderr._flush()
            except io.UnsupportedOperation:  # pragma: no cover
                self.wrapped_stderr = False
                logger.warning(
                    'Disabling stderr redirection, %r is not seekable',
                    sys.stderr,
                )

    def excepthook(
        self,
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: types.TracebackType | None,
    ) -> None:
        self.original_excepthook(exc_type, exc_value, exc_traceback)
        self.flush()


class AttributeDict(dict):
    """
    A dict that can be accessed with .attribute.

    >>> attrs = AttributeDict(spam=123)

    # Reading

    >>> attrs['spam']
    123
    >>> attrs.spam
    123

    # Read after update using attribute

    >>> attrs.spam = 456
    >>> attrs['spam']
    456
    >>> attrs.spam
    456

    # Read after update using dict access

    >>> attrs['spam'] = 123
    >>> attrs['spam']
    123
    >>> attrs.spam
    123

    # Read after update using dict access

    >>> del attrs.spam
    >>> attrs['spam']
    Traceback (most recent call last):
    ...
    KeyError: 'spam'
    >>> attrs.spam
    Traceback (most recent call last):
    ...
    AttributeError: No such attribute: spam
    >>> del attrs.spam
    Traceback (most recent call last):
    ...
    AttributeError: No such attribute: spam
    """

    def __getattr__(self, name: str) -> int:
        if name in self:
            return self[name]
        else:
            raise AttributeError(f'No such attribute: {name}')

    def __setattr__(self, name: str, value: int) -> None:
        self[name] = value

    def __delattr__(self, name: str) -> None:
        if name in self:
            del self[name]
        else:
            raise AttributeError(f'No such attribute: {name}')


logger: logging.Logger = logging.getLogger(__name__)
streams = StreamWrapper()
atexit.register(streams.flush)
