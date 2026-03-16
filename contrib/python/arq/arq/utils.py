import asyncio
import logging
import os
from collections.abc import AsyncGenerator, Sequence
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from time import time
from typing import TYPE_CHECKING, Any, Optional, overload

from .constants import timezone_env_vars

logger = logging.getLogger('arq.utils')

if TYPE_CHECKING:
    import pytz

    from .typing import SecondsTimedelta
else:
    try:
        import pytz
    except ImportError:  # pragma: no cover
        pytz = None  # type: ignore


def as_int(f: float) -> int:
    return int(round(f))


def timestamp_ms() -> int:
    return as_int(time() * 1000)


def to_unix_ms(dt: datetime) -> int:
    """
    convert a datetime to epoch with milliseconds as int
    """
    return as_int(dt.timestamp() * 1000)


@lru_cache
def get_tz() -> Optional['pytz.BaseTzInfo']:
    if pytz:  # pragma: no branch
        for timezone_key in timezone_env_vars:
            tz_name = os.getenv(timezone_key)
            if tz_name:
                try:
                    return pytz.timezone(tz_name)
                except KeyError:
                    logger.warning('unknown timezone: %r', tz_name)

    return None


def ms_to_datetime(unix_ms: int) -> datetime:
    """
    convert milliseconds to datetime, use the timezone in os.environ
    """
    dt = datetime.fromtimestamp(unix_ms / 1000, tz=timezone.utc)
    tz = get_tz()
    if tz:
        dt = dt.astimezone(tz)
    return dt


@overload
def to_ms(td: None) -> None:
    pass


@overload
def to_ms(td: 'SecondsTimedelta') -> int:
    pass


def to_ms(td: Optional['SecondsTimedelta']) -> Optional[int]:
    if td is None:
        return td
    elif isinstance(td, timedelta):
        td = td.total_seconds()
    return as_int(td * 1000)


@overload
def to_seconds(td: None) -> None:
    pass


@overload
def to_seconds(td: 'SecondsTimedelta') -> float:
    pass


def to_seconds(td: Optional['SecondsTimedelta']) -> Optional[float]:
    if td is None:
        return td
    elif isinstance(td, timedelta):
        return td.total_seconds()
    return td


async def poll(step: float = 0.5) -> AsyncGenerator[float, None]:
    loop = asyncio.get_event_loop()
    start = loop.time()
    while True:
        before = loop.time()
        yield before - start
        after = loop.time()
        wait = max([0, step - after + before])
        await asyncio.sleep(wait)


DEFAULT_CURTAIL = 80


def truncate(s: str, length: int = DEFAULT_CURTAIL) -> str:
    """
    Truncate a string and add an ellipsis (three dots) to the end if it was too long

    :param s: string to possibly truncate
    :param length: length to truncate the string to
    """
    if len(s) > length:
        s = s[: length - 1] + '…'
    return s


def args_to_string(args: Sequence[Any], kwargs: dict[str, Any]) -> str:
    arguments = ''
    if args:
        arguments = ', '.join(map(repr, args))
    if kwargs:
        if arguments:
            arguments += ', '
        arguments += ', '.join(f'{k}={v!r}' for k, v in sorted(kwargs.items()))
    return truncate(arguments)


def import_string(dotted_path: str) -> Any:
    """
    Taken from pydantic.utils.
    """
    from importlib import import_module

    try:
        module_path, class_name = dotted_path.strip(' ').rsplit('.', 1)
    except ValueError as e:
        raise ImportError(f'"{dotted_path}" doesn\'t look like a module path') from e

    module = import_module(module_path)
    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise ImportError(f'Module "{module_path}" does not define a "{class_name}" attribute') from e
