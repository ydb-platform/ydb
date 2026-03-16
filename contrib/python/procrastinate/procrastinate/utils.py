from __future__ import annotations

import asyncio
import contextlib
import datetime
import importlib
import inspect
import logging
import pathlib
import types
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Iterable,
)
from typing import (
    Any,
    Generic,
    TypeVar,
)

import dateutil.parser
from asgiref import sync

from procrastinate import exceptions
from procrastinate.types import TimeDeltaParams

T = TypeVar("T")
U = TypeVar("U")

logger = logging.getLogger(__name__)


def load_from_path(path: str, allowed_type: type[T] | None = None) -> T:
    """
    Import and return then object at the given full python path.
    """
    if "." not in path:
        raise exceptions.LoadFromPathError(f"{path} is not a valid path")
    module_path, name = path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise exceptions.LoadFromPathError(str(exc)) from exc

    try:
        imported = getattr(module, name)
    except AttributeError as exc:
        raise exceptions.LoadFromPathError(str(exc)) from exc

    if allowed_type and not isinstance(imported, allowed_type):
        raise exceptions.LoadFromPathError(
            f"Object at {path} is not of type {allowed_type.__name__} "
            f"but {type(imported).__name__}"
        )

    return imported


def import_all(import_paths: Iterable[str]) -> None:
    """
    Given a list of paths, just import them all
    """
    for import_path in import_paths:
        logger.debug(
            f"Importing module {import_path}",
            extra={"action": "import_module", "module_name": import_path},
        )
        importlib.import_module(import_path)


def caller_module_name(prefix: str = "procrastinate") -> str:
    """
    Returns the module name of the first module of the stack that isn't under ``prefix``.
    If any problem occurs, raise `CallerModuleUnknown`.
    """

    try:
        frame = inspect.currentframe()
        while True:
            if not frame:
                raise ValueError("Empty frame")
            name = frame.f_globals["__name__"]  # May raise ValueError
            if not name.startswith(f"{prefix}."):
                break
            frame = frame.f_back
        return name
    except Exception as exc:
        raise exceptions.CallerModuleUnknown from exc


def async_to_sync(
    awaitable: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any
) -> T:
    """
    Given a callable returning an awaitable, call the callable, await it
    synchronously. Returns the result after it's done.
    """
    return sync.async_to_sync(awaitable)(*args, **kwargs)


async def sync_to_async(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """
    Given a callable, return a callable that will call the original one in an
    async context.
    """
    return await sync.sync_to_async(func, thread_sensitive=False)(*args, **kwargs)


def causes(exc: BaseException | None):
    """
    From a single exception with a chain of causes and contexts, make an iterable
    going through every exception in the chain.
    """
    while exc:
        yield exc
        exc = exc.__cause__ or exc.__context__


def _get_module_name(obj: Any) -> str:
    module_name = obj.__module__
    if module_name != "__main__":
        return module_name

    module = inspect.getmodule(obj)
    # obj could be None, or has no __file__ if in an interactive shell
    # in which case, there's not a lot we can do.
    if not module or not getattr(module, "__file__", None):
        return module_name

    # We've checked this just above but mypy needs a direct proof
    assert module.__file__

    path = pathlib.Path(module.__file__)
    # If the path is absolute, it probably means __main__ is an executable from an
    # installed package
    if path.is_absolute():
        return module_name

    # Creating the dotted path from the path
    return ".".join([*path.parts[:-1], path.stem])


def get_full_path(obj: Any) -> str:
    try:
        name = obj.__name__
    except AttributeError as exc:
        raise exceptions.FunctionPathError(
            f"Couldn't extract a relevant task name for {obj}. Try passing `name=` to the task decorator"
        ) from exc
    return f"{_get_module_name(obj)}.{name}"


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


def utcmax() -> datetime.datetime:
    return datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)


def parse_datetime(raw: str) -> datetime.datetime:
    try:
        # this parser is the stricter one, so we try it first
        dt = dateutil.parser.isoparse(raw)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt
    except ValueError:
        pass
    # this parser is quite forgiving, and will attempt to return
    # a value in most circumstances, so we use it as last option
    dt = dateutil.parser.parse(raw)
    dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt


class AwaitableContext(Generic[U]):
    """
    Provides an object that can be called this way:
    - value = await AppContext(...)
    - async with AppContext(...) as value: ...

    open_coro and close_coro are functions taking on arguments and returning coroutines.
    """

    def __init__(
        self,
        open_coro: Callable[[], Awaitable[Any]],
        close_coro: Callable[[], Awaitable[Any]],
        return_value: U,
    ):
        self._open_coro = open_coro
        self._close_coro = close_coro
        self._return_value = return_value

    async def __aenter__(self) -> U:
        await self._open_coro()
        return self._return_value

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any):
        await self._close_coro()

    def __await__(self):
        async def _inner_coro() -> U:
            await self._open_coro()
            return self._return_value

        return _inner_coro().__await__()


async def cancel_and_capture_errors(tasks: list[asyncio.Task[Any]]):
    """
    Cancel all tasks and capture any error returned by any of those tasks (except the CancellationError itself)
    """

    def log_task_exception(task: asyncio.Task[Any], error: BaseException):
        logger.exception(
            f"{task.get_name()} error: {error!r}",
            exc_info=error,
            extra={
                "action": f"{task.get_name()}_error",
            },
        )

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    for task in (task for task in tasks if task.done() and not task.cancelled()):
        error = task.exception()
        if error:
            log_task_exception(task, error=error)
        else:
            logger.debug(f"Cancelled task {task.get_name()}")


async def wait_any(*coros_or_futures: Coroutine[Any, Any, Any] | asyncio.Future[Any]):
    """Starts and wait on the first coroutine to complete and return it
    Other pending coroutines are either cancelled or left running"""
    futures = set(asyncio.ensure_future(fut) for fut in coros_or_futures)

    _, pending = await asyncio.wait(
        futures,
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


def add_namespace(name: str, namespace: str) -> str:
    return f"{namespace}:{name}" if namespace else name


def import_or_wrapper(*names: str) -> Iterable[types.ModuleType]:
    """
    Import given modules, or return a dummy wrapper that will raise an
    ImportError when used.
    """
    try:
        for name in names:
            yield importlib.import_module(name)
    except ImportError as exc:
        # In case psycopg is not installed, we'll raise an explicit error
        # only when the connector is used.
        exception = exc

        class Wrapper:
            def __getattr__(self, item: str):
                raise exception

        yield Wrapper()  # pyright: ignore[reportReturnType]


class MovedElsewhere:
    def __init__(self, name: str, new_location: str):
        self.name = name
        self.new_location = new_location

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.x

    def __getattr__(self, item: str):
        raise exceptions.MovedElsewhere(
            f"procrastinate.{self.name} has been moved to {self.new_location}"
        )


V = TypeVar("V")


async def gen_with_timeout(
    aiterable: AsyncIterator[V],
    timeout: float,
    raise_timeout: bool,
) -> AsyncGenerator[V, None]:
    """
    Wrap an async generator to add a timeout to each iteration.
    """
    aiterator = aiterable.__aiter__()
    while True:
        try:
            yield await asyncio.wait_for(aiterator.__anext__(), timeout=timeout)
        except StopAsyncIteration:
            break
        except asyncio.TimeoutError:
            if raise_timeout:
                raise
            else:
                return


def async_context_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
    return contextlib.asynccontextmanager(func)


def datetime_from_timedelta_params(params: TimeDeltaParams) -> datetime.datetime:
    try:
        return utcnow() + datetime.timedelta(**params)
    except OverflowError:
        return utcmax()


def queues_display(queues: Iterable[str] | None) -> str:
    if queues:
        return f"queues {', '.join(queues)}"
    else:
        return "all queues"
