from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Iterable, Iterator
from typing import TYPE_CHECKING, Any, TypeVar, overload

from ..types import AnyType

__all__ = [
    "as_completed",
    "wait",
]


def as_completed(
    fs: Iterable[Awaitable[AnyType]], *, timeout: float | None = None
) -> Iterator[Awaitable[AnyType]]:
    """Return an iterator whose values are coroutines.

    This is a deterministic version of :py:func:`asyncio.as_completed`. This
    function should be used instead of that one in workflows.
    """
    # Taken almost verbatim from
    # https://github.com/python/cpython/blob/v3.12.3/Lib/asyncio/tasks.py#L584
    # but the "set" is changed out for a "list" and fixed up some typing/format

    if asyncio.isfuture(fs) or asyncio.iscoroutine(fs):
        raise TypeError(f"expect an iterable of futures, not {type(fs).__name__}")

    done: asyncio.Queue[asyncio.Future | None] = asyncio.Queue()

    loop = asyncio.get_event_loop()
    todo: list[asyncio.Future] = [asyncio.ensure_future(f, loop=loop) for f in list(fs)]
    timeout_handle = None

    def _on_timeout():
        for f in todo:
            f.remove_done_callback(_on_completion)
            done.put_nowait(None)  # Queue a dummy value for _wait_for_one().
        todo.clear()  # Can't do todo.remove(f) in the loop.

    def _on_completion(f):  # type:ignore[reportMissingParameterType]
        if not todo:
            return  # _on_timeout() was here first.
        todo.remove(f)
        done.put_nowait(f)
        if not todo and timeout_handle is not None:
            timeout_handle.cancel()

    async def _wait_for_one():
        f = await done.get()
        if f is None:
            # Dummy value from _on_timeout().
            raise asyncio.TimeoutError
        return f.result()  # May raise f.exception().

    for f in todo:
        f.add_done_callback(_on_completion)
    if todo and timeout is not None:
        timeout_handle = loop.call_later(timeout, _on_timeout)
    for _ in range(len(todo)):
        yield _wait_for_one()


if TYPE_CHECKING:
    _FT = TypeVar("_FT", bound=asyncio.Future[Any])
else:
    _FT = TypeVar("_FT", bound=asyncio.Future)


@overload
async def wait(  # type: ignore[misc]
    fs: Iterable[_FT],
    *,
    timeout: float | None = None,
    return_when: str = asyncio.ALL_COMPLETED,
) -> tuple[list[_FT], list[_FT]]: ...


@overload
async def wait(
    fs: Iterable[asyncio.Task[AnyType]],
    *,
    timeout: float | None = None,
    return_when: str = asyncio.ALL_COMPLETED,
) -> tuple[list[asyncio.Task[AnyType]], list[asyncio.Task[AnyType]]]: ...


async def wait(
    fs: Iterable,
    *,
    timeout: float | None = None,
    return_when: str = asyncio.ALL_COMPLETED,
) -> tuple:
    """Wait for the Futures or Tasks given by fs to complete.

    This is a deterministic version of :py:func:`asyncio.wait`. This function
    should be used instead of that one in workflows.
    """
    # Taken almost verbatim from
    # https://github.com/python/cpython/blob/v3.12.3/Lib/asyncio/tasks.py#L435
    # but the "set" is changed out for a "list" and fixed up some typing/format

    if asyncio.isfuture(fs) or asyncio.iscoroutine(fs):
        raise TypeError(f"Expect an iterable of Tasks/Futures, not {type(fs).__name__}")
    if not fs:
        raise ValueError("Sequence of Tasks/Futures must not be empty.")
    if return_when not in (
        asyncio.FIRST_COMPLETED,
        asyncio.FIRST_EXCEPTION,
        asyncio.ALL_COMPLETED,
    ):
        raise ValueError(f"Invalid return_when value: {return_when}")

    fs = list(fs)

    if any(asyncio.iscoroutine(f) for f in fs):
        raise TypeError("Passing coroutines is forbidden, use tasks explicitly.")

    loop = asyncio.get_running_loop()
    return await _wait(fs, timeout, return_when, loop)


async def _wait(
    fs: Iterable[asyncio.Future | asyncio.Task],
    timeout: float | None,
    return_when: str,
    loop: asyncio.AbstractEventLoop,
) -> tuple[list, list]:
    # Taken almost verbatim from
    # https://github.com/python/cpython/blob/v3.12.3/Lib/asyncio/tasks.py#L522
    # but the "set" is changed out for a "list" and fixed up some typing/format

    assert fs, "Sequence of Tasks/Futures must not be empty."
    waiter = loop.create_future()
    timeout_handle = None
    if timeout is not None:
        timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
    counter = len(fs)  # type: ignore[arg-type]

    def _on_completion(f):  # type:ignore[reportMissingParameterType]
        nonlocal counter
        counter -= 1
        if (
            counter <= 0
            or return_when == asyncio.FIRST_COMPLETED
            or return_when == asyncio.FIRST_EXCEPTION
            and (not f.cancelled() and f.exception() is not None)
        ):
            if timeout_handle is not None:
                timeout_handle.cancel()
            if not waiter.done():
                waiter.set_result(None)

    for f in fs:
        f.add_done_callback(_on_completion)

    try:
        await waiter
    finally:
        if timeout_handle is not None:
            timeout_handle.cancel()
        for f in fs:
            f.remove_done_callback(_on_completion)

    done, pending = [], []
    for f in fs:
        if f.done():
            done.append(f)
        else:
            pending.append(f)
    return done, pending


def _release_waiter(waiter: asyncio.Future[Any], *_args: Any) -> None:
    # Taken almost verbatim from
    # https://github.com/python/cpython/blob/v3.12.3/Lib/asyncio/tasks.py#L467

    if not waiter.done():
        waiter.set_result(None)
