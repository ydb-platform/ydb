"""
This module provides generator utilities for batching items from
iterables and async iterables.

Functions:
    abatcher(generator, batch_size=None, interval=None):
        Asyncio generator wrapper that returns items with a given batch
        size or interval (whichever is reached first).

    batcher(iterable, batch_size=10):
        Generator wrapper that returns items with a given batch size.
"""

import asyncio
import time

import python_utils
from python_utils import types

_T = types.TypeVar('_T')


async def abatcher(
    generator: types.Union[
        types.AsyncGenerator[_T, None],
        types.AsyncIterator[_T],
    ],
    batch_size: types.Optional[int] = None,
    interval: types.Optional[types.delta_type] = None,
) -> types.AsyncGenerator[types.List[_T], None]:
    """
    Asyncio generator wrapper that returns items with a given batch size or
    interval (whichever is reached first).

    Args:
        generator: The async generator or iterator to batch.
        batch_size (types.Optional[int], optional): The number of items per
            batch. Defaults to None.
        interval (types.Optional[types.delta_type], optional): The time
            interval to wait before yielding a batch. Defaults to None.

    Yields:
        types.AsyncGenerator[types.List[_T], None]: A generator that yields
        batches of items.
    """
    batch: types.List[_T] = []

    assert batch_size or interval, 'Must specify either batch_size or interval'

    # If interval is specified, use it to determine when to yield the batch
    # Alternatively set a really long timeout to keep the code simpler
    if interval:
        interval_s = python_utils.delta_to_seconds(interval)
    else:
        # Set the timeout to 10 years
        interval_s = 60 * 60 * 24 * 365 * 10.0

    next_yield: float = time.perf_counter() + interval_s

    done: types.Set[asyncio.Task[_T]]
    pending: types.Set[asyncio.Task[_T]] = set()

    while True:
        try:
            done, pending = await asyncio.wait(
                pending
                or [
                    asyncio.create_task(
                        types.cast(
                            types.Coroutine[None, None, _T],
                            generator.__anext__(),
                        )
                    ),
                ],
                timeout=interval_s,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if done:
                for result in done:
                    batch.append(result.result())

        except StopAsyncIteration:
            if batch:
                yield batch

            break

        if batch_size is not None and len(batch) == batch_size:
            yield batch
            batch = []

        if interval and batch and time.perf_counter() > next_yield:
            yield batch
            batch = []
            # Always set the next yield time to the current time. If the
            # loop is running slow due to blocking functions we do not
            # want to burst too much
            next_yield = time.perf_counter() + interval_s


def batcher(
    iterable: types.Iterable[_T],
    batch_size: int = 10,
) -> types.Generator[types.List[_T], None, None]:
    """
    Generator wrapper that returns items with a given batch size.

    Args:
        iterable (types.Iterable[_T]): The iterable to batch.
        batch_size (int, optional): The number of items per batch. Defaults
            to 10.

    Yields:
        types.Generator[types.List[_T], None, None]: A generator that yields
        batches of items.
    """
    batch: types.List[_T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []

    if batch:
        yield batch
