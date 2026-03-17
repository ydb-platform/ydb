"""Asyncio equivalents to regular Python functions."""

import asyncio
import itertools
import typing

from . import types

_N = types.TypeVar('_N', int, float)
_T = types.TypeVar('_T')
_K = types.TypeVar('_K')
_V = types.TypeVar('_V')


async def acount(
    start: _N = 0,
    step: _N = 1,
    delay: float = 0,
    stop: types.Optional[_N] = None,
) -> types.AsyncIterator[_N]:
    """Asyncio version of itertools.count()."""
    for item in itertools.count(start, step):  # pragma: no branch
        if stop is not None and item >= stop:
            break

        yield item
        await asyncio.sleep(delay)


@typing.overload
async def acontainer(
    iterable: types.Union[
        types.AsyncIterable[_T],
        types.Callable[..., types.AsyncIterable[_T]],
    ],
    container: types.Type[types.Tuple[_T, ...]],
) -> types.Tuple[_T, ...]: ...


@typing.overload
async def acontainer(
    iterable: types.Union[
        types.AsyncIterable[_T],
        types.Callable[..., types.AsyncIterable[_T]],
    ],
    container: types.Type[types.List[_T]] = list,
) -> types.List[_T]: ...


@typing.overload
async def acontainer(
    iterable: types.Union[
        types.AsyncIterable[_T],
        types.Callable[..., types.AsyncIterable[_T]],
    ],
    container: types.Type[types.Set[_T]],
) -> types.Set[_T]: ...


async def acontainer(
    iterable: types.Union[
        types.AsyncIterable[_T],
        types.Callable[..., types.AsyncIterable[_T]],
    ],
    container: types.Callable[
        [types.Iterable[_T]], types.Collection[_T]
    ] = list,
) -> types.Collection[_T]:
    """
    Asyncio version of list()/set()/tuple()/etc() using an async for loop.

    So instead of doing `[item async for item in iterable]` you can do
    `await acontainer(iterable)`.

    """
    iterable_: types.AsyncIterable[_T]
    if callable(iterable):
        iterable_ = iterable()
    else:
        iterable_ = iterable

    item: _T
    items: types.List[_T] = []
    async for item in iterable_:  # pragma: no branch
        items.append(item)

    return container(items)


async def adict(
    iterable: types.Union[
        types.AsyncIterable[types.Tuple[_K, _V]],
        types.Callable[..., types.AsyncIterable[types.Tuple[_K, _V]]],
    ],
    container: types.Callable[
        [types.Iterable[types.Tuple[_K, _V]]], types.Mapping[_K, _V]
    ] = dict,
) -> types.Mapping[_K, _V]:
    """
    Asyncio version of dict() using an async for loop.

    So instead of doing `{key: value async for key, value in iterable}` you
    can do `await adict(iterable)`.

    """
    iterable_: types.AsyncIterable[types.Tuple[_K, _V]]
    if callable(iterable):
        iterable_ = iterable()
    else:
        iterable_ = iterable

    item: types.Tuple[_K, _V]
    items: types.List[types.Tuple[_K, _V]] = []
    async for item in iterable_:  # pragma: no branch
        items.append(item)

    return container(items)
