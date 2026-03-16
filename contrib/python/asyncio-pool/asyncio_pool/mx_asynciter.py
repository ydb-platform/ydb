'''Mixin for BaseAioPool with async generator _simulation_ for python 3.5'''

import asyncio as aio
from collections import deque
from functools import partial
from .results import getres


class iterwait:

    def __init__(self, futures, *, flat=True, get_result=getres.flat,
            timeout=None, yield_when=aio.ALL_COMPLETED, loop=None):

        self.results = deque()
        self.flat = flat
        self._futures = futures
        self._getres = get_result
        self._wait = partial(aio.wait, timeout=timeout, loop=loop,
                             return_when=yield_when)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not (self._futures or self.results):
            raise StopAsyncIteration()
        while not self.results:
            await self._wait_next()
        return self.results.popleft()

    async def _wait_next(self):
        while True:
            done, self._futures = await self._wait(self._futures)
            if done:
                batch = [self._getres(fut) for fut in done]
                if self.flat:
                    self.results.extend(batch)
                else:
                    self.results.append(batch)
                break


class MxAsyncIterPool(object):

    def itermap(self, fn, iterable, cb=None, ctx=None, *, flat=True,
            get_result=getres.flat, timeout=None,
            yield_when=aio.ALL_COMPLETED):
        '''Spawns coroutines created with `fn` for each item in `iterable`, then
        waits for results with `iterwait`. See docs for `map_n` and `iterwait`.
        '''
        mk_map = partial(self.map_n, fn, iterable, cb=cb, ctx=ctx)
        mk_waiter = partial(iterwait, flat=flat, loop=self.loop,
                            get_result=get_result, timeout=timeout,
                            yield_when=yield_when)

        class _itermap:
            def __aiter__(_self):
                return _self

            async def __anext__(_self):
                if not hasattr(_self, 'waiter'):
                    _self.waiter = mk_waiter(mk_map())
                return await _self.waiter.__anext__()

        return _itermap()
