class AsyncResponseIterator(object):
    def __init__(self, it, wrapper):
        self.it = it.__aiter__()
        self.wrapper = wrapper

    def cancel(self):
        self.it.cancel()
        return self

    def __iter__(self):
        return self

    def __aiter__(self):
        return self

    async def _next(self):
        return self.wrapper(await self.it.__anext__())

    async def next(self):
        return await self._next()

    async def __anext__(self):
        return await self._next()
