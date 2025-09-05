import asyncio


class AsyncResponseIterator(object):
    def __init__(self, it, wrapper, error_converter=None):
        self.it = it.__aiter__()
        self.wrapper = wrapper
        self.error_converter = error_converter

    def cancel(self):
        self.it.cancel()
        return self

    def __iter__(self):
        return self

    def __aiter__(self):
        return self

    async def _next(self):
        try:
            res = self.wrapper(await self.it.__anext__())
        except BaseException as e:
            if self.error_converter:
                raise self.error_converter(e) from e
            raise e

        if res is not None:
            return res
        return await self._next()

    async def next(self):
        return await self._next()

    async def __anext__(self):
        return await self._next()


async def get_first_message_with_timeout(stream: AsyncResponseIterator, timeout: int):
    async def get_first_response():
        return await stream.next()

    return await asyncio.wait_for(get_first_response(), timeout)
