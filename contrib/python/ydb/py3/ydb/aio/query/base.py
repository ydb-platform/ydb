from .. import _utilities


class AsyncResponseContextIterator(_utilities.AsyncResponseIterator):
    def __init__(self, it, wrapper, on_error=None):
        super().__init__(it, wrapper)
        self._on_error = on_error

    async def __aenter__(self) -> "AsyncResponseContextIterator":
        return self

    async def _next(self):
        try:
            return await super()._next()
        except Exception as e:
            if self._on_error:
                self._on_error(e)
            raise e

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        #  To close stream on YDB it is necessary to scroll through it to the end
        async for _ in self:
            pass
