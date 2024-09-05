from .. import _utilities


class AsyncResponseContextIterator(_utilities.AsyncResponseIterator):
    async def __aenter__(self) -> "AsyncResponseContextIterator":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        #  To close stream on YDB it is necessary to scroll through it to the end
        async for _ in self:
            pass
