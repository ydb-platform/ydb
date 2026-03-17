import asyncio
import io

from ._async_common import _get_all


class AsyncReader:
    def __init__(self, data):
        if type(data) == bytes:
            self.data = io.BytesIO(data)
        else:
            self.data = io.StringIO(data)

    async def read(self, n=-1):
        await asyncio.sleep(0)
        return self.data.read(n)

get_all = _get_all(AsyncReader)