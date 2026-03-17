import io
import types

from ._async_common import _get_all

class AsyncReaderTypesCoroutine:
    def __init__(self, data):
        if type(data) == bytes:
            self.data = io.BytesIO(data)
        else:
            self.data = io.StringIO(data)

    async def _read(self, n=-1):
        return self.data.read(n)

    @types.coroutine
    def read(self, n=-1):
        return (yield from self._read(n))

get_all = _get_all(AsyncReaderTypesCoroutine)
