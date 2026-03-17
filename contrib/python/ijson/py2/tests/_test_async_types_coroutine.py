# -*- coding:utf-8 -*-

import io
import types

from ijson import compat

from ._test_async_common import _get_all, _get_first

class AsyncReaderTypesCoroutine(object):
    def __init__(self, data):
        if type(data) == compat.bytetype:
            self.data = io.BytesIO(data)
        else:
            self.data = io.StringIO(data)

    async def _read(self, n=-1):
        return self.data.read(n)

    @types.coroutine
    def read(self, n=-1):
        return (yield from self._read(n))

get_all = _get_all(AsyncReaderTypesCoroutine)
get_first = _get_first(AsyncReaderTypesCoroutine)
