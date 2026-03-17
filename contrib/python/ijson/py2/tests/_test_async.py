# -*- coding:utf-8 -*-

import asyncio
import io

from ijson import compat

from ._test_async_common import _get_all, _get_first


class AsyncReader(object):
    def __init__(self, data):
        if type(data) == compat.bytetype:
            self.data = io.BytesIO(data)
        else:
            self.data = io.StringIO(data)

    async def read(self, n=-1):
        await asyncio.sleep(0)
        return self.data.read(n)

get_all = _get_all(AsyncReader)
get_first = _get_first(AsyncReader)
