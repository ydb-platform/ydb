# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import asyncio
import inspect
import traceback
from functools import wraps

from .. import _typing as t


if t.TYPE_CHECKING:
    _T = t.TypeVar("_T")
    _P = t.ParamSpec("_P")


__all__ = [
    "AsyncUtil",
    "Util",
]


class AsyncUtil:
    @staticmethod
    async def list(it):
        return [x async for x in it]

    @staticmethod
    @t.overload
    async def callback(cb: None, *args: object, **kwargs: object) -> None: ...

    @staticmethod
    @t.overload
    async def callback(
        cb: (
            t.Callable[_P, _T | t.Awaitable[_T]]
            | t.Callable[_P, t.Awaitable[_T]]
            | t.Callable[_P, _T]
        ),
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T: ...

    @staticmethod
    async def callback(cb, *args, **kwargs):
        if callable(cb):
            res = cb(*args, **kwargs)
            if inspect.isawaitable(res):
                return await res
            return res
        return None

    @staticmethod
    def shielded(coro_function):
        assert inspect.iscoroutinefunction(coro_function)

        @wraps(coro_function)
        async def shielded_function(*args, **kwargs):
            return await asyncio.shield(coro_function(*args, **kwargs))

        return shielded_function

    is_async_code: t.ClassVar = True

    @staticmethod
    def extract_stack(limit=None):
        # can maybe be improved in the future
        # https://github.com/python/cpython/issues/91048
        stack = asyncio.current_task().get_stack(limit=limit)
        stack_walk = ((f, f.f_lineno) for f in stack)
        return traceback.StackSummary.extract(stack_walk, limit=limit)


class Util:
    list: t.ClassVar = list

    @staticmethod
    @t.overload
    def callback(cb: None, *args: object, **kwargs: object) -> None: ...

    @staticmethod
    @t.overload
    def callback(
        cb: t.Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs
    ) -> _T: ...

    @staticmethod
    def callback(cb, *args, **kwargs):
        if callable(cb):
            return cb(*args, **kwargs)
        return None

    @staticmethod
    def shielded(coro_function):
        return coro_function

    is_async_code: t.ClassVar = False

    @staticmethod
    def extract_stack(limit=None):
        return traceback.extract_stack(limit=limit)[:-1]
