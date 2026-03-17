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

import inspect
import traceback
from copy import deepcopy
from functools import wraps

from ... import _typing as t
from ..._async_compat.concurrency import (
    AsyncLock,
    AsyncRLock,
)
from ..._async_compat.util import AsyncUtil
from ..._debug import ENABLED
from ..._meta import copy_signature


_TWrapped = t.TypeVar("_TWrapped", bound=t.Callable[..., t.Awaitable[t.Any]])
_TWrappedIter = t.TypeVar(
    "_TWrappedIter", bound=t.Callable[..., t.AsyncIterator]
)


class NonConcurrentMethodError(RuntimeError):
    pass


class AsyncNonConcurrentMethodChecker:
    if ENABLED:

        def __init__(self):
            self.__lock = AsyncRLock()
            self.__tracebacks_lock = AsyncLock()
            self.__tracebacks = []

        def __make_error(self, tbs):
            msg = (
                f"Methods of {self.__class__} are not concurrency "
                "safe, but were invoked concurrently."
            )
            if tbs:
                msg += (
                    "\n\nOther invocation site:\n\n"
                    f"{''.join(traceback.format_list(tbs[0]))}"
                )
            return NonConcurrentMethodError(msg)

        @classmethod
        def _non_concurrent_method(cls, f: _TWrapped) -> _TWrapped:
            if AsyncUtil.is_async_code:
                if not inspect.iscoroutinefunction(f):
                    raise TypeError(
                        "cannot decorate non-coroutine function with "
                        "AsyncNonConcurrentMethodChecked._non_concurrent_method"
                    )
            elif not callable(f):
                raise TypeError(
                    "cannot decorate non-callable object with "
                    "NonConcurrentMethodChecked._non_concurrent_method"
                )

            @copy_signature(f)
            @wraps(f)
            async def inner(*args, **kwargs):
                self = args[0]
                assert isinstance(self, cls)

                async with self.__tracebacks_lock:
                    acquired = await self.__lock.acquire(blocking=False)
                    if acquired:
                        self.__tracebacks.append(AsyncUtil.extract_stack())
                    else:
                        tbs = deepcopy(self.__tracebacks)
                if acquired:
                    try:
                        return await f(*args, **kwargs)
                    finally:
                        async with self.__tracebacks_lock:
                            self.__tracebacks.pop()
                            self.__lock.release()
                else:
                    raise self.__make_error(tbs)

            return inner

        @classmethod
        def _non_concurrent_iter(cls, f: _TWrappedIter) -> _TWrappedIter:
            if AsyncUtil.is_async_code:
                if not inspect.isasyncgenfunction(f):
                    raise TypeError(
                        "cannot decorate non-async-generator function with "
                        "AsyncNonConcurrentMethodChecked._non_concurrent_iter"
                    )
            elif not inspect.isgeneratorfunction(f):
                raise TypeError(
                    "cannot decorate non-generator function with "
                    "NonConcurrentMethodChecked._non_concurrent_iter"
                )

            @copy_signature(f)
            @wraps(f)
            async def inner(*args, **kwargs):
                self = args[0]
                assert isinstance(self, cls)

                iter_ = f(*args, **kwargs)
                while True:
                    async with self.__tracebacks_lock:
                        acquired = await self.__lock.acquire(blocking=False)
                        if acquired:
                            self.__tracebacks.append(AsyncUtil.extract_stack())
                        else:
                            tbs = deepcopy(self.__tracebacks)
                    if acquired:
                        try:
                            item = await anext(iter_)
                        except StopAsyncIteration:
                            return
                        finally:
                            async with self.__tracebacks_lock:
                                self.__tracebacks.pop()
                                self.__lock.release()
                        yield item
                    else:
                        raise self.__make_error(tbs)

            return inner

    else:

        @classmethod
        def _non_concurrent_method(cls, f: _TWrapped) -> _TWrapped:
            return f

        @classmethod
        def _non_concurrent_iter(cls, f: _TWrappedIter) -> _TWrappedIter:
            return f
