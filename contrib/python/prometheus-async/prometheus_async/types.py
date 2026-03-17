# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2016 Hynek Schlawack
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import sys

from typing import TYPE_CHECKING, Awaitable, Callable, TypeVar


if TYPE_CHECKING:
    from prometheus_async.aio.web import MetricsHTTPServer

try:
    from twisted.internet.defer import Deferred
    from twisted.python.failure import Failure

    # Having these types here results in nicer API docs without
    # private modules in identifiers.
    D = TypeVar("D", bound=Deferred)
    F = TypeVar("F", bound=Failure)
except ImportError:
    pass

# This construct works with Mypy.
# Doing the obvious ImportError route leads to an 'Incompatible import of
# "Protocol"' error.
from typing import Protocol


if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

__all__ = [
    "Deregisterer",
    "IncDecrementer",
    "Incrementer",
    "Observer",
    "ParamSpec",
    "ServiceDiscovery",
]

P = ParamSpec("P")
R = TypeVar("R", bound=Awaitable)
T = TypeVar("T")
C = TypeVar("C", bound=Callable)


Deregisterer = Callable[[], Awaitable[None]]


class ServiceDiscovery(Protocol):
    async def register(
        self, metrics_server: MetricsHTTPServer
    ) -> Deregisterer | None: ...


class Observer(Protocol):
    def observe(self, value: float, /) -> None: ...


class Incrementer(Protocol):
    def inc(
        self, amount: float = 1, exemplar: dict[str, str] | None = None
    ) -> None: ...


class IncDecrementer(Protocol):
    """
    Not used anymore!

    .. deprecated:: 22.2.0
    """

    def inc(
        self, amount: float = 1, exemplar: dict[str, str] | None = None
    ) -> None: ...

    def dec(self, amount: float = 1) -> None: ...
