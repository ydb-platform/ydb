# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
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

import math
from time import monotonic

from .. import _typing as t
from .._async_compat.concurrency import CooperativeLock


if t.TYPE_CHECKING:
    import typing_extensions as te

    TKey: te.TypeAlias = str | tuple[tuple[str, t.Hashable], ...] | tuple[None]
    TVal: te.TypeAlias = tuple[float, str]


class HomeDbCache:
    _ttl: float
    _enabled: bool
    _max_size: int | None

    def __init__(
        self,
        enabled: bool = True,
        ttl: float = float("inf"),
        max_size: int | None = None,
    ) -> None:
        if math.isnan(ttl) or ttl <= 0:
            raise ValueError(f"home db cache ttl must be greater 0, got {ttl}")
        self._enabled = enabled
        self._ttl = ttl
        self._cache: dict[TKey, TVal] = {}
        self._lock = CooperativeLock()
        self._oldest_entry = monotonic()
        if max_size is not None and max_size <= 0:
            raise ValueError(
                f"home db cache max_size must be greater 0 or None, "
                f"got {max_size}"
            )
        self._max_size = max_size
        self._truncate_size = (
            min(max_size, int(0.01 * max_size * math.log(max_size)))
            if max_size is not None
            else None
        )

    def compute_key(
        self,
        imp_user: str | None,
        auth: dict | None,
    ) -> TKey:
        if not self._enabled:
            return (None,)
        if imp_user is not None:
            return imp_user
        if auth is not None:
            return _consolidate_auth_token(auth)
        return (None,)

    def get(self, key: TKey) -> str | None:
        if not self._enabled:
            return None
        with self._lock:
            self._clean(monotonic())
            val = self._cache.get(key)
            if val is None:
                return None
            return val[1]

    def set(self, key: TKey, value: str | None) -> None:
        if not self._enabled:
            return
        with self._lock:
            now = monotonic()
            self._clean(now)
            if value is None:
                self._cache.pop(key, None)
            else:
                self._cache[key] = (now, value)

    def clear(self) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._cache = {}
            self._oldest_entry = monotonic()

    def _clean(self, now: float | None = None) -> None:
        now = monotonic() if now is None else now
        if now - self._oldest_entry > self._ttl:
            self._cache = {
                k: v
                for k, v in self._cache.items()
                if now - v[0] < self._ttl * 0.9
            }
            self._oldest_entry = min(
                (v[0] for v in self._cache.values()), default=now
            )
        if self._max_size and len(self._cache) > self._max_size:
            self._cache = dict(
                sorted(
                    self._cache.items(),
                    key=lambda item: item[1][0],
                    reverse=True,
                )[: self._truncate_size]
            )

    def __len__(self) -> int:
        return len(self._cache)

    @property
    def enabled(self) -> bool:
        return self._enabled


def _consolidate_auth_token(auth: dict) -> tuple | str:
    if auth.get("scheme") == "basic" and isinstance(
        auth.get("principal"), str
    ):
        return auth["principal"]
    return _hashable_dict(auth)


def _hashable_dict(d: dict) -> tuple:
    return tuple(
        (k, _hashable_dict(v) if isinstance(v, dict) else v)
        for k, v in sorted(d.items())
    )
