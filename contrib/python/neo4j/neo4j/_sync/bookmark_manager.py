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

from .. import _typing as t
from .._async_compat.concurrency import CooperativeLock
from .._async_compat.util import Util
from ..api import (
    BookmarkManager,
    Bookmarks,
)


TBmSupplier = t.Callable[[], Bookmarks | t.Union[Bookmarks]]
TBmConsumer = t.Callable[[Bookmarks], t.Union[None] | None]


class Neo4jBookmarkManager(BookmarkManager):
    def __init__(
        self,
        initial_bookmarks: Bookmarks | None = None,
        bookmarks_supplier: TBmSupplier | None = None,
        bookmarks_consumer: TBmConsumer | None = None,
    ) -> None:
        super().__init__()
        self._bookmarks_supplier = bookmarks_supplier
        self._bookmarks_consumer = bookmarks_consumer
        if not initial_bookmarks:
            self._bookmarks = set()
        else:
            self._bookmarks = set(initial_bookmarks.raw_values)
        self._lock = CooperativeLock()

    def update_bookmarks(
        self,
        previous_bookmarks: t.Collection[str],
        new_bookmarks: t.Collection[str],
    ) -> None:
        if not new_bookmarks:
            return
        with self._lock:
            self._bookmarks.difference_update(previous_bookmarks)
            self._bookmarks.update(new_bookmarks)
            if self._bookmarks_consumer:
                curr_bms_snapshot = Bookmarks.from_raw_values(self._bookmarks)
        if self._bookmarks_consumer:
            Util.callback(
                self._bookmarks_consumer, curr_bms_snapshot
            )

    def get_bookmarks(self) -> set[str]:
        with self._lock:
            bms = set(self._bookmarks)
        if self._bookmarks_supplier:
            extra_bms = Util.callback(self._bookmarks_supplier)
            bms.update(extra_bms.raw_values)
        return bms
