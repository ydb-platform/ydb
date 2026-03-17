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


if t.TYPE_CHECKING:
    from .._work import GqlStatusObject


class NotificationPrinter:
    notification: GqlStatusObject
    query: str | None

    def __init__(
        self,
        notification: GqlStatusObject,
        query: str | None = None,
        one_line: bool = False,
    ) -> None:
        self.notification = notification
        self.query = query
        self._one_line = one_line

    def __str__(self):
        if self.query is None:
            return repr(self.notification)
        if self._one_line:
            return f"{self.notification!r} for query: {self.query!r}"
        pos = self.notification.position
        if pos is None:
            return f"{self.notification!r} for query:\n{self.query}"
        s = f"{self.notification!r} for query:\n"
        query_lines = self.query.splitlines()
        if pos.line <= 0 or pos.line > len(query_lines) or pos.column <= 0:
            return s + self.query
        query_lines = (
            *query_lines[: pos.line],
            " " * (pos.column - 1) + "^",
            *query_lines[pos.line :],
        )
        s += "\n".join(query_lines)
        return s
