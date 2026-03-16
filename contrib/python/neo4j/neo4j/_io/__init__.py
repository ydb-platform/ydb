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

import math

from .. import _typing as t  # noqa: TC001


__all__ = [
    "BoltProtocolVersion",
]


class BoltProtocolVersion:
    def __init__(self, major: int, minor: int) -> None:
        if major < 0 or minor < 0:
            raise ValueError(
                "Major and minor versions must be non-negative integers. "
                f"Found {major}, {minor}."
            )
        self.version = (major, minor)

    @property
    def major(self) -> int:
        return self.version[0]

    @property
    def minor(self) -> int:
        return self.version[1]

    def __hash__(self) -> int:
        return hash(self.version)

    def __iter__(self) -> t.Iterator[int]:
        return iter(self.version)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, BoltProtocolVersion):
            return self.version == other.version
        if isinstance(other, tuple):
            return self.version == other
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        if isinstance(other, BoltProtocolVersion):
            return self.version != other.version
        if isinstance(other, tuple):
            return self.version != other
        return NotImplemented

    def __lt__(self, other: BoltProtocolVersion | tuple) -> bool:
        if isinstance(other, BoltProtocolVersion):
            return self.version < other.version
        if isinstance(other, tuple):
            return self.version < other
        return NotImplemented

    def __le__(self, other: BoltProtocolVersion | tuple) -> bool:
        if isinstance(other, BoltProtocolVersion):
            return self.version <= other.version
        if isinstance(other, tuple):
            return self.version <= other
        return NotImplemented

    def __gt__(self, other: BoltProtocolVersion | tuple) -> bool:
        if isinstance(other, BoltProtocolVersion):
            return self.version > other.version
        if isinstance(other, tuple):
            return self.version > other
        return NotImplemented

    def __ge__(self, other: BoltProtocolVersion | tuple) -> bool:
        if isinstance(other, BoltProtocolVersion):
            return self.version >= other.version
        if isinstance(other, tuple):
            return self.version >= other
        return NotImplemented

    def __repr__(self) -> str:
        return f"BoltProtocolVersion({self.major}, {self.minor})"

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}"


def min_timeout(*timeouts: float | None) -> float | None:
    """Return the minimum timeout from an iterable of timeouts."""
    return min(
        (
            to
            for to in timeouts
            if to is not None and not math.isnan(to) and to >= 0
        ),
        default=None,
    )
