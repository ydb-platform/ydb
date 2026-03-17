from collections import defaultdict
from collections.abc import Iterator
from itertools import count
from typing import Any, TypeAlias

from dishka.entities.component import Component
from dishka.entities.key import DependencyKey

RawKey: TypeAlias = tuple[Any, Component | None]


class MovedObjectsTracker:
    """
    A key transformer to distinguish decorated or united factories.
    """
    def __init__(self) -> None:
        self.depth: dict[RawKey, Iterator[int]] = defaultdict(count)

    def move(self, provides: DependencyKey) -> DependencyKey:
        return DependencyKey(
            provides.type_hint,
            provides.component,
            next(self.depth[provides.type_hint, provides.component]) + 1,
        )
