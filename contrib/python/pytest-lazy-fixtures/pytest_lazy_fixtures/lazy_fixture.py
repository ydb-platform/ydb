from __future__ import annotations

from dataclasses import dataclass
from operator import attrgetter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest


@dataclass
class LazyFixtureWrapper:
    name: str

    @property
    def fixture_name(self) -> str:
        return self.name.split(".", maxsplit=1)[0]

    def _get_attr(self, fixture) -> str | None:
        splitted = self.name.split(".", maxsplit=1)
        if len(splitted) == 1:
            return fixture
        return attrgetter(splitted[1])(fixture)

    def __repr__(self) -> str:
        return self.name

    def load_fixture(self, request: pytest.FixtureRequest):
        return self._get_attr(request.getfixturevalue(self.fixture_name))

    def __hash__(self) -> int:
        return hash(self.name)


def lf(name: str) -> LazyFixtureWrapper:
    """lf is a lazy fixture."""
    return LazyFixtureWrapper(name)
