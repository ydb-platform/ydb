from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from clickhouse_connect.driver._backend.models import _freeze_mapping


@dataclass(frozen=True)
class CommandOp:
    text: str
    settings: Mapping[str, Any] = field(default_factory=dict)
    use_database: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "settings", _freeze_mapping(self.settings))


@dataclass(frozen=True)
class QueryOp:
    text: str
    settings: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "settings", _freeze_mapping(self.settings))


@dataclass(frozen=True)
class RawQueryOp:
    text: str
    settings: Mapping[str, Any] = field(default_factory=dict)
    fmt: str = "Native"

    def __post_init__(self) -> None:
        object.__setattr__(self, "settings", _freeze_mapping(self.settings))


Operation = CommandOp | QueryOp | RawQueryOp
