from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import tzinfo
from types import MappingProxyType
from typing import Any

from clickhouse_connect.driver.models import SettingDef
from clickhouse_connect.driver.query import TzSource


def _freeze_mapping(values: Mapping[str, Any]) -> Mapping[str, Any]:
    # MappingProxyType fields make the frozen dataclasses that hold them
    # unhashable. They are value objects compared by equality only.
    return MappingProxyType(dict(values))


@dataclass(frozen=True)
class Capabilities:
    """Feature flags a backend reports about its transport and engine.

    native_async: the transport is genuinely asynchronous rather than sync
        calls offloaded to threads.
    sessions: the backend supports server-side sessions (session_id).

    New backend-varying features get a field here rather than loose
    supports_* attributes (PR #811's flags map to fields when reconciled).
    """

    native_async: bool = False
    sessions: bool = False


@dataclass(frozen=True)
class ClientConfig:
    database: str | None = None
    query_limit: int = 0
    query_retries: int = 2
    settings: Mapping[str, Any] = field(default_factory=dict)
    timezone_policy: TzSource = "auto"

    def __post_init__(self) -> None:
        object.__setattr__(self, "settings", _freeze_mapping(self.settings))


@dataclass(frozen=True)
class ServerInfo:
    version: str
    timezone: tzinfo
    settings: Mapping[str, SettingDef]

    def __post_init__(self) -> None:
        object.__setattr__(self, "settings", _freeze_mapping(self.settings))


@dataclass(frozen=True)
class QueryRuntime:
    """Backend-neutral per-call execution inputs resolved by the facade."""

    database: str | None = None
    protocol_version: int = 0
    settings: Mapping[str, str] = field(default_factory=dict)
    retries: int = 0


@dataclass
class CommandExecution:
    """Result of a backend command execution: the response body, decoded per
    transport (possibly empty), the query summary, and the output format of
    the result set. result_format is None when the statement produced no
    result set, such as a DDL or other control command."""

    body: bytes
    summary: dict[str, Any] = field(default_factory=dict)
    result_format: str | None = None


@dataclass
class QueryExecution:
    """Result of a backend query execution.

    Either a byte source (an object exposing a chunk generator via .gen,
    consumed by the response buffer and closable) or, for a columns-only
    metadata probe, the column metadata as name/type mappings.
    """

    source: Any | None = None
    columns: list[dict[str, Any]] | None = None
    summary: dict[str, Any] = field(default_factory=dict)
    response_tz_name: str | None = None
