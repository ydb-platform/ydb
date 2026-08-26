from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Generator, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timezone, tzinfo
from typing import Any, Protocol, TypeVar, cast, runtime_checkable
from zoneinfo import ZoneInfoNotFoundError

from clickhouse_connect import common
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import tzutil
from clickhouse_connect.driver._backend.models import ClientConfig, ServerInfo
from clickhouse_connect.driver._backend.operations import CommandOp, Operation, QueryOp, RawQueryOp
from clickhouse_connect.driver.binding import quote_identifier
from clickhouse_connect.driver.common import version_at_least
from clickhouse_connect.driver.constants import CH_VERSION_WITH_PROTOCOL, PROTOCOL_VERSION_WITH_LOW_CARD
from clickhouse_connect.driver.exceptions import OperationalError, ProgrammingError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.models import ColumnDef, SettingDef, setting_status

logger = logging.getLogger(__name__)

ClientSettingWrite = tuple[str, Any]


@dataclass(frozen=True)
class InitializationResult:
    server_info: ServerInfo
    client_setting_writes: tuple[ClientSettingWrite, ...]
    protocol_version: int
    json_serialization_format: int | None
    timezone_dst_safe: bool
    apply_server_timezone: bool


InitializationSequence = Generator[Operation, object, InitializationResult]
SequenceResult = TypeVar("SequenceResult")
OperationSequence = Generator[Operation, object, SequenceResult]

# Sequences yield semantic operations, so their executor is a client method
# (Client._execute_operation), not a transport backend.
ExecuteOperation = Callable[[Operation], object]
AsyncExecuteOperation = Callable[[Operation], Awaitable[object]]


@runtime_checkable
class _NamedResults(Protocol):
    def named_results(self) -> Iterable[Mapping[str, Any]]: ...


def _version_timezone(result: object) -> tuple[str, str]:
    if not isinstance(result, Sequence) or isinstance(result, (str, bytes, bytearray)) or len(result) < 2:
        raise OperationalError(f"Unexpected response to server version query: {result!r}")
    version, server_timezone = result[0], result[1]
    if not isinstance(version, str) or not isinstance(server_timezone, str):
        raise OperationalError(f"Unexpected response to server version query: {result!r}")
    return version, server_timezone


def _named_rows(result: object, description: str) -> Iterable[Mapping[str, Any]]:
    if isinstance(result, _NamedResults):
        return result.named_results()
    if isinstance(result, Iterable) and not isinstance(result, (str, bytes, bytearray)):
        return cast(Iterable[Mapping[str, Any]], result)
    raise OperationalError(f"Unexpected response to {description} query: {result!r}")


def _setting_definitions(result: object) -> dict[str, SettingDef]:
    definitions: dict[str, SettingDef] = {}
    for row in _named_rows(result, "server settings"):
        if not isinstance(row, Mapping):
            raise OperationalError(f"Unexpected row in server settings query: {row!r}")
        try:
            name = row["name"]
            value = row["value"]
            readonly = row["readonly"]
        except KeyError as ex:
            raise OperationalError(f"Unexpected row in server settings query: {row!r}") from ex
        if not isinstance(name, str) or not isinstance(value, str) or not isinstance(readonly, int):
            raise OperationalError(f"Unexpected row in server settings query: {row!r}")
        setting = SettingDef(name=name, value=value, readonly=readonly)
        definitions[setting.name] = setting
    return definitions


def init_sequence(config: ClientConfig) -> InitializationSequence:
    version_result = yield CommandOp("SELECT version(), timezone()", use_database=False)
    server_version, server_timezone_name = _version_timezone(version_result)

    server_timezone: tzinfo = timezone.utc
    timezone_dst_safe = True
    try:
        resolved_timezone = tzutil.resolve_zone(server_timezone_name)
        server_timezone, timezone_dst_safe = tzutil.normalize_timezone(resolved_timezone, trust_fixed_offset=True)
    except ZoneInfoNotFoundError:
        logger.warning(
            "Server timezone %s could not be resolved, falling back to UTC; %s",
            server_timezone_name,
            tzutil.TZDATA_HINT,
        )

    if config.timezone_policy == "auto":
        apply_server_timezone = timezone_dst_safe
    else:
        apply_server_timezone = config.timezone_policy == "server"
    if not apply_server_timezone and not tzutil.local_tz_dst_safe:
        logger.warning(
            "local timezone %s may return unexpected times due to Daylight Savings Time/Summer Time differences",
            tzutil.local_tz.tzname(None),
        )

    readonly = "readonly" if version_at_least(server_version, "19.17") else str(common.get_setting("readonly"))
    settings_result = yield QueryOp(f"SELECT name, value, {readonly} as readonly FROM system.settings LIMIT 10000")
    server_settings = _setting_definitions(settings_result)

    protocol_version = 0
    if version_at_least(server_version, CH_VERSION_WITH_PROTOCOL) and common.get_setting("use_protocol_version"):
        # The response bytes must be validated because a proxy such as CHProxy
        # can strip the client_protocol_version query parameter.
        # Probe failures leave protocol_version at 0, the pre-existing
        # AsyncClient._initialize behavior. The old sync path propagated them.
        try:
            protocol_result = yield RawQueryOp(
                "SELECT 1 AS check",
                settings={"client_protocol_version": PROTOCOL_VERSION_WITH_LOW_CARD},
                fmt="Native",
            )
            if isinstance(protocol_result, (bytes, bytearray)) and protocol_result[8:16] == b"\x01\x01\x05check":
                protocol_version = PROTOCOL_VERSION_WITH_LOW_CARD
        except Exception as ex:
            logger.debug("client_protocol_version probe failed, continuing with protocol version 0: %s", ex)

    # Generated defaults skip keys the user supplied. Clients apply user
    # settings themselves, so the returned writes are defaults only.
    client_settings: dict[str, Any] = {}
    if "date_time_input_format" not in config.settings and setting_status(server_settings, "date_time_input_format").is_writable:
        client_settings["date_time_input_format"] = "best_effort"
    if (
        "cast_string_to_dynamic_use_inference" not in config.settings
        and setting_status(server_settings, "allow_experimental_json_type").is_set
        and setting_status(server_settings, "cast_string_to_dynamic_use_inference").is_writable
    ):
        client_settings["cast_string_to_dynamic_use_inference"] = "1"

    json_serialization_format = 0 if version_at_least(server_version, "24.8") and not version_at_least(server_version, "24.10") else None
    server_info = ServerInfo(
        version=server_version,
        timezone=server_timezone,
        settings=server_settings,
    )
    return InitializationResult(
        server_info=server_info,
        client_setting_writes=tuple(client_settings.items()),
        protocol_version=protocol_version,
        json_serialization_format=json_serialization_format,
        timezone_dst_safe=timezone_dst_safe,
        apply_server_timezone=apply_server_timezone,
    )


def insert_context_sequence(
    table: str,
    column_names: str | Sequence[str] | None = None,
    database: str | None = None,
    column_types: Sequence[ClickHouseType] | None = None,
    column_type_names: Sequence[str] | None = None,
    column_oriented: bool = False,
    settings: dict[str, Any] | None = None,
    data: Sequence[Sequence[Any]] | None = None,
    transport_settings: dict[str, str] | None = None,
) -> Generator[Operation, object, InsertContext]:
    full_table = table
    if "." not in table:
        if database:
            full_table = f"{quote_identifier(database)}.{quote_identifier(table)}"
        else:
            full_table = quote_identifier(table)
    column_defs: list[ColumnDef] = []
    if column_types is None and column_type_names is None:
        describe_result = yield QueryOp(f"DESCRIBE TABLE {full_table}", settings=settings or {})
        column_defs = [
            ColumnDef(**row)
            for row in _named_rows(describe_result, "DESCRIBE TABLE")
            if row["default_type"] not in ("ALIAS", "MATERIALIZED")
        ]
    if column_names is None or isinstance(column_names, str) and column_names == "*":
        column_names = [cd.name for cd in column_defs]
        column_types = [cd.ch_type for cd in column_defs]
    elif isinstance(column_names, str):
        column_names = [column_names]
    if len(column_names) == 0:
        raise ValueError("Column names must be specified for insert")
    if not column_types:
        if column_type_names:
            column_types = [get_from_name(name) for name in column_type_names]
        else:
            column_map = {d.name: d for d in column_defs}
            try:
                column_types = [column_map[name].ch_type for name in column_names]
            except KeyError as ex:
                raise ProgrammingError(f"Unrecognized column {ex} in table {table}") from None
    if len(column_names) != len(column_types):
        raise ProgrammingError("Column names do not match column types") from None
    return InsertContext(
        full_table,
        column_names,
        column_types,
        column_oriented=column_oriented,
        settings=settings,
        transport_settings=transport_settings,
        data=data,
    )


def run_sync(sequence: OperationSequence[SequenceResult], execute: ExecuteOperation) -> SequenceResult:
    response: object = None
    execution_error: Exception | None = None
    try:
        while True:
            try:
                if execution_error is None:
                    operation = sequence.send(response)
                else:
                    operation = sequence.throw(execution_error)
            except StopIteration as stop:
                return stop.value
            try:
                response = execute(operation)
                execution_error = None
            except Exception as ex:
                execution_error = ex
    finally:
        sequence.close()


async def run_async(sequence: OperationSequence[SequenceResult], execute: AsyncExecuteOperation) -> SequenceResult:
    response: object = None
    execution_error: Exception | None = None
    try:
        while True:
            try:
                if execution_error is None:
                    operation = sequence.send(response)
                else:
                    operation = sequence.throw(execution_error)
            except StopIteration as stop:
                return stop.value
            try:
                response = await execute(operation)
                execution_error = None
            except Exception as ex:
                execution_error = ex
    finally:
        sequence.close()
