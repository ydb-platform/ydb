"""Client facade for the in-process chDB backend.

Construction only: the semantic client surface is inherited from
`SyncBackendClient`, which drives the `ChdbBackend` through the typed
execute_* seam, and the server handshake is the shared orchestration
`init_sequence` running against chdb's embedded engine.

Known engine limitations (chdb, not this client): external_data and the
async client are unsupported; one engine per process; the reported server
timezone is the host process timezone; some chdb versions drop the zone from
`DateTime('tz')` columns in Native output (`DateTime64('tz')` keeps it), so
those values decode as server-timezone datetimes.
"""

from __future__ import annotations

import logging
import os
from typing import Any, cast
from urllib.parse import urlencode

from clickhouse_connect import common
from clickhouse_connect.driver._backend.chdb_backend import CHDB_TRANSPORT_SETTINGS, ChdbBackend
from clickhouse_connect.driver._backendclient import SyncBackendClient
from clickhouse_connect.driver.query import TzMode, TzSource
from clickhouse_connect.driver.transform import NativeTransform

logger = logging.getLogger(__name__)


def build_connection_string(path: str | None, chdb_options: dict[str, Any] | None) -> str:
    resolved = path or ":memory:"
    if not resolved.startswith(":memory:") and not resolved.startswith("file:"):
        # chdb compares engine paths literally, so spellings of the same
        # directory (trailing slash, relative path, symlink) must normalize to
        # one path before they reach the engine. Only plain paths are
        # normalized; :memory: and file: forms pass through verbatim.
        resolved = os.path.realpath(resolved)
    if not chdb_options:
        return resolved
    return f"{resolved}?{urlencode(chdb_options)}"


class ChdbClient(SyncBackendClient):
    _backend: ChdbBackend
    valid_transport_settings = set(CHDB_TRANSPORT_SETTINGS)

    def __init__(
        self,
        path: str | None = None,
        database: str | None = None,
        settings: dict[str, Any] | None = None,
        query_limit: int = 0,
        tz_source: TzSource | None = None,
        tz_mode: str | None = None,
        show_clickhouse_errors: bool | None = None,
        chdb_options: dict[str, Any] | None = None,
        rename_response_column: str | None = None,
    ):
        """
        Create a ClickHouse Connect client backed by an in-process chDB engine
        :param path: chDB data location, ":memory:" (default) or a directory path
        :param database: Default database for the connection
        :param settings: ClickHouse server settings applied to the session
        :param query_limit: Default LIMIT on returned rows, 0 means no limit
        :param tz_source: See clickhouse_connect.get_client
        :param tz_mode: See clickhouse_connect.get_client
        :param show_clickhouse_errors: Include engine error details in exceptions
        :param chdb_options: Extra chDB engine options appended to the connection string
        :param rename_response_column: See clickhouse_connect.get_client

        Each client owns its own chdb connection handle, so session-level
        settings and the USE database state are per client. chdb allows one
        engine path per process: clients on the same path share the engine
        data, and connecting to a different path while other clients are
        open raises ProgrammingError. The database is handle state applied
        with USE, so setting `client.database = None` after a database was
        applied does not reset the handle to the engine default; set an
        explicit database instead.
        """
        self.path = path or ":memory:"
        self._rename_response_column = rename_response_column
        self._transform = NativeTransform()
        self._client_settings: dict[str, str] = {}
        self._backend = ChdbBackend(connection_string=build_connection_string(path, chdb_options))
        self._initial_settings = settings
        try:
            super().__init__(
                database=database,
                uri=f"chdb://{self.path}",
                query_limit=query_limit,
                query_retries=0,
                server_host_name=None,
                tz_source=tz_source,
                tz_mode=cast("TzMode | None", tz_mode),
                show_clickhouse_errors=show_clickhouse_errors,
                autoconnect=True,
            )
            for key, value in (settings or {}).items():
                self.set_client_setting(key, value)
        except Exception:
            self._backend.close()
            raise

    @property
    def show_clickhouse_errors(self) -> bool:  # type: ignore[override]
        return self._backend.show_clickhouse_errors

    @show_clickhouse_errors.setter
    def show_clickhouse_errors(self, value: bool) -> None:
        self._backend.show_clickhouse_errors = value

    def set_client_setting(self, key: str, value: Any) -> None:
        str_value = self._validate_setting(key, value, common.get_setting("invalid_setting_action"))
        if str_value is None:
            return
        if key not in CHDB_TRANSPORT_SETTINGS:
            self._backend.set_client_setting(key, str_value)
        self._client_settings[key] = str_value

    def get_client_setting(self, key: str) -> str | None:
        return self._client_settings.get(key)

    def set_access_token(self, access_token: str) -> None:
        # chdb has no authentication concept; accept silently so token-based
        # callers work unchanged against the in-process engine.
        logger.debug("Ignoring access token for the chdb backend")
