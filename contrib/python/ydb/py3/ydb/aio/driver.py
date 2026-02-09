from typing import Any, Optional, TYPE_CHECKING

from . import pool, scheme, table
import ydb
from .. import _utilities
from ydb.driver import get_config, default_credentials

if TYPE_CHECKING:
    from ydb.credentials import Credentials


class DriverConfig(ydb.DriverConfig):
    @classmethod
    def default_from_endpoint_and_database(
        cls,
        endpoint: str,
        database: Optional[str] = None,
        root_certificates: Optional[bytes] = None,
        credentials: Optional["Credentials"] = None,
        **kwargs: Any,
    ) -> "DriverConfig":
        return cls(
            endpoint,
            database,
            credentials=default_credentials(credentials),
            root_certificates=root_certificates,
            **kwargs,
        )

    @classmethod
    def default_from_connection_string(
        cls,
        connection_string: str,
        root_certificates: Optional[bytes] = None,
        credentials: Optional["Credentials"] = None,
        **kwargs: Any,
    ) -> "DriverConfig":
        endpoint, database = _utilities.parse_connection_string(connection_string)
        return cls(
            endpoint,
            database,
            credentials=default_credentials(credentials),
            root_certificates=root_certificates,
            **kwargs,
        )


class Driver(pool.ConnectionPool):
    _credentials: Optional["Credentials"]  # used for topic clients

    def __init__(
        self,
        driver_config: Optional[ydb.DriverConfig] = None,
        connection_string: Optional[str] = None,
        endpoint: Optional[str] = None,
        database: Optional[str] = None,
        root_certificates: Optional[bytes] = None,
        credentials: Optional["Credentials"] = None,
        **kwargs: Any,
    ) -> None:
        from .. import topic  # local import for prevent cycle import error
        from . import coordination  # local import for prevent cycle import error

        config = get_config(
            driver_config,
            connection_string,
            endpoint,
            database,
            root_certificates,
            credentials,
            config_class=DriverConfig,
        )

        super(Driver, self).__init__(config)

        self._credentials = config.credentials

        self.scheme_client = scheme.SchemeClient(self)
        self.table_client = table.TableClient(self, config.table_client_settings)
        self.topic_client = topic.TopicClientAsyncIO(self, config.topic_client_settings)
        self.coordination_client = coordination.CoordinationClient(self)

    async def stop(self, timeout: int = 10) -> None:  # type: ignore[override]  # async override of sync method
        await self.table_client._stop_pool_if_needed(timeout=timeout)
        self.topic_client.close()
        await super().stop(timeout=timeout)
