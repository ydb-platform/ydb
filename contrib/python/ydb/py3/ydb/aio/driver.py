from . import pool, scheme, table
import ydb
from .. import _utilities
from ydb.driver import get_config, default_credentials


class DriverConfig(ydb.DriverConfig):
    @classmethod
    def default_from_endpoint_and_database(cls, endpoint, database, root_certificates=None, credentials=None, **kwargs):
        return cls(
            endpoint,
            database,
            credentials=default_credentials(credentials),
            root_certificates=root_certificates,
            **kwargs
        )

    @classmethod
    def default_from_connection_string(cls, connection_string, root_certificates=None, credentials=None, **kwargs):
        endpoint, database = _utilities.parse_connection_string(connection_string)
        return cls(
            endpoint,
            database,
            credentials=default_credentials(credentials),
            root_certificates=root_certificates,
            **kwargs
        )


class Driver(pool.ConnectionPool):
    _credentials: ydb.Credentials  # used for topic clients

    def __init__(
        self,
        driver_config=None,
        connection_string=None,
        endpoint=None,
        database=None,
        root_certificates=None,
        credentials=None,
        **kwargs
    ):
        from .. import topic  # local import for prevent cycle import error

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
