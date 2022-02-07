import os

from . import pool, scheme, table
import ydb
from ydb.driver import get_config


def default_credentials(credentials=None):
    if credentials is not None:
        return credentials

    service_account_key_file = os.getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")
    if service_account_key_file is not None:
        from .iam import ServiceAccountCredentials

        return ServiceAccountCredentials.from_file(service_account_key_file)

    anonymous_credetials = os.getenv("YDB_ANONYMOUS_CREDENTIALS", "0") == "1"
    if anonymous_credetials:
        return ydb.credentials.AnonymousCredentials()

    metadata_credentials = os.getenv("YDB_METADATA_CREDENTIALS", "0") == "1"
    if metadata_credentials:
        from .iam import MetadataUrlCredentials

        return MetadataUrlCredentials()

    access_token = os.getenv("YDB_ACCESS_TOKEN_CREDENTIALS")
    if access_token is not None:
        return ydb.credentials.AccessTokenCredentials(access_token)

    # (legacy instantiation)
    creds = ydb.auth_helpers.construct_credentials_from_environ()
    if creds is not None:
        return creds

    from .iam import MetadataUrlCredentials

    return MetadataUrlCredentials()


class DriverConfig(ydb.DriverConfig):
    @classmethod
    def default_from_endpoint_and_database(
        cls, endpoint, database, root_certificates=None, credentials=None, **kwargs
    ):
        return cls(
            endpoint,
            database,
            credentials=default_credentials(credentials),
            root_certificates=root_certificates,
            **kwargs
        )

    @classmethod
    def default_from_connection_string(
        cls, connection_string, root_certificates=None, credentials=None, **kwargs
    ):
        endpoint, database = ydb.parse_connection_string(connection_string)
        return cls(
            endpoint,
            database,
            credentials=default_credentials(credentials),
            root_certificates=root_certificates,
            **kwargs
        )


class Driver(pool.ConnectionPool):
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

        self.scheme_client = scheme.SchemeClient(self)
        self.table_client = table.TableClient(self, config.table_client_settings)
