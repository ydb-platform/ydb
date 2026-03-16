# pylint: disable=no-member
import typing
import urllib.parse

import psycopg2.extensions


class _NotSet:
    pass


class PgConnectionInfo(typing.NamedTuple):
    """
    PostgreSQL connection parameters
    """

    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    options: str | None = None
    sslmode: str | None = None
    dbname: str | None = None

    def get_dsn(self) -> str:
        """PostgreSQL connection string in DSN format"""
        return psycopg2.extensions.make_dsn(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            options=self.options,
            sslmode=self.sslmode,
            dbname=self.dbname,
        )

    def get_uri(self) -> str:
        """PostgreSQL connection string in URI format"""
        return get_connection_uri(**self._asdict())

    def replace(self, **kwargs) -> 'PgConnectionInfo':
        """Return a new :py:class:`PgConnectionInfo` value replacing specified
        fields with new values
        """
        return self._replace(**kwargs)


def parse_connection_string(connstr: str) -> PgConnectionInfo:
    """Parse PostgreSQL connection string.
    :param connstr: connection string in DSN or URI format as specified in
    https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
    """
    kwargs = psycopg2.extensions.parse_dsn(connstr)
    for key, value in kwargs.items():
        if key not in PgConnectionInfo._fields:
            continue
        if key == 'port':
            kwargs[key] = int(value)
        else:
            kwargs[key] = value
    return PgConnectionInfo(**kwargs)


def get_connection_uri(**kwargs):
    kwargs = {key: value for key, value in kwargs.items() if value is not None}
    dbname = kwargs.pop('dbname', '')
    if kwargs:
        items = (
            (key, urllib.parse.quote(str(value)))
            for key, value in kwargs.items()
        )
        query = '?' + '&'.join(f'{key}={value}' for key, value in items)
    else:
        query = ''
    return f'postgresql:///{dbname}{query}'
