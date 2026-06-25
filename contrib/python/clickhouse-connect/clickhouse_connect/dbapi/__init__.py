from clickhouse_connect.dbapi.connection import Connection

apilevel = "2.0"  # PEP 249  DB API level
threadsafety = 2  # PEP 249  Threads may share the module and connections.
paramstyle = "pyformat"  # PEP 249  Python extended format codes, e.g. ...WHERE name=%(name)s


class Error(Exception):
    pass


def connect(
    host: str | None = None,
    database: str | None = None,
    username: str | None = "",
    password: str | None = "",
    port: int | None = None,
    **kwargs,
):
    secure = kwargs.pop("secure", False)
    return Connection(
        host=host,
        database=database,
        username=username,
        password=password,
        port=port,
        secure=secure,
        **kwargs,
    )
