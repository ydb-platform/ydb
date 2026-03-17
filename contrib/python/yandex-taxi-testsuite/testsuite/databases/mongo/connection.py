import dataclasses
import urllib.parse

_BOOL_TO_STR = {True: 'true', False: 'false'}
_STR_TO_BOOL = {value: key for key, value in _BOOL_TO_STR.items()}


@dataclasses.dataclass(frozen=True)
class ConnectionInfo:
    """Mongodb connection uri parameters"""

    host: str
    port: int
    dbname: str | None = None
    retry_writes: bool | None = None

    def get_uri(
        self,
        dbname: str | None = None,
        retry_writes: bool | None = None,
    ) -> str:
        """Get mongodb connection uri"""
        if dbname is None:
            dbname = self.dbname
        if retry_writes is None:
            retry_writes = self.retry_writes
        result = f'mongodb://{self.host}:{self.port}/'
        if dbname is not None:
            result += dbname
        if retry_writes is not None:
            retry_writes_str = _BOOL_TO_STR[retry_writes]
            result += f'?retryWrites={retry_writes_str}'
        return result


def parse_connection_uri(uri: str) -> ConnectionInfo:
    url = urllib.parse.urlparse(uri)
    if url.scheme != 'mongodb':
        raise ValueError(f'Invalid scheme in mongodb uri {uri}')
    parsed_query = urllib.parse.parse_qs(url.query)
    path = url.path.lstrip('/')
    return ConnectionInfo(
        host=url.hostname or 'localhost',
        port=url.port or 27017,
        dbname=path or None,
        retry_writes=_get_boolean_param(parsed_query, 'retryWrites'),
    )


def _get_boolean_param(
    parsed_query: dict[str, list[str]],
    key: str,
) -> bool | None:
    values = parsed_query.get(key, None)
    if not values or not values[0]:
        return None
    if len(values) > 1:
        raise ValueError(f'Multiple values of {key!r}: {values}')
    value = values[0].lower()
    if value not in _STR_TO_BOOL:
        raise ValueError(f'Invalid {key!r} value: {values[0]}')
    return _STR_TO_BOOL[value]
