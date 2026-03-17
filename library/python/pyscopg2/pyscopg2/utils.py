import statistics
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from functools import lru_cache
from typing import List, Optional, Union
from urllib.parse import urlencode

from psycopg2._psycopg import parse_dsn


def host_is_ipv6_address(host: str) -> bool:
    return host.count(":") > 1


class Dsn:
    def __init__(
            self,
            host: str,
            port: Union[str, int],
            user: Optional[str] = None,
            password: Optional[str] = None,
            dbname: Optional[str] = None,
            **kwargs,
    ):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._dbname = dbname
        self._kwargs = kwargs
        self._compiled_dsn = self._compile_dsn()

    def _compile_dsn(self) -> str:
        dsn = "postgresql://"
        if self._user is not None:
            dsn += self._user
            if self._password is not None:
                dsn += f":{self._password}"
            dsn += "@"

        if host_is_ipv6_address(self._host):
            dsn += f"[{self._host}]:{self._port}"
        else:
            dsn += f"{self._host}:{self._port}"

        if self._dbname is not None:
            dsn += f"/{self._dbname}"

        if self._kwargs:
            qs_params = urlencode(self._kwargs, safe="/~.\"'")
            dsn += f"?{qs_params}"

        return dsn

    @lru_cache()
    def with_(self, **kwargs) -> "Dsn":
        params = {
            "host": self._host,
            "port": self._port,
            "user": self._user,
            "password": self._password,
            "dbname": self._dbname,
            **self._kwargs,
            **kwargs,
        }
        return self.__class__(**params)

    def __str__(self) -> str:
        return self._compiled_dsn

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash(str(self))


def split_dsn(dsn: str, default_port: int = 5432) -> List[Dsn]:
    parsed_dsn = parse_dsn(dsn)

    hosts = parsed_dsn["host"].split(",")
    if "port" in parsed_dsn:
        ports = parsed_dsn["port"].split(",")
        if len(ports) != len(hosts):
            raise ValueError("Host and port amounts dismatch")
    else:
        ports = [None] * len(hosts)

    splited_dsn = []
    used_dsn = set()
    for host, port in zip(hosts, ports):
        current_dsn = parsed_dsn.copy()
        current_dsn["host"] = host
        current_dsn["port"] = port or default_port
        dsn = Dsn(**current_dsn)
        compiled_dsn = str(dsn)
        if compiled_dsn not in used_dsn:
            used_dsn.add(compiled_dsn)
            splited_dsn.append(dsn)
    return splited_dsn


class Stopwatch:
    def __init__(self, window_size: int):
        self._times = defaultdict(lambda: deque(maxlen=window_size))
        self._cache = {}

    def get_time(self, obj) -> Optional[float]:
        if obj not in self._times:
            return None
        if self._cache.get(obj) is None:
            self._cache[obj] = statistics.median(self._times[obj])
        return self._cache[obj]

    @contextmanager
    def __call__(self, obj):
        start_at = time.monotonic()
        yield
        self._times[obj].append(time.monotonic() - start_at)
        self._cache[obj] = None


def sa_patch():
    """
    Patch aiopg.sa to sqlalchemy 2
    https://github.com/aio-libs/aiopg/issues/897#issuecomment-1462703069
    """
    import sqlalchemy

    sa_version = tuple(map(int, sqlalchemy.__version__.split(".")))
    if sa_version[0] >= 2:
        from sqlalchemy.dialects.postgresql import base, psycopg2

        class PGDialect_psycopg2(psycopg2.PGDialect_psycopg2):  # noqa
            case_sensitive = True
            description_encoding = None

        setattr(sqlalchemy.dialects.postgresql.psycopg2, 'PGCompiler_psycopg2', base.PGCompiler)
        setattr(sqlalchemy.dialects.postgresql.psycopg2, 'PGDialect_psycopg2', PGDialect_psycopg2)
