import io
import re
import statistics
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from typing import (
    Any, DefaultDict, Deque, Dict, Generator, Iterable, List, Optional, Tuple,
    Union,
)
from urllib.parse import unquote, urlencode


def host_is_ipv6_address(netloc: str) -> bool:
    return netloc.count(":") > 1


class Dsn:
    __slots__ = (
        "_netloc", "_user", "_password", "_dbname", "_kwargs",
        "_scheme", "_compiled_dsn",
    )

    URL_EXP = re.compile(
        r"^(?P<scheme>[^\:]+):\/\/"
        r"((((?P<user>[^:^@]+))?"
        r"((\:(?P<password>[^@]+)?))?\@)?"
        r"(?P<netloc>([^\/^\?]+|\[([^\/]+)\])))?"
        r"(((?P<path>\/[^\?]*)?"
        r"(\?(?P<query>[^\#]+)?)?"
        r"(\#(?P<fragment>.*))?)?)?$",
    )

    def __init__(
        self,
        netloc: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
        scheme: str = "postgresql",
        **kwargs: Any,
    ):
        self._netloc = netloc
        self._user = user
        self._password = password
        self._dbname = dbname
        self._kwargs = kwargs
        self._scheme = scheme
        self._compiled_dsn = self._compile_dsn()

    @classmethod
    def parse(cls, dsn: str) -> "Dsn":
        # First try URL format
        match = cls.URL_EXP.match(dsn)

        if match is not None:
            # URL format parsing
            groupdict = match.groupdict()
            scheme = groupdict["scheme"]
            user = groupdict.get("user")
            password = groupdict.get("password")
            netloc: str = groupdict["netloc"]
            dbname = (groupdict.get("path") or "").lstrip("/")
            query = groupdict.get("query") or ""

            params = {}
            for item in query.split("&"):
                if not item:
                    continue
                key, value = item.split("=", 1)
                params[key] = unquote(value)

            return cls(
                scheme=scheme,
                netloc=netloc,
                user=user,
                password=password,
                dbname=dbname,
                **params
            )

        # Try connection string format:
        # 'host=localhost,localhost port=5432,5432 dbname=mydb'
        return cls._parse_connection_string(dsn)

    @classmethod
    def _parse_connection_string_params(cls, conn_str: str) -> Dict[str, str]:
        """Parse key=value pairs from connection string."""
        params = {}
        current_key = None
        current_value = ""
        in_quotes = False
        quote_char = None

        i = 0
        while i < len(conn_str):
            char = conn_str[i]

            if not in_quotes:
                if char in ("'", '"'):
                    in_quotes = True
                    quote_char = char
                elif char == '=' and current_key is None:
                    # Found key=value separator
                    current_key = current_value.strip()
                    current_value = ""
                elif char.isspace() and current_key is not None:
                    # End of value
                    params[current_key] = current_value.strip()
                    current_key = None
                    current_value = ""
                else:
                    current_value += char
            else:
                if char == quote_char:
                    in_quotes = False
                    quote_char = None
                else:
                    current_value += char

            i += 1

        # Handle final key=value pair
        if current_key is not None:
            params[current_key] = current_value.strip()

        return params

    @classmethod
    def _build_netloc(cls, hosts: str, ports: str) -> str:
        """Build netloc from comma-separated hosts and ports."""
        host_list = [h.strip() for h in hosts.split(',')]
        port_list = [p.strip() for p in ports.split(',')]

        # If single port, use it for all hosts
        if len(port_list) == 1 and len(host_list) > 1:
            port_list = port_list * len(host_list)
        # If single host, use it for all ports
        elif len(host_list) == 1 and len(port_list) > 1:
            host_list = host_list * len(port_list)

        # Build netloc (use first host:port for the main DSN)
        if len(host_list) > 0 and len(port_list) > 0:
            return ','.join(
                f"{host}:{port}" for host, port in zip(host_list, port_list)
            )
        else:
            return 'localhost:5432'

    @classmethod
    def _parse_connection_string(cls, conn_str: str) -> "Dsn":
        """Parse libpq-style connection string format."""
        params = cls._parse_connection_string_params(conn_str)

        # Extract standard connection parameters
        hosts = params.pop('host', 'localhost')
        ports = params.pop('port', '5432')
        user = params.pop('user', None)
        password = params.pop('password', None)
        dbname = params.pop('dbname', None)

        netloc = cls._build_netloc(hosts, ports)

        return cls(
            scheme="postgresql",
            netloc=netloc,
            user=user,
            password=password,
            dbname=dbname,
            **params
        )

    def _compile_dsn(self) -> str:
        with io.StringIO() as fp:
            fp.write(self._scheme)
            fp.write("://")

            if self._user is not None:
                fp.write(self._user)

            if self._password is not None:
                fp.write(":")
                fp.write(self._password)

            if self._user is not None or self._password is not None:
                fp.write("@")

            fp.write(self._netloc)

            if self._dbname is not None:
                fp.write("/")
                fp.write(self._dbname)

            if self._kwargs:
                fp.write("?")
                fp.write(urlencode(self._kwargs, safe="/~.\"'"))

            return fp.getvalue()

    def with_(
        self,
        netloc: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
    ) -> "Dsn":
        params = {
            "netloc": netloc if netloc is not None else self._netloc,
            "user": user if user is not None else self._user,
            "password": password if password is not None else self._password,
            "dbname": dbname if dbname is not None else self._dbname,
            **self._kwargs,
        }
        return self.__class__(**params)

    def __str__(self) -> str:
        return self._compiled_dsn

    def __eq__(self, other: Any) -> bool:
        return str(self) == str(other)

    def __hash__(self) -> int:
        return hash(str(self))

    @property
    def netloc(self) -> str:
        return self._netloc

    @property
    def user(self) -> Optional[str]:
        return self._user

    @property
    def password(self) -> Optional[str]:
        return self._password

    @property
    def dbname(self) -> Optional[str]:
        return self._dbname

    @property
    def params(self) -> Dict[str, str]:
        return self._kwargs

    @property
    def scheme(self) -> str:
        return self._scheme

    @property
    def compiled_dsn(self) -> str:
        return self._compiled_dsn


def split_dsn(dsn: Union[Dsn, str], default_port: int = 5432) -> List[Dsn]:
    if not isinstance(dsn, Dsn):
        dsn = Dsn.parse(dsn)

    host_port_pairs: List[Tuple[str, Optional[int]]] = []
    port_count = 0
    port: Optional[int]
    for host in dsn.netloc.split(","):
        if ":" in host:
            host, port_str = host.rsplit(":", 1)
            port = int(port_str)
            port_count += 1
        else:
            host = host
            port = None
        host_port_pairs.append((host, port))

    def deduplicate(dsns: Iterable[Dsn]) -> List[Dsn]:
        cache = set()
        result = []
        for dsn in dsns:
            if dsn in cache:
                continue
            result.append(dsn)
            cache.add(dsn)
        return result

    if port_count == len(host_port_pairs):
        return deduplicate(
            dsn.with_(netloc=f"{host}:{port}")
            for host, port in host_port_pairs
        )

    if port_count == 1 and host_port_pairs[-1][1] is not None:
        port = host_port_pairs[-1][1]
        return deduplicate(
            dsn.with_(netloc=f"{host}:{port}")
            for host, _ in host_port_pairs
        )

    return deduplicate(
        dsn.with_(netloc=f"{host}:{port or default_port}")
        for host, port in host_port_pairs
    )


class Stopwatch:
    def __init__(self, window_size: int):
        self._times: DefaultDict[Any, Deque] = defaultdict(
            lambda: deque(maxlen=window_size),
        )
        self._cache: Dict[Any, Optional[int]] = {}

    def get_time(self, obj: Any) -> Optional[float]:
        if obj not in self._times:
            return None
        if self._cache.get(obj) is None:
            self._cache[obj] = statistics.median(self._times[obj])
        return self._cache[obj]

    @contextmanager
    def __call__(self, obj: Any) -> Generator[None, None, None]:
        start_at = time.monotonic()
        yield
        self._times[obj].append(time.monotonic() - start_at)
        self._cache[obj] = None


__all__ = ("Dsn", "split_dsn", "Stopwatch", "host_is_ipv6_address")
