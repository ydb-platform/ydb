# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""

This module implements the Trino protocol to submit SQL statements, track
their state and retrieve their result as described in
https://github.com/trinodb/trino/wiki/HTTP-Protocol
and Trino source code.

The outline of a query is:
- Send HTTP POST to the coordinator
- Retrieve HTTP response with ``nextUri``
- Get status of the query execution by sending a HTTP GET to the coordinator

Trino queries are managed by the ``TrinoQuery`` class. HTTP requests are
managed by the ``TrinoRequest`` class. the status of a query is represented
by ``TrinoStatus`` and the result by ``TrinoResult``.


The main interface is :class:`TrinoQuery`: ::

    >> request = TrinoRequest(host='coordinator', port=8080, user='test')
    >> query =  TrinoQuery(request, sql)
    >> rows = list(query.execute())
"""
from __future__ import annotations

import abc
import atexit
import base64
import copy
import functools
import os
import random
import re
import threading
import urllib.parse
import warnings
from abc import abstractmethod
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from enum import Enum
from time import sleep
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Tuple
from typing import TypedDict
from typing import Union
from zoneinfo import ZoneInfo

import lz4.block
try:
    import orjson as json
except ImportError:
    import json

import requests
import zstandard
from requests import Response
from requests import Session
from requests.structures import CaseInsensitiveDict

import trino.logging
from trino import constants
from trino import exceptions
from trino._version import __version__
from trino.auth import Authentication
from trino.exceptions import TrinoExternalError
from trino.exceptions import TrinoQueryError
from trino.exceptions import TrinoUserError
from trino.mapper import RowMapper
from trino.mapper import RowMapperFactory

__all__ = [
    "ClientSession",
    "TrinoQuery",
    "TrinoRequest",
    "PROXIES",
    "DecodableSegment",
    "SpooledSegment",
    "InlineSegment",
    "Segment"
]

logger = trino.logging.get_logger(__name__)
executor = ThreadPoolExecutor(max_workers=4)


def close_executor():
    executor.shutdown(wait=True)


atexit.register(close_executor)

MAX_ATTEMPTS = constants.DEFAULT_MAX_ATTEMPTS
SOCKS_PROXY = os.environ.get("SOCKS_PROXY")
if SOCKS_PROXY:
    PROXIES = {"http": "socks5://" + SOCKS_PROXY, "https": "socks5://" + SOCKS_PROXY}
else:
    PROXIES = {}

_HEADER_EXTRA_CREDENTIAL_KEY_REGEX = re.compile(r'^\S[^\s=]*$')

ROLE_PATTERN = re.compile(r"^ROLE\{(.*)\}$")


class ClientSession:
    """
    Manage the current Client Session properties of a specific connection. This class is thread-safe.

    :param user: associated with the query. It is useful for access control
                 and query scheduling.
    :param authorization_user: associated with the query. It is useful for access control
                               and query scheduling.
    :param source: associated with the query. It is useful for access
                   control and query scheduling.
    :param catalog: to query. The *catalog* is associated with a Trino
                    connector. This variable sets the default catalog used
                    by SQL statements. For example, if *catalog* is set
                    to ``some_catalog``, the SQL statement
                    ``SELECT * FROM some_schema.some_table`` will actually
                    query the table
                    ``some_catalog.some_schema.some_table``.
    :param schema: to query. The *schema* is a logical abstraction to group
                   table. This variable sets the default schema used by
                   SQL statements. For example, if *schema* is set to
                   ``some_schema``, the SQL statement
                   ``SELECT * FROM some_table`` will actually query the
                   table ``some_catalog.some_schema.some_table``.
    :param properties: set specific Trino behavior for the current
                               session. Please refer to the output of
                               ``SHOW SESSION`` to check the available
                               properties.
    :param headers: HTTP headers to POST/GET in the HTTP requests
    :param extra_credential: extra credentials. as list of ``(key, value)``
                             tuples.
    :param client_tags: Client tags as list of strings.
    :param roles: roles for the current session. Some connectors do not
                 support role management. See connector documentation for more details.
    :param timezone: The timezone for query processing. Defaults to the system's local timezone.
    :param encoding: The encoding for the spooling protocol. Defaults to None.
    """

    def __init__(
        self,
        user: str,
        authorization_user: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        source: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        transaction_id: Optional[str] = None,
        extra_credential: Optional[List[Tuple[str, str]]] = None,
        client_tags: Optional[List[str]] = None,
        roles: Optional[Union[Dict[str, str], str]] = None,
        timezone: Optional[str] = None,
        encoding: Optional[Union[str, List[str]]] = None,
    ):
        self._object_lock = threading.Lock()
        self._prepared_statements: Dict[str, str] = {}

        self._user = user
        self._authorization_user = authorization_user
        self._catalog = catalog
        self._schema = schema
        self._source = source
        self._properties = properties.copy() if properties is not None else {}
        self._headers = headers.copy() if headers is not None else {}
        self._transaction_id = transaction_id
        self._extra_credential = extra_credential
        self._client_tags = client_tags.copy() if client_tags is not None else list()
        self._roles = self._format_roles(roles) if roles is not None else {}
        if timezone:  # Check timezone validity
            ZoneInfo(timezone)
            self._timezone = timezone
        else:
            from tzlocal import get_localzone_name
            self._timezone = get_localzone_name()
        self._encoding = encoding

    @property
    def user(self) -> str:
        return self._user

    @property
    def authorization_user(self) -> Optional[str]:
        with self._object_lock:
            return self._authorization_user

    @authorization_user.setter
    def authorization_user(self, authorization_user: Optional[str]) -> None:
        with self._object_lock:
            self._authorization_user = authorization_user

    @property
    def catalog(self) -> Optional[str]:
        with self._object_lock:
            return self._catalog

    @catalog.setter
    def catalog(self, catalog: Optional[str]) -> None:
        with self._object_lock:
            self._catalog = catalog

    @property
    def schema(self) -> Optional[str]:
        with self._object_lock:
            return self._schema

    @schema.setter
    def schema(self, schema: Optional[str]) -> None:
        with self._object_lock:
            self._schema = schema

    @property
    def source(self) -> Optional[str]:
        return self._source

    @property
    def properties(self) -> Dict[str, str]:
        with self._object_lock:
            return self._properties

    @properties.setter
    def properties(self, properties: Dict[str, str]) -> None:
        with self._object_lock:
            self._properties = properties

    @property
    def headers(self) -> Dict[str, str]:
        return self._headers

    @property
    def transaction_id(self) -> Optional[str]:
        with self._object_lock:
            return self._transaction_id

    @transaction_id.setter
    def transaction_id(self, transaction_id: Optional[str]) -> None:
        with self._object_lock:
            self._transaction_id = transaction_id

    @property
    def extra_credential(self) -> Optional[List[Tuple[str, str]]]:
        return self._extra_credential

    @property
    def client_tags(self) -> List[str]:
        return self._client_tags

    @property
    def roles(self) -> Dict[str, str]:
        with self._object_lock:
            return self._roles

    @roles.setter
    def roles(self, roles: Dict[str, str]) -> None:
        with self._object_lock:
            self._roles = roles

    @property
    def prepared_statements(self) -> Dict[str, str]:
        return self._prepared_statements

    @prepared_statements.setter
    def prepared_statements(self, prepared_statements: Dict[str, str]) -> None:
        with self._object_lock:
            self._prepared_statements = prepared_statements

    @property
    def timezone(self) -> str:
        with self._object_lock:
            return self._timezone

    @property
    def encoding(self) -> Union[str, List[str]]:
        with self._object_lock:
            return self._encoding

    @staticmethod
    def _format_roles(roles: Union[Dict[str, str], str]) -> Dict[str, str]:
        if isinstance(roles, str):
            roles = {"system": roles}
        formatted_roles = {}
        for catalog, role in roles.items():
            is_legacy_role_pattern = ROLE_PATTERN.match(role) is not None
            if role in ("NONE", "ALL") or is_legacy_role_pattern:
                if is_legacy_role_pattern:
                    warnings.warn(f"A role '{role}' is provided using a legacy format. "
                                  "Please remove the ROLE{} wrapping. Support for the legacy format might be "
                                  "removed in a future release.",
                                  DeprecationWarning)
                formatted_roles[catalog] = role
            else:
                formatted_roles[catalog] = f"ROLE{{{role}}}"
        return formatted_roles

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_object_lock"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._object_lock = threading.Lock()


def get_header_values(headers: CaseInsensitiveDict[str], header: str) -> List[str]:
    return [val.strip() for val in headers[header].split(",")]


def get_session_property_values(headers: CaseInsensitiveDict[str], header: str) -> List[Tuple[str, str]]:
    kvs = get_header_values(headers, header)
    return [
        (k.strip(), urllib.parse.unquote_plus(v.strip()))
        for k, v in (kv.split("=", 1) for kv in kvs if kv)
    ]


def get_prepared_statement_values(headers: CaseInsensitiveDict[str], header: str) -> List[Tuple[str, str]]:
    kvs = get_header_values(headers, header)
    return [
        (k.strip(), urllib.parse.unquote_plus(v.strip()))
        for k, v in (kv.split("=", 1) for kv in kvs if kv)
    ]


def get_roles_values(headers: CaseInsensitiveDict[str], header: str) -> List[Tuple[str, str]]:
    kvs = get_header_values(headers, header)
    return [
        (k.strip(), urllib.parse.unquote_plus(v.strip()))
        for k, v in (kv.split("=", 1) for kv in kvs if kv)
    ]


@dataclass
class TrinoStatus:
    id: str
    stats: Dict[str, str]
    warnings: List[Any]
    info_uri: str
    next_uri: Optional[str]
    update_type: Optional[str]
    update_count: Optional[int]
    rows: Union[List[Any], Dict[str, Any]]
    columns: List[Any]

    def __repr__(self):
        return (
            "TrinoStatus("
            "id={}, stats={{...}}, warnings={}, info_uri={}, next_uri={}, rows=<count={}>"
            ")".format(
                self.id,
                len(self.warnings),
                self.info_uri,
                self.next_uri,
                len(self.rows),
            )
        )


class _DelayExponential:
    def __init__(
            self, base=0.1, exponent=2, jitter=True, max_delay=1800  # 100ms  # 30 min
    ):
        self._base = base
        self._exponent = exponent
        self._jitter = jitter
        self._max_delay = max_delay

    def __call__(self, attempt):
        delay = float(self._base) * (self._exponent ** attempt)
        if self._jitter:
            delay *= random.random()
        delay = min(float(self._max_delay), delay)
        return delay


class _RetryWithExponentialBackoff:
    def __init__(
            self, base=0.1, exponent=2, jitter=True, max_delay=1800  # 100ms  # 30 min
    ):
        self._get_delay = _DelayExponential(base, exponent, jitter, max_delay)

    def retry(self, func, args, kwargs, err, attempt):
        delay = self._get_delay(attempt)
        sleep(delay)


class _RetryAfterSleep:
    def __init__(self, retry_after_header):
        self._retry_after_header = retry_after_header

    def retry(self):
        sleep(self._retry_after_header)


class TrinoRequest:
    """
    Manage the HTTP requests of a Trino query.

    :param host: name of the coordinator
    :param port: TCP port to connect to the coordinator
    :param http_scheme: "http" or "https"
    :param auth: class that manages user authentication. ``None`` means no
                 authentication.
    :max_attempts: maximum number of attempts when sending HTTP requests. An
                   attempt is an HTTP request. 5 attempts means 4 retries.
    :request_timeout: How long (in seconds) to wait for the server to send
                      data before giving up, as a float or a
                      ``(connect timeout, read timeout)`` tuple.

    The client initiates a query by sending an HTTP POST to the
    coordinator. It then gets a response back from the coordinator with:
    - An URI to query to get the status for the query and the remaining
      data
    - An URI to get more information about the execution of the query
    - Statistics about the current query execution

    Please refer to :class:`TrinoStatus` to access the status returned by
    :meth:`TrinoRequest.process`.

    When the client makes an HTTP request, it may encounter the following
    errors:
    - Connection or read timeout:
      - There is a network partition and TCP segments are
        either dropped or delayed.
      - The coordinator stalled because of an OS level stall (page allocation
        stall, long time to page in pages, etc...), a JVM stall (full GC), or
        an application level stall (thread starving, lock contention)
    - Connection refused: Configuration or runtime issue on the coordinator
    - Connection closed:

    As most of these errors are transient, the question the caller should set
    retries with respect to when they want to notify the application that uses
    the client.
    """

    http = requests

    HTTP_EXCEPTIONS = (
        http.ConnectionError,
        http.Timeout,
    )

    def __init__(
        self,
        host: str,
        port: int,
        client_session: ClientSession,
        http_session: Optional[Session] = None,
        http_scheme: Optional[str] = None,
        auth: Optional[Authentication] = constants.DEFAULT_AUTH,
        max_attempts: int = MAX_ATTEMPTS,
        request_timeout: Union[float, Tuple[float, float]] = constants.DEFAULT_REQUEST_TIMEOUT,
        handle_retry=_RetryWithExponentialBackoff(),
        verify: bool = True,
    ) -> None:
        self._client_session = client_session
        self._host = host
        self._port = port
        self._next_uri: Optional[str] = None

        if http_scheme is None:
            if self._port == constants.DEFAULT_TLS_PORT:
                self._http_scheme = constants.HTTPS
            else:
                self._http_scheme = constants.HTTP
        else:
            self._http_scheme = http_scheme

        if http_session is not None:
            self._http_session = http_session
        else:
            self._http_session = self.http.Session()
            self._http_session.verify = verify
        self._http_session.headers.update(self.http_headers)
        self._exceptions = self.HTTP_EXCEPTIONS
        self._auth = auth
        if self._auth:
            self._auth.set_http_session(self._http_session)
            self._exceptions += self._auth.get_exceptions()

        self._request_timeout = request_timeout
        self._handle_retry = handle_retry
        self.max_attempts = max_attempts

    @property
    def transaction_id(self) -> Optional[str]:
        return self._client_session.transaction_id

    @transaction_id.setter
    def transaction_id(self, value: Optional[str]) -> None:
        self._client_session.transaction_id = value

    @property
    def http_headers(self) -> CaseInsensitiveDict[str]:
        headers: CaseInsensitiveDict[str] = CaseInsensitiveDict()

        headers[constants.HEADER_CATALOG] = self._client_session.catalog
        headers[constants.HEADER_SCHEMA] = self._client_session.schema
        headers[constants.HEADER_SOURCE] = self._client_session.source
        if self._client_session.authorization_user is not None:
            headers[constants.HEADER_ORIGINAL_USER] = self._client_session.user
            headers[constants.HEADER_USER] = self._client_session.authorization_user
        else:
            headers[constants.HEADER_USER] = self._client_session.user
        headers[constants.HEADER_TIMEZONE] = self._client_session.timezone
        if self._client_session.encoding is None:
            pass
        elif isinstance(self._client_session.encoding, list):
            headers[constants.HEADER_ENCODING] = ",".join(self._client_session.encoding)
        elif isinstance(self._client_session.encoding, str):
            headers[constants.HEADER_ENCODING] = self._client_session.encoding
        else:
            raise ValueError("Invalid type for encoding: expected str or list")
        headers[constants.HEADER_CLIENT_CAPABILITIES] = constants.CLIENT_CAPABILITIES

        headers["user-agent"] = f"{constants.CLIENT_NAME}/{__version__}"
        if len(self._client_session.roles.values()):
            headers[constants.HEADER_ROLE] = ",".join(
                # ``name`` must not contain ``=``
                "{}={}".format(catalog, urllib.parse.quote(str(role)))
                for catalog, role in self._client_session.roles.items()
            )
        if self._client_session.client_tags is not None and len(self._client_session.client_tags) > 0:
            headers[constants.HEADER_CLIENT_TAGS] = ",".join(self._client_session.client_tags)

        headers[constants.HEADER_SESSION] = ",".join(
            # ``name`` must not contain ``=``
            "{}={}".format(name, urllib.parse.quote(str(value)))
            for name, value in self._client_session.properties.items()
        )

        if len(self._client_session.prepared_statements) != 0:
            # ``name`` must not contain ``=``
            headers[constants.HEADER_PREPARED_STATEMENT] = ",".join(
                "{}={}".format(name, urllib.parse.quote_plus(statement))
                for name, statement in self._client_session.prepared_statements.items()
            )

        # merge custom http headers
        for key in self._client_session.headers:
            if key in headers.keys():
                raise ValueError("cannot override reserved HTTP header {}".format(key))
        headers.update(self._client_session.headers)

        transaction_id = self._client_session.transaction_id
        headers[constants.HEADER_TRANSACTION] = transaction_id

        if self._client_session.extra_credential is not None and \
                len(self._client_session.extra_credential) > 0:

            for tup in self._client_session.extra_credential:
                self._verify_extra_credential(tup)

            # HTTP 1.1 section 4.2 combine multiple extra credentials into a
            # comma-separated value
            # extra credential value is encoded per spec (application/x-www-form-urlencoded MIME format)
            headers[constants.HEADER_EXTRA_CREDENTIAL] = \
                ", ".join(
                    [f"{tup[0]}={urllib.parse.quote_plus(str(tup[1]))}"
                     for tup in self._client_session.extra_credential])

        return headers

    def unauthenticated(self):
        return TrinoRequest(
            host=self._host,
            port=self._port,
            max_attempts=self.max_attempts,
            request_timeout=self._request_timeout,
            handle_retry=self._handle_retry,
            client_session=ClientSession(user=self._client_session.user),
            verify=self._http_session.verify)

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

    @max_attempts.setter
    def max_attempts(self, value: int) -> None:
        self._max_attempts = value
        if value == 1:  # No retry
            self._get = self._http_session.get
            self._post = self._http_session.post
            self._delete = self._http_session.delete
            return

        with_retry = _retry_with(
            self._handle_retry,
            handled_exceptions=self._exceptions,
            conditions=(
                # need retry when there is no exception but the status code is 429, 502, 503, or 504
                lambda response: getattr(response, "status_code", None)
                in (429, 502, 503, 504),
            ),
            max_attempts=self._max_attempts,
        )
        self._get = with_retry(self._http_session.get)
        self._post = with_retry(self._http_session.post)
        self._delete = with_retry(self._http_session.delete)

    def get_url(self, path: str) -> str:
        return "{protocol}://{host}:{port}{path}".format(
            protocol=self._http_scheme, host=self._host, port=self._port, path=path
        )

    @property
    def statement_url(self) -> str:
        return self.get_url(constants.URL_STATEMENT_PATH)

    @property
    def next_uri(self) -> Optional[str]:
        return self._next_uri

    def post(self, sql: str, additional_http_headers: Optional[Dict[str, Any]] = None) -> Response:
        data = sql.encode("utf-8")
        # Deep copy of the http_headers dict since they may be modified for this
        # request by the provided additional_http_headers
        http_headers = copy.deepcopy(self.http_headers)

        # Update the request headers with the additional_http_headers
        http_headers.update(additional_http_headers or {})

        http_response = self._post(
            self.statement_url,
            data=data,
            headers=http_headers,
            timeout=self._request_timeout,
            proxies=PROXIES,
        )
        return http_response

    def get(self, url: str) -> Response:
        return self._get(
            url,
            headers=self.http_headers,
            timeout=self._request_timeout,
            proxies=PROXIES,
        )

    def delete(self, url: str) -> Response:
        return self._delete(url, timeout=self._request_timeout, proxies=PROXIES)

    @staticmethod
    def _process_error(error, query_id: Optional[str]) -> Union[TrinoExternalError, TrinoQueryError, TrinoUserError]:
        error_type = error["errorType"]
        if error_type == "EXTERNAL":
            raise exceptions.TrinoExternalError(error, query_id)
        elif error_type == "USER_ERROR":
            return exceptions.TrinoUserError(error, query_id)

        return exceptions.TrinoQueryError(error, query_id)

    @staticmethod
    def raise_response_error(http_response: Response) -> None:
        if http_response.status_code == 502:
            raise exceptions.Http502Error("error 502: bad gateway")

        if http_response.status_code == 503:
            raise exceptions.Http503Error("error 503: service unavailable")

        if http_response.status_code == 504:
            raise exceptions.Http504Error("error 504: gateway timeout")

        raise exceptions.HttpError(
            "error {}{}".format(
                http_response.status_code,
                ": {}".format(http_response.content) if http_response.content else "",
            )
        )

    def process(self, http_response: Response) -> TrinoStatus:
        if not http_response.ok:
            self.raise_response_error(http_response)

        http_response.encoding = "utf-8"
        response = json.loads(http_response.text)
        if "error" in response and response["error"]:
            raise self._process_error(response["error"], response.get("id"))

        if constants.HEADER_CLEAR_SESSION in http_response.headers:
            for prop in get_header_values(
                http_response.headers, constants.HEADER_CLEAR_SESSION
            ):
                self._client_session.properties.pop(prop, None)

        if constants.HEADER_SET_SESSION in http_response.headers:
            for key, value in get_session_property_values(
                http_response.headers, constants.HEADER_SET_SESSION
            ):
                self._client_session.properties[key] = value

        if constants.HEADER_SET_CATALOG in http_response.headers:
            self._client_session.catalog = http_response.headers[constants.HEADER_SET_CATALOG]

        if constants.HEADER_SET_SCHEMA in http_response.headers:
            self._client_session.schema = http_response.headers[constants.HEADER_SET_SCHEMA]

        if constants.HEADER_SET_ROLE in http_response.headers:
            for key, value in get_roles_values(
                    http_response.headers, constants.HEADER_SET_ROLE
            ):
                self._client_session.roles[key] = value

        if constants.HEADER_ADDED_PREPARE in http_response.headers:
            for name, statement in get_prepared_statement_values(
                http_response.headers, constants.HEADER_ADDED_PREPARE
            ):
                self._client_session.prepared_statements[name] = statement

        if constants.HEADER_DEALLOCATED_PREPARE in http_response.headers:
            for name in get_header_values(
                http_response.headers, constants.HEADER_DEALLOCATED_PREPARE
            ):
                self._client_session.prepared_statements.pop(name, None)

        if constants.HEADER_SET_AUTHORIZATION_USER in http_response.headers:
            self._client_session.authorization_user = http_response.headers[constants.HEADER_SET_AUTHORIZATION_USER]

        if constants.HEADER_RESET_AUTHORIZATION_USER in http_response.headers:
            self._client_session.authorization_user = None

        self._next_uri = response.get("nextUri")

        data = response.get("data") if response.get("data") else []

        return TrinoStatus(
            id=response["id"],
            stats=response["stats"],
            warnings=response.get("warnings", []),
            info_uri=response["infoUri"],
            next_uri=self._next_uri,
            update_type=response.get("updateType"),
            update_count=response.get("updateCount"),
            rows=data,
            columns=response.get("columns"),
        )

    @staticmethod
    def _verify_extra_credential(header: Tuple[str, str]) -> None:
        """
        Verifies that key has ASCII only and non-whitespace characters.
        """
        key = header[0]

        if not _HEADER_EXTRA_CREDENTIAL_KEY_REGEX.match(key):
            raise ValueError(f"whitespace or '=' are disallowed in extra credential '{key}'")

        try:
            key.encode().decode('ascii')
        except UnicodeDecodeError:
            raise ValueError(f"only ASCII characters are allowed in extra credential '{key}'")


class TrinoResult:
    """
    Represent the result of a Trino query as an iterator on rows.

    This class implements the iterator protocol as a generator type
    https://docs.python.org/3/library/stdtypes.html#generator-types
    """

    def __init__(self, query, rows: List[Any]):
        self._query = query
        # Initial rows from the first POST request
        self._rows = rows
        self._rownumber = 0

    @property
    def rows(self):
        return self._rows

    @rows.setter
    def rows(self, rows):
        self._rows = rows

    @property
    def rownumber(self) -> int:
        return self._rownumber

    def __iter__(self):
        # A query only transitions to a FINISHED state when the results are fully consumed:
        # The reception of the data is acknowledged by calling the next_uri before exposing the data through dbapi.
        while not self._query.finished or self._rows is not None:
            next_rows = self._query.fetch() if not self._query.finished else None
            for row in self._rows:
                self._rownumber += 1
                yield row

            self._rows = next_rows


class TrinoQuery:
    """Represent the execution of a SQL statement by Trino."""

    def __init__(
            self,
            request: TrinoRequest,
            query: str,
            legacy_primitive_types: bool = False,
            fetch_mode: Literal["mapped", "segments"] = "mapped"
    ) -> None:
        self._query_id: Optional[str] = None
        self._stats: Dict[Any, Any] = {}
        self._info_uri: Optional[str] = None
        self._warnings: List[Dict[Any, Any]] = []
        self._columns: Optional[List[str]] = None
        self._finished = False
        self._cancelled = False
        self._request = request
        self._update_type = None
        self._update_count = None
        self._next_uri = None
        self._query = query
        self._result: Optional[TrinoResult] = None
        self._legacy_primitive_types = legacy_primitive_types
        self._row_mapper: Optional[RowMapper] = None
        self._fetch_mode = fetch_mode

    @property
    def query_id(self) -> Optional[str]:
        return self._query_id

    @property
    def query(self) -> Optional[str]:
        return self._query

    @property
    def columns(self):
        if self.query_id:
            while not self._columns and not self.finished and not self.cancelled:
                # Columns are not returned immediately after query is submitted.
                # Continue fetching data until columns information is available and push fetched rows into buffer.
                self._result.rows += self.fetch()
        return self._columns

    @property
    def stats(self):
        return self._stats

    @property
    def update_type(self):
        return self._update_type

    @property
    def update_count(self):
        return self._update_count

    @property
    def warnings(self):
        return self._warnings

    @property
    def result(self):
        return self._result

    @property
    def info_uri(self):
        return self._info_uri

    def execute(self, additional_http_headers=None) -> TrinoResult:
        """Initiate a Trino query by sending the SQL statement

        This is the first HTTP request sent to the coordinator.
        It sets the query_id and returns a Result object used to
        track the rows returned by the query. To fetch all rows,
        call fetch() until finished is true.
        """
        if self.cancelled:
            raise exceptions.TrinoUserError("Query has been cancelled", self.query_id)

        try:
            response = self._request.post(self._query, additional_http_headers)
        except requests.exceptions.RequestException as e:
            raise trino.exceptions.TrinoConnectionError("failed to execute: {}".format(e))
        status = self._request.process(response)
        self._info_uri = status.info_uri
        self._query_id = status.id
        self._stats.update({"queryId": self.query_id})
        self._update_state(status)
        self._warnings = getattr(status, "warnings", [])
        if status.next_uri is None:
            self._finished = True

        rows = self._row_mapper.map(status.rows) if self._row_mapper else status.rows
        self._result = TrinoResult(self, rows)

        # Execute should block until at least one row is received or query is finished or cancelled
        while not self.finished and not self.cancelled and len(self._result.rows) == 0:
            self._result.rows += self.fetch()
        return self._result

    def _update_state(self, status):
        self._stats.update(status.stats)
        self._update_type = status.update_type
        self._update_count = status.update_count
        self._next_uri = status.next_uri
        if not self._row_mapper and status.columns:
            self._row_mapper = RowMapperFactory().create(columns=status.columns,
                                                         legacy_primitive_types=self._legacy_primitive_types)
        if status.columns:
            self._columns = status.columns

    def fetch(self) -> List[Union[List[Any]], Any]:
        """Continue fetching data for the current query_id"""
        try:
            response = self._request.get(self._request.next_uri)
        except requests.exceptions.RequestException as e:
            raise trino.exceptions.TrinoConnectionError("failed to fetch: {}".format(e))
        status = self._request.process(response)
        self._update_state(status)
        if status.next_uri is None:
            self._finished = True

        if not self._row_mapper:
            return []

        rows = status.rows
        if isinstance(status.rows, dict):
            # spooling protocol
            rows = cast(_SpooledProtocolResponseTO, rows)
            spooled = self._to_segments(rows)
            if self._fetch_mode == "segments":
                return spooled
            return list(SegmentIterator(spooled, self._row_mapper))
        elif isinstance(status.rows, list):
            return self._row_mapper.map(rows)
        else:
            raise ValueError(f"Unexpected type: {type(status.rows)}")

    def _to_segments(self, rows: _SpooledProtocolResponseTO) -> List[DecodableSegment]:
        encoding = rows["encoding"]
        metadata = rows["metadata"] if "metadata" in rows else None
        segments = []
        for segment in rows["segments"]:
            segment_type = segment["type"]
            if segment_type == SegmentType.INLINE:
                inline_segment = cast(_InlineSegmentTO, segment)
                segments.append(InlineSegment(inline_segment))
            elif segment_type == SegmentType.SPOOLED:
                spooled_segment = cast(_SpooledSegmentTO, segment)
                segments.append(SpooledSegment(spooled_segment, self._request.unauthenticated()))
            else:
                raise ValueError(f"Unsupported segment type: {segment_type}")

        return list(map(lambda segment: DecodableSegment(encoding, metadata, segment), segments))

    def cancel(self) -> None:
        """Cancel the current query"""
        if self._next_uri is None:
            return

        logger.debug("cancelling query: %s", self.query_id)
        try:
            response = self._request.delete(self._next_uri)
        except requests.exceptions.RequestException as e:
            raise trino.exceptions.TrinoConnectionError("failed to cancel query: {}".format(e))
        if response.status_code == requests.codes.no_content:
            self._cancelled = True
            logger.debug("query cancelled: %s", self.query_id)
            return

        self._request.raise_response_error(response)

    def is_finished(self) -> bool:
        import warnings
        warnings.warn("is_finished is deprecated, use finished instead", DeprecationWarning)
        return self.finished

    @property
    def finished(self) -> bool:
        return self._finished

    @property
    def cancelled(self) -> bool:
        return self._cancelled


def _retry_with(handle_retry, handled_exceptions, conditions, max_attempts):
    def wrapper(func):
        @functools.wraps(func)
        def decorated(*args, **kwargs):
            error = None
            result = None
            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if any(guard(result) for guard in conditions):
                        if result.status_code == 429 and "Retry-After" in result.headers:
                            retry_after = _parse_retry_after_header(result.headers.get("Retry-After"))
                            handle_retry_sleep = _RetryAfterSleep(retry_after)
                            handle_retry_sleep.retry()
                        else:
                            handle_retry.retry(func, args, kwargs, None, attempt)
                        continue
                    return result
                except Exception as err:
                    error = err
                    if any(isinstance(err, exc) for exc in handled_exceptions):
                        handle_retry.retry(func, args, kwargs, err, attempt)
                        continue
                    break
            logger.info("failed after %s attempts", attempt)
            if error is not None:
                raise error
            return result

        return decorated

    return wrapper


def _parse_retry_after_header(retry_after):
    if isinstance(retry_after, int):
        return retry_after
    elif isinstance(retry_after, str) and retry_after.isdigit():
        return int(retry_after)
    else:
        retry_date = parsedate_to_datetime(retry_after)
        now = datetime.utcnow()
        return (retry_date - now).total_seconds()


# Trino Spooled protocol transfer objects
class _SpooledProtocolResponseTO(TypedDict):
    encoding: Literal["json", "json+std", "json+lz4"]
    metadata: _SegmentMetadataTO
    segments: List[_SegmentTO]


class _SegmentMetadataTO(TypedDict):
    uncompressedSize: str
    segmentSize: str


class _SegmentTO(_SegmentMetadataTO):
    type: Literal["spooled", "inline"]
    metadata: _SegmentMetadataTO


class _SpooledSegmentTO(_SegmentTO):
    uri: str
    ackUri: str
    headers: Dict[str, List[str]]


class _InlineSegmentTO(_SegmentTO):
    data: str


class SegmentType(str, Enum):
    """Enum with string values that can be compared to strings."""
    INLINE = "inline"
    SPOOLED = "spooled"


class Segment(abc.ABC):
    """
    Abstract base class representing a segment of data produced by the spooling protocol.

    Attributes:
        metadata (property): Metadata associated with the segment.
        rows (property): Returns the decoded and mapped data.
    """
    def __init__(self, segment: _SegmentTO) -> None:
        self._segment = segment

    @property
    @abstractmethod
    def data(self):
        pass

    @property
    def metadata(self) -> _SegmentMetadataTO:
        return self._segment["metadata"]


class InlineSegment(Segment):
    """
    A subclass of Segment that handles inline data segments. The data is base64 encoded and
    requires mapping to rows using the provided row_mapper.

    Attributes:
        rows (property): The data in the segment, decoded and mapped from the base64 encoded data.
    """
    def __init__(self, segment: _InlineSegmentTO) -> None:
        super().__init__(segment)
        self._segment = cast(_InlineSegmentTO, segment)

    @property
    def data(self) -> bytes:
        return base64.b64decode(self._segment["data"])

    def __repr__(self):
        return f"InlineSegment(metadata={self.metadata})"


class SpooledSegment(Segment):
    """
    A subclass of Segment that handles spooled data segments, where data may be compressed and needs to be
    retrieved via HTTP requests. The segment includes methods for acknowledging processing and loading the
    segment from remote storage.

    Attributes:
        rows (property): The data, loaded and mapped from the spooled segment.
        uri (property): The URI for the spooled segment.
        ack_uri (property): The URI for acknowledging the processing of the spooled segment.
        headers (property): The headers associated with the spooled segment.

    Methods:
        acknowledge(): Sends an acknowledgment request for the segment.
    """
    def __init__(
        self,
        segment: _SpooledSegmentTO,
        request: TrinoRequest,
    ) -> None:
        super().__init__(segment)
        self._segment = cast(_SpooledSegmentTO, segment)
        self._request = request

    @property
    def data(self) -> bytes:
        http_response = self._send_spooling_request(self.uri)
        if not http_response.ok:
            self._request.raise_response_error(http_response)
        return http_response.content

    @property
    def uri(self) -> str:
        return self._segment["uri"]

    @property
    def ack_uri(self) -> str:
        return self._segment["ackUri"]

    @property
    def headers(self) -> Dict[str, List[str]]:
        return self._segment.get("headers", {})

    def acknowledge(self) -> None:
        def acknowledge_request():
            try:
                http_response = self._send_spooling_request(self.ack_uri, timeout=2)
                if not http_response.ok:
                    self._request.raise_response_error(http_response)
            except Exception as e:
                logger.error(f"Failed to acknowledge spooling request for segment {self}: {e}")
        # Start the acknowledgment in the executor thread
        executor.submit(acknowledge_request)

    def _send_spooling_request(self, uri: str, **kwargs) -> requests.Response:
        headers_with_single_value = {}
        for key, values in self.headers.items():
            if len(values) > 1:
                raise ValueError(f"Header '{key}' contains multiple values: {values}")
            headers_with_single_value[key] = values[0]
        return self._request._get(uri, headers=headers_with_single_value, **kwargs)

    def __repr__(self):
        return (
            f"SpooledSegment(metadata={self.metadata})"
        )


class DecodableSegment:
    """
    Represents a collection of spooled segments of data, with an encoding format.

    Attributes:
        encoding (str): The encoding format of the spooled data.
        metadata (_SegmentMetadataTO): Metadata for all segments in the query
        segment (Segment): The spooled segment data
    """
    def __init__(self, encoding: str, metadata: _SegmentMetadataTO, segment: Segment) -> None:
        self._encoding = encoding
        self._metadata = metadata
        self._segment = segment

    @property
    def encoding(self):
        return self._encoding

    @property
    def segment(self):
        return self._segment

    @property
    def metadata(self):
        return self._metadata

    def __repr__(self):
        return (f"DecodableSegment(encoding={self._encoding}, metadata={self._metadata}, segment={self._segment})")


class SegmentIterator:
    def __init__(self, segments: Union[DecodableSegment, List[DecodableSegment]], mapper: RowMapper) -> None:
        self._segments = iter(segments if isinstance(segments, List) else [segments])
        self._mapper = mapper
        self._decoder = None
        self._rows: Iterator[List[List[Any]]] = iter([])
        self._finished = False
        self._current_segment: Optional[DecodableSegment] = None

    def __iter__(self) -> Iterator[List[Any]]:
        return self

    def __next__(self) -> List[Any]:
        # If rows are exhausted, fetch the next segment
        while True:
            try:
                return next(self._rows)
            except StopIteration:
                if self._finished:
                    raise StopIteration
                self._load_next_segment()

    def _load_next_segment(self):
        try:
            if self._current_segment:
                segment = self._current_segment.segment
                if isinstance(segment, SpooledSegment):
                    segment.acknowledge()

            self._current_segment = next(self._segments)
            if self._decoder is None:
                self._decoder = SegmentDecoder(CompressedQueryDataDecoderFactory(self._mapper)
                                               .create(self._current_segment.encoding))
            self._rows = iter(self._decoder.decode(self._current_segment.segment))
        except StopIteration:
            self._finished = True


class SegmentDecoder():
    def __init__(self, decoder: QueryDataDecoder):
        self._decoder = decoder

    def decode(self, segment: Segment) -> List[List[Any]]:
        if isinstance(segment, InlineSegment):
            inline_segment = cast(InlineSegment, segment)
            return self._decoder.decode(inline_segment.data, inline_segment.metadata)
        elif isinstance(segment, SpooledSegment):
            spooled_data = cast(SpooledSegment, segment)
            return self._decoder.decode(spooled_data.data, spooled_data.metadata)
        else:
            raise ValueError(f"Unsupported segment type: {type(segment)}")


class CompressedQueryDataDecoderFactory():
    def __init__(self, mapper: RowMapper) -> None:
        self._mapper = mapper

    def create(self, encoding: str) -> QueryDataDecoder:
        if encoding == "json+zstd":
            return ZStdQueryDataDecoder(JsonQueryDataDecoder(self._mapper))
        elif encoding == "json+lz4":
            return Lz4QueryDataDecoder(JsonQueryDataDecoder(self._mapper))
        elif encoding == "json":
            return JsonQueryDataDecoder(self._mapper)
        else:
            raise ValueError(f"Unsupported encoding: {encoding}")


class QueryDataDecoder(abc.ABC):
    @abstractmethod
    def decode(self, data: bytes, metadata: _SegmentMetadataTO) -> List[List[Any]]:
        pass


class JsonQueryDataDecoder(QueryDataDecoder):
    def __init__(self, mapper: RowMapper) -> None:
        self._mapper = mapper

    def decode(self, data: bytes, metadata: Dict[str, Any]) -> List[List[Any]]:
        return self._mapper.map(json.loads(data.decode("utf8")))


class CompressedQueryDataDecoder(QueryDataDecoder):
    def __init__(self, delegate: QueryDataDecoder) -> None:
        self._delegate = delegate

    @abstractmethod
    def decompress(self, data: bytes, metadata: _SegmentMetadataTO) -> bytes:
        pass

    def decode(self, data: bytes, metadata: _SegmentMetadataTO) -> List[List[Any]]:
        if "uncompressedSize" not in metadata:
            # Data not compressed - below threshold
            return self._delegate.decode(data, metadata)

        # Data is compressed
        expected_compressed_size = metadata["segmentSize"]
        if not len(data) == expected_compressed_size:
            raise RuntimeError(f"Expected to read {expected_compressed_size} bytes but got {len(data)}")
        decompressed_data = self.decompress(data, metadata)
        expected_uncompressed_size = metadata["uncompressedSize"]
        if not len(decompressed_data) == expected_uncompressed_size:
            raise RuntimeError(
                "Decompressed size does not match expected segment size, "
                f"expected {expected_uncompressed_size}, got {len(decompressed_data)}"
            )
        return self._delegate.decode(decompressed_data, metadata)


class ZStdQueryDataDecoder(CompressedQueryDataDecoder):
    zstd_decompressor = zstandard.ZstdDecompressor()

    def decompress(self, data: bytes, metadata: _SegmentMetadataTO) -> bytes:
        return ZStdQueryDataDecoder.zstd_decompressor.decompress(data)


class Lz4QueryDataDecoder(CompressedQueryDataDecoder):
    def decompress(self, data: bytes, metadata: _SegmentMetadataTO) -> bytes:
        expected_uncompressed_size = metadata["uncompressedSize"]
        decoded_bytes = lz4.block.decompress(data, uncompressed_size=int(expected_uncompressed_size))
        return decoded_bytes
