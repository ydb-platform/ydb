import json as json_
import warnings
from enum import Enum
from types import TracebackType
from typing import Any, AsyncGenerator, BinaryIO, Dict, List, Optional, Type

from aiochclient.exceptions import ChClientError
from aiochclient.http_clients.abc import HttpClientABC
from aiochclient.records import FromJsonFabric, Record, RecordsFabric
from aiochclient.sql import sqlparse

# Optional cython extension:
try:
    from aiochclient._types import json2ch, py2ch, rows2ch
except ImportError:
    from aiochclient.types import json2ch, py2ch, rows2ch


class QueryTypes(Enum):
    FETCH = 0
    INSERT = 1
    OTHER = 2


class ChClient:
    """ChClient connection class.

    Usage:

    .. code-block:: python

        async with aiohttp.ClientSession() as s:
            client = ChClient(s, compress_response=True)
            nums = await client.fetch("SELECT number FROM system.numbers LIMIT 100")

    :param aiohttp.ClientSession session:
        aiohttp client session. Please, use one session
        and one ChClient for all connections in your app.

    :param str url:
        Clickhouse server url. Need full path, like "http://localhost:8123/".

    :param str user:
        User name for authorization.

    :param str password:
        Password for authorization.

    :param str database:
        Database name.

    :param bool compress_response:
        Pass True if you want Clickhouse to compress its responses with gzip.
        They will be decompressed automatically. But overall it will be slightly slower.

    :param **settings:
        Any settings from https://clickhouse.yandex/docs/en/operations/settings
    """

    __slots__ = ("_session", "url", "params", "headers", "_json", "_http_client")

    def __init__(
        self,
        session=None,
        url: str = "http://localhost:8123/",
        user: str = None,
        password: str = None,
        database: str = "default",
        compress_response: bool = False,
        json=json_,  # type: ignore
        **settings,
    ):
        _http_client = HttpClientABC.choose_http_client(session)
        self._http_client = _http_client(session)
        self.url = url
        self.params = {}
        self.headers = {}
        if user:
            self.headers["X-ClickHouse-User"] = user
        if password:
            self.headers["X-ClickHouse-Key"] = password
        if database:
            self.params["database"] = database
        if compress_response:
            self.params["enable_http_compression"] = 1
        self._json = json
        self.params.update(settings)

    async def __aenter__(self) -> 'ChClient':
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the session"""
        await self._http_client.close()

    async def is_alive(self) -> bool:
        """Checks if connection is Ok.

        Usage:

        .. code-block:: python

            assert await client.is_alive()

        :return: True if connection Ok. False instead.
        """
        try:
            await self._http_client.get(
                url=self.url,
                params={**self.params, "query": "SELECT 1"},
                headers=self.headers,
            )
        except ChClientError:
            return False
        return True

    @staticmethod
    def _prepare_query_params(params: Optional[Dict[str, Any]] = None):
        if params is None:
            return {}
        if not isinstance(params, dict):
            raise TypeError('Query params must be a Dict[str, Any]')
        prepared_query_params = {}
        for key, value in params.items():
            prepared_query_params[key] = py2ch(value).decode('utf-8')
        return prepared_query_params

    async def _execute(
        self,
        query: str,
        *args,
        json: bool = False,
        query_params: Optional[Dict[str, Any]] = None,
        query_id: str = None,
        decode: bool = True,
    ) -> AsyncGenerator[Record, None]:
        query_params = self._prepare_query_params(query_params)
        if query_params:
            query = query.format(**query_params)
        need_fetch, is_json, statement_type = self._parse_squery(query)

        if not is_json and json:
            query += " FORMAT JSONEachRow"
            is_json = True

        if not is_json and need_fetch:
            query += " FORMAT TSVWithNamesAndTypes"

        if args:
            if statement_type != 'INSERT':
                raise ChClientError(
                    "It is possible to pass arguments only for INSERT queries"
                )
            params = {**self.params, "query": query}

            if is_json:
                data = json2ch(*args, dumps=self._json.dumps)
            else:
                data = rows2ch(*args)
        else:
            params = {**self.params}
            data = query.encode()

        if query_id is not None:
            params["query_id"] = query_id

        if need_fetch:
            response = self._http_client.post_return_lines(
                url=self.url, params=params, headers=self.headers, data=data
            )
            if is_json:
                rf = FromJsonFabric(loads=self._json.loads)
                async for line in response:
                    yield rf.new(line)
            else:
                rf = RecordsFabric(
                    names=await response.__anext__(),
                    tps=await response.__anext__(),
                    convert=decode,
                )
                async for line in response:
                    yield rf.new(line)
        else:
            await self._http_client.post_no_return(
                url=self.url, params=params, headers=self.headers, data=data
            )

    async def execute(
        self,
        query: str,
        *args,
        json: bool = False,
        params: Optional[Dict[str, Any]] = None,
        query_id: str = None,
    ) -> None:
        """Execute query. Returns None.

        :param str query: Clickhouse query string.
        :param args: Arguments for insert queries.
        :param bool json: Execute query in JSONEachRow mode.
        :param Optional[Dict[str, Any]] params: Params to escape
               inside query string on field values.
        :param str query_id: Clickhouse query_id.

        Usage:

        .. code-block:: python

            await client.execute(
                "CREATE TABLE t (a UInt8,
                                 b Tuple(Date, Nullable(Float32))
                                 ) ENGINE = Memory"
            )
            await client.execute(
                "INSERT INTO t VALUES",
                (1, (dt.date(2018, 9, 7), None)),
                (2, (dt.date(2018, 9, 8), 3.14)),
            )
            await client.execute(
                "SELECT * FROM t WHERE a={u8}",
                params={"u8": 12}
            )

        :return: Nothing.
        """
        async for _ in self._execute(
            query, *args, json=json, query_params=params, query_id=query_id
        ):
            return None

    async def fetch(
        self,
        query: str,
        *args,
        json: bool = False,
        params: Optional[Dict[str, Any]] = None,
        query_id: str = None,
        decode: bool = True,
    ) -> List[Record]:
        """Execute query and fetch all rows from query result at once in a list.

        :param query: Clickhouse query string.
        :param bool json: Execute query in JSONEachRow mode.
        :param Optional[Dict[str, Any]] params: Params to escape inside query string.
        :param str query_id: Clickhouse query_id.
        :param decode: Decode to python types.
                       If False, returns bytes for each field instead.

        Usage:

        .. code-block:: python

            all_rows = await client.fetch("SELECT * FROM t")

        :return: All rows from query.
        """
        return [
            row
            async for row in self._execute(
                query,
                *args,
                json=json,
                query_params=params,
                query_id=query_id,
                decode=decode,
            )
        ]

    async def fetchrow(
        self,
        query: str,
        *args,
        json: bool = False,
        params: Optional[Dict[str, Any]] = None,
        query_id: str = None,
        decode: bool = True,
    ) -> Optional[Record]:
        """Execute query and fetch first row from query result or None.

        :param query: Clickhouse query string.
        :param bool json: Execute query in JSONEachRow mode.
        :param Optional[Dict[str, Any]] params: Params to escape inside query string.
        :param str query_id: Clickhouse query_id.
        :param decode: Decode to python types. If False,
                       returns bytes for each field instead.

        Usage:

        .. code-block:: python

            row = await client.fetchrow("SELECT * FROM t WHERE a=1")
            assert row[0] == 1
            assert row["b"] == (dt.date(2018, 9, 7), None)

        :return: First row from query or None if there no results.
        """
        async for row in self._execute(
            query,
            *args,
            json=json,
            query_params=params,
            query_id=query_id,
            decode=decode,
        ):
            return row
        return None

    async def fetchone(self, query: str, *args) -> Optional[Record]:
        """Deprecated. Use ``fetchrow`` method instead"""
        warnings.warn(
            "'fetchone' method is deprecated. Use 'fetchrow' method instead",
            PendingDeprecationWarning,
        )
        return await self.fetchrow(query, *args)

    async def fetchval(
        self,
        query: str,
        *args,
        json: bool = False,
        params: Optional[Dict[str, Any]] = None,
        query_id: str = None,
        decode: bool = True,
    ) -> Any:
        """Execute query and fetch first value of the first
           row from query result or None.

        :param query: Clickhouse query string.
        :param bool json: Execute query in JSONEachRow mode.
        :param Optional[Dict[str, Any]] params: Params to escape inside query string.
        :param str query_id: Clickhouse query_id.
        :param decode: Decode to python types.
                       If False, returns bytes for each field instead.

        Usage:

        .. code-block:: python

            val = await client.fetchval("SELECT b FROM t WHERE a=2")
            assert val == (dt.date(2018, 9, 8), 3.14)

        :return: First value of the first row or None if there no results.
        """
        async for row in self._execute(
            query,
            *args,
            json=json,
            query_params=params,
            query_id=query_id,
            decode=decode,
        ):
            if row:
                return row[0]
        return None

    async def iterate(
        self,
        query: str,
        *args,
        json: bool = False,
        params: Optional[Dict[str, Any]] = None,
        query_id: str = None,
        decode: bool = True,
    ) -> AsyncGenerator[Record, None]:
        """Async generator by all rows from query result.

        :param str query: Clickhouse query string.
        :param bool json: Execute query in JSONEachRow mode.
        :param Optional[Dict[str, Any]] params: Params to escape inside query string.
        :param str query_id: Clickhouse query_id.
        :param decode: Decode to python types.
                       If False, returns bytes for each field instead.

        Usage:

        .. code-block:: python

            async for row in client.iterate(
                "SELECT number, number*2 FROM system.numbers LIMIT 10000"
            ):
                assert row[0] * 2 == row[1]

            async for row in client.iterate(
                "SELECT number, number*2 FROM system.numbers LIMIT {numbers_limit}",
                params={"numbers_limit": 10000}
            ):
                assert row[0] * 2 == row[1]

        :return: Rows one by one.
        """
        async for row in self._execute(
            query,
            *args,
            json=json,
            query_params=params,
            query_id=query_id,
            decode=decode,
        ):
            yield row

    async def cursor(self, query: str, *args) -> AsyncGenerator[Record, None]:
        """Deprecated. Use ``iterate`` method instead"""
        warnings.warn(
            "'cursor' method is deprecated. Use 'iterate' method instead",
            PendingDeprecationWarning,
        )
        async for row in self.iterate(query, *args):
            yield row

    async def insert_file(
        self,
        query: str,
        file_obj: BinaryIO,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Insert file in any suppoted by ClickHouse format. Returns None.

        :param str query: Clickhouse query string which include format part.
        :param bool file_obj: File object to insert.
        :param Optional[Dict[str, Any]] params: Params to escape inside query string.

        Usage:

        .. code-block:: python
            with open('data.csv', 'rb') as f: 
                await client.insert_file(
                    "INSERT INTO t FORMAT CSV",
                    f.read(),
                )

            with open('data.json', 'rb') as f:
                await client.insert_file(
                    "INSERT INTO t FORMAT JSONEachRow",
                    f.read(),
                )

            response = requests.get("https://url_to_download_parquet_file")
            await client.insert_file(
                "INSERT INTO t FORMAT Parquet",
                response.content,
            )

        :return: Nothing.
        """
        self._check_insert_file_query(query)

        query_params = self._prepare_query_params(params)
        if query_params:
            query = query.format(**query_params)

        params = {**self.params, "query": query}

        await self._http_client.post_no_return(
            url=self.url,
            params=params,
            headers=self.headers,
            data=file_obj,
        )

    @staticmethod
    def _parse_squery(query):
        statement = sqlparse.parse(query)[0]
        statement_type = statement.get_type()
        if statement_type in ('SELECT', 'SHOW', 'DESCRIBE', 'EXISTS'):
            need_fetch = True
        else:
            need_fetch = False

        fmt = statement.token_matching(
            (lambda tk: tk.match(sqlparse.tokens.Keyword, 'FORMAT'),), 0
        )
        if fmt:
            is_json = statement.token_matching(
                (lambda tk: tk.match(None, ['JSONEachRow']),),
                statement.token_index(fmt) + 1,
            )
        else:
            is_json = False
        return need_fetch, is_json, statement_type

    @staticmethod
    def _check_insert_file_query(query: str) -> None:
        statement = sqlparse.parse(query)[0]
        if statement.get_type() != 'INSERT':
            raise ChClientError('It is possible to insert file only with INSERT query')

        if not statement.token_matching(
            (lambda tk: tk.match(sqlparse.tokens.Keyword, 'FORMAT'),), 0
        ):
            raise ChClientError(
                'To insert file its required to specify `FORMAT [...] in the query.'
            )
