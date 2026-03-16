#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import typing as t

from elastic_transport import ObjectApiResponse

from ._base import NamespacedClient
from .utils import SKIP_IN_PATH, _quote, _rewrite_parameters


class SqlClient(NamespacedClient):

    @_rewrite_parameters(
        body_fields=("cursor",),
    )
    async def clear_cursor(
        self,
        *,
        cursor: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Clear an SQL search cursor.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-clear-cursor>`_

        :param cursor: Cursor to clear.
        """
        if cursor is None and body is None:
            raise ValueError("Empty value passed for parameter 'cursor'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_sql/close"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if cursor is not None:
                __body["cursor"] = cursor
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="sql.clear_cursor",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def delete_async(
        self,
        *,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete an async SQL search.
          Delete an async SQL search or a stored synchronous SQL search.
          If the search is still running, the API cancels it.</p>
          <p>If the Elasticsearch security features are enabled, only the following users can use this API to delete a search:</p>
          <ul>
          <li>Users with the <code>cancel_task</code> cluster privilege.</li>
          <li>The user who first submitted the search.</li>
          </ul>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-delete-async>`_

        :param id: The identifier for the search.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_sql/async/delete/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="sql.delete_async",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_async(
        self,
        *,
        id: str,
        delimiter: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        format: t.Optional[str] = None,
        human: t.Optional[bool] = None,
        keep_alive: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        wait_for_completion_timeout: t.Optional[
            t.Union[str, t.Literal[-1], t.Literal[0]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get async SQL search results.
          Get the current status and available results for an async SQL search or stored synchronous SQL search.</p>
          <p>If the Elasticsearch security features are enabled, only the user who first submitted the SQL search can retrieve the search using this API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-get-async>`_

        :param id: The identifier for the search.
        :param delimiter: The separator for CSV results. The API supports this parameter
            only for CSV responses.
        :param format: The format for the response. You must specify a format using this
            parameter or the `Accept` HTTP header. If you specify both, the API uses
            this parameter.
        :param keep_alive: The retention period for the search and its results. It defaults
            to the `keep_alive` period for the original SQL search.
        :param wait_for_completion_timeout: The period to wait for complete results.
            It defaults to no timeout, meaning the request waits for complete search
            results.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_sql/async/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if delimiter is not None:
            __query["delimiter"] = delimiter
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if format is not None:
            __query["format"] = format
        if human is not None:
            __query["human"] = human
        if keep_alive is not None:
            __query["keep_alive"] = keep_alive
        if pretty is not None:
            __query["pretty"] = pretty
        if wait_for_completion_timeout is not None:
            __query["wait_for_completion_timeout"] = wait_for_completion_timeout
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="sql.get_async",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_async_status(
        self,
        *,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get the async SQL search status.
          Get the current status of an async SQL search or a stored synchronous SQL search.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-get-async-status>`_

        :param id: The identifier for the search.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_sql/async/status/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="sql.get_async_status",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "allow_partial_search_results",
            "catalog",
            "columnar",
            "cursor",
            "fetch_size",
            "field_multi_value_leniency",
            "filter",
            "index_using_frozen",
            "keep_alive",
            "keep_on_completion",
            "page_timeout",
            "params",
            "query",
            "request_timeout",
            "runtime_mappings",
            "time_zone",
            "wait_for_completion_timeout",
        ),
        ignore_deprecated_options={"params", "request_timeout"},
    )
    async def query(
        self,
        *,
        allow_partial_search_results: t.Optional[bool] = None,
        catalog: t.Optional[str] = None,
        columnar: t.Optional[bool] = None,
        cursor: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        fetch_size: t.Optional[int] = None,
        field_multi_value_leniency: t.Optional[bool] = None,
        filter: t.Optional[t.Mapping[str, t.Any]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        format: t.Optional[
            t.Union[
                str, t.Literal["cbor", "csv", "json", "smile", "tsv", "txt", "yaml"]
            ]
        ] = None,
        human: t.Optional[bool] = None,
        index_using_frozen: t.Optional[bool] = None,
        keep_alive: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        keep_on_completion: t.Optional[bool] = None,
        page_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        params: t.Optional[t.Sequence[t.Any]] = None,
        pretty: t.Optional[bool] = None,
        query: t.Optional[str] = None,
        request_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        runtime_mappings: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        time_zone: t.Optional[str] = None,
        wait_for_completion_timeout: t.Optional[
            t.Union[str, t.Literal[-1], t.Literal[0]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get SQL search results.
          Run an SQL request.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query>`_

        :param allow_partial_search_results: If `true`, the response has partial results
            when there are shard request timeouts or shard failures. If `false`, the
            API returns an error with no partial results.
        :param catalog: The default catalog (cluster) for queries. If unspecified, the
            queries execute on the data in the local cluster only.
        :param columnar: If `true`, the results are in a columnar fashion: one row represents
            all the values of a certain column from the current page of results. The
            API supports this parameter only for CBOR, JSON, SMILE, and YAML responses.
        :param cursor: The cursor used to retrieve a set of paginated results. If you
            specify a cursor, the API only uses the `columnar` and `time_zone` request
            body parameters. It ignores other request body parameters.
        :param fetch_size: The maximum number of rows (or entries) to return in one response.
        :param field_multi_value_leniency: If `false`, the API returns an exception when
            encountering multiple values for a field. If `true`, the API is lenient and
            returns the first value from the array with no guarantee of consistent results.
        :param filter: The Elasticsearch query DSL for additional filtering.
        :param format: The format for the response. You can also specify a format using
            the `Accept` HTTP header. If you specify both this parameter and the `Accept`
            HTTP header, this parameter takes precedence.
        :param index_using_frozen: If `true`, the search can run on frozen indices.
        :param keep_alive: The retention period for an async or saved synchronous search.
        :param keep_on_completion: If `true`, Elasticsearch stores synchronous searches
            if you also specify the `wait_for_completion_timeout` parameter. If `false`,
            Elasticsearch only stores async searches that don't finish before the `wait_for_completion_timeout`.
        :param page_timeout: The minimum retention period for the scroll cursor. After
            this time period, a pagination request might fail because the scroll cursor
            is no longer available. Subsequent scroll requests prolong the lifetime of
            the scroll cursor by the duration of `page_timeout` in the scroll request.
        :param params: The values for parameters in the query.
        :param query: The SQL query to run.
        :param request_timeout: The timeout before the request fails.
        :param runtime_mappings: One or more runtime fields for the search request. These
            fields take precedence over mapped fields with the same name.
        :param time_zone: The ISO-8601 time zone ID for the search.
        :param wait_for_completion_timeout: The period to wait for complete results.
            It defaults to no timeout, meaning the request waits for complete search
            results. If the search doesn't finish within this period, the search becomes
            async. To save a synchronous search, you must specify this parameter and
            the `keep_on_completion` parameter.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_sql"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if format is not None:
            __query["format"] = format
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if allow_partial_search_results is not None:
                __body["allow_partial_search_results"] = allow_partial_search_results
            if catalog is not None:
                __body["catalog"] = catalog
            if columnar is not None:
                __body["columnar"] = columnar
            if cursor is not None:
                __body["cursor"] = cursor
            if fetch_size is not None:
                __body["fetch_size"] = fetch_size
            if field_multi_value_leniency is not None:
                __body["field_multi_value_leniency"] = field_multi_value_leniency
            if filter is not None:
                __body["filter"] = filter
            if index_using_frozen is not None:
                __body["index_using_frozen"] = index_using_frozen
            if keep_alive is not None:
                __body["keep_alive"] = keep_alive
            if keep_on_completion is not None:
                __body["keep_on_completion"] = keep_on_completion
            if page_timeout is not None:
                __body["page_timeout"] = page_timeout
            if params is not None:
                __body["params"] = params
            if query is not None:
                __body["query"] = query
            if request_timeout is not None:
                __body["request_timeout"] = request_timeout
            if runtime_mappings is not None:
                __body["runtime_mappings"] = runtime_mappings
            if time_zone is not None:
                __body["time_zone"] = time_zone
            if wait_for_completion_timeout is not None:
                __body["wait_for_completion_timeout"] = wait_for_completion_timeout
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="sql.query",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("query", "fetch_size", "filter", "time_zone"),
    )
    async def translate(
        self,
        *,
        query: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        fetch_size: t.Optional[int] = None,
        filter: t.Optional[t.Mapping[str, t.Any]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        time_zone: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Translate SQL into Elasticsearch queries.
          Translate an SQL search into a search API request containing Query DSL.
          It accepts the same request body parameters as the SQL search API, excluding <code>cursor</code>.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-translate>`_

        :param query: The SQL query to run.
        :param fetch_size: The maximum number of rows (or entries) to return in one response.
        :param filter: The Elasticsearch query DSL for additional filtering.
        :param time_zone: The ISO-8601 time zone ID for the search.
        """
        if query is None and body is None:
            raise ValueError("Empty value passed for parameter 'query'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_sql/translate"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if query is not None:
                __body["query"] = query
            if fetch_size is not None:
                __body["fetch_size"] = fetch_size
            if filter is not None:
                __body["filter"] = filter
            if time_zone is not None:
                __body["time_zone"] = time_zone
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="sql.translate",
            path_parts=__path_parts,
        )
