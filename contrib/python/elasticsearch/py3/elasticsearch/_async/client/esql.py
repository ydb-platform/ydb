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
from .utils import (
    SKIP_IN_PATH,
    Stability,
    _quote,
    _rewrite_parameters,
    _stability_warning,
)

if t.TYPE_CHECKING:
    from elasticsearch.esql import ESQLBase


class EsqlClient(NamespacedClient):

    @_rewrite_parameters(
        body_fields=(
            "query",
            "columnar",
            "filter",
            "include_ccs_metadata",
            "keep_alive",
            "keep_on_completion",
            "locale",
            "params",
            "profile",
            "tables",
            "wait_for_completion_timeout",
        ),
        ignore_deprecated_options={"params"},
    )
    async def async_query(
        self,
        *,
        query: t.Optional[t.Union[str, "ESQLBase"]] = None,
        allow_partial_results: t.Optional[bool] = None,
        columnar: t.Optional[bool] = None,
        delimiter: t.Optional[str] = None,
        drop_null_columns: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter: t.Optional[t.Mapping[str, t.Any]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        format: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "arrow", "cbor", "csv", "json", "smile", "tsv", "txt", "yaml"
                ],
            ]
        ] = None,
        human: t.Optional[bool] = None,
        include_ccs_metadata: t.Optional[bool] = None,
        keep_alive: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        keep_on_completion: t.Optional[bool] = None,
        locale: t.Optional[str] = None,
        params: t.Optional[t.Sequence[t.Union[None, bool, float, int, str]]] = None,
        pretty: t.Optional[bool] = None,
        profile: t.Optional[bool] = None,
        tables: t.Optional[
            t.Mapping[str, t.Mapping[str, t.Mapping[str, t.Any]]]
        ] = None,
        wait_for_completion_timeout: t.Optional[
            t.Union[str, t.Literal[-1], t.Literal[0]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Run an async ES|QL query.
          Asynchronously run an ES|QL (Elasticsearch query language) query, monitor its progress, and retrieve results when they become available.</p>
          <p>The API accepts the same parameters and request body as the synchronous query API, along with additional async related properties.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query>`_

        :param query: The ES|QL query API accepts an ES|QL query string in the query
            parameter, runs it, and returns the results.
        :param allow_partial_results: If `true`, partial results will be returned if
            there are shard failures, but the query can continue to execute on other
            clusters and shards. If `false`, the query will fail if there are any failures.
            To override the default behavior, you can set the `esql.query.allow_partial_results`
            cluster setting to `false`.
        :param columnar: By default, ES|QL returns results as rows. For example, FROM
            returns each individual document as one row. For the JSON, YAML, CBOR and
            smile formats, ES|QL can return the results in a columnar fashion where one
            row represents all the values of a certain column in the results.
        :param delimiter: The character to use between values within a CSV row. It is
            valid only for the CSV format.
        :param drop_null_columns: Indicates whether columns that are entirely `null`
            will be removed from the `columns` and `values` portion of the results. If
            `true`, the response will include an extra section under the name `all_columns`
            which has the name of all the columns.
        :param filter: Specify a Query DSL query in the filter parameter to filter the
            set of documents that an ES|QL query runs on.
        :param format: A short version of the Accept header, e.g. json, yaml. `csv`,
            `tsv`, and `txt` formats will return results in a tabular format, excluding
            other metadata fields from the response. For async requests, nothing will
            be returned if the async query doesn't finish within the timeout. The query
            ID and running status are available in the `X-Elasticsearch-Async-Id` and
            `X-Elasticsearch-Async-Is-Running` HTTP headers of the response, respectively.
        :param include_ccs_metadata: When set to `true` and performing a cross-cluster
            query, the response will include an extra `_clusters` object with information
            about the clusters that participated in the search along with info such as
            shards count.
        :param keep_alive: The period for which the query and its results are stored
            in the cluster. The default period is five days. When this period expires,
            the query and its results are deleted, even if the query is still ongoing.
            If the `keep_on_completion` parameter is false, Elasticsearch only stores
            async queries that do not complete within the period set by the `wait_for_completion_timeout`
            parameter, regardless of this value.
        :param keep_on_completion: Indicates whether the query and its results are stored
            in the cluster. If false, the query and its results are stored in the cluster
            only if the request does not complete during the period set by the `wait_for_completion_timeout`
            parameter.
        :param locale:
        :param params: To avoid any attempts of hacking or code injection, extract the
            values in a separate list of parameters. Use question mark placeholders (?)
            in the query string for each of the parameters.
        :param profile: If provided and `true` the response will include an extra `profile`
            object with information on how the query was executed. This information is
            for human debugging and its format can change at any time but it can give
            some insight into the performance of each part of the query.
        :param tables: Tables to use with the LOOKUP operation. The top level key is
            the table name and the next level key is the column name.
        :param wait_for_completion_timeout: The period to wait for the request to finish.
            By default, the request waits for 1 second for the query results. If the
            query completes during this period, results are returned Otherwise, a query
            ID is returned that can later be used to retrieve the results.
        """
        if query is None and body is None:
            raise ValueError("Empty value passed for parameter 'query'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_query/async"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if allow_partial_results is not None:
            __query["allow_partial_results"] = allow_partial_results
        if delimiter is not None:
            __query["delimiter"] = delimiter
        if drop_null_columns is not None:
            __query["drop_null_columns"] = drop_null_columns
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
            if query is not None:
                __body["query"] = str(query)
            if columnar is not None:
                __body["columnar"] = columnar
            if filter is not None:
                __body["filter"] = filter
            if include_ccs_metadata is not None:
                __body["include_ccs_metadata"] = include_ccs_metadata
            if keep_alive is not None:
                __body["keep_alive"] = keep_alive
            if keep_on_completion is not None:
                __body["keep_on_completion"] = keep_on_completion
            if locale is not None:
                __body["locale"] = locale
            if params is not None:
                __body["params"] = params
            if profile is not None:
                __body["profile"] = profile
            if tables is not None:
                __body["tables"] = tables
            if wait_for_completion_timeout is not None:
                __body["wait_for_completion_timeout"] = wait_for_completion_timeout
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="esql.async_query",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def async_query_delete(
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

          <p>Delete an async ES|QL query.
          If the query is still running, it is cancelled.
          Otherwise, the stored results are deleted.</p>
          <p>If the Elasticsearch security features are enabled, only the following users can use this API to delete a query:</p>
          <ul>
          <li>The authenticated user that submitted the original query request</li>
          <li>Users with the <code>cancel_task</code> cluster privilege</li>
          </ul>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query-delete>`_

        :param id: The unique identifier of the query. A query ID is provided in the
            ES|QL async query API response for a query that does not complete in the
            designated time. A query ID is also provided when the request was submitted
            with the `keep_on_completion` parameter set to `true`.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_query/async/{__path_parts["id"]}'
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
            endpoint_id="esql.async_query_delete",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def async_query_get(
        self,
        *,
        id: str,
        drop_null_columns: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        format: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "arrow", "cbor", "csv", "json", "smile", "tsv", "txt", "yaml"
                ],
            ]
        ] = None,
        human: t.Optional[bool] = None,
        keep_alive: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        wait_for_completion_timeout: t.Optional[
            t.Union[str, t.Literal[-1], t.Literal[0]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get async ES|QL query results.
          Get the current status and available results or stored results for an ES|QL asynchronous query.
          If the Elasticsearch security features are enabled, only the user who first submitted the ES|QL query can retrieve the results using this API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query-get>`_

        :param id: The unique identifier of the query. A query ID is provided in the
            ES|QL async query API response for a query that does not complete in the
            designated time. A query ID is also provided when the request was submitted
            with the `keep_on_completion` parameter set to `true`.
        :param drop_null_columns: Indicates whether columns that are entirely `null`
            will be removed from the `columns` and `values` portion of the results. If
            `true`, the response will include an extra section under the name `all_columns`
            which has the name of all the columns.
        :param format: A short version of the Accept header, for example `json` or `yaml`.
        :param keep_alive: The period for which the query and its results are stored
            in the cluster. When this period expires, the query and its results are deleted,
            even if the query is still ongoing.
        :param wait_for_completion_timeout: The period to wait for the request to finish.
            By default, the request waits for complete query results. If the request
            completes during the period specified in this parameter, complete query results
            are returned. Otherwise, the response returns an `is_running` value of `true`
            and no results.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_query/async/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if drop_null_columns is not None:
            __query["drop_null_columns"] = drop_null_columns
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
            endpoint_id="esql.async_query_get",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def async_query_stop(
        self,
        *,
        id: str,
        drop_null_columns: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Stop async ES|QL query.</p>
          <p>This API interrupts the query execution and returns the results so far.
          If the Elasticsearch security features are enabled, only the user who first submitted the ES|QL query can stop it.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query-stop>`_

        :param id: The unique identifier of the query. A query ID is provided in the
            ES|QL async query API response for a query that does not complete in the
            designated time. A query ID is also provided when the request was submitted
            with the `keep_on_completion` parameter set to `true`.
        :param drop_null_columns: Indicates whether columns that are entirely `null`
            will be removed from the `columns` and `values` portion of the results. If
            `true`, the response will include an extra section under the name `all_columns`
            which has the name of all the columns.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_query/async/{__path_parts["id"]}/stop'
        __query: t.Dict[str, t.Any] = {}
        if drop_null_columns is not None:
            __query["drop_null_columns"] = drop_null_columns
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
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="esql.async_query_stop",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def get_query(
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

          <p>Get a specific running ES|QL query information.
          Returns an object extended information about a running ES|QL query.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-get-query>`_

        :param id: The query ID
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_query/queries/{__path_parts["id"]}'
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
            endpoint_id="esql.get_query",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def list_queries(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get running ES|QL queries information.
          Returns an object containing IDs and other information about the running ES|QL queries.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-list-queries>`_
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_query/queries"
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
            endpoint_id="esql.list_queries",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "query",
            "columnar",
            "filter",
            "include_ccs_metadata",
            "locale",
            "params",
            "profile",
            "tables",
        ),
        ignore_deprecated_options={"params"},
    )
    async def query(
        self,
        *,
        query: t.Optional[t.Union[str, "ESQLBase"]] = None,
        allow_partial_results: t.Optional[bool] = None,
        columnar: t.Optional[bool] = None,
        delimiter: t.Optional[str] = None,
        drop_null_columns: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter: t.Optional[t.Mapping[str, t.Any]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        format: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "arrow", "cbor", "csv", "json", "smile", "tsv", "txt", "yaml"
                ],
            ]
        ] = None,
        human: t.Optional[bool] = None,
        include_ccs_metadata: t.Optional[bool] = None,
        locale: t.Optional[str] = None,
        params: t.Optional[t.Sequence[t.Union[None, bool, float, int, str]]] = None,
        pretty: t.Optional[bool] = None,
        profile: t.Optional[bool] = None,
        tables: t.Optional[
            t.Mapping[str, t.Mapping[str, t.Mapping[str, t.Any]]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Run an ES|QL query.
          Get search results for an ES|QL (Elasticsearch query language) query.</p>


        `<https://www.elastic.co/docs/explore-analyze/query-filter/languages/esql-rest>`_

        :param query: The ES|QL query API accepts an ES|QL query string in the query
            parameter, runs it, and returns the results.
        :param allow_partial_results: If `true`, partial results will be returned if
            there are shard failures, but the query can continue to execute on other
            clusters and shards. If `false`, the query will fail if there are any failures.
            To override the default behavior, you can set the `esql.query.allow_partial_results`
            cluster setting to `false`.
        :param columnar: By default, ES|QL returns results as rows. For example, FROM
            returns each individual document as one row. For the JSON, YAML, CBOR and
            smile formats, ES|QL can return the results in a columnar fashion where one
            row represents all the values of a certain column in the results.
        :param delimiter: The character to use between values within a CSV row. Only
            valid for the CSV format.
        :param drop_null_columns: Should columns that are entirely `null` be removed
            from the `columns` and `values` portion of the results? Defaults to `false`.
            If `true` then the response will include an extra section under the name
            `all_columns` which has the name of all columns.
        :param filter: Specify a Query DSL query in the filter parameter to filter the
            set of documents that an ES|QL query runs on.
        :param format: A short version of the Accept header, e.g. json, yaml. `csv`,
            `tsv`, and `txt` formats will return results in a tabular format, excluding
            other metadata fields from the response.
        :param include_ccs_metadata: When set to `true` and performing a cross-cluster
            query, the response will include an extra `_clusters` object with information
            about the clusters that participated in the search along with info such as
            shards count.
        :param locale:
        :param params: To avoid any attempts of hacking or code injection, extract the
            values in a separate list of parameters. Use question mark placeholders (?)
            in the query string for each of the parameters.
        :param profile: If provided and `true` the response will include an extra `profile`
            object with information on how the query was executed. This information is
            for human debugging and its format can change at any time but it can give
            some insight into the performance of each part of the query.
        :param tables: Tables to use with the LOOKUP operation. The top level key is
            the table name and the next level key is the column name.
        """
        if query is None and body is None:
            raise ValueError("Empty value passed for parameter 'query'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_query"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if allow_partial_results is not None:
            __query["allow_partial_results"] = allow_partial_results
        if delimiter is not None:
            __query["delimiter"] = delimiter
        if drop_null_columns is not None:
            __query["drop_null_columns"] = drop_null_columns
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
            if query is not None:
                __body["query"] = str(query)
            if columnar is not None:
                __body["columnar"] = columnar
            if filter is not None:
                __body["filter"] = filter
            if include_ccs_metadata is not None:
                __body["include_ccs_metadata"] = include_ccs_metadata
            if locale is not None:
                __body["locale"] = locale
            if params is not None:
                __body["params"] = params
            if profile is not None:
                __body["profile"] = profile
            if tables is not None:
                __body["tables"] = tables
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="esql.query",
            path_parts=__path_parts,
        )
