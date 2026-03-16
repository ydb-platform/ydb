# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
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


# ------------------------------------------------------------------------------------------
# THIS CODE IS AUTOMATICALLY GENERATED AND MANUAL EDITS WILL BE LOST
#
# To contribute, kindly make modifications in the opensearch-py client generator
# or in the OpenSearch API specification, and run `nox -rs generate`. See DEVELOPER_GUIDE.md
# and https://github.com/opensearch-project/opensearch-api-specification for details.
# -----------------------------------------------------------------------------------------+


from typing import Any

from .utils import SKIP_IN_PATH, NamespacedClient, _make_path, query_params


class IndicesClient(NamespacedClient):
    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def analyze(
        self,
        *,
        body: Any = None,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Performs the analysis process on a text and return the tokens breakdown of the
        text.


        :arg body: Define analyzer/tokenizer parameters and the text on
            which the analysis should be performed
        :arg index: The name of the index to scope the operation.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "POST",
            _make_path(index, "_analyze"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "pretty",
        "source",
    )
    def refresh(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Performs the refresh operation in one or more indexes.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "POST", _make_path(index, "_refresh"), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "force",
        "human",
        "ignore_unavailable",
        "pretty",
        "source",
        "wait_if_ongoing",
    )
    def flush(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Performs the flush operation on one or more indexes.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases to flush. Supports wildcards (`*`). To flush all data streams
            and indexes, omit this parameter or use `*` or `_all`.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg force: If `true`, the request forces a flush even if there
            are no changes to commit to the index.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg wait_if_ongoing: If `true`, the flush operation blocks
            until execution when another flush operation is running. If `false`,
            OpenSearch returns an error if you request a flush when another flush
            operation is running. Default is True.
        """
        return self.transport.perform_request(
            "POST", _make_path(index, "_flush"), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
        "wait_for_active_shards",
    )
    def create(
        self,
        *,
        index: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates an index with optional settings and mappings.


        :arg index: The name of the index you wish to create.
        :arg body: The configuration for the index (`settings` and
            `mappings`)
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to `all` or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "PUT", _make_path(index), params=params, headers=headers, body=body
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "task_execution_timeout",
        "timeout",
        "wait_for_active_shards",
        "wait_for_completion",
    )
    def clone(
        self,
        *,
        index: Any,
        target: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Clones an index.


        :arg index: Name of the source index to clone.
        :arg target: Name of the target index to create.
        :arg body: The configuration for the target index (`settings`
            and `aliases`)
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg task_execution_timeout: Explicit task execution timeout,
            only useful when `wait_for_completion` is false, defaults to `1h`.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to `all` or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        :arg wait_for_completion: Should this request wait until the
            operation has completed before returning. Default is True.
        """
        for param in (index, target):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path(index, "_clone", target),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "allow_no_indices",
        "cluster_manager_timeout",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "flat_settings",
        "human",
        "ignore_unavailable",
        "include_defaults",
        "local",
        "master_timeout",
        "pretty",
        "source",
    )
    def get(
        self,
        *,
        index: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about one or more indexes.


        :arg index: A comma-separated list of data streams, indexes, and
            index aliases used to limit the request. Wildcard expressions (*) are
            supported.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. For example, a request targeting foo*,bar*
            returns an error if an index starts with foo but no index starts with
            bar. Default is false.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard expressions
            can match. If the request can target data streams, this argument
            determines whether wildcard expressions match hidden data streams.
            Supports comma-separated values, such as `open,hidden`. Valid choices
            are all, closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: If `true`, returns settings in flat format.
            Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, requests that target a
            missing index return an error. Default is false.
        :arg include_defaults: If `true`, return all default settings in
            the response. Default is false.
        :arg local: If `true`, the request retrieves information from
            the local node only. Defaults to false, which means information is
            retrieved from the cluster-manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "GET", _make_path(index), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "cluster_manager_timeout",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "master_timeout",
        "pretty",
        "source",
        "task_execution_timeout",
        "timeout",
        "wait_for_active_shards",
        "wait_for_completion",
    )
    def open(
        self,
        *,
        index: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Opens an index.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). By default,
            you must explicitly name the indexes you using to limit the request. To
            limit a request using `_all`, `*`, or other wildcard expressions, change
            the `action.destructive_requires_name` setting to false. You can update
            this setting in the `opensearch.yml` file or using the cluster update
            settings API.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg task_execution_timeout: Explicit task execution timeout,
            only useful when `wait_for_completion` is false, defaults to `1h`.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to `all` or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        :arg wait_for_completion: Should this request wait until the
            operation has completed before returning. Default is True.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "POST", _make_path(index, "_open"), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "cluster_manager_timeout",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
        "wait_for_active_shards",
    )
    def close(
        self,
        *,
        index: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Closes an index.


        :arg index: A comma-separated list or wildcard expression of
            index names used to limit the request.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to `all` or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "POST", _make_path(index, "_close"), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "cluster_manager_timeout",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def delete(
        self,
        *,
        index: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes an index.


        :arg index: A comma-separated list of indexes to delete. You
            cannot specify index aliases. By default, this parameter does not
            support wildcards (`*`) or `_all`. To use wildcards or `_all`, set the
            `action.destructive_requires_name` cluster setting to `false`.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. Default is false.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "DELETE", _make_path(index), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "cluster_manager_timeout",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "flat_settings",
        "human",
        "ignore_unavailable",
        "include_defaults",
        "local",
        "pretty",
        "source",
    )
    def exists(
        self,
        *,
        index: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about whether a particular index exists.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases. Supports wildcards (`*`).
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. Default is false.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: If `true`, returns settings in flat format.
            Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index. Default is false.
        :arg include_defaults: If `true`, return all default settings in
            the response. Default is false.
        :arg local: If `true`, the request retrieves information from
            the local node only. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "HEAD", _make_path(index), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "cluster_manager_timeout",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
        "write_index_only",
    )
    def put_mapping(
        self,
        *,
        body: Any,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Updates the index mappings.


        :arg body: The mapping definition
        :arg index: Comma-separated list of indices; use `_all` or empty
            string to perform the operation on all indices.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        :arg write_index_only: If `true`, the mappings are applied only
            to the current write index for the target. Default is false.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        if index in SKIP_IN_PATH:
            index = "_all"

        return self.transport.perform_request(
            "PUT",
            _make_path(index, "_mapping"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "allow_no_indices",
        "cluster_manager_timeout",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "local",
        "master_timeout",
        "pretty",
        "source",
    )
    def get_mapping(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns mappings for one or more indexes.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg local: If `true`, the request retrieves information from
            the local node only. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", _make_path(index, "_mapping"), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "include_defaults",
        "local",
        "pretty",
        "source",
    )
    def get_field_mapping(
        self,
        *,
        fields: Any,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns mapping for one or more fields.


        :arg fields: A comma-separated list or wildcard expression of
            fields used to limit returned information.
        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg include_defaults: If `true`, return all default settings in
            the response.
        :arg local: If `true`, the request retrieves information from
            the local node only. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if fields in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'fields'.")

        return self.transport.perform_request(
            "GET",
            _make_path(index, "_mapping", "field", fields),
            params=params,
            headers=headers,
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "local",
        "pretty",
        "source",
    )
    def exists_alias(
        self,
        *,
        name: Any,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about whether a particular alias exists.


        :arg name: A comma-separated list of aliases to check. Supports
            wildcards (`*`).
        :arg index: A comma-separated list of data streams or indexes
            used to limit the request. Supports wildcards (`*`). To target all data
            streams and indexes, omit this parameter or use `*` or `_all`.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, requests that include a
            missing data stream or index in the target indexes or data streams
            return an error.
        :arg local: If `true`, the request retrieves information from
            the local node only. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "HEAD", _make_path(index, "_alias", name), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "local",
        "pretty",
        "source",
    )
    def get_alias(
        self,
        *,
        index: Any = None,
        name: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns an alias.


        :arg index: A comma-separated list of data streams or indexes
            used to limit the request. Supports wildcards (`*`). To target all data
            streams and indexes, omit this parameter or use `*` or `_all`.
        :arg name: A comma-separated list of aliases to retrieve.
            Supports wildcards (`*`). To retrieve all aliases, omit this parameter
            or use `*` or `_all`.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg local: If `true`, the request retrieves information from
            the local node only. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", _make_path(index, "_alias", name), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def update_aliases(
        self,
        *,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Updates index aliases.


        :arg body: The definition of `actions` to perform
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return self.transport.perform_request(
            "POST", "/_aliases", params=params, headers=headers, body=body
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def delete_alias(
        self,
        *,
        index: Any,
        name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes an alias.


        :arg index: A comma-separated list of data streams or indexes
            used to limit the request. Supports wildcards (`*`).
        :arg name: A comma-separated list of aliases to remove. Supports
            wildcards (`*`). To remove all aliases, use `*` or `_all`.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        """
        for param in (index, name):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "DELETE", _make_path(index, "_alias", name), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "create",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "order",
        "pretty",
        "source",
    )
    def put_template(
        self,
        *,
        name: Any,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates or updates an index template.


        :arg name: The name of the template
        :arg body: The template definition
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg create: If `true`, this request cannot replace or update
            existing index templates. Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg order: Order in which OpenSearch applies this template if
            index matches multiple templates.  Templates with lower 'order' values
            are merged first. Templates with higher 'order' values are merged later,
            overriding templates with lower values.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        for param in (name, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_template", name),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "flat_settings",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "source",
    )
    def exists_template(
        self,
        *,
        name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about whether a particular index template exists.


        :arg name: The comma separated names of the index templates
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: Return settings in flat format. Default is
            false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Return local information, do not retrieve the state
            from cluster-manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Explicit operation timeout for
            connection to cluster-manager node
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "HEAD", _make_path("_template", name), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "flat_settings",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "source",
    )
    def get_template(
        self,
        *,
        name: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns an index template.


        :arg name: A comma-separated list of index template names used
            to limit the request. Wildcard (`*`) expressions are supported. To
            return all index templates, omit this parameter or use a value of `_all`
            or `*`.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: If `true`, returns settings in flat format.
            Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: If `true`, the request retrieves information from
            the local node only. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", _make_path("_template", name), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def delete_template(
        self,
        *,
        name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes an index template.


        :arg name: The name of the legacy index template to delete.
            Wildcard (`*`) expressions are supported.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "DELETE", _make_path("_template", name), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "cluster_manager_timeout",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "flat_settings",
        "human",
        "ignore_unavailable",
        "include_defaults",
        "local",
        "master_timeout",
        "pretty",
        "source",
    )
    def get_settings(
        self,
        *,
        index: Any = None,
        name: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns settings for one or more indexes.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg name: A comma-separated list or wildcard expression of
            settings to retrieve.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. For example, a request targeting `foo*,bar*`
            returns an error if an index starts with foo but no index starts with
            `bar`.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid choices are all, closed,
            hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: If `true`, returns settings in flat format.
            Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg include_defaults: If `true`, return all default settings in
            the response. Default is false.
        :arg local: If `true`, the request retrieves information from
            the local node only. If `false`, information is retrieved from the
            cluster-manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", _make_path(index, "_settings", name), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "cluster_manager_timeout",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "flat_settings",
        "human",
        "ignore_unavailable",
        "master_timeout",
        "preserve_existing",
        "pretty",
        "source",
        "timeout",
    )
    def put_settings(
        self,
        *,
        body: Any,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Updates the index settings.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. For example, a request targeting `foo*,bar*`
            returns an error if an index starts with `foo` but no index starts with
            `bar`.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid choices are all, closed,
            hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: If `true`, returns settings in flat format.
            Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: Whether specified concrete indexes
            should be ignored when unavailable (missing or closed).
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg preserve_existing: If `true`, existing index settings
            remain unchanged. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return self.transport.perform_request(
            "PUT",
            _make_path(index, "_settings"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "completion_fields",
        "error_trace",
        "expand_wildcards",
        "fielddata_fields",
        "fields",
        "filter_path",
        "forbid_closed_indices",
        "groups",
        "human",
        "include_segment_file_sizes",
        "include_unloaded_segments",
        "level",
        "pretty",
        "source",
    )
    def stats(
        self,
        *,
        index: Any = None,
        metric: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Provides statistics on operations happening in an index.


        :arg index: A comma-separated list of index names; use `_all` or
            empty string to perform the operation on all indexes
        :arg metric: Limit the information returned the specific
            metrics.
        :arg completion_fields: A comma-separated list or wildcard
            expressions of fields to include in field data and suggest statistics.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid choices are all, closed,
            hidden, none, open.
        :arg fielddata_fields: A comma-separated list or wildcard
            expressions of fields to include in field data statistics.
        :arg fields: A comma-separated list or wildcard expressions of
            fields to include in the statistics.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg forbid_closed_indices: If `true`, statistics are not
            collected from closed indexes. Default is True.
        :arg groups: A comma-separated list of search groups to include
            in the search statistics.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg include_segment_file_sizes: If `true`, the call reports the
            aggregated disk usage of each one of the Lucene index files (only
            applies if segment stats are requested). Default is false.
        :arg include_unloaded_segments: If `true`, the response includes
            information from segments that are not loaded into memory. Default is
            false.
        :arg level: Indicates whether statistics are aggregated at the
            cluster, index, or shard level. Valid choices are cluster, indices,
            shards.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", _make_path(index, "_stats", metric), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "pretty",
        "source",
        "verbose",
    )
    def segments(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Provides low-level information about segments in a Lucene index.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg verbose: If `true`, the request returns a verbose response.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", _make_path(index, "_segments"), params=params, headers=headers
        )

    @query_params(
        "all_shards",
        "allow_no_indices",
        "analyze_wildcard",
        "analyzer",
        "default_operator",
        "df",
        "error_trace",
        "expand_wildcards",
        "explain",
        "filter_path",
        "human",
        "ignore_unavailable",
        "lenient",
        "pretty",
        "q",
        "rewrite",
        "source",
    )
    def validate_query(
        self,
        *,
        body: Any = None,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows a user to validate a potentially expensive query without executing it.


        :arg body: The query definition specified with the Query DSL
        :arg index: A comma-separated list of data streams, indexes, and
            aliases to search. Supports wildcards (`*`). To search all data streams
            or indexes, omit this parameter or use `*` or `_all`.
        :arg all_shards: If `true`, the validation is executed on all
            shards instead of one random shard per index.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg analyze_wildcard: If `true`, wildcard and prefix queries
            are analyzed. Default is false.
        :arg analyzer: Analyzer to use for the query string. This
            parameter can only be used when the `q` query string parameter is
            specified.
        :arg default_operator: The default operator for query string
            query: `AND` or `OR`. Valid choices are and, or.
        :arg df: Field to use as default where no field prefix is given
            in the query string. This parameter can only be used when the `q` query
            string parameter is specified.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg explain: If `true`, the response returns detailed
            information if an error has occurred.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg lenient: If `true`, format-based query failures (such as
            providing text to a numeric field) in the query string will be ignored.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg q: Query in the Lucene query string syntax.
        :arg rewrite: If `true`, returns a more detailed explanation
            showing the actual Lucene query that will be executed.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "POST",
            _make_path(index, "_validate", "query"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "fielddata",
        "fields",
        "file",
        "filter_path",
        "human",
        "ignore_unavailable",
        "pretty",
        "query",
        "request",
        "source",
    )
    def clear_cache(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Clears all or specific caches for one or more indexes.


        :arg index: A comma-separated list of indexes; use `_all` or
            empty string to perform the operation on all indexes.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg fielddata: If `true`, clears the fields cache. Use the
            `fields` parameter to clear the cache of specific fields only.
        :arg fields: A comma-separated list of field names used to limit
            the `fielddata` parameter.
        :arg file: If `true`, clears the unused entries from the file
            cache on nodes with the Search role. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg query: If `true`, clears the query cache.
        :arg request: If `true`, clears the request cache.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "POST", _make_path(index, "_cache", "clear"), params=params, headers=headers
        )

    @query_params(
        "active_only",
        "detailed",
        "error_trace",
        "filter_path",
        "human",
        "pretty",
        "source",
    )
    def recovery(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about ongoing index shard recoveries.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg active_only: If `true`, the response only includes ongoing
            shard recoveries. Default is false.
        :arg detailed: If `true`, the response includes detailed
            information about shard recoveries. Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", _make_path(index, "_recovery"), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "only_ancient_segments",
        "pretty",
        "source",
        "wait_for_completion",
    )
    def upgrade(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        The `_upgrade` API is no longer useful and will be removed.


        :arg index: A comma-separated list of indexes; use `_all` or
            empty string to perform the operation on all indexes.
        :arg allow_no_indices: Whether to ignore if a wildcard indexes
            expression resolves into no concrete indexes. (This includes `_all`
            string or when no indexes have been specified).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indexes that are open, closed or both. Valid choices are all,
            closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: Whether specified concrete indexes
            should be ignored when unavailable (missing or closed).
        :arg only_ancient_segments: If `true`, only ancient (an older
            Lucene major release) segments will be upgraded.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg wait_for_completion: Should this request wait until the
            operation has completed before returning. Default is false.
        """
        return self.transport.perform_request(
            "POST", _make_path(index, "_upgrade"), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "pretty",
        "source",
    )
    def get_upgrade(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        The `_upgrade` API is no longer useful and will be removed.


        :arg index: A comma-separated list of indexes; use `_all` or
            empty string to perform the operation on all indexes.
        :arg allow_no_indices: Whether to ignore if a wildcard indexes
            expression resolves into no concrete indexes. (This includes `_all`
            string or when no indexes have been specified).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indexes that are open, closed or both. Valid choices are all,
            closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: Whether specified concrete indexes
            should be ignored when unavailable (missing or closed).
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", _make_path(index, "_upgrade"), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "pretty",
        "source",
        "status",
    )
    def shard_stores(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Provides store information for shard copies of indexes.


        :arg index: Limits health reporting to a specific source. Can be
            a single source or a comma-separated list of sources (comprised of data
            streams, indexes, and aliases).
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Valid choices
            are all, closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `true`, missing or closed indexes
            are not included in the response. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg status: A list of shard health statuses used to limit the
            request. Default is ['yellow', 'red'].
        """
        return self.transport.perform_request(
            "GET", _make_path(index, "_shard_stores"), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "flush",
        "human",
        "ignore_unavailable",
        "max_num_segments",
        "only_expunge_deletes",
        "pretty",
        "primary_only",
        "source",
        "wait_for_completion",
    )
    def forcemerge(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Performs the force merge operation on one or more indexes.


        :arg index: A comma-separated list of index names; use `_all` or
            empty string to perform the operation on all indexes
        :arg allow_no_indices: Whether to ignore if a wildcard indexes
            expression resolves into no concrete indexes. (This includes `_all`
            string or when no indexes have been specified)
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indexes that are open, closed or both. Valid choices are all,
            closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flush: Specify whether the index should be flushed after
            performing the operation. Default is True.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: Whether specified concrete indexes
            should be ignored when unavailable (missing or closed)
        :arg max_num_segments: The number of larger segments into which
            smaller segments are merged. Set this parameter to 1 to merge all
            segments into one segment. The default behavior is to perform the merge
            as necessary.
        :arg only_expunge_deletes: Specify whether the operation should
            only expunge deleted documents
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg primary_only: Specify whether the operation should only
            perform on primary shards. Defaults to false. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg wait_for_completion: Should the request wait until the
            force merge is completed. Default is True.
        """
        return self.transport.perform_request(
            "POST", _make_path(index, "_forcemerge"), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "copy_settings",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "task_execution_timeout",
        "timeout",
        "wait_for_active_shards",
        "wait_for_completion",
    )
    def shrink(
        self,
        *,
        index: Any,
        target: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allow to shrink an existing index into a new index with fewer primary shards.


        :arg index: Name of the source index to shrink.
        :arg target: Name of the target index to create.
        :arg body: The configuration for the target index (`settings`
            and `aliases`)
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg copy_settings: whether or not to copy settings from the
            source index. Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg task_execution_timeout: Explicit task execution timeout,
            only useful when `wait_for_completion` is false, defaults to `1h`.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to `all` or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        :arg wait_for_completion: Should this request wait until the
            operation has completed before returning. Default is True.
        """
        for param in (index, target):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path(index, "_shrink", target),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "cluster_manager_timeout",
        "copy_settings",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "task_execution_timeout",
        "timeout",
        "wait_for_active_shards",
        "wait_for_completion",
    )
    def split(
        self,
        *,
        index: Any,
        target: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows you to split an existing index into a new index with more primary
        shards.


        :arg index: Name of the source index to split.
        :arg target: Name of the target index to create.
        :arg body: The configuration for the target index (`settings`
            and `aliases`)
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg copy_settings: whether or not to copy settings from the
            source index. Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg task_execution_timeout: Explicit task execution timeout,
            only useful when `wait_for_completion` is false, defaults to `1h`.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to `all` or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        :arg wait_for_completion: Should this request wait until the
            operation has completed before returning. Default is True.
        """
        for param in (index, target):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path(index, "_split", target),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "cluster_manager_timeout",
        "dry_run",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
        "wait_for_active_shards",
    )
    def rollover(
        self,
        *,
        alias: Any,
        body: Any = None,
        new_index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Updates an alias to point to a new index when the existing index is considered
        to be too large or too old.


        :arg alias: Name of the data stream or index alias to roll over.
        :arg body: The conditions that needs to be met for executing
            rollover
        :arg new_index: The name of the index to create. Supports date
            math. Data streams do not support this parameter.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg dry_run: If `true`, checks whether the current index
            satisfies the specified conditions but does not perform a rollover.
            Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to all or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        """
        if alias in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'alias'.")

        return self.transport.perform_request(
            "POST",
            _make_path(alias, "_rollover", new_index),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def create_data_stream(
        self,
        *,
        name: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates or updates a data stream.


        :arg name: Name of the data stream, which must meet the
            following criteria: Lowercase only; Cannot include `/`, `*`, `?`, `"`,
            `<`, `>`, `|`, `,`, `#`, `:`, backslash, or a space character; Cannot
            start with `-`, `_`, `+`, or `.ds-`; Cannot be `.` or `..`; Cannot be
            longer than 255 bytes. Multi-byte characters count towards this limit
            faster.
        :arg body: The data stream definition
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_data_stream", name),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete_data_stream(
        self,
        *,
        name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes a data stream.


        :arg name: A comma-separated list of data streams to delete.
            Wildcard (`*`) expressions are supported.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "DELETE", _make_path("_data_stream", name), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def delete_index_template(
        self,
        *,
        name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes an index template.


        :arg name: The name of the index template to delete. Wildcard
            (*) expressions are supported.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_index_template", name),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "flat_settings",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "source",
    )
    def exists_index_template(
        self,
        *,
        name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about whether a particular index template exists.


        :arg name: The name of the index template to check existence of.
            Wildcard (*) expressions are supported.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: Return settings in flat format. Default is
            false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Return local information, do not retrieve the state
            from cluster-manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "HEAD", _make_path("_index_template", name), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "flat_settings",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "source",
    )
    def get_index_template(
        self,
        *,
        name: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns an index template.


        :arg name: The name of the index template to retrieve. Wildcard
            (*) expressions are supported.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: If `true`, returns settings in flat format.
            Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: If `true`, the request retrieves information from
            the local node only. Defaults to false, which means information is
            retrieved from the cluster-manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", _make_path("_index_template", name), params=params, headers=headers
        )

    @query_params(
        "cause",
        "cluster_manager_timeout",
        "create",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
    )
    def put_index_template(
        self,
        *,
        name: Any,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates or updates an index template.


        :arg name: Index or template name
        :arg body: The template definition
        :arg cause: User defined reason for creating/updating the index
            template. Default is false.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg create: If `true`, this request cannot replace or update
            existing index templates. Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Operation timeout for
            connection to cluster-manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        for param in (name, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_index_template", name),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
    )
    def simulate_index_template(
        self,
        *,
        name: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Simulate matching the given index name against the index templates in the
        system.


        :arg name: Index or template name to simulate
        :arg body: New index template definition, which will be included
            in the simulation, as if it already exists in the system
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "POST",
            _make_path("_index_template", "_simulate_index", name),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get_data_stream(
        self,
        *,
        name: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns data streams.


        :arg name: A comma-separated list of data stream names used to
            limit the request. Wildcard (`*`) expressions are supported. If omitted,
            all data streams are returned.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", _make_path("_data_stream", name), params=params, headers=headers
        )

    @query_params(
        "cause",
        "cluster_manager_timeout",
        "create",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
    )
    def simulate_template(
        self,
        *,
        body: Any = None,
        name: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Simulate resolving the given template name or body.


        :arg name: The name of the index template to simulate. To test a
            template configuration before you add it to the cluster, omit this
            parameter and specify the template configuration in the request body.
        :arg cause: User defined reason for dry-run creating the new
            template for simulation purposes. Default is false.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg create: If `true`, the template passed in the body is only
            used if no existing templates match the same index patterns. If `false`,
            the simulation uses the template with the highest priority. Note that
            the template is not permanently added or updated in either case; it is
            only used for the simulation. Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "POST",
            _make_path("_index_template", "_simulate", name),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "error_trace", "expand_wildcards", "filter_path", "human", "pretty", "source"
    )
    def resolve_index(
        self,
        *,
        name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about any matching indexes, aliases, and data streams.


        :arg name: Comma-separated name(s) or index pattern(s) of the
            indexes, aliases, and data streams to resolve. Resources on remote
            clusters can be specified using the `<cluster>`:`<name>` syntax.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "GET", _make_path("_resolve", "index", name), params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "cluster_manager_timeout",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def add_block(
        self,
        *,
        index: Any,
        block: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Adds a block to an index.


        :arg index: A comma separated list of indexes to add a block to.
        :arg block: The block to add (one of `read`, `write`,
            `read_only` or `metadata`).
        :arg allow_no_indices: Whether to ignore if a wildcard indexes
            expression resolves into no concrete indexes. (This includes `_all`
            string or when no indexes have been specified).
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indexes that are open, closed or both. Valid choices are all,
            closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: Whether specified concrete indexes
            should be ignored when unavailable (missing or closed).
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Specify timeout for connection
            to cluster manager.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Explicit operation timeout
        """
        for param in (index, block):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT", _make_path(index, "_block", block), params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def data_streams_stats(
        self,
        *,
        name: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Provides statistics on operations happening in a data stream.


        :arg name: A comma-separated list of data streams used to limit
            the request. Wildcard expressions (`*`) are supported. To target all
            data streams in a cluster, omit this parameter or use `*`.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_data_stream", name, "_stats"),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def put_alias(
        self,
        *,
        index: Any = None,
        name: Any = None,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates or updates an alias.


        :arg body: The settings for the alias, such as `routing` or
            `filter`
        :arg index: A comma-separated list of data streams or indexes to
            add. Supports wildcards (`*`). Wildcard patterns that match both data
            streams and indexes return an error.
        :arg name: Alias to update. If the alias doesn't exist, the
            request creates it. Index alias names support date math.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        """
        return self.transport.perform_request(
            "PUT",
            _make_path(index, "_alias", name),
            params=params,
            headers=headers,
            body=body,
        )
