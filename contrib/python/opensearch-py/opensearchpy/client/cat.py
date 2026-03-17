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

from .utils import NamespacedClient, _make_path, query_params


class CatClient(NamespacedClient):
    @query_params(
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "local",
        "pretty",
        "s",
        "source",
        "v",
    )
    def aliases(
        self,
        *,
        name: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Shows information about aliases currently configured to indexes, including
        filter and routing information.


        :arg name: A comma-separated list of aliases to retrieve.
            Supports wildcards (`*`).  To retrieve all aliases, omit this parameter
            or use `*` or `_all`.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Specifies the type of index that wildcard
            expressions can match. Supports comma-separated values. Valid choices
            are all, closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Whether to return information from the local node
            only instead of from the cluster manager node. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "aliases", name), params=params, headers=headers
        )

    @query_params(
        "bytes",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "pretty",
        "s",
        "source",
        "v",
    )
    def all_pit_segments(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Lists all active CAT point-in-time segments.


        :arg bytes: The units used to display byte values. Valid choices
            are b, kb, k, mb, m, gb, g, tb, t, pb, p.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", "/_cat/pit_segments/_all", params=params, headers=headers
        )

    @query_params(
        "bytes",
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "s",
        "source",
        "v",
    )
    def allocation(
        self,
        *,
        node_id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Provides a snapshot of how many shards are allocated to each data node and how
        much disk space they are using.


        :arg node_id: A comma-separated list of node IDs or names used
            to limit the returned information.
        :arg bytes: The units used to display byte values. Valid choices
            are b, kb, k, mb, m, gb, g, tb, t, pb, p.
        :arg cluster_manager_timeout: A timeout for connection to the
            cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the HTTP `Accept` header, such
            as `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Returns local information but does not retrieve the
            state from cluster-manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): A timeout for connection to the
            cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "allocation", node_id),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "s",
        "source",
        "v",
    )
    def cluster_manager(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about the cluster-manager node.


        :arg cluster_manager_timeout: A timeout for connection to the
            cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the HTTP `Accept` header, such
            as `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Returns local information but does not retrieve the
            state from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): A timeout for connection to the
            cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", "/_cat/cluster_manager", params=params, headers=headers
        )

    @query_params(
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "pretty",
        "s",
        "source",
        "v",
    )
    def count(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Provides quick access to the document count of the entire cluster or of an
        individual index.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "count", index), params=params, headers=headers
        )

    @query_params(
        "bytes",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "pretty",
        "s",
        "source",
        "v",
    )
    def fielddata(
        self,
        *,
        fields: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Shows how much heap memory is currently being used by field data on every data
        node in the cluster.


        :arg fields: A comma-separated list of fields used to limit the
            amount of returned information.
        :arg bytes: The units used to display byte values. Valid choices
            are b, kb, k, mb, m, gb, g, tb, t, pb, p.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "fielddata", fields),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "pretty",
        "s",
        "source",
        "time",
        "ts",
        "v",
    )
    def health(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns a concise representation of the cluster health.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg time: The unit used to display time values. Valid choices
            are nanos, micros, ms, s, m, h, d.
        :arg ts: When `true`, returns `HH:MM:SS` and Unix epoch
            timestamps. Default is True.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", "/_cat/health", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def help(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns help for the Cat APIs.


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
            "GET", "/_cat", params=params, headers=headers
        )

    @query_params(
        "bytes",
        "cluster_manager_timeout",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "format",
        "h",
        "health",
        "help",
        "human",
        "include_unloaded_segments",
        "local",
        "master_timeout",
        "pretty",
        "pri",
        "s",
        "source",
        "time",
        "v",
    )
    def indices(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Lists information related to indexes, that is, how much disk space they are
        using, how many shards they have, their health status, and so on.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg bytes: The units used to display byte values. Valid choices
            are b, kb, k, mb, m, gb, g, tb, t, pb, p.
        :arg cluster_manager_timeout: The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Specifies the type of index that wildcard
            expressions can match. Supports comma-separated values. Valid choices
            are all, closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg health: Limits indexes based on their health status.
            Supported values are `green`, `yellow`, and `red`. Valid choices are
            green, yellow, red.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg include_unloaded_segments: Whether to include information
            from segments not loaded into memory. Default is false.
        :arg local: Returns local information but does not retrieve the
            state from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg pri: When `true`, returns information only from the primary
            shards. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg time: Specifies the time units. Valid choices are nanos,
            micros, ms, s, m, h, d.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "indices", index), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "s",
        "source",
        "v",
    )
    def master(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about the cluster-manager node.


        :arg cluster_manager_timeout: The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Returns local information but does not retrieve the
            state from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        from warnings import warn

        warn(
            "Deprecated: To promote inclusive language, use '/_cat/cluster_manager' instead."
        )
        return self.transport.perform_request(
            "GET", "/_cat/master", params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "s",
        "source",
        "v",
    )
    def nodeattrs(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about custom node attributes.


        :arg cluster_manager_timeout: The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Returns local information but does not retrieve the
            state from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", "/_cat/nodeattrs", params=params, headers=headers
        )

    @query_params(
        "bytes",
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "full_id",
        "h",
        "help",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "s",
        "source",
        "time",
        "v",
    )
    def nodes(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns basic statistics about the performance of cluster nodes.


        :arg bytes: The units used to display byte values. Valid choices
            are b, kb, k, mb, m, gb, g, tb, t, pb, p.
        :arg cluster_manager_timeout: The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg full_id: When `true`, returns the full node ID. When
            `false`, returns the shortened node ID.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local (Deprecated: This parameter does not cause this API
            to act locally.): Returns local information but does not retrieve the
            state from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg time: Specifies the time units, for example, `5d` or `7h`.
            For more information, see [Supported
            units](https://opensearch.org/docs/latest/api-reference/units/). Valid
            choices are nanos, micros, ms, s, m, h, d.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", "/_cat/nodes", params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "s",
        "source",
        "time",
        "v",
    )
    def pending_tasks(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns a concise representation of the cluster's pending tasks.


        :arg cluster_manager_timeout: The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Returns local information but does not retrieve the
            state from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg time: Specifies the time units, for example, `5d` or `7h`.
            For more information, see [Supported
            units](https://opensearch.org/docs/latest/api-reference/units/). Valid
            choices are nanos, micros, ms, s, m, h, d.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", "/_cat/pending_tasks", params=params, headers=headers
        )

    @query_params(
        "bytes",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "pretty",
        "s",
        "source",
        "v",
    )
    def pit_segments(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Lists one or several CAT point-in-time segments.


        :arg bytes: The units used to display byte values. Valid choices
            are b, kb, k, mb, m, gb, g, tb, t, pb, p.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", "/_cat/pit_segments", params=params, headers=headers, body=body
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "s",
        "source",
        "v",
    )
    def plugins(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about the names, components, and versions of the installed
        plugins.


        :arg cluster_manager_timeout: The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Returns local information but does not retrieve the
            state from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", "/_cat/plugins", params=params, headers=headers
        )

    @query_params(
        "active_only",
        "bytes",
        "detailed",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "pretty",
        "s",
        "source",
        "time",
        "v",
    )
    def recovery(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns all completed and ongoing index and shard recoveries.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg active_only: If `true`, the response only includes ongoing
            shard recoveries. Default is false.
        :arg bytes: The units used to display byte values. Valid choices
            are b, kb, k, mb, m, gb, g, tb, t, pb, p.
        :arg detailed: When `true`, includes detailed information about
            shard recoveries. Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg time: Specifies the time units, for example, `5d` or `7h`.
            For more information, see [Supported
            units](https://opensearch.org/docs/latest/api-reference/units/). Valid
            choices are nanos, micros, ms, s, m, h, d.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "recovery", index), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "s",
        "source",
        "v",
    )
    def repositories(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about all snapshot repositories for a cluster.


        :arg cluster_manager_timeout: The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Returns local information but does not retrieve the
            state from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", "/_cat/repositories", params=params, headers=headers
        )

    @query_params(
        "active_only",
        "allow_no_indices",
        "bytes",
        "completed_only",
        "detailed",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "ignore_throttled",
        "ignore_unavailable",
        "pretty",
        "s",
        "shards",
        "source",
        "time",
        "timeout",
        "v",
    )
    def segment_replication(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about active and last-completed segment replication events
        on each replica shard, including related shard-level metrics.  These metrics
        provide information about how far behind the primary shard the replicas are
        lagging.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg active_only: When `true`, the response only includes
            ongoing segment replication events. Default is false.
        :arg allow_no_indices: Whether to ignore the index if a wildcard
            index expression resolves to no concrete indexes. This includes the
            `_all` string or when no indexes have been specified.
        :arg bytes: The units used to display byte values. Valid choices
            are b, kb, k, mb, m, gb, g, tb, t, pb, p.
        :arg completed_only: When `true`, the response only includes the
            last-completed segment replication events. Default is false.
        :arg detailed: When `true`, the response includes additional
            metrics for each stage of a segment replication event. Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Specifies the type of index that wildcard
            expressions can match. Supports comma-separated values. Valid choices
            are all, closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_throttled: Whether specified concrete, expanded, or
            aliased indexes should be ignored when throttled.
        :arg ignore_unavailable: Whether the specified concrete indexes
            should be ignored when missing or closed.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg shards: A comma-separated list of shards to display.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg time: Specifies the time units, for example, `5d` or `7h`.
            For more information, see [Supported
            units](https://opensearch.org/docs/latest/api-reference/units/). Valid
            choices are nanos, micros, ms, s, m, h, d.
        :arg timeout: The operation timeout.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "segment_replication", index),
            params=params,
            headers=headers,
        )

    @query_params(
        "bytes",
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "master_timeout",
        "pretty",
        "s",
        "source",
        "v",
    )
    def segments(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Provides low-level information about the segments in the shards of an index.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg bytes: The units used to display byte values. Valid choices
            are b, kb, k, mb, m, gb, g, tb, t, pb, p.
        :arg cluster_manager_timeout: The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "segments", index), params=params, headers=headers
        )

    @query_params(
        "bytes",
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "s",
        "source",
        "time",
        "v",
    )
    def shards(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Lists the states of all primary and replica shards and how they are
        distributed.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg bytes: The units used to display byte values. Valid choices
            are b, kb, k, mb, m, gb, g, tb, t, pb, p.
        :arg cluster_manager_timeout: The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Returns local information but does not retrieve the
            state from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg time: Specifies the time units, for example, `5d` or `7h`.
            For more information, see [Supported
            units](https://opensearch.org/docs/latest/api-reference/units/). Valid
            choices are nanos, micros, ms, s, m, h, d.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "shards", index), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "s",
        "size",
        "source",
        "v",
    )
    def thread_pool(
        self,
        *,
        thread_pool_patterns: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns cluster-wide thread pool statistics per node. By default the active,
        queued, and rejected statistics are returned for all thread pools.


        :arg thread_pool_patterns: A comma-separated list of thread pool
            names used to limit the request. Accepts wildcard expressions.
        :arg cluster_manager_timeout: A timeout for connection to the
            cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Returns local information but does not retrieve the
            state from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg size: The multiplier in which to display values.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "thread_pool", thread_pool_patterns),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "ignore_unavailable",
        "master_timeout",
        "pretty",
        "s",
        "source",
        "time",
        "v",
    )
    def snapshots(
        self,
        *,
        repository: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Lists all of the snapshots stored in a specific repository.


        :arg repository: A comma-separated list of snapshot repositories
            used to limit the request. Accepts wildcard expressions. `_all` returns
            all repositories. If any repository fails during the request, OpenSearch
            returns an error.
        :arg cluster_manager_timeout: The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: When `true`, the response does not
            include information from unavailable snapshots. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg time: Specifies the time units, for example, `5d` or `7h`.
            For more information, see [Supported
            units](https://opensearch.org/docs/latest/api-reference/units/). Valid
            choices are nanos, micros, ms, s, m, h, d.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "snapshots", repository),
            params=params,
            headers=headers,
        )

    @query_params(
        "actions",
        "detailed",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "nodes",
        "parent_task_id",
        "pretty",
        "s",
        "source",
        "time",
        "v",
    )
    def tasks(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Lists the progress of all tasks currently running on the cluster.


        :arg actions: The task action names used to limit the response.
        :arg detailed: If `true`, the response includes detailed
            information about shard recoveries. Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg nodes: A comma-separated list of node IDs or names used to
            limit the returned information. Use `_local` to return information from
            the node to which you're connecting, specify a specific node from which
            to get information, or keep the parameter empty to get information from
            all nodes.
        :arg parent_task_id: The parent task identifier, which is used
            to limit the response.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg time: Specifies the time units, for example, `5d` or `7h`.
            For more information, see [Supported
            units](https://opensearch.org/docs/latest/api-reference/units/). Valid
            choices are nanos, micros, ms, s, m, h, d.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", "/_cat/tasks", params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "format",
        "h",
        "help",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "s",
        "source",
        "v",
    )
    def templates(
        self,
        *,
        name: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Lists the names, patterns, order numbers, and version numbers of index
        templates.


        :arg name: The name of the template to return. Accepts wildcard
            expressions. If omitted, all templates are returned.
        :arg cluster_manager_timeout: The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the `Accept` header, such as
            `json` or `yaml`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Returns help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Returns local information but does not retrieve the
            state from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): The amount of time allowed to
            establish a connection to the cluster manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg v: Enables verbose mode, which displays column headers.
            Default is false.
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "templates", name), params=params, headers=headers
        )
