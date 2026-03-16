# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

# ------------------------------------------------------------------------------------------
# THIS CODE IS AUTOMATICALLY GENERATED AND MANUAL EDITS WILL BE LOST
#
# To contribute, kindly make modifications in the opensearch-py client generator
# or in the OpenSearch API specification, and run `nox -rs generate`. See DEVELOPER_GUIDE.md
# and https://github.com/opensearch-project/opensearch-api-specification for details.
# -----------------------------------------------------------------------------------------+


from typing import Any

from .utils import NamespacedClient, _make_path, query_params


class ListClient(NamespacedClient):
    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def help(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns help for the List APIs.


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
        return await self.transport.perform_request(
            "GET", "/_list", params=params, headers=headers
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
        "next_token",
        "pretty",
        "pri",
        "s",
        "size",
        "sort",
        "source",
        "time",
        "v",
    )
    async def indices(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns paginated information about indexes including number of primaries and
        replicas, document counts, disk size.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg bytes: The unit used to display byte values. Valid choices
            are b, kb, k, mb, m, gb, g, tb, t, pb, p.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: The type of index that wildcard patterns
            can match. Valid choices are all, closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the Accept header, such as
            `JSON`, `YAML`.
        :arg h: A comma-separated list of column names to display.
        :arg health: The health status used to limit returned indexes.
            By default, the response includes indexes of any health status. Valid
            choices are green, yellow, red.
        :arg help: Return help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg include_unloaded_segments: If `true`, the response includes
            information from segments that are not loaded into memory. Default is
            false.
        :arg local: Return local information, do not retrieve the state
            from cluster-manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Operation timeout for
            connection to cluster-manager node.
        :arg next_token: Token to retrieve next page of indexes.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg pri: If `true`, the response only includes information from
            primary shards. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg size: Maximum number of indexes to be displayed in a page.
        :arg sort: Defines order in which indexes will be displayed.
            Accepted values are `asc` and `desc`. If `desc`, most recently created
            indexes would be displayed first. Valid choices are asc, desc.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg time: The unit used to display time values. Valid choices
            are nanos, micros, ms, s, m, h, d.
        :arg v: Verbose mode. Display column headers. Default is false.
        """
        return await self.transport.perform_request(
            "GET", _make_path("_list", "indices", index), params=params, headers=headers
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
        "next_token",
        "pretty",
        "s",
        "size",
        "sort",
        "source",
        "time",
        "v",
    )
    async def shards(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns paginated details of shard allocation on nodes.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg bytes: The unit used to display byte values. Valid choices
            are b, kb, k, mb, m, gb, g, tb, t, pb, p.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg format: A short version of the Accept header, such as
            `JSON`, `YAML`.
        :arg h: A comma-separated list of column names to display.
        :arg help: Return help information. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Return local information, do not retrieve the state
            from cluster-manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Operation timeout for
            connection to cluster-manager node.
        :arg next_token: Token to retrieve next page of shards.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg s: A comma-separated list of column names or column aliases
            to sort by.
        :arg size: Maximum number of shards to be displayed in a page.
        :arg sort: Defines order in which shards will be displayed.
            Accepted values are `asc` and `desc`. If `desc`, most recently created
            shards would be displayed first. Valid choices are asc, desc.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg time: The unit in which to display time values. Valid
            choices are nanos, micros, ms, s, m, h, d.
        :arg v: Verbose mode. Display column headers. Default is false.
        """
        return await self.transport.perform_request(
            "GET", _make_path("_list", "shards", index), params=params, headers=headers
        )
