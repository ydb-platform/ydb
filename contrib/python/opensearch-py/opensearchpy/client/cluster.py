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


class ClusterClient(NamespacedClient):
    @query_params(
        "awareness_attribute",
        "cluster_manager_timeout",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "level",
        "local",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
        "wait_for_active_shards",
        "wait_for_events",
        "wait_for_no_initializing_shards",
        "wait_for_no_relocating_shards",
        "wait_for_nodes",
        "wait_for_status",
    )
    def health(
        self,
        *,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns basic information about the health of the cluster.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg awareness_attribute: The name of the awareness attribute
            for which to return the cluster health status (for example, `zone`).
            Applicable only if `level` is set to `awareness_attributes`.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Specifies the type of index that wildcard
            expressions can match. Supports comma-separated values. Valid choices
            are all, closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg level: Controls the amount of detail included in the
            cluster health response. Valid choices are awareness_attributes,
            cluster, indices, shards.
        :arg local: Whether to return information from the local node
            only instead of from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): A duration. Units can be
            `nanos`, `micros`, `ms` (milliseconds), `s` (seconds), `m` (minutes),
            `h` (hours) and `d` (days). Also accepts `0` without a unit and `-1` to
            indicate an unspecified value.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: The amount of time to wait for a response from the
            cluster manager node. For more information about supported time units,
            see [Common parameters](https://opensearch.org/docs/latest/api-
            reference/common-parameters/#time-units).
        :arg wait_for_active_shards: Waits until the specified number of
            shards is active before returning a response. Use `all` for all shards.
        :arg wait_for_events: Waits until all currently queued events
            with the given priority are processed. Valid choices are immediate,
            urgent, high, normal, low, languid.
        :arg wait_for_no_initializing_shards: Whether to wait until
            there are no initializing shards in the cluster. Default is false.
        :arg wait_for_no_relocating_shards: Whether to wait until there
            are no relocating shards in the cluster. Default is false.
        :arg wait_for_nodes: Waits until the specified number of nodes
            (`N`) is available. Accepts `>=N`, `<=N`, `>N`, and `<N`. You can also
            use `ge(N)`, `le(N)`, `gt(N)`, and `lt(N)` notation.
        :arg wait_for_status: Waits until the cluster health reaches the
            specified status or better. Valid choices are green, yellow, red.
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cluster", "health", index),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "source",
    )
    def pending_tasks(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns a list of pending cluster-level tasks, such as index creation, mapping
        updates, or new allocations.


        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: When `true`, the request retrieves information from
            the local node only. When `false`, information is retrieved from the
            cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): A duration. Units can be
            `nanos`, `micros`, `ms` (milliseconds), `s` (seconds), `m` (minutes),
            `h` (hours) and `d` (days). Also accepts `0` without a unit and `-1` to
            indicate an unspecified value.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", "/_cluster/pending_tasks", params=params, headers=headers
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
        "local",
        "master_timeout",
        "pretty",
        "source",
        "wait_for_metadata_version",
        "wait_for_timeout",
    )
    def state(
        self,
        *,
        metric: Any = None,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns comprehensive information about the state of the cluster.


        :arg metric: Limits the information returned to only the
            [specified metric groups](https://opensearch.org/docs/latest/api-
            reference/cluster-api/cluster-stats/#metric-groups).
        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (`*`). To target
            all data streams and indexes, omit this parameter or use `*` or `_all`.
        :arg allow_no_indices: Whether to ignore a wildcard index
            expression that resolves into no concrete indexes. This includes the
            `_all` string or when no indexes have been specified.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Specifies the type of index that wildcard
            expressions can match. Supports comma-separated values. Valid choices
            are all, closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: Whether to return settings in the flat form,
            which can improve readability, especially for heavily nested settings.
            For example, the flat form of `"cluster": { "max_shards_per_node": 500
            }` is `"cluster.max_shards_per_node": "500"`. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: Whether the specified concrete indexes
            should be ignored when unavailable (missing or closed).
        :arg local: Whether to return information from the local node
            only instead of from the cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): A duration. Units can be
            `nanos`, `micros`, `ms` (milliseconds), `s` (seconds), `m` (minutes),
            `h` (hours) and `d` (days). Also accepts `0` without a unit and `-1` to
            indicate an unspecified value.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg wait_for_metadata_version: Wait for the metadata version to
            be equal or greater than the specified metadata version.
        :arg wait_for_timeout: The maximum time to wait for
            `wait_for_metadata_version` before timing out.
        """
        if index and metric in SKIP_IN_PATH:
            metric = "_all"

        return self.transport.perform_request(
            "GET",
            _make_path("_cluster", "state", metric, index),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "flat_settings",
        "human",
        "pretty",
        "source",
        "timeout",
    )
    def stats(
        self,
        *,
        node_id: Any = None,
        params: Any = None,
        headers: Any = None,
        metric: Any = None,
        index_metric: Any = None,
    ) -> Any:
        """
        Returns a high-level overview of cluster statistics.


        :arg metric: Limit the information returned to the specified
            metrics.
        :arg index_metric: A comma-separated list of [index metric
            groups](https://opensearch.org/docs/latest/api-reference/cluster-
            api/cluster-stats/#index-metric-groups), for example, `docs,store`.
        :arg node_id: A comma-separated list of node IDs used to filter
            results. Supports [node filters](https://opensearch.org/docs/latest/api-
            reference/nodes-apis/index/#node-filters).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: Whether to return settings in the flat form,
            which can improve readability, especially for heavily nested settings.
            For example, the flat form of `"cluster": { "max_shards_per_node": 500
            }` is `"cluster.max_shards_per_node": "500"`. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: The amount of time to wait for each node to
            respond. If a node does not respond before its timeout expires, the
            response does not include its stats. However, timed out nodes are
            included in the response's `_nodes.failed` property. Defaults to no
            timeout.
        """
        return self.transport.perform_request(
            "GET",
            (
                "/_cluster/stats"
                if node_id in SKIP_IN_PATH
                else _make_path(
                    "_cluster", "stats", metric, index_metric, "nodes", node_id
                )
            ),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "dry_run",
        "error_trace",
        "explain",
        "filter_path",
        "human",
        "master_timeout",
        "metric",
        "pretty",
        "retry_failed",
        "source",
        "timeout",
    )
    def reroute(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows to manually change the allocation of individual shards in the cluster.


        :arg body: The definition of `commands` to perform (`move`,
            `cancel`, `allocate`)
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg dry_run: When `true`, the request simulates the operation
            and returns the resulting state.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg explain: When `true`, the response contains an explanation
            of why reroute certain commands can or cannot be executed.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): A duration. Units can be
            `nanos`, `micros`, `ms` (milliseconds), `s` (seconds), `m` (minutes),
            `h` (hours) and `d` (days). Also accepts `0` without a unit and `-1` to
            indicate an unspecified value.
        :arg metric: Limits the information returned to the specified
            metrics.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg retry_failed: When `true`, retries shard allocation if it
            was blocked because of too many subsequent failures.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: A duration. Units can be `nanos`, `micros`, `ms`
            (milliseconds), `s` (seconds), `m` (minutes), `h` (hours) and `d`
            (days). Also accepts `0` without a unit and `-1` to indicate an
            unspecified value.
        """
        return self.transport.perform_request(
            "POST", "/_cluster/reroute", params=params, headers=headers, body=body
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "flat_settings",
        "human",
        "include_defaults",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def get_settings(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns cluster settings.


        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: Whether to return settings in the flat form,
            which can improve readability, especially for heavily nested settings.
            For example, the flat form of `"cluster": { "max_shards_per_node": 500
            }` is `"cluster.max_shards_per_node": "500"`. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg include_defaults: When `true`, returns default cluster
            settings from the local node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): A duration. Units can be
            `nanos`, `micros`, `ms` (milliseconds), `s` (seconds), `m` (minutes),
            `h` (hours) and `d` (days). Also accepts `0` without a unit and `-1` to
            indicate an unspecified value.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: A duration. Units can be `nanos`, `micros`, `ms`
            (milliseconds), `s` (seconds), `m` (minutes), `h` (hours) and `d`
            (days). Also accepts `0` without a unit and `-1` to indicate an
            unspecified value.
        """
        return self.transport.perform_request(
            "GET", "/_cluster/settings", params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "flat_settings",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def put_settings(
        self,
        *,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Updates the cluster settings.


        :arg body: The cluster settings to update.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: Whether to return settings in the flat form,
            which can improve readability, especially for heavily nested settings.
            For example, the flat form of `"cluster": { "max_shards_per_node": 500
            }` is `"cluster.max_shards_per_node": "500"`. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): A duration. Units can be
            `nanos`, `micros`, `ms` (milliseconds), `s` (seconds), `m` (minutes),
            `h` (hours) and `d` (days). Also accepts `0` without a unit and `-1` to
            indicate an unspecified value.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: A duration. Units can be `nanos`, `micros`, `ms`
            (milliseconds), `s` (seconds), `m` (minutes), `h` (hours) and `d`
            (days). Also accepts `0` without a unit and `-1` to indicate an
            unspecified value.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return self.transport.perform_request(
            "PUT", "/_cluster/settings", params=params, headers=headers, body=body
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def remote_info(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns the information about configured remote clusters.


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
            "GET", "/_remote/info", params=params, headers=headers
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "include_disk_info",
        "include_yes_decisions",
        "pretty",
        "source",
    )
    def allocation_explain(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Explains how shards are allocated in the current cluster and provides an
        explanation for why unassigned shards can't be allocated to a node.


        :arg body: The index, shard, and primary flag for which to
            generate an explanation. Leave this empty to generate an explanation for
            the first unassigned shard.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg include_disk_info: When `true`, returns information about
            disk usage and shard sizes. Default is false.
        :arg include_yes_decisions: When `true`, returns any `YES`
            decisions in the allocation explanation. `YES` decisions indicate when a
            particular shard allocation attempt was successful for the given node.
            Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "POST",
            "/_cluster/allocation/explain",
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
        "timeout",
    )
    def delete_component_template(
        self,
        *,
        name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes a component template.


        :arg name: The name of the component template to delete.
            Supports wildcard (*) expressions.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): A duration. Units can be
            `nanos`, `micros`, `ms` (milliseconds), `s` (seconds), `m` (minutes),
            `h` (hours) and `d` (days). Also accepts `0` without a unit and `-1` to
            indicate an unspecified value.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: A duration. Units can be `nanos`, `micros`, `ms`
            (milliseconds), `s` (seconds), `m` (minutes), `h` (hours) and `d`
            (days). Also accepts `0` without a unit and `-1` to indicate an
            unspecified value.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_component_template", name),
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
    def get_component_template(
        self,
        *,
        name: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns one or more component templates.


        :arg name: The name of the component template to retrieve.
            Wildcard (`*`) expressions are supported.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_settings: Whether to return settings in the flat form,
            which can improve readability, especially for heavily nested settings.
            For example, the flat form of `"cluster": { "max_shards_per_node": 500
            }` is `"cluster.max_shards_per_node": "500"`. Default is false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: When `true`, the request retrieves information from
            the local node only. When `false`, information is retrieved from the
            cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): A duration. Units can be
            `nanos`, `micros`, `ms` (milliseconds), `s` (seconds), `m` (minutes),
            `h` (hours) and `d` (days). Also accepts `0` without a unit and `-1` to
            indicate an unspecified value.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_component_template", name),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "create",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def put_component_template(
        self,
        *,
        name: Any,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates or updates a component template.


        :arg name: The name of the component template to create.
            OpenSearch includes the following built-in component templates: `logs-
            mappings`, `logs-settings`, `metrics-mappings`, `metrics-settings`,
            `synthetics-mapping`, and `synthetics-settings`. OpenSearch uses these
            templates to configure backing indexes for its data streams. If you want
            to overwrite one of these templates, set the replacement template
            `version` to a higher value than the current version. If you want to
            disable all built-in component and index templates, set
            `stack.templates.enabled` to `false` using the [Cluster Update Settings
            API](https://opensearch.org/docs/latest/api-reference/cluster-
            api/cluster-settings/).
        :arg body: The template definition.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg create: When `true`, this request cannot replace or update
            existing component templates. Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): A duration. Units can be
            `nanos`, `micros`, `ms` (milliseconds), `s` (seconds), `m` (minutes),
            `h` (hours) and `d` (days). Also accepts `0` without a unit and `-1` to
            indicate an unspecified value.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: A duration. Units can be `nanos`, `micros`, `ms`
            (milliseconds), `s` (seconds), `m` (minutes), `h` (hours) and `d`
            (days). Also accepts `0` without a unit and `-1` to indicate an
            unspecified value.
        """
        for param in (name, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_component_template", name),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "source",
    )
    def exists_component_template(
        self,
        *,
        name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about whether a particular component template exist.


        :arg name: The name of the component template. Wildcard (*)
            expressions are supported.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: When `true`, the request retrieves information from
            the local node only. When `false`, information is retrieved from the
            cluster manager node. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): A duration. Units can be
            `nanos`, `micros`, `ms` (milliseconds), `s` (seconds), `m` (minutes),
            `h` (hours) and `d` (days). Also accepts `0` without a unit and `-1` to
            indicate an unspecified value.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "HEAD",
            _make_path("_component_template", name),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace", "filter_path", "human", "pretty", "source", "wait_for_removal"
    )
    def delete_voting_config_exclusions(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Clears any cluster voting configuration exclusions.


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
        :arg wait_for_removal: Specifies whether to wait for all
            excluded nodes to be removed from the cluster before clearing the voting
            configuration exclusions list. When `true`, all excluded nodes are
            removed from the cluster before this API takes any action. When `false`,
            the voting configuration exclusions list is cleared even if some
            excluded nodes are still in the cluster. Default is True.
        """
        return self.transport.perform_request(
            "DELETE",
            "/_cluster/voting_config_exclusions",
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "node_ids",
        "node_names",
        "pretty",
        "source",
        "timeout",
    )
    def post_voting_config_exclusions(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Updates the cluster voting configuration by excluding certain node IDs or
        names.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg node_ids: A comma-separated list of node IDs to exclude
            from the voting configuration. When using this setting, you cannot also
            specify `node_names`. Either `node_ids` or `node_names` are required to
            receive a valid response.
        :arg node_names: A comma-separated list of node names to exclude
            from the voting configuration. When using this setting, you cannot also
            specify `node_ids`. Either `node_ids` or `node_names` are required to
            receive a valid response.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: When adding a voting configuration exclusion, the
            API waits for the specified nodes to be excluded from the voting
            configuration before returning a response. If the timeout expires before
            the appropriate condition is satisfied, the request fails and returns an
            error.
        """
        return self.transport.perform_request(
            "POST", "/_cluster/voting_config_exclusions", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete_decommission_awareness(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Recommissions a decommissioned zone.


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
            "DELETE", "/_cluster/decommission/awareness", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete_weighted_routing(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Delete weighted shard routing weights.


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
            "DELETE",
            "/_cluster/routing/awareness/weights",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get_decommission_awareness(
        self,
        *,
        awareness_attribute_name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Retrieves the decommission status for all zones.


        :arg awareness_attribute_name: The name of the awareness
            attribute.
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
        if awareness_attribute_name in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'awareness_attribute_name'."
            )

        return self.transport.perform_request(
            "GET",
            _make_path(
                "_cluster",
                "decommission",
                "awareness",
                awareness_attribute_name,
                "_status",
            ),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get_weighted_routing(
        self,
        *,
        attribute: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Fetches weighted shard routing weights.


        :arg attribute: The name of the awareness attribute.
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
        if attribute in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'attribute'.")

        return self.transport.perform_request(
            "GET",
            _make_path("_cluster", "routing", "awareness", attribute, "weights"),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def put_decommission_awareness(
        self,
        *,
        awareness_attribute_name: Any,
        awareness_attribute_value: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Decommissions a cluster zone based on awareness. This can greatly benefit
        multi-zone deployments, where awareness attributes can aid in applying new
        upgrades to a cluster in a controlled fashion.


        :arg awareness_attribute_name: The name of the awareness
            attribute.
        :arg awareness_attribute_value: The value of the awareness
            attribute. For example, if you have shards allocated in two different
            zones, you can give each zone a value of `zone-a` or `zoneb`. The
            cluster decommission operation decommissions the zone listed in the
            method.
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
        for param in (awareness_attribute_name, awareness_attribute_value):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path(
                "_cluster",
                "decommission",
                "awareness",
                awareness_attribute_name,
                awareness_attribute_value,
            ),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def put_weighted_routing(
        self,
        *,
        attribute: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Updates weighted shard routing weights.


        :arg attribute: The name of awareness attribute, usually `zone`.
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
        if attribute in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'attribute'.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_cluster", "routing", "awareness", attribute, "weights"),
            params=params,
            headers=headers,
            body=body,
        )
