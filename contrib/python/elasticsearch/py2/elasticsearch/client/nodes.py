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

from .utils import SKIP_IN_PATH, NamespacedClient, _make_path, query_params


class NodesClient(NamespacedClient):
    @query_params(
        "timeout",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def reload_secure_settings(
        self, body=None, node_id=None, params=None, headers=None
    ):
        """
        Reloads secure settings.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/secure-settings.html#reloadable-secure-settings>`_

        :arg body: An object containing the password for the
            elasticsearch keystore
        :arg node_id: A comma-separated list of node IDs to span the
            reload/reinit call. Should stay empty because reloading usually involves
            all cluster nodes.
        :arg timeout: Explicit operation timeout
        """
        return self.transport.perform_request(
            "POST",
            _make_path("_nodes", node_id, "reload_secure_settings"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "flat_settings",
        "timeout",
        response_mimetypes=["application/json"],
    )
    def info(self, node_id=None, metric=None, params=None, headers=None):
        """
        Returns information about nodes in the cluster.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-nodes-info.html>`_

        :arg node_id: A comma-separated list of node IDs or names to
            limit the returned information; use `_local` to return information from
            the node you're connecting to, leave empty to get information from all
            nodes
        :arg metric: A comma-separated list of metrics you wish
            returned. Use `_all` to retrieve all metrics and `_none` to retrieve the
            node identity without any additional metrics.  Valid choices: settings,
            os, process, jvm, thread_pool, transport, http, plugins, ingest,
            indices, aggregations, _all, _none
        :arg flat_settings: Return settings in flat format (default:
            false)
        :arg timeout: Explicit operation timeout
        """
        return self.transport.perform_request(
            "GET", _make_path("_nodes", node_id, metric), params=params, headers=headers
        )

    @query_params(
        "completion_fields",
        "fielddata_fields",
        "fields",
        "groups",
        "include_segment_file_sizes",
        "include_unloaded_segments",
        "level",
        "timeout",
        "types",
        response_mimetypes=["application/json"],
    )
    def stats(
        self, node_id=None, metric=None, index_metric=None, params=None, headers=None
    ):
        """
        Returns statistical information about nodes in the cluster.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-nodes-stats.html>`_

        :arg node_id: A comma-separated list of node IDs or names to
            limit the returned information; use `_local` to return information from
            the node you're connecting to, leave empty to get information from all
            nodes
        :arg metric: Limit the information returned to the specified
            metrics  Valid choices: _all, breaker, fs, http, indices, jvm, os,
            process, thread_pool, transport, discovery, indexing_pressure
        :arg index_metric: Limit the information returned for `indices`
            metric to the specific index metrics. Isn't used if `indices` (or `all`)
            metric isn't specified.  Valid choices: _all, completion, docs,
            fielddata, query_cache, flush, get, indexing, merge, request_cache,
            refresh, search, segments, store, warmer, suggest, shard_stats
        :arg completion_fields: A comma-separated list of fields for
            `fielddata` and `suggest` index metric (supports wildcards)
        :arg fielddata_fields: A comma-separated list of fields for
            `fielddata` index metric (supports wildcards)
        :arg fields: A comma-separated list of fields for `fielddata`
            and `completion` index metric (supports wildcards)
        :arg groups: A comma-separated list of search groups for
            `search` index metric
        :arg include_segment_file_sizes: Whether to report the
            aggregated disk usage of each one of the Lucene index files (only
            applies if segment stats are requested)
        :arg include_unloaded_segments: If set to true segment stats
            will include stats for segments that are not currently loaded into
            memory
        :arg level: Return indices stats aggregated at index, node or
            shard level  Valid choices: indices, node, shards  Default: node
        :arg timeout: Explicit operation timeout
        :arg types: A comma-separated list of document types for the
            `indexing` index metric
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_nodes", node_id, "stats", metric, index_metric),
            params=params,
            headers=headers,
        )

    @query_params(
        "ignore_idle_threads",
        "interval",
        "snapshots",
        "sort",
        "threads",
        "timeout",
        "type",
        response_mimetypes=["text/plain"],
    )
    def hot_threads(self, node_id=None, params=None, headers=None):
        """
        Returns information about hot threads on each node in the cluster.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-nodes-hot-threads.html>`_

        :arg node_id: A comma-separated list of node IDs or names to
            limit the returned information; use `_local` to return information from
            the node you're connecting to, leave empty to get information from all
            nodes
        :arg ignore_idle_threads: Don't show threads that are in known-
            idle places, such as waiting on a socket select or pulling from an empty
            task queue (default: true)
        :arg interval: The interval for the second sampling of threads
        :arg snapshots: Number of samples of thread stacktrace (default:
            10)
        :arg sort: The sort order for 'cpu' type (default: total)  Valid
            choices: cpu, total
        :arg threads: Specify the number of threads to provide
            information for (default: 3)
        :arg timeout: Explicit operation timeout
        :arg type: The type to sample (default: cpu)  Valid choices:
            cpu, wait, block, mem
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_nodes", node_id, "hot_threads"),
            params=params,
            headers=headers,
        )

    @query_params(
        "timeout",
        response_mimetypes=["application/json"],
    )
    def usage(self, node_id=None, metric=None, params=None, headers=None):
        """
        Returns low-level information about REST actions usage on nodes.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-nodes-usage.html>`_

        :arg node_id: A comma-separated list of node IDs or names to
            limit the returned information; use `_local` to return information from
            the node you're connecting to, leave empty to get information from all
            nodes
        :arg metric: Limit the information returned to the specified
            metrics  Valid choices: _all, rest_actions
        :arg timeout: Explicit operation timeout
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_nodes", node_id, "usage", metric),
            params=params,
            headers=headers,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def clear_repositories_metering_archive(
        self, node_id, max_archive_version, params=None, headers=None
    ):
        """
        Removes the archived repositories metering information present in the cluster.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/clear-repositories-metering-archive-api.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg node_id: Comma-separated list of node IDs or names used to
            limit returned information.
        :arg max_archive_version: Specifies the maximum archive_version
            to be cleared from the archive.
        """
        for param in (node_id, max_archive_version):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "DELETE",
            _make_path(
                "_nodes", node_id, "_repositories_metering", max_archive_version
            ),
            params=params,
            headers=headers,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def get_repositories_metering_info(self, node_id, params=None, headers=None):
        """
        Returns cluster repositories metering information.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/get-repositories-metering-api.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg node_id: A comma-separated list of node IDs or names to
            limit the returned information.
        """
        if node_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'node_id'.")

        return self.transport.perform_request(
            "GET",
            _make_path("_nodes", node_id, "_repositories_metering"),
            params=params,
            headers=headers,
        )
